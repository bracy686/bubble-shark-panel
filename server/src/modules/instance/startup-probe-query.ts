import { randomUUID } from 'node:crypto'
import type { ContainerRef, ContainerRuntime } from '../../infra/container/types'
import { buildDstStartupProbe, parseDstStartupProbe, type DstStartupProbe } from '../../infra/game-adapter/dst/startup-probe'
import { instanceConsoleLogStore } from '../../shared/instance-runtime/console-log-store'

type Shard = 'master' | 'caves'
interface ProbeChannel {
  token: string
  shard: Shard
  response: DstStartupProbe | null
  waiting: Set<() => void>
}

/** 同一启动任务持续接收延迟回包；分片、运行时及远端连接查询各用独立标记。 */
export function createStartupProbeQuery(input: {
  instanceId: string
  runtime: Pick<ContainerRuntime, 'execStdin'>
  signal: AbortSignal
  responseWaitMs?: number
}) {
  const channels = new Map<string, ProbeChannel>()
  const pending = new Set<() => void>()
  let closed = false
  const unsubscribe = instanceConsoleLogStore.subscribe(input.instanceId, (line) => {
    if (closed || line.stream !== 'stdout') return
    for (const channel of channels.values()) {
      if (line.shard !== channel.shard) continue
      const response = parseDstStartupProbe(line.text, channel.token)
      if (!response) continue
      channel.response = response
      for (const notify of channel.waiting) notify()
    }
  })
  const dispose = () => {
    if (closed) return
    closed = true
    unsubscribe()
    input.signal.removeEventListener('abort', dispose)
    for (const cancel of pending) cancel()
    channels.clear()
  }
  input.signal.addEventListener('abort', dispose, { once: true })
  if (input.signal.aborted) dispose()

  const query = async (ref: ContainerRef, shard: Shard, remoteShardId?: string): Promise<DstStartupProbe | null> => {
    if (closed) return null
    const key = JSON.stringify([ref.id, ref.runtimeIdentity ?? null, shard, remoteShardId ?? null])
    let channel = channels.get(key)
    if (!channel) {
      channel = { token: randomUUID(), shard, response: null, waiting: new Set() }
      channels.set(key, channel)
    }
    const current = channel
    const takeResponse = () => {
      const response = current.response
      current.response = null
      return response
    }
    if (current.response) return takeResponse()
    const command = buildDstStartupProbe(current.token, remoteShardId)
    return new Promise((resolve) => {
      let settled = false
      const finish = (response: DstStartupProbe | null) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        current.waiting.delete(receive)
        pending.delete(cancel)
        resolve(response)
      }
      const receive = () => { if (current.response) finish(takeResponse()) }
      const cancel = () => finish(null)
      const timer = setTimeout(cancel, input.responseWaitMs ?? 1000)
      current.waiting.add(receive)
      pending.add(cancel)
      // 即使写入通道卡住，单次查询仍能超时/取消；日志订阅保持到整个启动任务结束。
      void Promise.resolve().then(() => {
        if (closed) return null
        return input.runtime.execStdin(ref, command)
      }).then(result => { if (result && result.exitCode !== 0) finish(null) }, cancel)
    })
  }
  return { query, dispose }
}
