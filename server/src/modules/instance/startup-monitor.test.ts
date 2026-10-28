import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { after, before, it, type TestContext } from 'node:test'
import { fileURLToPath } from 'node:url'
import Fastify from 'fastify'
import { getContainerRuntime, type ContainerRef, type ContainerRuntime } from '../../infra/container'
import { emptyResourceSnapshot } from '../../infra/container/dst-container-resources'
import { buildHostResourceSnapshot } from '../../infra/container/memory-budget'
import { closeDatabase, createGameInstance, deleteGameInstanceById, getGameInstanceById, initDatabase, updateGameInstanceRuntime } from '../../shared/db'
import { instanceConsoleLogStore } from '../../shared/instance-runtime/console-log-store'
import { runStartupMonitor } from './startup-monitor'
import { createStartupProbeQuery } from './startup-probe-query'
import {
  cancelStartupTask, changeStartupPhase, createStartupTask, currentStartupTask,
  enqueueStartupTask, getStartupSnapshot, persistStartupTask, releaseStartupTask,
  recordStartupSwapAdvice,
} from './startup-state'

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'bsp-startup-monitor-'))
before(async () => {
  await initDatabase(path.join(dir, 'test.sqlite'), path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../drizzle'), {
    adminUsername: 'admin', adminPassword: '123456', seedDevelopmentUsers: false,
  })
})
after(() => { closeDatabase(); fs.rmSync(dir, { recursive: true, force: true }) })

function gate() {
  let resolve!: () => void
  const promise = new Promise<void>((done) => { resolve = done })
  return { promise, resolve }
}

async function fixture(t: TestContext, name: string) {
  const app = Fastify()
  t.after(() => app.close())
  const instance = await createGameInstance({ nodeId: 'local-node', name, gameCode: '343050', status: 'running' })
  const task = createStartupTask(instance.id)
  t.after(async () => {
    task.controller.abort()
    releaseStartupTask(task)
    instanceConsoleLogStore.removeInstance(instance.id)
    await deleteGameInstanceById(instance.id)
  })
  const masterRef = { id: `${instance.id}-master`, name: 'test-master' }
  const cavesRef = { id: `${instance.id}-caves`, name: 'test-caves' }
  await updateGameInstanceRuntime(instance.id, { containerId: masterRef.id, runtimeStartedAt: task.snapshot.startedAt })
  await persistStartupTask(task)
  const runtime = getContainerRuntime() as ContainerRuntime & Required<Pick<ContainerRuntime, 'resourceSnapshot'>>
  const inspect = t.mock.method(runtime, 'inspect', async (ref: ContainerRef) => ({ ...ref, running: true, restarts: 7 }))
  t.mock.method(runtime, 'resourceSnapshot', async () => ({ ...emptyResourceSnapshot(), memoryCurrentMb: 1024, memoryPeakMb: 1200, restarts: 7, oomKillCount: 0 }))
  const probe = t.mock.fn(async () => ({ worldReady: true, shardId: '1', remoteConnected: null as boolean | null }))
  const cleanup = t.mock.fn(async (_refs: ContainerRef[]) => {})
  const run = (overrides: Partial<Parameters<typeof runStartupMonitor>[1]> = {}) => runStartupMonitor(app, {
    task, runtime, masterRef, baselineRestarts: 7, startedAt: task.snapshot.startedAt,
    waitSec: 0.5, pollIntervalMs: 20, isCancelled: () => false,
    luaFailure: () => null, probe, cleanup, ...overrides,
  })
  return { instance, task, runtime, masterRef, cavesRef, inspect, probe, cleanup, run }
}

it('单世界进程和就绪日志不替代本轮游戏查询，查询成功后才持久化成功', async (t) => {
  const f = await fixture(t, 'single-ready')
  instanceConsoleLogStore.appendDockerLine(f.instance.id, 'Sim paused. Server is ready.', 'master')
  let attempts = 0
  await f.run({ probe: async () => {
    assert.equal(f.task.snapshot.status, 'running')
    assert.equal(f.task.snapshot.phase, 'master_loading')
    return { worldReady: ++attempts > 1, shardId: '1', remoteConnected: null }
  } })
  assert.equal(attempts, 2)
  assert.equal(f.cleanup.mock.callCount(), 0)
  const saved = await getGameInstanceById(f.instance.id)
  assert.equal(saved?.lastStartupReport?.status, 'success')
  assert.equal(saved?.lastStartupReport?.master.state, 'ready')
  assert.equal(saved?.lastStartupReport?.caves.state, 'disabled')
  assert.ok(saved?.runtimeReadyAt)
})

it('真实探针的延迟就绪回包跨越单次等待窗口后仍启动成功，不清理世界', async (t) => {
  const f = await fixture(t, 'delayed-startup-reply')
  let attempts = 0
  const timers: NodeJS.Timeout[] = []
  t.mock.method(f.runtime, 'execStdin', async (_ref: ContainerRef, command: string) => {
    const token = /BSPSTART:([a-f0-9-]{36})\|/.exec(command)![1]
    const reply = (ready: boolean) => instanceConsoleLogStore.appendDockerLine(f.instance.id, `BSPSTART:${token}|${ready ? 1 : 0}|1|x`, 'master')
    if (++attempts === 1) reply(false)
    else if (attempts === 2) timers.push(setTimeout(() => reply(true), 50))
    return { exitCode: 0, output: '' }
  })
  const probe = createStartupProbeQuery({ instanceId: f.instance.id, runtime: f.runtime, signal: f.task.controller.signal, responseWaitMs: 2 })
  try {
    await f.run({ probe: probe.query })
    assert.equal(f.task.snapshot.status, 'success')
    assert.equal(f.task.snapshot.master.lastProbe?.worldReady, true)
    assert.equal(f.cleanup.mock.callCount(), 0)
    assert.equal((await getGameInstanceById(f.instance.id))?.lastStartupReport?.status, 'success')
  }
  finally { probe.dispose(); timers.forEach(clearTimeout) }
})

it('双世界使用洞穴实际 ID 查询主世界连接表，连接确认前保持 connecting', async (t) => {
  const f = await fixture(t, 'both-ready')
  const order: string[] = []
  let connections = 0
  await f.run({
    startCaves: async () => {
      assert.equal(f.task.snapshot.master.state, 'ready')
      order.push('start-caves')
      return { ok: true, ref: f.cavesRef, baselineRestarts: 7 }
    },
    probe: async (ref, shard, remoteShardId) => {
      if (shard === 'caves') {
        order.push('caves-ready')
        return { worldReady: true, shardId: 'deep-caves-7', remoteConnected: null }
      }
      assert.equal(ref.id, f.masterRef.id)
      if (!remoteShardId) return { worldReady: true, shardId: '1', remoteConnected: null }
      assert.equal(remoteShardId, 'deep-caves-7')
      if (++connections === 2) assert.equal(f.task.snapshot.phase, 'connecting')
      order.push('master-connection')
      return { worldReady: true, shardId: '1', remoteConnected: connections === 2 }
    },
  })
  assert.deepEqual(order, ['start-caves', 'caves-ready', 'master-connection', 'caves-ready', 'master-connection'])
  assert.equal(f.task.snapshot.status, 'success')
  assert.equal(f.task.snapshot.caves.state, 'ready')
  assert.equal(f.cleanup.mock.callCount(), 0)
})

for (const shardId of ['actual-caves', null]) {
  it(`洞穴已加载但${shardId ? '连接未确认' : '实际 ID 不可读'}时不能报告完整成功`, async (t) => {
    const f = await fixture(t, `unconnected-${shardId}`)
    let connectionQueries = 0
    await f.run({
      waitSec: 0.15,
      startCaves: async () => ({ ok: true, ref: f.cavesRef, baselineRestarts: 7 }),
      probe: async (_ref, shard, remoteShardId) => {
        if (remoteShardId) {
          connectionQueries++
          assert.equal(remoteShardId, shardId)
        }
        return { worldReady: true, shardId: shard === 'caves' ? shardId : '1', remoteConnected: remoteShardId ? false : null }
      },
    })
    assert.equal(f.task.snapshot.status, 'failed')
    assert.equal(f.task.snapshot.master.state, 'ready')
    assert.equal(f.task.snapshot.caves.state, 'failed')
    assert.equal(f.task.snapshot.diagnosis?.code, 'shard_connect_timeout')
    assert.equal(connectionQueries > 0, shardId !== null)
    assert.equal(f.cleanup.mock.callCount(), 1)
    const row = await getGameInstanceById(f.instance.id)
    assert.equal(row?.status, 'running')
    assert.equal(row?.containerId, f.masterRef.id)
    assert.ok(row?.runtimeReadyAt)
    assert.deepEqual(f.cleanup.mock.calls[0]?.arguments[0], [f.cavesRef])
    assert.equal(row?.lastStartupReport?.caves.cleanupCompleted, true)
    assert.equal(row?.lastStartupReport?.master.lastProbe?.remoteConnected, shardId ? false : null)
  })
}

it('probeFailed 保持未知，探测恢复后可正常就绪', async (t) => {
  const f = await fixture(t, 'probe-recovered')
  let checks = 0
  t.mock.method(f.runtime, 'inspect', async (ref: ContainerRef) => ({ ...ref, running: ++checks > 1, probeFailed: checks === 1, restarts: 7 }))
  await f.run({ baselineRestarts: null })
  assert.equal(checks, 2)
  assert.equal(f.probe.mock.callCount(), 1)
  assert.equal(f.cleanup.mock.callCount(), 0)
  assert.equal(f.task.snapshot.status, 'success')
})

for (const failure of ['lua', 'exit', 'cleanup'] as const) {
  it(`洞穴 ${failure} 失败只清理洞穴，并保留主世界就绪时间和引用`, async t => {
    const f = await fixture(t, `caves-${failure}`)
    let caveStarted = false
    if (failure === 'exit') t.mock.method(f.runtime, 'inspect', async (ref: ContainerRef) => ({ ...ref, running: ref.id !== f.cavesRef.id, restarts: 7 }))
    await f.run({
      waitSec: .1,
      startCaves: async () => { caveStarted = true; return { ok: true, ref: f.cavesRef, baselineRestarts: 7 } },
      luaFailure: shard => failure === 'lua' && caveStarted && shard === 'caves' ? 'LUA ERROR stack traceback: private-cave-path' : null,
      probe: async (_ref, shard) => ({ worldReady: shard === 'master', shardId: shard === 'master' ? '1' : '2', remoteConnected: null }),
      cleanup: async refs => { assert.deepEqual(refs, [f.cavesRef]); if (failure === 'cleanup') throw new Error('fixture cleanup failed') },
    })
    const row = (await getGameInstanceById(f.instance.id))!
    assert.equal(row.status, 'running')
    assert.equal(row.containerId, f.masterRef.id)
    assert.ok(row.runtimeReadyAt)
    assert.equal(row.lastStartupReport?.master.state, 'ready')
    assert.equal(row.lastStartupReport?.caves.state, 'failed')
    assert.equal(row.lastStartupReport?.caves.cleanupCompleted, failure !== 'cleanup')
    assert.equal(JSON.stringify(row.lastStartupReport).includes('private-cave-path'), false)
  })
}

it('恢复的世界静默超过 120 秒仍可成功，重复 IPC 警告不当作进展', async (t) => {
  const f = await fixture(t, 'silent-ready')
  const oldProgressAt = new Date(Date.now() - 160_000).toISOString()
  f.task.snapshot.status = 'running'
  f.task.snapshot.phase = 'master_loading'
  f.task.snapshot.master.state = 'loading'
  f.task.snapshot.lastProgressAt = oldProgressAt
  f.task.snapshot.phaseStartedAt = oldProgressAt
  f.task.snapshot.phaseDeadlineAt = new Date(Date.now() + 2000).toISOString()
  instanceConsoleLogStore.appendDockerLine(f.instance.id, 'SteamNetworkingSockets IPC function call IsP2PPacketAvailable', 'master')
  let attempts = 0
  await f.run({ probe: async () => {
    assert.equal(f.task.snapshot.lastProgressAt, oldProgressAt)
    assert.equal(f.task.snapshot.diagnosis?.code, 'no_recent_progress')
    return { worldReady: ++attempts > 1, shardId: '1', remoteConnected: null }
  } })
  assert.equal(attempts, 2)
  assert.equal(f.task.snapshot.status, 'success')
  assert.equal(f.task.snapshot.diagnosis, null)
  assert.equal(f.cleanup.mock.callCount(), 0)
})

it('明确 OOM 立即失败，并在清理之前持久化诊断', async (t) => {
  const f = await fixture(t, 'oom')
  f.task.snapshot.master.resourceBaseline = { ...emptyResourceSnapshot(), oomKilled: false }
  t.mock.method(f.runtime, 'resourceSnapshot', async () => ({ ...emptyResourceSnapshot(), oomKilled: true }))
  let cleaned = false
  await f.run({ cleanup: async (refs) => {
    assert.deepEqual(refs, [f.masterRef])
    const saved = await getGameInstanceById(f.instance.id)
    assert.equal(saved?.lastStartupReport?.status, 'failed')
    assert.equal(saved?.lastStartupReport?.diagnosis?.code, 'oom')
    cleaned = true
  } })
  assert.equal(cleaned, true)
  assert.equal(f.probe.mock.callCount(), 0)
  const saved = await getGameInstanceById(f.instance.id)
  assert.equal(saved?.status, 'error')
  assert.equal(saved?.runtimeFailureKind, 'memory')
})

it('Lua 致命错误在 runtime 查询之前失败，原始错误只进入控制台', async (t) => {
  const f = await fixture(t, 'lua')
  const raw = 'LUA ERROR stack traceback: secret-path/scripts/example.lua:42'
  await f.run({ luaFailure: () => raw })
  assert.equal(f.inspect.mock.callCount(), 0)
  assert.equal(f.probe.mock.callCount(), 0)
  assert.equal(f.cleanup.mock.callCount(), 1)
  const saved = await getGameInstanceById(f.instance.id)
  assert.equal(saved?.lastStartupReport?.diagnosis?.code, 'lua_error')
  assert.equal(saved?.lastError?.includes('secret-path'), false)
  assert.ok(instanceConsoleLogStore.listLogs(f.instance.id).some(line => line.text.includes(raw)))
})

it('历史重启和 OOM 累计数作为基线，主世界和洞穴均不误报失败', async (t) => {
  const f = await fixture(t, 'historical-counters')
  f.task.snapshot.master.resourceBaseline = { ...emptyResourceSnapshot(), restarts: 7, oomKillCount: 3 }
  f.task.snapshot.caves.resourceBaseline = { ...emptyResourceSnapshot(), restarts: 9, oomKillCount: 3 }
  t.mock.method(f.runtime, 'inspect', async (ref: ContainerRef) => ({ ...ref, running: true, restarts: ref.id === f.cavesRef.id ? 9 : 7 }))
  t.mock.method(f.runtime, 'resourceSnapshot', async (ref: ContainerRef) => ({ ...emptyResourceSnapshot(), restarts: ref.id === f.cavesRef.id ? 9 : 7, oomKillCount: 3 }))
  await f.run({
    startCaves: async () => ({ ok: true, ref: f.cavesRef, baselineRestarts: 9 }),
    probe: async (_ref, shard, remoteShardId) => ({ worldReady: true, shardId: shard === 'caves' ? '2' : '1', remoteConnected: remoteShardId ? true : null }),
  })
  assert.equal(f.task.snapshot.status, 'success')
  assert.equal(f.cleanup.mock.callCount(), 0)
})

it('本轮 OOM 计数增加立即失败，不被历史基线或当前值覆盖', async (t) => {
  const f = await fixture(t, 'new-oom-counter')
  f.task.snapshot.master.resourceBaseline = { ...emptyResourceSnapshot(), restarts: 7, oomKillCount: 3 }
  let samples = 0
  t.mock.method(f.runtime, 'resourceSnapshot', async () => ({ ...emptyResourceSnapshot(), oomKillCount: ++samples > 1 ? 4 : 3 }))
  await f.run({ probe: async () => ({ worldReady: false, shardId: '1', remoteConnected: null }) })
  assert.equal(samples, 2)
  assert.equal(f.task.snapshot.diagnosis?.code, 'oom')
  assert.equal(f.cleanup.mock.callCount(), 1)
})

it('恢复的资源计数沿用旧基线，报告增量不从最新检查点重新归零', async (t) => {
  const f = await fixture(t, 'restored-counters')
  f.task.snapshot.status = 'running'
  f.task.snapshot.phase = 'master_loading'
  f.task.snapshot.phaseDeadlineAt = new Date(Date.now() + 2000).toISOString()
  f.task.snapshot.master.state = 'loading'
  f.task.snapshot.master.resourceBaseline = { ...emptyResourceSnapshot(), highEvents: 10, maxEvents: 20, oomKillCount: 3, throttledUsec: 100, restarts: 7 }
  f.task.snapshot.master.resources = { ...emptyResourceSnapshot(), highEvents: 12, maxEvents: 22, oomKillCount: 3, throttledUsec: 150, restarts: 7 }
  t.mock.method(f.runtime, 'resourceSnapshot', async () => ({ ...emptyResourceSnapshot(), highEvents: 14, maxEvents: 25, oomKillCount: 3, throttledUsec: 200, restarts: 7, memoryPeakMb: 2000 }))
  await f.run()
  assert.equal(f.task.snapshot.status, 'success')
  const report = (await getGameInstanceById(f.instance.id))?.lastStartupReport
  assert.equal(report?.master.resourceBaseline?.highEvents, 10)
  assert.equal(report?.master.resourceBaseline?.maxEvents, 20)
  assert.deepEqual(report?.master.resourceDeltas, { highEvents: 4, maxEvents: 5, oomKillCount: 0, throttledUsec: 100, restarts: 0 })
  assert.equal(report?.master.memoryPeakMb, 2000)
})

it('等待中取消并开启新一轮，旧查询迟到不能启动洞穴或覆盖新状态', async (t) => {
  const f = await fixture(t, 'cancel-new-task')
  const entered = gate()
  const resume = gate()
  const startCaves = t.mock.fn(async () => ({ ok: true as const, ref: f.cavesRef }))
  const oldMonitor = f.run({ startCaves, probe: async () => {
    entered.resolve()
    await resume.promise
    return { worldReady: true, shardId: '1', remoteConnected: null }
  } })
  await entered.promise
  cancelStartupTask(f.instance.id)
  const next = createStartupTask(f.instance.id)
  t.after(() => { next.controller.abort(); releaseStartupTask(next) })
  await persistStartupTask(next)
  await updateGameInstanceRuntime(f.instance.id, { status: 'running', containerId: 'next-master', runtimeReadyAt: null, lastError: null })
  const saved = await getGameInstanceById(f.instance.id)
  resume.resolve()
  await oldMonitor
  assert.equal(currentStartupTask(f.instance.id), next)
  assert.equal(f.task.snapshot.status, 'cancelled')
  assert.equal(startCaves.mock.callCount(), 0)
  assert.equal(f.cleanup.mock.callCount(), 0)
  assert.deepEqual(await getGameInstanceById(f.instance.id), saved)
  assert.equal(instanceConsoleLogStore.listLogs(f.instance.id).some(line => /启动失败|实例已就绪/.test(line.text)), false)
})

it('失败清理时新任务接管，迟到失败不写入新一轮运行状态', async (t) => {
  const f = await fixture(t, 'failure-new-task')
  let saved: Awaited<ReturnType<typeof getGameInstanceById>>
  await f.run({ luaFailure: () => 'fatal fixture', cleanup: async () => {
    const next = createStartupTask(f.instance.id)
    t.after(() => { next.controller.abort(); releaseStartupTask(next) })
    await persistStartupTask(next)
    await updateGameInstanceRuntime(f.instance.id, { status: 'running', containerId: 'new-master', lastError: null })
    saved = await getGameInstanceById(f.instance.id)
  } })
  assert.ok(saved)
  assert.deepEqual(await getGameInstanceById(f.instance.id), saved)
  assert.equal(saved?.lastStartupReport?.status, 'queued')
})

it('同节点串行启动，取消的队列任务跳过，排队不消耗分片预算', async (t) => {
  const first = await fixture(t, 'queue-first')
  const skipped = await fixture(t, 'queue-cancelled')
  const second = await fixture(t, 'queue-second')
  const entered = gate()
  const resume = gate()
  const complete = gate()
  const order: string[] = []
  second.task.snapshot.startedAt = new Date(Date.now() - 600_000).toISOString()
  enqueueStartupTask(first.task, async () => {
    order.push('first-start')
    await changeStartupPhase(first.task, 'master_loading', new Date(Date.now() + 300_000).toISOString())
    entered.resolve()
    await resume.promise
    await changeStartupPhase(first.task, 'ready')
    order.push('first-end')
  })
  enqueueStartupTask(skipped.task, async () => { order.push('cancelled-ran') })
  enqueueStartupTask(second.task, async () => {
    order.push('second-start')
    assert.equal(second.task.snapshot.phase, 'queued')
    await changeStartupPhase(second.task, 'master_loading', new Date(Date.now() + 300_000).toISOString())
    assert.equal(getStartupSnapshot(second.instance.id)?.remainingSeconds, 300)
    await changeStartupPhase(second.task, 'ready')
    order.push('second-end')
    complete.resolve()
  })
  await entered.promise
  cancelStartupTask(skipped.instance.id)
  await persistStartupTask(skipped.task)
  assert.equal(second.task.snapshot.phaseDeadlineAt, null)
  assert.equal(getStartupSnapshot(second.instance.id)?.remainingSeconds, null)
  assert.ok((getStartupSnapshot(second.instance.id)?.elapsedSeconds ?? 0) >= 600)
  assert.deepEqual(order, ['first-start'])
  resume.resolve()
  await complete.promise
  assert.deepEqual(order, ['first-start', 'first-end', 'second-start', 'second-end'])
  assert.equal((await getGameInstanceById(skipped.instance.id))?.lastStartupReport?.status, 'cancelled')
})

it('恢复保留原始 phase deadline，已过期时立即失败而不重置 300 秒', async (t) => {
  const f = await fixture(t, 'restore-expired')
  const originalStartedAt = new Date(Date.now() - 400_000).toISOString()
  f.task.snapshot.status = 'running'
  f.task.snapshot.phase = 'master_loading'
  f.task.snapshot.master.state = 'loading'
  f.task.snapshot.startedAt = originalStartedAt
  f.task.snapshot.phaseStartedAt = originalStartedAt
  f.task.snapshot.phaseDeadlineAt = new Date(Date.now() - 1000).toISOString()
  await persistStartupTask(f.task)
  await f.run({ waitSec: 300 })
  assert.equal(f.inspect.mock.callCount(), 0)
  assert.equal(f.probe.mock.callCount(), 0)
  assert.equal(f.cleanup.mock.callCount(), 1)
  assert.equal(f.task.snapshot.status, 'failed')
  assert.equal(f.task.snapshot.startedAt, originalStartedAt)
})

it('恢复 connecting 先复核主世界，沿用洞穴原期限而不再开启完整预算', async (t) => {
  const f = await fixture(t, 'restore-connecting')
  const phaseStartedAt = new Date(Date.now() - 200_000).toISOString()
  const deadlineAt = new Date(Date.now() + 150).toISOString()
  f.task.snapshot.status = 'running'
  f.task.snapshot.phase = 'connecting'
  f.task.snapshot.master.state = 'ready'
  f.task.snapshot.caves.state = 'loading'
  f.task.snapshot.caves.resources = { ...emptyResourceSnapshot(), restarts: 7, oomKillCount: 0 }
  f.task.snapshot.phaseStartedAt = phaseStartedAt
  f.task.snapshot.phaseDeadlineAt = deadlineAt
  let masterRechecked = false
  await f.run({ waitSec: 300, cavesRef: f.cavesRef, probe: async (_ref, shard, remoteShardId) => {
    assert.equal(f.task.snapshot.phaseDeadlineAt, deadlineAt)
    assert.equal(f.task.snapshot.phaseStartedAt, phaseStartedAt)
    if (shard === 'master' && !remoteShardId) masterRechecked = true
    if (shard === 'caves') assert.equal(masterRechecked, true)
    return { worldReady: true, shardId: shard === 'caves' ? 'restored-caves' : '1', remoteConnected: remoteShardId ? false : null }
  } })
  assert.equal(masterRechecked, true)
  assert.equal(f.task.snapshot.diagnosis?.code, 'shard_connect_timeout')
  assert.equal(f.task.snapshot.status, 'failed')
  assert.equal(f.cleanup.mock.callCount(), 1)
})

it('游戏查询挂起仍受本片 deadline 约束', { timeout: 2000 }, async (t) => {
  const f = await fixture(t, 'probe-hung')
  await f.run({ waitSec: 0.15, probe: async () => new Promise(() => {}) })
  assert.equal(f.task.snapshot.status, 'failed')
  assert.equal(f.task.snapshot.diagnosis?.code, 'probe_unavailable')
  assert.equal(f.cleanup.mock.callCount(), 1)
})

it('启动准备失败前持久化 swap 命令，并保留首次不足现场', async (t) => {
  const f = await fixture(t, 'swap-fast-failure')
  const host = buildHostResourceSnapshot({ source: 'native-host', meminfo: 'MemTotal: 6291456 kB\nMemAvailable: 524288 kB\nSwapTotal: 2097152 kB\nSwapFree: 524288 kB' })
  await recordStartupSwapAdvice(f.task, { ...host, source: 'unknown' }, 4096)
  assert.equal(f.task.snapshot.swapAdvice, undefined)
  await recordStartupSwapAdvice(f.task, host, 4096)
  const advice = (await getGameInstanceById(f.instance.id))?.lastStartupReport?.swapAdvice
  assert.equal(advice?.state, 'low')
  assert.match(advice?.command ?? '', /extra-3g BSP_SWAP_SIZE=3G/)
  assert.deepEqual((await getGameInstanceById(f.instance.id))?.lastStartupReport?.swapAdvice, advice)
  await recordStartupSwapAdvice(f.task, host, 8192)
  assert.deepEqual(f.task.snapshot.swapAdvice, advice)
  await f.run({ luaFailure: () => 'Lua 致命错误' })
  assert.equal(f.task.snapshot.status, 'failed')
  assert.deepEqual((await getGameInstanceById(f.instance.id))?.lastStartupReport?.swapAdvice, advice)
})

it('加载采样共享宿主快照，使用当前安全余量而不重复加上主世界占用', async (t) => {
  const f = await fixture(t, 'swap-during-loading')
  const host = buildHostResourceSnapshot({ source: 'docker-host', meminfo: 'MemTotal: 6291456 kB\nMemAvailable: 131072 kB\nSwapTotal: 2097152 kB\nSwapFree: 0 kB' })
  const hostRead = t.mock.fn(async () => host)
  const runtime = { ...f.runtime, inspect: f.runtime.inspect.bind(f.runtime), resourceSnapshot: f.runtime.resourceSnapshot.bind(f.runtime), hostResources: hostRead }
  let attempts = 0
  await f.run({ runtime, probe: async () => ({ worldReady: ++attempts > 1, shardId: '1', remoteConnected: null }) })
  assert.equal(f.task.snapshot.status, 'success')
  assert.equal(f.task.snapshot.swapAdvice?.state, 'exhausted')
  assert.match(f.task.snapshot.swapAdvice?.message ?? '', /还缺约 384 MiB/)
  assert.equal(hostRead.mock.callCount(), 1, 'startup samples reuse the cached host reading')
  const stale = createStartupTask(f.instance.id)
  t.after(() => releaseStartupTask(stale))
  delete f.task.snapshot.swapAdvice
  await recordStartupSwapAdvice(f.task, host, 4096)
  assert.equal(f.task.snapshot.swapAdvice, undefined, 'obsolete tasks cannot save a new warning')
})
