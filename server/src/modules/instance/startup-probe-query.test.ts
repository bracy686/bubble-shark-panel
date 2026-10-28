import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { it, type TestContext } from 'node:test'
import type { ContainerRef } from '../../infra/container/types'
import { instanceConsoleLogStore } from '../../shared/instance-runtime/console-log-store'
import { createStartupProbeQuery } from './startup-probe-query'

const master: ContainerRef = { id: 'master-runtime', name: 'master' }
const caves: ContainerRef = { id: 'caves-runtime', name: 'caves' }

function fixture(t: TestContext) {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const instanceId = randomUUID()
  const controller = new AbortController()
  const commands: string[] = []
  const runtime = { execStdin: t.mock.fn(async (_ref: ContainerRef, command: string) => {
    commands.push(command)
    return { exitCode: 0, output: '' }
  }) }
  const probe = createStartupProbeQuery({ instanceId, runtime, signal: controller.signal })
  t.after(() => { probe.dispose(); instanceConsoleLogStore.removeInstance(instanceId) })
  const token = (index = commands.length - 1) => /BSPSTART:([a-f0-9-]{36})\|/.exec(commands[index]!)![1]!
  const line = (id: string, ready = true, connected = 'x') => `[00:03:47]: BSPSTART:${id}|${ready ? 1 : 0}|1|${connected}\t`
  const reply = (id: string, shard: 'master' | 'caves' = 'master', ready = true, connected = 'x') =>
    instanceConsoleLogStore.appendDockerLine(instanceId, line(id, ready, connected), shard)
  return { ...probe, instanceId, controller, runtime, commands, token, line, reply }
}

it('查询发送前订阅回包，及时响应无需轮询日志', async (t) => {
  const f = fixture(t)
  const pending = f.query(master, 'master')
  await Promise.resolve()
  f.reply(f.token())
  assert.equal((await pending)?.worldReady, true)
})

it('单次等待结束后的就绪回包保留到下一次查询', async (t) => {
  const f = fixture(t)
  const pending = f.query(master, 'master')
  await Promise.resolve()
  t.mock.timers.tick(1000)
  assert.equal(await pending, null)
  f.reply(f.token())
  for (let i = 0; i < 2500; i++) instanceConsoleLogStore.appendDockerLine(f.instanceId, `加载日志 ${i}`, 'master')
  assert.equal((await f.query(master, 'master'))?.worldReady, true)
  assert.equal(f.commands.length, 1, '消费延迟响应不再发送多余命令')
})

it('前一次命令在后一次查询等待期间回包仍能确认就绪', async (t) => {
  const f = fixture(t)
  const first = f.query(master, 'master')
  await Promise.resolve()
  const oldToken = f.token()
  t.mock.timers.tick(1000)
  assert.equal(await first, null)
  const second = f.query(master, 'master')
  await Promise.resolve()
  assert.equal(f.token(), oldToken)
  f.reply(oldToken)
  assert.equal((await second)?.worldReady, true)
})

it('未就绪结果不会遮住后续就绪响应', async (t) => {
  const f = fixture(t)
  const first = f.query(master, 'master')
  await Promise.resolve()
  f.reply(f.token(), 'master', false)
  assert.equal((await first)?.worldReady, false)
  const second = f.query(master, 'master')
  await Promise.resolve()
  f.reply(f.token())
  assert.equal((await second)?.worldReady, true)
})

it('不同启动任务拒绝上一轮迟到回包，结束时取消订阅', async (t) => {
  const f = fixture(t)
  const old = f.query(master, 'master')
  await Promise.resolve()
  const oldToken = f.token()
  f.dispose()
  assert.equal(await old, null)
  const next = createStartupProbeQuery({ instanceId: f.instanceId, runtime: f.runtime, signal: f.controller.signal })
  t.after(() => next.dispose())
  const pending = next.query(master, 'master')
  await Promise.resolve()
  assert.notEqual(f.token(), oldToken)
  f.reply(oldToken)
  t.mock.timers.tick(1000)
  assert.equal(await pending, null)
  assert.equal(await f.query(master, 'master'), null)
})

it('主世界、洞穴、运行时身份与远端连接查询分别隔离', async (t) => {
  const f = fixture(t)
  const first = f.query(master, 'master')
  await Promise.resolve()
  const masterToken = f.token()
  f.reply(masterToken)
  await first
  const targets: Array<[ContainerRef, 'master' | 'caves', string | undefined]> = [
    [caves, 'caves', undefined], [master, 'master', '2'],
    [{ ...master, runtimeIdentity: 'replacement' }, 'master', undefined],
  ]
  for (const [ref, shard, peer] of targets) {
    const pending = f.query(ref, shard, peer)
    await Promise.resolve()
    assert.notEqual(f.token(), masterToken)
    f.reply(masterToken, shard)
    t.mock.timers.tick(1000)
    assert.equal(await pending, null)
  }
})

it('回显、系统消息与错误分片日志不能作为就绪证据', async (t) => {
  const f = fixture(t)
  const pending = f.query(master, 'master')
  await Promise.resolve()
  const token = f.token()
  instanceConsoleLogStore.appendDockerLine(f.instanceId, `RemoteCommandInput: ${f.commands[0]}`, 'master')
  instanceConsoleLogStore.appendSystem(f.instanceId, f.line(token), 'master')
  f.reply(token, 'caves')
  t.mock.timers.tick(1000)
  assert.equal(await pending, null)
})

it('写入通道卡住仍能按时返回，并持续接受已发送命令的回包', async (t) => {
  const f = fixture(t)
  f.runtime.execStdin.mock.mockImplementation(async (_ref, command) => {
    f.commands.push(command)
    return new Promise(() => {})
  })
  const pending = f.query(master, 'master')
  await Promise.resolve()
  t.mock.timers.tick(1000)
  assert.equal(await pending, null)
  f.reply(f.token())
  assert.equal((await f.query(master, 'master'))?.worldReady, true)
})

it('取消立即结束等待并拒绝后续回包', async (t) => {
  const f = fixture(t)
  const pending = f.query(master, 'master')
  await Promise.resolve()
  const token = f.token()
  f.controller.abort()
  assert.equal(await pending, null)
  f.reply(token)
  assert.equal(await f.query(master, 'master'), null)
})

it('写入失败或异常返回未知，不生成未处理的拒绝', async (t) => {
  const f = fixture(t)
  f.runtime.execStdin.mock.mockImplementationOnce(async () => ({ exitCode: 1, output: 'failed' }))
  assert.equal(await f.query(master, 'master'), null)
  f.runtime.execStdin.mock.mockImplementationOnce(async () => { throw new Error('unavailable') })
  assert.equal(await f.query(master, 'master'), null)
})
