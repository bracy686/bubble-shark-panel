import { readBrandEnv } from '../../../../shared/brand-env'
import { withInstanceContentActivity } from '../../shared/instance-content/operation'
import type { FastifyInstance } from 'fastify'
import type { HostMemoryPressureFailure } from '../../infra/container/host-resource-guard'
import fs from 'node:fs'
import path from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import { randomUUID } from 'node:crypto'
import { createStartupProbeQuery } from './startup-probe-query'
import { resolveDstContainerResourceLimits } from '../../infra/container/dst-container-resources'
import { DST_MEMORY_SLICE, sampleHostResources, sampleRuntimeResources } from '../../infra/container/memory-budget'
import { emitPanelEvent } from '../notify/events'
import type { MemoryProtectionStop } from '../../../../shared/contracts/instance-resources'
import { resolveInstanceResourceSettings } from './resource-settings'
import { runStartupMonitor } from './startup-monitor'
import { cancelStartupTask, changeStartupPhase, createStartupTask, currentStartupTask, enqueueStartupTask, getStartupSnapshot, isCurrentStartupTask, persistStartupTask, recordStartupSwapAdvice, releaseStartupTask, startupIsActive, type StartupTask } from './startup-state'
import { isSteamcmdAppUpdateBusy } from '../../infra/container/steamcmd-app-update-queue'
import { resolveDockerStatus } from '../../infra/docker'
import { createDockerClient } from '../../infra/docker-connect'
import {
  assessHostMemoryForHeavyOperation,
  ensureSteamcmdImage,
  formatGameDstImageError,
  getContainerRuntime,
  isSteamcmdImagePresent,
  pullGameDstImage,
} from '../../infra/container'
import { resolveInstanceContainerBind } from '../../infra/container/steamcmd-install-bind'
import { buildCavesContainerName, buildMasterContainerName } from '../../infra/container/naming'
import type { ContainerInspect, ContainerRef, ContainerRuntime, ShardContainerSpec } from '../../infra/container/types'
import { DST_APP_ID, DST_CLUSTER_NAME, DST_CONF_DIR, DST_STORAGE_DIR } from '../../infra/game-adapter/dst/constants'
import {
  buildDstStartBlockedMessage,
  diagnoseDstInstallReadiness,
} from '../../infra/game-adapter/dst/install-readiness'
import { findHostUdpPortConflicts, validateShardPortsForStart } from '../../infra/game-adapter/dst/port-conflict'
import {
  buildDstCavesShardContainerSpec,
  buildDstMasterShardContainerSpec,
} from '../../infra/game-adapter/dst/runtime-spec'
import { ensureDstCavesShardConfig } from '../../infra/game-adapter/dst/cluster-config'
import { syncInstanceModFilesFromDb } from '../mod/mod-file-sync-service'
import { collectReservedDstPortsOnNode } from './dst-port-service'
import { LOCAL_NODE_ID } from '../../shared/dst/local-dst-instance'
import {
  ensureDockerShardInterconnectConfig,
  ensureNativeShardInterconnectConfig,
} from '../../infra/game-adapter/dst/shard-network-config'
import {
  isCavesShardConfigured,
  readClusterShardEnabledFromInstall,
  readCavesServerIniFields,
  readMasterServerIniFields,
} from '../../infra/game-adapter/dst/shard-service'
import { getServerContainerConfig } from '../../shared/config/container'
import { isSteamcmdRuntimeReady, resolveRuntimeStatus } from '../../infra/runtime'
import { instanceConsoleLogStore } from '../../shared/instance-runtime/console-log-store'
import { getGameInstanceById, listInstanceMods, updateGameInstanceRuntime } from '../../shared/db/index'
import type { DbGameInstance } from '../../shared/db'
import type { HostResourceSnapshot, InstanceStartupSnapshot } from '../../../../shared/contracts/instance-resources'
import { resolveClusterPaths } from '../../infra/game-adapter/dst/cluster-service'
import { resolveShardRoot } from '../../infra/game-adapter/dst/shard-layout'
import { parseClusterIni } from '../../infra/game-adapter/dst/cluster-ini'
import { describeSystemdExitReason, readHostMemorySnapshot, readRuntimeMemoryCapMb } from '../../infra/container/exit-reason'

/** 主世界分片互联端口（cluster.ini [SHARD] master_port）；读不到时退回 DST 默认值 */
const DEFAULT_DST_MASTER_PORT = 10888
const memoryProtectionInFlight = new Set<string>()
const stoppingInstances = new Map<string, number>()
export const isMemoryProtectionInFlight = (id: string) => memoryProtectionInFlight.has(id)
export const isInstanceStopping = (id: string) => stoppingInstances.has(id)
export const runtimeMemoryEpoch = (instance: DbGameInstance) => `${instance.containerId ?? ''}|${instance.runtimeStartedAt ?? ''}|${instance.lastStartupReport?.taskId ?? ''}`

function hostGuardReading(host: HostResourceSnapshot) {
  return { ...host, poolAvailableMb: host.budget.state === 'protected' && host.budget.maxMb != null && host.budget.currentMb != null ? Math.max(0, host.budget.maxMb - host.budget.currentMb) : null }
}
function measuredStartupDemand(report: InstanceStartupSnapshot | null | undefined, caves: boolean) {
  const master = report?.planningDemand?.masterMb ?? report?.master.memoryAndSwapPeakMb
  const cave = report?.planningDemand?.cavesMb ?? report?.caves.memoryAndSwapPeakMb
  return master == null && cave == null ? null : (master ?? 0) + (caves ? cave ?? 0 : 0)
}

/** 先写保护锁与现场，再取消旧任务。清理失败保留引用，手动重试前不能新建分片。 */
export async function protectInstanceMemory(app: FastifyInstance, observed: DbGameInstance,
  evidence: Omit<MemoryProtectionStop, 'at' | 'cleanupCompleted'>, observedRefs?: ContainerRef[], scope: 'instance' | 'caves' = 'instance'): Promise<void> {
  const id = observed.id
  if (memoryProtectionInFlight.has(id)) return
  memoryProtectionInFlight.add(id)
  try {
    const current = await getGameInstanceById(id)
    if (!current || current.runtimeFailureKind === 'memory_protection' || (scope === 'caves' && current.lastStartupReport?.caves.protectionStop) || runtimeMemoryEpoch(current) !== runtimeMemoryEpoch(observed)) return
    const runtime = getContainerRuntime()
    const refs = (await Promise.all((scope === 'caves' ? [buildCavesContainerName(id)] : [buildMasterContainerName(id), buildCavesContainerName(id)]).map(name => runtime.findByName(name)))).filter((r): r is ContainerRef => !!r)
    if (observedRefs?.some(ref => !refs.some(actual => actual.id === ref.id))) return
    for (const ref of refs) {
      const before = ref.name.endsWith('-caves') ? evidence.caves : evidence.master
      const now = await runtime.resourceSnapshot?.(ref)
      if (before?.runtimeIdentity && before.runtimeIdentity !== now?.runtimeIdentity) return
      if (now?.runtimeIdentity) ref.runtimeIdentity = now.runtimeIdentity
    }
    const at = new Date().toISOString()
    const stop: MemoryProtectionStop = { ...evidence, at, cleanupCompleted: false }
    const task = currentStartupTask(id)
    const report = structuredClone(task?.snapshot ?? current.lastStartupReport ?? {
      taskId: randomUUID(), startedAt: current.runtimeStartedAt ?? at, updatedAt: at, phaseStartedAt: at,
      status: 'success', phase: 'ready', phaseDeadlineAt: null, elapsedSeconds: 0, remainingSeconds: null,
      master: { state: 'ready', memoryPeakMb: null }, caves: { state: 'disabled', memoryPeakMb: null }, diagnosis: null,
    } satisfies InstanceStartupSnapshot)
    if (scope === 'caves') {
      report.caves.protectionStop = stop
      report.caves.state = 'failed'
      report.caves.cleanupCompleted = false
      report.caves.diagnosis = { code: evidence.code, message: evidence.message }
      report.caves.readyAt = null
    }
    else {
      report.protectionStop = stop
      report.master.state = 'failed'; report.master.readyAt = null
      report.master.diagnosis = { code: evidence.code, message: evidence.message }
      if (report.caves.state !== 'disabled') { report.caves.state = 'stopped'; report.caves.readyAt = null }
    }
    if (startupIsActive(report)) {
      report.status = 'failed'; report.phase = 'failed'; report.phaseDeadlineAt = null
      report.diagnosis = { code: evidence.code, message: evidence.message }; report.updatedAt = at
    }
    if (task) { task.snapshot = report; await persistStartupTask(task) }
    await updateGameInstanceRuntime(id, { status: scope === 'caves' ? 'running' : 'error', runtimeFailureKind: scope === 'caves' ? 'memory' : 'memory_protection', runtimeWarning: evidence.message,
      lastError: evidence.message, lastErrorPhase: 'runtime', lastStartupReport: report })
    const generation = bumpCavesStartGeneration(id, scope === 'caves' ? 'caves' : 'all')
    if (scope === 'caves') stopShardLogFollow(id, 'caves')
    else stopLogFollow(id)
    const stale = () => !isCurrentCavesStartGeneration(id, generation)
    const outcomes = await Promise.allSettled(refs.map(async ref => {
      if (stale()) throw new Error('保护清理轮次已失效')
      let timer: NodeJS.Timeout | undefined
      try {
        await Promise.race([
          runtime.emergencyRemove ? runtime.emergencyRemove(ref) : runtime.remove(ref),
          new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('保护清理超时')), 20_000) }),
        ])
        if (stale()) throw new Error('保护清理轮次已失效')
        const remains = await runtime.findByName(ref.name)
        if (remains) throw new Error('分片仍存在，保留引用')
      }
      finally { clearTimeout(timer) }
    }))
    if (stale()) return
    const errors = outcomes.filter(result => result.status === 'rejected').map(result => String((result as PromiseRejectedResult).reason))
    stop.cleanupCompleted = errors.length === 0
    if (scope === 'caves') { report.caves.protectionStop = stop; report.caves.cleanupCompleted = stop.cleanupCompleted }
    else report.protectionStop = stop
    if (task) task.snapshot = report
    if (scope === 'instance' && stop.cleanupCompleted) await runtime.removeShardNetwork(id).catch(error => app.log.warn({ err: error, instanceId: id }, '保护停机网络清理失败'))
    await updateGameInstanceRuntime(id, { lastStartupReport: report,
      ...(stop.cleanupCompleted ? scope === 'instance' ? { containerId: null, runtimePid: null, runtimeStartedAt: null, runtimeReadyAt: null } : {} : { lastError: `${evidence.message}；清理未完成：${errors.join('；')}` }) })
    const mb = (value: number | null | undefined) => value == null ? '未知' : `${Math.round(value)} MiB`
    emitPanelEvent({ type: 'instance_exited_unexpectedly', subjectId: id, subjectName: current.name, severity: 'critical', at, occurrenceKey: `memory:${report.taskId}:${at}`,
      message: `实例「${current.name}」内存保护停止：${evidence.message}。主世界 ${mb(evidence.master?.memoryCurrentMb)}/${mb(evidence.master?.memoryMaxMb)}，洞穴 ${mb(evidence.caves?.memoryCurrentMb)}/${mb(evidence.caves?.memoryMaxMb)}，共享预算 ${mb(evidence.host?.budget.maxMb)}，可用 swap ${mb(evidence.host?.swapFreeMb)}。请在服务器 panel.env 检查限额，或以 root 追加 swap 后手动启动。${errors.length ? '分片清理失败，请先再次停止。' : ''}` })
    instanceConsoleLogStore.appendSystem(id, `内存保护停止：${evidence.message}${errors.length ? '；清理未完成' : scope === 'caves' ? '；洞穴已清理，主世界继续运行' : '；两片已清理'}`)
  }
  finally { memoryProtectionInFlight.delete(id) }
}

/** 当前分片确认 OOM 时按分片隔离；主世界 OOM 才停止失去依赖的洞穴。 */
export async function protectExitedShardOom(app: FastifyInstance, instance: DbGameInstance, ref: ContainerRef, inspected?: ContainerInspect): Promise<boolean> {
  const runtime = getContainerRuntime()
  const state = inspected ?? await runtime.inspect(ref)
  if (state.probeFailed || (state.running && !state.restarting) || (state.oomKilled !== true && state.exitResult !== 'oom-kill')) return false
  const resources = await sampleRuntimeResources(runtime, ref)
  if (resources?.oomKilled !== true || (state.runtimeIdentity && state.runtimeIdentity !== resources.runtimeIdentity)) return false
  const isCaves = ref.name.endsWith('-caves')
  const other = await runtime.findByName(isCaves ? buildMasterContainerName(instance.id) : buildCavesContainerName(instance.id))
  const peer = other ? await sampleRuntimeResources(runtime, other) : null
  await protectInstanceMemory(app, instance, { code: 'oom', message: isCaves ? '洞穴因 OOM 退出，已保护停止洞穴，主世界继续运行' : '主世界因 OOM 退出，已保护停止实例',
    master: isCaves ? peer : resources, caves: isCaves ? resources : peer, host: await sampleHostResources(runtime) }, isCaves ? [ref] : [ref, ...(other ? [other] : [])], isCaves ? 'caves' : 'instance')
  const saved = await getGameInstanceById(instance.id)
  return saved?.runtimeFailureKind === 'memory_protection' || !!saved?.lastStartupReport?.caves.protectionStop
}
/**
 * 等待主世界就绪的默认上限（秒）。
 *
 * 每片默认 300 秒；实例设置或显式环境配置可延长。完整就绪由游戏内查询确认。
 */
const DEFAULT_SHARD_READY_WAIT_SEC = 300

/** 就绪等待上限；可用 BSP_SHARD_READY_WAIT_SEC 覆盖（小机器上 Mod 特别多时可再调大） */
export function resolveShardReadyWaitSec(): number {
  const raw = readBrandEnv('BSP_SHARD_READY_WAIT_SEC')?.trim()
  const parsed = raw ? Number(raw) : Number.NaN
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : DEFAULT_SHARD_READY_WAIT_SEC
}

/**
 * 分片端口早于这个秒数就被占用时，不认为世界已经加载完。
 *
 * DST 的分片网络是在世界初始化过程中建立的，正常情况下端口出现得比较晚；一旦它出现得过早，
 * 说明「端口被占用」只能证明进程起来了，不能证明能接客。60 秒足够区分这两种情形：
 * 多 Mod 大存档的冷启动远不止 60 秒。
 *
 * 这里只约束**端口这条辅助判据**：主判据是 `hasMasterReadyMarker()`，命中即放行，与端口无关。
 */
const SHARD_PORT_EARLY_BIND_GRACE_SEC = 60

/**
 * 主世界「世界已经加载完、分片端口已经在监听」的标记。
 *
 * 每一条都注明是在**哪一份真实日志**里核实的。教训：上一轮拿洞穴的日志去否定
 * `Starting master server`，而它只出现在主世界的日志里——两个分片打印的行并不相同，
 * 用错日志会得出相反的结论。三个标记都取自用户服务器上的真实输出：
 *
 *   主世界日志（server_log.txt / 面板采集的 console-logs）：
 *     [00:01:22]: About to start a shard with these settings:
 *     [00:01:22]:   ShardRole: MASTER
 *     [00:01:22]: [Shard] Starting master server
 *     [00:01:22]: [Shard] Shard server started on port: 10888   ← 最精确：端口已在监听
 *   洞穴日志（同一台机器、完整成功的一次运行）：
 *     [00:02:49]: Reconstructing topology
 *     [00:02:50]: About to start a shard with these settings:
 */
const MASTER_READY_MARKER = /Shard server started on port|About to start a shard with these settings|Reconstructing topology/

/** DST 执行某个 Mod 的 modmain 时会打印的行（取自用户服务器上的真实输出） */
const MOD_LOADED_LINE_PATTERN = /Mod:\s*workshop-(\d+)/g
/** 本轮分片的 Mod 索引流程已完成；只有它出现后才能对「加载了几个 Mod」下结论 */
const MOD_INDEX_DONE_MARKER = /ModIndex: Load sequence finished successfully/
/** Mod 数量级：一行的 Mod 下 2 MiB 足以覆盖整个索引段 */
const MOD_LOAD_REPORT_MAX_BYTES = 2 * 1024 * 1024
/** 小机器上 36 个 Mod 的索引实测约 40 秒完成，留足余量 */
const MOD_LOAD_REPORT_TIMEOUT_MS = 5 * 60 * 1000
const MOD_LOAD_REPORT_POLL_INTERVAL_MS = 3000

/** 读文件尾部若干字节；日志可达数百 KB，只关心结尾 */
function readTailText(filePath: string, maxBytes = 64 * 1024): string | null {
  let descriptor: number | undefined
  try {
    const size = fs.statSync(filePath).size
    if (size === 0) {
      return ''
    }
    const start = size > maxBytes ? size - maxBytes : 0
    descriptor = fs.openSync(filePath, 'r')
    const buffer = Buffer.allocUnsafe(size - start)
    const read = fs.readSync(descriptor, buffer, 0, buffer.length, start)
    return buffer.subarray(0, read).toString('utf8')
  }
  catch {
    return null
  }
  finally {
    if (descriptor !== undefined) {
      fs.closeSync(descriptor)
    }
  }
}

interface CachedShardLogProbe {
  logPath: string
  startedAt: string | null | undefined
  fingerprint: string
  ready: boolean
  luaFailure: string | null
}

const shardLogProbeCache = new Map<string, CachedShardLogProbe>()

function parseShardLuaFailure(text: string, shard: 'master' | 'caves'): string | null {
  if (!/LUA ERROR stack traceback:/.test(text)) return null
  const detail = text.split(/\r?\n/).find(line => /\[string .+\]:\d+:/.test(line))?.trim()
  return `${shard === 'master' ? '主世界' : '洞穴'}分片发生 Lua 致命错误${detail ? `：${detail.slice(0, 500)}` : ''}。请检查 Mod 与存档，完整日志见控制台。`
}

/** 只缓存解析结果；文件未变化时，启动等待与状态对账共用结果，不重复读取大日志。 */
function readShardLogProbe(
  instanceId: string,
  installPath: string,
  shard: 'master' | 'caves',
  startedAt?: string | null,
): CachedShardLogProbe | null {
  const key = `${instanceId}:${shard}`
  const logPath = path.join(resolveShardRoot(installPath, shard), 'server_log.txt')
  try {
    const stat = fs.statSync(logPath)
    const since = startedAt ? Date.parse(startedAt) : 0
    if (since && stat.mtimeMs < since) {
      shardLogProbeCache.delete(key)
      return null
    }
    const fingerprint = `${stat.dev}:${stat.ino}:${stat.size}:${stat.mtimeMs}:${stat.ctimeMs}`
    const cached = shardLogProbeCache.get(key)
    if (cached?.logPath === logPath && cached.startedAt === startedAt && cached.fingerprint === fingerprint) {
      return cached
    }
    const text = readTailText(logPath, MOD_LOAD_REPORT_MAX_BYTES)
    if (text === null) {
      shardLogProbeCache.delete(key)
      return null
    }
    const result = {
      logPath,
      startedAt,
      fingerprint,
      ready: MASTER_READY_MARKER.test(text),
      luaFailure: parseShardLuaFailure(text, shard),
    }
    shardLogProbeCache.set(key, result)
    return result
  }
  catch {
    shardLogProbeCache.delete(key)
    return null
  }
}

/**
 * 主世界分片是否已经打印过世界就绪的标记。
 *
 * 两个来源都看，因为它们的可靠性不同：
 *   1. 面板采集到控制台的行——依赖 systemd 的 stdout 采集链路，链路断了就永远匹配不到；
 *   2. **DST 自己写的 `server_log.txt`**——面板从实例目录直接读，不依赖任何采集链路，
 *      而且该文件每次分片启动都会被重写，天然只包含「本轮」的日志，不会匹配到上一轮的旧标记。
 */
export function hasMasterReadyMarker(instanceId: string, installPath?: string, startedAt?: string | null): boolean {
  const since = startedAt ? Date.parse(startedAt) : 0
  try {
    const hit = instanceConsoleLogStore
      .listLogs(instanceId)
      .some(line => line.stream === 'stdout' && (line.shard == null || line.shard === 'master') && (!since || Date.parse(line.at) >= since) && MASTER_READY_MARKER.test(line.text))
    if (hit) {
      return true
    }
  }
  catch {
    // 控制台存储不可用不影响下面的文件判定
  }
  if (!installPath) {
    return false
  }
  return readShardLogProbe(instanceId, installPath, 'master', startedAt)?.ready ?? false
}

/** 致命 Lua 错误会让游戏停在错误界面，进程却仍存活；普通 prefab 警告不算失败。 */
export function findShardLuaFailure(
  instanceId: string,
  installPath?: string,
  startedAt?: string | null,
  onlyShard?: 'master' | 'caves',
): string | null {
  const since = startedAt ? Date.parse(startedAt) : 0
  for (const shard of ['master', 'caves'] as const) {
    if (onlyShard && shard !== onlyShard) continue
    const lines = instanceConsoleLogStore.listLogs(instanceId, 0, 2000)
      .filter(line => line.stream === 'stdout' && line.shard === shard && (!since || Date.parse(line.at) >= since))
      .map(line => line.text)
    const failure = parseShardLuaFailure(lines.join('\n'), shard)
      ?? (installPath ? readShardLogProbe(instanceId, installPath, shard, startedAt)?.luaFailure : null)
    if (failure) return failure
  }
  return null
}

/**
 * 从分片日志文本里读出「本轮 Mod 索引是否完成」与「实际被加载的 Mod ID」。
 *
 * 抽成纯函数是为了能用真实日志片段固化口径：DST 只在实际执行某个 Mod 的 modmain 时
 * 打印 `Mod: workshop-<id> ... Registering prefabs`，因此这个集合就是游戏真正加载的 Mod。
 */
export function summarizeLoadedMods(logText: string): { indexFinished: boolean, workshopIds: string[] } {
  const workshopIds = new Set<string>()
  for (const match of logText.matchAll(MOD_LOADED_LINE_PATTERN)) {
    workshopIds.add(match[1] as string)
  }
  return {
    indexFinished: MOD_INDEX_DONE_MARKER.test(logText),
    workshopIds: [...workshopIds],
  }
}

/**
 * 启动后在控制台回答「游戏这次到底加载了几个 Mod」。
 *
 * 面板此前只能回答「内容已就绪」，而「内容在盘上」与「游戏真的加载」是两件事：
 * 落位目录与游戏读取目录不一致时，面板显示全部就绪、游戏里一个 Mod 都没有，且没有任何报错
 * （`-skip_update_server_mods` 还禁止了游戏自己补下载）。
 *
 * 读 DST 自己写的 `server_log.txt`：该文件每次分片启动都会被重写，天然只含本轮。
 * 只有看到本轮的 Mod 索引完成标记才下结论，否则宁可不说话——把「还没加载到那一步」
 * 说成「一个都没加载」会误导排查方向。
 */
async function reportLoadedModsAfterStart(input: { instanceId: string, installPath: string, signal: AbortSignal }): Promise<void> {
  const logPath = path.join(resolveShardRoot(input.installPath, 'master'), 'server_log.txt')
  const deadline = Date.now() + MOD_LOAD_REPORT_TIMEOUT_MS
  let loadedIds: Set<string> | null = null
  // NaN 保证首轮必读：文件还不存在时 size 也是 -1，用 -1 初始化会一直跳过读取
  let lastSize = Number.NaN
  while (Date.now() < deadline && !input.signal.aborted) {
    let size = -1
    try {
      size = fs.statSync(logPath).size
    }
    catch {
      // 日志还没写出来：下一轮再看
    }
    // 日志没长就不重复读：索引完成后它基本不再变化，避免一直空转读盘
    if (size !== lastSize) {
      lastSize = size
      const summary = summarizeLoadedMods(readTailText(logPath, MOD_LOAD_REPORT_MAX_BYTES) ?? '')
      if (summary.indexFinished) {
        loadedIds = new Set(summary.workshopIds)
        break
      }
    }
    await sleep(MOD_LOAD_REPORT_POLL_INTERVAL_MS, undefined, { signal: input.signal }).catch(() => {})
  }
  if (!loadedIds || input.signal.aborted) {
    return
  }

  const expected = (await listInstanceMods(input.instanceId))
    .filter(mod => mod.enabled && mod.installStatus === 'ready')
    .map(mod => mod.workshopId)
  if (input.signal.aborted) return

  if (loadedIds.size === 0) {
    if (expected.length === 0) {
      return
    }
    instanceConsoleLogStore.appendSystem(
      input.instanceId,
      `游戏本次没有加载任何 Mod（面板启用 ${expected.length} 个）：请到「模组管理」核对这些 Mod 的状态，`
      + '并确认实例目录下 mods/workshop-<ID> 有内容',
      'master',
    )
    return
  }

  instanceConsoleLogStore.appendSystem(
    input.instanceId,
    `本次启动已加载 ${loadedIds.size} 个 Mod（面板启用 ${expected.length} 个）`,
    'master',
  )
  const missing = expected.filter(workshopId => !loadedIds.has(workshopId))
  if (missing.length > 0) {
    instanceConsoleLogStore.appendSystem(
      input.instanceId,
      `以下已启用的 Mod 未被游戏加载：${missing.slice(0, 10).join('、')}`
      + `${missing.length > 10 ? ` 等 ${missing.length} 个` : ''}`,
      'master',
    )
  }
}

export type ConsoleCommandShard = 'master' | 'caves'

const CONSOLE_SHARD_LABEL: Record<ConsoleCommandShard, string> = {
  master: '地上',
  caves: '洞穴',
}

const logFollowAbortControllers = new Map<string, AbortController>()

function logFollowKey(instanceId: string, shard: ConsoleCommandShard) {
  return `${instanceId}:${shard}`
}

export async function ensureContainerRuntimeReady(options: import('../../infra/container/image-candidates').ImagePreparationOptions = {}): Promise<{ ok: boolean, message?: string }> {
  const { runtimeMode } = getServerContainerConfig()
  if (runtimeMode === 'native') {
    if ((await resolveRuntimeStatus()) !== 'running') {
      return { ok: false, message: '无法连接 systemd 用户服务管理器，请确认 bsp 用户已启用 linger 且 user bus 正常' }
    }
    if (!(await isSteamcmdRuntimeReady())) {
      return { ok: false, message: 'SteamCMD 未就绪，请检查 BSP_NATIVE_STEAMCMD_PATH 或重新运行 Native 安装器' }
    }
    return { ok: true }
  }
  if ((await resolveDockerStatus()) !== 'running') {
    return { ok: false, message: '无法连接 Docker，请确认面板已挂载 docker.sock（或 Windows 下 Docker Desktop 已启动）' }
  }
  const pullResult = await ensureSteamcmdImage(options)
  if (!pullResult.ok) {
    return {
      ok: false,
      message: `SteamCMD 镜像未就绪：${pullResult.error}。请检查网络或 panel.env 中的 BSP_STEAMCMD_IMAGE`,
    }
  }
  return { ok: true }
}

export function resolveDefaultInstanceInstallPath(instanceId: string): string {
  const { instancesRoot } = getServerContainerConfig()
  return path.join(instancesRoot, instanceId)
}

export async function resolveInstanceContainerRef(instanceId: string): Promise<ContainerRef | undefined> {
  const instance = await getGameInstanceById(instanceId)
  const existing = await getContainerRuntime().findByName(buildMasterContainerName(instanceId))
  if (existing) return existing
  if (instance?.containerId) {
    return {
      id: instance.containerId,
      name: instance.containerId.endsWith('.service') ? instance.containerId.slice(0, -8) : buildMasterContainerName(instanceId),
    }
  }
  const runtime = getContainerRuntime()
  return runtime.findByName(buildMasterContainerName(instanceId))
}

export async function resolveCavesContainerRef(instanceId: string): Promise<ContainerRef | undefined> {
  const runtime = getContainerRuntime()
  return runtime.findByName(buildCavesContainerName(instanceId))
}

export async function isInstanceContainerRunning(
  instanceId: string,
  shard: ConsoleCommandShard = 'master',
): Promise<boolean> {
  const ref = await resolveConsoleCommandContainerRef(instanceId, shard)
  if (!ref) {
    return false
  }
  const runtime = getContainerRuntime()
  const inspect = await runtime.inspect(ref)
  return inspect.running
}

export interface InstanceShardRuntimeProbe {
  /** 该分片是否存在运行时单元（从未创建或已被移除时为 false） */
  unitExists: boolean
  /** 运行时快照；运行时不可达时为 null（此时不能据此判定已停止） */
  snapshot: ContainerInspect | null
}

/**
 * 分片运行时快照：状态对账用它区分「运行中 / 崩溃后等待重启 / 真的停了 / 问不到」。
 * 把这四种情况压成一个布尔值正是实例状态在运行与停止之间来回跳的根源。
 */
export async function inspectInstanceShardRuntime(
  instanceId: string,
  shard: ConsoleCommandShard = 'master',
): Promise<InstanceShardRuntimeProbe> {
  const ref = await resolveConsoleCommandContainerRef(instanceId, shard)
  if (!ref) {
    return { unitExists: false, snapshot: null }
  }
  try {
    const snapshot = await getContainerRuntime().inspect(ref)
    return { unitExists: true, snapshot: snapshot.probeFailed ? null : snapshot }
  }
  catch {
    return { unitExists: true, snapshot: null }
  }
}

function stopLogFollow(instanceId: string) {
  for (const [key, controller] of logFollowAbortControllers.entries()) {
    if (key === instanceId || key.startsWith(`${instanceId}:`)) {
      controller.abort()
      logFollowAbortControllers.delete(key)
    }
  }
}

function stopShardLogFollow(instanceId: string, shard: ConsoleCommandShard) {
  const key = logFollowKey(instanceId, shard)
  logFollowAbortControllers.get(key)?.abort()
  logFollowAbortControllers.delete(key)
}

/**
 * 分片日志来源已改为 systemd 直接追加到面板可读的文件（见 NativeSystemdRuntime），
 * 不再依赖 journald 权限。这里保留一条兜底：万一文件读不到，至少告诉用户去哪找
 * 游戏自己写的 server_log.txt。
 */
const JOURNAL_PERMISSION_HINT = /No journal files were opened|insufficient permissions/i

/** 实例目录内游戏自己写的启动日志（面板读不到分片日志时，用户据此排查崩溃原因） */
function resolveShardGameLogHint(shard: ConsoleCommandShard): string {
  const shardDir = shard === 'caves' ? 'Caves' : 'Master'
  return `${DST_STORAGE_DIR}/${DST_CONF_DIR}/${DST_CLUSTER_NAME}/${shardDir}/server_log.txt`
}

function startShardLogFollow(instanceId: string, ref: ContainerRef, shard: ConsoleCommandShard) {
  stopShardLogFollow(instanceId, shard)
  const key = logFollowKey(instanceId, shard)
  const controller = new AbortController()
  logFollowAbortControllers.set(key, controller)
  const label = CONSOLE_SHARD_LABEL[shard]
  instanceConsoleLogStore.appendSystem(instanceId, `已连接${label}运行时，开始采集控制台输出`, shard)
  void (async () => {
    const runtime = getContainerRuntime()
    let permissionHintShown = false
    try {
      for await (const line of runtime.logs(ref, { follow: true, tail: 100, signal: controller.signal })) {
        if (controller.signal.aborted) {
          break
        }
        if (JOURNAL_PERMISSION_HINT.test(line.text)) {
          // 万一还是读到了系统日志的报错（不是游戏输出）：换成一句能指导排查的说明，
          // 否则控制台看起来像「服务器什么都没说」，把真正的崩溃原因藏起来。
          if (!permissionHintShown) {
            permissionHintShown = true
            instanceConsoleLogStore.appendSystem(
              instanceId,
              `${label}分片日志暂时取不到。请查看实例目录的 ${resolveShardGameLogHint(shard)}，或服务器上的 systemctl --user status 输出`,
              shard,
            )
          }
          continue
        }
        instanceConsoleLogStore.appendDockerLine(instanceId, line.text, shard)
      }
    }
    catch (error) {
      if (!controller.signal.aborted) {
        const message = error instanceof Error ? error.message : String(error)
        instanceConsoleLogStore.appendSystem(instanceId, `${label}日志流中断: ${message}`, shard)
      }
    }
  })()
}

export function startContainerLogFollow(instanceId: string, ref: ContainerRef) {
  startShardLogFollow(instanceId, ref, 'master')
}

async function readRecentContainerLogLines(runtime: ContainerRuntime, ref: ContainerRef, tail = 20): Promise<string[]> {
  const lines: string[] = []
  try {
    for await (const line of runtime.logs(ref, { tail })) {
      lines.push(line.text)
    }
  }
  catch {
    return []
  }
  return lines.slice(-tail)
}

async function readRecentContainerLogs(runtime: ContainerRuntime, ref: ContainerRef, tail = 20): Promise<string> {
  return readRecentContainerLogLines(runtime, ref, tail).then(rows => rows.join('\n').trim())
}

/** 读取指定分片容器最近日志行（不依赖面板内存日志流，面板重启后仍可用） */
export async function readRecentInstanceContainerLogLines(
  instanceId: string,
  tail = 80,
  shard: ConsoleCommandShard = 'master',
): Promise<string[]> {
  const ref = await resolveConsoleCommandContainerRef(instanceId, shard)
  if (!ref) {
    return []
  }
  const runtime = getContainerRuntime()
  const inspect = await runtime.inspect(ref)
  if (!inspect.running) {
    return []
  }
  return readRecentContainerLogLines(runtime, ref, tail)
}

/** 面板重启后，为仍在运行的实例重新挂载 Docker 日志流 */
export async function ensureInstanceContainerLogFollow(instanceId: string): Promise<void> {
  const runtime = getContainerRuntime()
  const masterRef = await resolveInstanceContainerRef(instanceId)
  if (masterRef) {
    const inspect = await runtime.inspect(masterRef)
    if (inspect.running && !logFollowAbortControllers.has(logFollowKey(instanceId, 'master'))) {
      startShardLogFollow(instanceId, masterRef, 'master')
    }
  }
  const cavesRef = await resolveCavesContainerRef(instanceId)
  if (cavesRef) {
    const inspect = await runtime.inspect(cavesRef)
    if (inspect.running && !logFollowAbortControllers.has(logFollowKey(instanceId, 'caves'))) {
      startShardLogFollow(instanceId, cavesRef, 'caves')
    }
  }
}

/**
 * 镜像类错误只在 Docker 模式翻译：Native 模式没有镜像，它的报错原文里带着
 * unit 诊断（systemd 的原始抱怨、unit 文件内容），必须原样透出，不能被镜像文案顶掉。
 */
function formatShardStartError(raw: string, gameDstImage: string, runtimeMode: 'docker' | 'native'): string {
  return runtimeMode === 'docker' ? formatGameDstImageError(raw, gameDstImage) : raw
}

export async function startSingleShardContainer(
  runtime: ContainerRuntime,
  spec: ShardContainerSpec,
  gameDstImage: string,
  runtimeMode: 'docker' | 'native',
): Promise<{ ok: true, ref: ContainerRef, inspect: ContainerInspect } | { ok: false, message: string, ref?: ContainerRef }> {
  let ref: ContainerRef
  try {
    ref = await runtime.createShardContainer(spec)
  }
  catch (error) {
    const raw = error instanceof Error ? error.message : '创建分片运行时失败'
    return { ok: false, message: formatShardStartError(raw, gameDstImage, runtimeMode) }
  }
  try {
    await runtime.start(ref)
  }
  catch (error) {
    const raw = error instanceof Error ? error.message : '分片运行时启动失败'
    try { await runtime.remove(ref) }
    catch { return { ok: false, message: `${formatShardStartError(raw, gameDstImage, runtimeMode)}；分片清理未完成`, ref } }
    return { ok: false, message: formatShardStartError(raw, gameDstImage, runtimeMode) }
  }
  const inspect = await runtime.inspect(ref).catch(() => ({ ...ref, running: false, probeFailed: true } as ContainerInspect))
  if (inspect.runtimeIdentity) ref.runtimeIdentity = inspect.runtimeIdentity
  if (!inspect.running && !inspect.probeFailed) {
    const logTail = await readRecentContainerLogs(runtime, ref)
    const hint = logTail || '分片启动后立即退出，请检查安装目录与分片配置'
    try { await runtime.remove(ref) }
    catch { return { ok: false, message: `${hint}；分片清理未完成`, ref } }
    return { ok: false, message: hint }
  }
  return { ok: true, ref, inspect }
}

function describeLifecycleError(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}

/**
 * 停止并移除分片。
 *
 * 两步都是 best-effort：失败不能阻断停机流程，否则一个卡住的容器会让实例永远停在
 * 「停止中」。但失败不再完全静默 —— 停止失败意味着分片可能仍在运行，而面板会继续
 * 往下走到「已停止」，控制台必须留下痕迹，否则「界面说停了、进程其实还在跑」不会
 * 被任何人看到（日志目录不可写这类成因另由环境自检覆盖）。
 */
async function stopAndRemoveShard(
  runtime: ContainerRuntime,
  ref: ContainerRef | undefined,
  instanceId: string,
  isCancelled?: () => boolean,
) {
  if (isCancelled?.()) return false
  if (!ref) return true
  try {
    await runtime.stop(ref, 15)
  }
  catch (error) {
    if (isCancelled?.()) return false
    instanceConsoleLogStore.appendSystem(instanceId, `停止分片失败：${describeLifecycleError(error)}，面板将继续尝试清理`)
  }
  if (isCancelled?.()) return false
  try {
    await runtime.remove(ref)
    return true
  }
  catch (error) {
    if (isCancelled?.()) return false
    instanceConsoleLogStore.appendSystem(instanceId, `移除分片失败：${describeLifecycleError(error)}`)
    return false
  }
}

/** 启用中且内容已就绪的 Mod 数量：内存估算的直接输入 */
export async function countEnabledInstanceMods(instanceId: string): Promise<number> {
  try {
    const mods = await listInstanceMods(instanceId)
    return mods.filter(mod => mod.enabled && mod.installStatus === 'ready').length
  }
  catch {
    return 0
  }
}

/**
 * DB 记为已停止时，运行时探测到的快照是否真的代表「实例还在正常服务」。
 *
 * 正在被 systemd 自动拉起（`Restart=on-failure` 的 auto-restart 窗口）或已经重启过，
 * 属于崩溃循环而不是「容器还在跑」。若此时把状态翻回运行中，就会抹掉上一趟对账刚写入
 * 的崩溃告警——线上实测同一个请求里两趟对账互相覆盖，服主永远看不到「主世界已停止」。
 */
export function isHealthyRuntimeForResurrect(snapshot: ContainerInspect | null): boolean {
  if (!snapshot?.running) {
    return false
  }
  return !snapshot.restarting && (snapshot.restarts ?? 0) === 0
}

/** 主世界分片互联端口（cluster.ini [SHARD] master_port）；读不到时退回 DST 默认值 */
export function readClusterMasterPort(installPath: string): number {  try {
    const { clusterIniPath } = resolveClusterPaths(installPath)
    const { fields } = parseClusterIni(fs.readFileSync(clusterIniPath, 'utf8'))
    return Number.isInteger(fields.masterPort) && fields.masterPort > 0 && fields.masterPort <= 65535
      ? fields.masterPort
      : DEFAULT_DST_MASTER_PORT
  }
  catch {
    return DEFAULT_DST_MASTER_PORT
  }
}

/**
 * 主世界的分片互联端口是否已被占用。
 *
 * **必须是 UDP**：DST 的端口全是 UDP（仓库里既有的端口冲突探测 `findHostUdpPortConflicts`
 * 用的就是 `dgram.createSocket('udp4')`）。此处原先用 TCP `net.connect` 探测，
 * TCP 连一个只监听 UDP 的端口会被内核直接回 RST，探测永远返回 false——
 * 线上表现为「房间已经能进、控制台却一路报主世界仍在加载」，直到 900 秒超时兜底才启动洞穴。
 *
 * **但 UDP 也不是充分条件**：bind 探测的是「调用方所在网络命名空间」里的占用情况，而
 * `master_port` 是 `cluster.ini [SHARD]` 的分片互联端口，只在实例容器网络内监听、从不发布到
 * 宿主机，面板容器里探测必然为空。因此它只是把握度较高的辅助判据，主判据必须是
 * `hasMasterReadyMarker()`（见 `waitForMasterShardReady`）。
 */
export function isShardPortBound(port: number): Promise<boolean> {
  return findHostUdpPortConflicts([port]).then(conflicts => conflicts.includes(port))
}

/**
 * 等待主世界就绪的结果。
 *
 * 为什么要等：两个分片同时加载会把整机内存吃穿（见 `waitForMasterShardReady`）。
 */
export type MasterReadyOutcome =
  /** 已确认世界加载完成（就绪标记，或同命名空间下已过宽限期的端口），洞穴可以起来了 */
  | { kind: 'ready' }
  /** 等满上限仍未就绪：中止启动，保留超时原因 */
  | { kind: 'timed-out' }
  /** 用户已停止或重新启动实例，旧任务不再产生状态或日志 */
  | { kind: 'cancelled' }
  /** 主世界进程已不在（退出或被运行时放弃拉起） */
  | { kind: 'stopped', detail: string }
  /** 主世界在崩溃循环里反复重启，永远不会就绪 */
  | { kind: 'restart-loop', detail: string }
  | { kind: 'lua-error', detail: string }

export type MasterProbeVerdict = 'healthy' | 'unknown' | 'stopped' | 'restart-loop'

/**
 * 等待期间对主世界分片快照的判决。
 *
 * `unknown`（问不到运行时）必须继续等：user bus 抖动一次就判崩溃会误伤正常启动。
 * `restart-loop` 必须判失败：`Restart=on-failure` 的重启窗口里单元仍算「在运行」，
 * 只按这一条判断就会白等满上限、然后照样把洞穴拉起来占内存。
 *
 * 重启计数一律与**本次启动时的基线**比较，而不是与 0 比较：systemd 是否在显式启动时
 * 把 `NRestarts` 清零是实现细节，赌错一次就会让每次正常启动都被误判成崩溃循环而中止。
 * 基线比较只关心「我们启动它之后有没有崩过」，与清零语义无关。
 */
export function classifyMasterProbe(
  snapshot: ContainerInspect | null,
  baselineRestarts = 0,
): MasterProbeVerdict {
  if (!snapshot || snapshot.probeFailed) {
    return 'unknown'
  }
  if (!snapshot.running) {
    return 'stopped'
  }
  if (snapshot.restarting) {
    return 'restart-loop'
  }
  if ((snapshot.restarts ?? 0) > baselineRestarts) {
    return 'restart-loop'
  }
  return 'healthy'
}

/**
 * 等主世界就绪后再拉起洞穴。
 *
 * 两个分片同时加载时，各自都要把整套 Mod 与世界读一遍：2 核 4G 机器上两个峰值叠在
 * 一起会触发整机 OOM（线上实测主世界 anon-rss 已达 2.0 GiB 时被内核杀掉），而洞穴
 * 此时连不上主世界，只会反复报 `Connection to master failed`，最后两个分片都白跑。
 * 主世界先跑完，洞穴再加载时页缓存已经热了，整机峰值只剩原来的一个多一点。
 *
 * 就绪判据以 DST 自己写的 `server_log.txt` 标记为主，端口探测只作辅助——理由见函数内注释。
 *
 * 崩溃循环必须当成失败：`Restart=on-failure` 的重启窗口里单元仍算「在运行」，
 * 若只按这一条判断就会白等满上限、然后照样把洞穴拉起来占内存。
 */
export async function waitForMasterShardReady(
  app: FastifyInstance,
  instanceId: string,
  masterRef: ContainerRef,
  masterPort: number,
  installPath: string,
  waitSec = resolveShardReadyWaitSec(),
  baselineRestarts = 0,
  startedAt?: string,
  signal?: AbortSignal,
): Promise<MasterReadyOutcome> {
  if (signal?.aborted) return { kind: 'cancelled' }
  const runtime = getContainerRuntime()
  const startAt = Date.now()
  const deadline = startAt + waitSec * 1000
  let lastHeartbeat = startAt
  let portBoundAt: number | null = null
  while (Date.now() < deadline) {
    if (signal?.aborted) return { kind: 'cancelled' }
    const luaFailure = findShardLuaFailure(instanceId, installPath, startedAt)
    if (luaFailure) return { kind: 'lua-error', detail: luaFailure }
    // 首选判据：DST 自己写的世界就绪标记（`server_log.txt`，或面板采集到的同一行）。
    //
    // 它不依赖任何网络命名空间，因此容器模式与 native 模式都成立。端口探测**不能**当主判据：
    // `master_port` 是 `cluster.ini [SHARD]` 的分片互联端口，只在实例容器的网络里监听、
    // 从不发布到宿主机（发布的是 `server.ini` 的三个游戏端口），面板容器里 bind 它必然成功，
    // 于是「端口已被占用」永远为 false——只按端口判断会让 Docker 模式一路等到上限，
    // 表现为「房间能进、控制台却一直报『尚未监听到分片端口』」。
    if (hasMasterReadyMarker(instanceId, installPath, startedAt)) {
      instanceConsoleLogStore.appendSystem(
        instanceId,
        `主世界已加载完成（启动后 ${Math.round((Date.now() - startAt) / 1000)} 秒），就绪后启动洞穴分片`,
        'master',
      )
      return { kind: 'ready' }
    }
    // 端口探测降级为辅助判据：只在「面板与游戏处于同一网络命名空间」（native 同机进程）
    // 时才探测得到，用于兜住就绪标记读不到（实例目录权限异常等）的情形。
    const portBound = await isShardPortBound(masterPort)
    if (signal?.aborted) return { kind: 'cancelled' }
    if (portBound) {
      if (portBoundAt === null) {
        portBoundAt = Date.now()
        // 把「端口是什么时候起来的」写进控制台：它同时是给用户看的进度，
        // 也是判断「端口是否早于世界加载就绑定」的唯一现场证据。
        instanceConsoleLogStore.appendSystem(
          instanceId,
          `主世界已监听分片端口 ${masterPort}（启动后 ${Math.round((portBoundAt - startAt) / 1000)} 秒），就绪后启动洞穴分片`,
          'master',
        )
      }
      // 端口绑得太早说明 DST 可能在进程启动早期就占住了它，此时还不能断定世界已加载完；
      // 晚绑定（超过宽限）则说明它随世界初始化一起起来。
      const portElapsedSec = (portBoundAt - startAt) / 1000
      if (portElapsedSec > SHARD_PORT_EARLY_BIND_GRACE_SEC) {
        return { kind: 'ready' }
      }
    }
    let snapshot: ContainerInspect | null = null
    try {
      snapshot = await runtime.inspect(masterRef)
    }
    catch {
      // 问不到运行时（user bus 抖动等）：当作还活着，继续等，别误判成崩溃
      snapshot = null
    }
    if (signal?.aborted) return { kind: 'cancelled' }
    const verdict = classifyMasterProbe(snapshot, baselineRestarts)
    let reason: string | null = null
    if (verdict === 'stopped' || verdict === 'restart-loop') {
      // 配置读取失败不能掩盖已经确认的退出；退回全局配置并明确未验证。
      const instance = await getGameInstanceById(instanceId).catch(() => undefined)
      const configuredCap = (instance?.lastStartupReport?.settings ?? resolveInstanceResourceSettings(instance?.resourceConfig)).masterMemoryMb ?? undefined
      const actualCap = snapshot?.exitResult === 'oom-kill' ? await readRuntimeMemoryCapMb(runtime, masterRef) : undefined
      if (signal?.aborted) return { kind: 'cancelled' }
      reason = describeSystemdExitReason(snapshot?.exitResult, configuredCap, readHostMemorySnapshot(), actualCap)
    }
    if (verdict === 'stopped') {
      return {
        kind: 'stopped',
        detail: reason ? `主世界分片在加载途中退出：${reason}` : '主世界分片在加载途中退出',
      }
    }
    if (verdict === 'restart-loop') {
      return {
        kind: 'restart-loop',
        detail: `主世界分片反复重启（已重启 ${snapshot?.restarts ?? 0} 次）${reason ? `，最近一次退出：${reason}` : ''}`,
      }
    }
    const now = Date.now()
    if (now - lastHeartbeat >= 30_000) {
      lastHeartbeat = now
      const waited = Math.round((now - startAt) / 1000)
      instanceConsoleLogStore.appendSystem(
        instanceId,
        portBoundAt === null
          ? `主世界仍在加载（已等待 ${waited} 秒，尚未看到世界就绪标记），就绪后再启动洞穴分片`
          : `主世界已监听端口但仍在加载世界（已等待 ${waited} 秒），就绪后再启动洞穴分片`,
        'master',
      )
    }
    try {
      await sleep(2000, undefined, { signal })
    }
    catch (error) {
      if (signal?.aborted) return { kind: 'cancelled' }
      throw error
    }
  }
  if (signal?.aborted) return { kind: 'cancelled' }
  app.log.warn({ instanceId, masterPort, waitSec }, '等待主世界就绪超时')
  return { kind: 'timed-out' }
}

async function startInstanceContainerUnlocked(
  app: FastifyInstance,
  input: {
    instanceId: string
    gameCode: string
    installPath: string
    instanceName: string
    gamePort: number | null
    source?: 'manual' | 'automatic'
    target?: 'all' | 'master' | 'caves'
  },
  task: StartupTask,
  generation: number,
): Promise<
  | { ok: true, ref: ContainerRef, displayCommand: string, completion: Promise<void> }
  | { ok: false, message: string, hostMemoryPressure?: HostMemoryPressureFailure }
> {
  const stale = () => !isCurrentStartupTask(task) || !isCurrentCavesStartGeneration(input.instanceId, generation) || memoryProtectionInFlight.has(input.instanceId)
  if (stale()) return { ok: false, message: '启动已取消' }
  const previous = await getGameInstanceById(input.instanceId)
  const reuseMaster = task.snapshot.master.state === 'ready' && task.snapshot.target !== 'master'
  if (previous?.runtimeFailureKind === 'memory_protection' && input.source !== 'manual') return { ok: false, message: '实例已因内存保护停止，需要手动检查资源后启动' }
  await changeStartupPhase(task, 'prepare')
  if (isSteamcmdAppUpdateBusy()) return { ok: false, message: '当前正在安装或更新游戏文件，请完成后再启动' }
  const settings = task.snapshot.settings ?? resolveInstanceResourceSettings()
  if (input.gameCode.trim() !== DST_APP_ID) {
    return { ok: false, message: '当前仅支持饥荒（343050）实例启动' }
  }
  const { gameDstImage, instancesRoot, runtimeMode } = getServerContainerConfig()
  let containerGameRoot = input.installPath
  let hostBinds: string[] = []
  let bindMode = 'native'
  if (runtimeMode === 'docker') {
    const docker = createDockerClient()
    const bindPlan = await resolveInstanceContainerBind(docker, input.installPath, instancesRoot)
    if (bindPlan.error) {
      return { ok: false, message: bindPlan.error }
    }
    containerGameRoot = bindPlan.containerGameRoot
    hostBinds = bindPlan.hostBinds
    bindMode = bindPlan.mode
  }
  const clusterInput = {
    instanceName: input.instanceName,
    gamePort: input.gamePort,
  }
  const masterSpec = buildDstMasterShardContainerSpec({
    instanceId: input.instanceId,
    hostInstallPath: input.installPath,
    image: gameDstImage,
    containerGameRoot,
    clusterInput,
  })
  if (!masterSpec) {
    const readiness = diagnoseDstInstallReadiness(input.installPath)
    const steamcmdImageReady = runtimeMode === 'docker'
      ? await isSteamcmdImagePresent()
      : await isSteamcmdRuntimeReady()
    return {
      ok: false,
      message: buildDstStartBlockedMessage(readiness, steamcmdImageReady, {
        instanceStatus: 'error',
      }),
    }
  }
  const shardEnabled = task.snapshot.target !== 'master' && readClusterShardEnabledFromInstall(input.installPath)
  if (shardEnabled && !isCavesShardConfigured(input.installPath)) {
    ensureDstCavesShardConfig(input.installPath, input.gamePort ?? undefined)
    try {
      // 洞穴目录新建后需重写 Caves modoverrides（与启动前全量 sync 条件不同）
      await syncInstanceModFilesFromDb(input.instanceId, input.installPath)
    }
    catch (error) {
      const message = error instanceof Error ? error.message : '同步 Mod 配置失败'
      return { ok: false, message }
    }
  }
  const cavesConfigured = shardEnabled && isCavesShardConfigured(input.installPath)
  // 内存守卫放在这里而不是函数开头：只有知道「要不要起洞穴、挂了多少 Mod」，
  // 估算才对得上实际峰值。线上就是因为固定按单分片 512 MiB 放行，
  // 36 个 Mod 的双分片启动在加载途中被内核 OOM 杀掉。
  const runtimeForGuard = getContainerRuntime()
  const host = await sampleHostResources(runtimeForGuard)
  const memoryPressure = assessHostMemoryForHeavyOperation('dst-container-start', {
    shardCount: reuseMaster ? 1 : cavesConfigured ? 2 : 1,
    modCount: await countEnabledInstanceMods(input.instanceId),
    memoryCapMb: reuseMaster ? settings.cavesMemoryMb : settings.masterMemoryMb,
    measuredDemandMb: reuseMaster ? task.snapshot.planningDemand?.cavesMb : measuredStartupDemand(previous?.lastStartupReport, cavesConfigured),
  }, host ? hostGuardReading(host) : undefined)
  await recordStartupSwapAdvice(task, host, memoryPressure.requiredMb)
  if (!memoryPressure.ok) {
    return { ok: false, message: memoryPressure.detail, hostMemoryPressure: memoryPressure }
  }
  if (shardEnabled && !cavesConfigured) {
    return { ok: false, message: '已开启洞穴分片但无法准备洞穴配置，请检查安装目录权限后重试' }
  }
  const masterFields = readMasterServerIniFields(input.installPath, input.gamePort)
  const cavesFields = shardEnabled && cavesConfigured
    ? readCavesServerIniFields(input.installPath)
    : null
  const reservedPorts = await collectReservedDstPortsOnNode(LOCAL_NODE_ID, input.instanceId, {
    onlyRunning: true,
  })
  const portError = await validateShardPortsForStart(reuseMaster ? null : masterFields, cavesFields, {
    reservedPorts,
  })
  if (portError) {
    return { ok: false, message: portError }
  }
  let cavesSpec: ShardContainerSpec | undefined
  if (shardEnabled && cavesConfigured) {
    cavesSpec = buildDstCavesShardContainerSpec({
      instanceId: input.instanceId,
      hostInstallPath: input.installPath,
      image: gameDstImage,
      containerGameRoot,
      clusterInput,
    })
    if (!cavesSpec) {
      return { ok: false, message: '无法构建洞穴分片运行规格，请检查游戏文件是否完整' }
    }
  }
  if (runtimeMode === 'docker') {
    app.log.info({ gameDstImage, bindMode, hostBinds, shardEnabled }, '确保 DST 运行镜像可用')
    const imagePull = await pullGameDstImage()
    if (!imagePull.ok) {
      return { ok: false, message: imagePull.error }
    }
    masterSpec.hostBinds = hostBinds
    if (cavesSpec) {
      cavesSpec.hostBinds = hostBinds
    }
  }
  else {
    app.log.info({ runtimeMode, installPath: input.installPath, shardEnabled }, '使用 Native systemd 运行 DST')
  }
  const runtime = getContainerRuntime()
  let shardNetworkName: string | undefined
  if (readClusterShardEnabledFromInstall(input.installPath)) {
    const interconnectChanged = reuseMaster ? false : runtimeMode === 'docker'
      ? ensureDockerShardInterconnectConfig(input.installPath, input.instanceId)
      : ensureNativeShardInterconnectConfig(input.installPath)
    if (interconnectChanged) {
      instanceConsoleLogStore.appendSystem(
        input.instanceId,
        '已自动配置地上与洞穴互联地址',
      )
    }
    shardNetworkName = await runtime.ensureShardNetwork(input.instanceId)
    masterSpec.networkName = shardNetworkName
    if (cavesSpec) {
      cavesSpec.networkName = shardNetworkName
    }
  }
  if (stale()) return { ok: false, message: '启动已取消' }
  if (host?.budget.state === 'protected') {
    masterSpec.memoryParent = DST_MEMORY_SLICE
    if (cavesSpec) cavesSpec.memoryParent = DST_MEMORY_SLICE
  }
  else instanceConsoleLogStore.appendSystem(input.instanceId, host?.budget.message ?? '共享内存保护无法核验，按现有配置启动')
  masterSpec.resourceLimits = resolveDstContainerResourceLimits({ memory: (settings.masterMemoryMb ?? 0) * 1024 * 1024 }) ?? { memory: 0 }
  if (cavesSpec) cavesSpec.resourceLimits = resolveDstContainerResourceLimits({ memory: (settings.cavesMemoryMb ?? 0) * 1024 * 1024 }) ?? { memory: 0 }
  const startedAt = reuseMaster ? task.snapshot.master.startedAt ?? previous?.runtimeStartedAt ?? task.snapshot.startedAt : new Date().toISOString()
  if (!reuseMaster) task.snapshot.master.startedAt = startedAt
  await changeStartupPhase(task, reuseMaster ? 'caves_loading' : 'master_loading', new Date(Date.now() + settings.shardReadyWaitSec * 1000).toISOString())
  if (stale()) return { ok: false, message: '启动已取消' }
  const existingMaster = reuseMaster ? await runtime.findByName(masterSpec.name) : undefined
  if (reuseMaster && !existingMaster) {
    task.snapshot.master.state = 'failed'; task.snapshot.master.readyAt = null
    return { ok: false, message: '主世界已退出，请先启动主世界' }
  }
  const masterStart = existingMaster ? { ok: true as const, ref: existingMaster, inspect: await runtime.inspect(existingMaster) } : await startSingleShardContainer(runtime, masterSpec, gameDstImage, runtimeMode)
  if (stale()) {
    // 节点队列仍归旧任务所有；新任务尚未创建同名单元，安全清理迟到的创建结果。
    if (masterStart.ok && !reuseMaster) await stopAndRemoveShard(runtime, masterStart.ref, input.instanceId)
    return { ok: false, message: '启动已取消' }
  }
  if (!masterStart.ok) {
    if (masterStart.ref) task.snapshot.master.cleanupCompleted = false
    return { ok: false, message: `主世界：${masterStart.message}` }
  }
  const ref = masterStart.ref
  if (reuseMaster && !masterStart.inspect.probeFailed) {
    if (!masterStart.inspect.running || masterStart.inspect.restarting) {
      task.snapshot.master.state = 'failed'; task.snapshot.master.readyAt = null
      return { ok: false, message: '主世界已退出，请先启动主世界' }
    }
    if (task.snapshot.master.resources?.runtimeIdentity && masterStart.inspect.runtimeIdentity && task.snapshot.master.resources.runtimeIdentity !== masterStart.inspect.runtimeIdentity) return { ok: false, message: '主世界运行轮次已变化，请刷新后重试' }
  }
  delete task.snapshot.protectionStop
  if (!reuseMaster) task.snapshot.master.restartBaseline = masterStart.inspect.probeFailed ? null : masterStart.inspect.restarts ?? 0
  await persistStartupTask(task)
  const displayCommand = masterSpec.cmd.join(' ')
  await updateGameInstanceRuntime(input.instanceId, {
    status: 'running', containerId: ref.id,
    ...(!reuseMaster ? { runtimePid: null, runtimeStartedAt: startedAt, lastCommand: displayCommand, lastExitCode: null, runtimeReadyAt: null, unexpectedExitAt: null } : {}),
    lastError: null, runtimeWarning: null, runtimeFailureKind: null,
    whereStartupTaskId: task.snapshot.taskId,
  })
  app.log.info({
    instanceId: input.instanceId,
    containerId: ref.id,
    name: ref.name,
    command: displayCommand,
    shardEnabled,
  }, '实例运行时已启动')
  if (!reuseMaster) startShardLogFollow(input.instanceId, ref, 'master')
  // 不阻塞启动：等本轮的 Mod 索引完成后，把「实际加载了几个 Mod」写进控制台
  if (!reuseMaster) void reportLoadedModsAfterStart({
    instanceId: input.instanceId,
    installPath: input.installPath,
    signal: task.controller.signal,
  }).catch((error) => {
    app.log.warn({ instanceId: input.instanceId, err: error }, 'Mod 加载情况上报失败')
  })

  instanceConsoleLogStore.appendSystem(input.instanceId, reuseMaster ? '主世界已就绪，正在启动洞穴分片' : cavesSpec
    ? '主世界正在加载，就绪后再启动洞穴分片' : '主世界正在加载，等待游戏确认就绪', 'master')
  const completion = startCavesAfterMasterReady(app, {
      instanceId: input.instanceId,
      installPath: input.installPath,
      masterRef: ref,
      cavesSpec,
      generation,
      baselineRestarts: task.snapshot.master.restartBaseline ?? null,
      startedAt,
      task,
      startCaves: cavesSpec ? async () => {
        const caveHost = await sampleHostResources(runtime)
        const pressure = assessHostMemoryForHeavyOperation('dst-container-start', {
          shardCount: 1, modCount: await countEnabledInstanceMods(input.instanceId), memoryCapMb: settings.cavesMemoryMb,
          measuredDemandMb: previous?.lastStartupReport?.planningDemand?.cavesMb ?? previous?.lastStartupReport?.caves.memoryAndSwapPeakMb,
        }, caveHost ? hostGuardReading(caveHost) : undefined)
        await recordStartupSwapAdvice(task, caveHost, pressure.requiredMb)
        if (!pressure.ok) return { ok: false as const, message: pressure.detail }
        task.snapshot.caves.startedAt = new Date().toISOString()
        await persistStartupTask(task)
        if (stale()) return { ok: false as const, message: '启动已取消' }
        const result = await startSingleShardContainer(runtime, cavesSpec, gameDstImage, runtimeMode)
        if (result.ok && stale()) {
          await stopAndRemoveShard(runtime, result.ref, input.instanceId)
          return { ok: false as const, message: '启动已取消' }
        }
        if (result.ok) {
          delete task.snapshot.caves.protectionStop
          delete task.snapshot.caves.cleanupCompleted
          task.snapshot.caves.diagnosis = null
          startShardLogFollow(input.instanceId, result.ref, 'caves')
        }
        return result.ok ? { ok: true as const, ref: result.ref, baselineRestarts: result.inspect.probeFailed ? null : result.inspect.restarts ?? 0 } : { ok: false as const, message: `洞穴：${result.message}`, ref: result.ref }
      } : undefined,
    })
  return { ok: true, ref, displayCommand, completion }
}

/**
 * 每次启动/停止自增的代号，用来作废还在等待中的洞穴启动任务。
 *
 * 后台任务要等主世界就绪（可能好几分钟）才动手。这段时间里用户完全可能又点了一次
 * 停止或重新启动：旧任务若不感知，就会在实例已经被停机之后把洞穴拉起来、还会顺手挂上
 * 一个再也停不掉的日志跟随。代号变了就静默退出，并把已经起来的残留分片收掉。
 */
const cavesStartGenerations = new Map<string, number>()

export function bumpCavesStartGeneration(instanceId: string, target: 'all' | 'master' | 'caves' = 'all'): number {
  cancelStartupTask(instanceId)
  if (target !== 'caves') shardLogProbeCache.delete(`${instanceId}:master`)
  if (target !== 'master') shardLogProbeCache.delete(`${instanceId}:caves`)
  const next = (cavesStartGenerations.get(instanceId) ?? 0) + 1
  cavesStartGenerations.set(instanceId, next)
  return next
}

export function isCurrentCavesStartGeneration(instanceId: string, generation: number): boolean {
  return (cavesStartGenerations.get(instanceId) ?? 0) === generation
}

/** 独立查询使用短生命周期；启动监控复用整个任务的订阅以保留延迟回包。 */
export async function queryStartupShard(ref: ContainerRef, instanceId: string, shard: 'master' | 'caves', signal: AbortSignal, remoteShardId?: string) {
  const probe = createStartupProbeQuery({ instanceId, runtime: getContainerRuntime(), signal })
  try { return await probe.query(ref, shard, remoteShardId) }
  finally { probe.dispose() }
}

export async function startCavesAfterMasterReady(
  app: FastifyInstance,
  input: {
    instanceId: string
    installPath: string
    masterRef: ContainerRef
    cavesSpec?: ShardContainerSpec
    generation: number
    baselineRestarts: number | null
    startedAt: string
    task?: StartupTask
    cavesRef?: ContainerRef
    startCaves?: () => Promise<{ ok: true, ref: ContainerRef, baselineRestarts?: number | null } | { ok: false, message: string, ref?: ContainerRef }>
  },
): Promise<void> {
  const stale = () => !isCurrentCavesStartGeneration(input.instanceId, input.generation)
  if (stale()) return
  const task = input.task ?? createStartupTask(input.instanceId)
  task.snapshot.settings ??= resolveInstanceResourceSettings()
  if (!input.task) await persistStartupTask(task)
  const runtime = getContainerRuntime()
  const probe = createStartupProbeQuery({ instanceId: input.instanceId, runtime, signal: task.controller.signal })
  try {
    await runStartupMonitor(app, {
      ...input, task, runtime, waitSec: task.snapshot.settings.shardReadyWaitSec,
      isCancelled: stale,
      luaFailure: shard => findShardLuaFailure(input.instanceId, input.installPath, task.snapshot[shard ?? 'master'].startedAt ?? input.startedAt, shard),
      probe: probe.query,
      protectOom: async shard => {
        const current = await getGameInstanceById(input.instanceId)
        if (!current || stale() || !isCurrentStartupTask(task)) return
        await protectInstanceMemory(app, current, { code: 'oom', message: shard === 'caves' ? '洞穴发生 OOM，已保护停止洞穴，主世界继续运行' : '主世界发生 OOM，已保护停止实例', master: task.snapshot.master.resources ?? null, caves: task.snapshot.caves.resources ?? null, host: await sampleHostResources(runtime) }, shard === 'caves' ? undefined : [input.masterRef], shard === 'caves' ? 'caves' : 'instance')
      },
      cleanup: async (refs) => {
        if (stale() || !isCurrentStartupTask(task)) return
        for (const ref of refs) {
          const shard = ref.id === input.masterRef.id ? 'master' : 'caves'
          ref.runtimeIdentity ??= task.snapshot[shard].resources?.runtimeIdentity ?? undefined
          stopShardLogFollow(input.instanceId, shard)
        }
        const removed = await Promise.all(refs.map(ref => stopAndRemoveShard(runtime, ref, input.instanceId, () => stale() || !isCurrentStartupTask(task))))
        if (removed.some(value => !value)) throw new Error('分片清理未完成，保留上一轮引用')
        if (stale() || !isCurrentStartupTask(task)) return
        const remaining = await Promise.all([runtime.findByName(buildMasterContainerName(input.instanceId)), runtime.findByName(buildCavesContainerName(input.instanceId))])
        if (stale() || !isCurrentStartupTask(task)) return
        if (remaining.every(ref => !ref)) await runtime.removeShardNetwork(input.instanceId)
        for (const ref of refs) shardLogProbeCache.delete(input.instanceId + (ref.id === input.masterRef.id ? ':master' : ':caves'))
      },
    })
  }
  finally { probe.dispose() }
}

/** 运行期洞穴失败只清理该分片；保护锁与失败现场留在同一份报告中。 */
export async function stopFailedCaves(app: FastifyInstance, observed: DbGameInstance, code: string, message: string): Promise<void> {
  if (memoryProtectionInFlight.has(observed.id) || startupIsActive(currentStartupTask(observed.id)?.snapshot)) return
  const current = await getGameInstanceById(observed.id)
  if (!current || runtimeMemoryEpoch(current) !== runtimeMemoryEpoch(observed)) return
  if (current.lastStartupReport?.caves.state === 'failed') return
  const runtime = getContainerRuntime()
  const ref = await resolveCavesContainerRef(current.id)
  const inspect = ref ? await runtime.inspect(ref) : null
  if (inspect?.probeFailed) return
  if (ref && inspect?.runtimeIdentity) ref.runtimeIdentity = inspect.runtimeIdentity
  const at = new Date().toISOString()
  const report = structuredClone(current.lastStartupReport ?? {
    taskId: randomUUID(), status: 'success', phase: 'ready', startedAt: current.runtimeStartedAt ?? at,
    updatedAt: at, phaseStartedAt: at, phaseDeadlineAt: null, elapsedSeconds: 0, remainingSeconds: null,
    master: { state: 'ready', memoryPeakMb: null }, caves: { state: 'ready', memoryPeakMb: null }, diagnosis: null,
  } satisfies InstanceStartupSnapshot)
  const recordedIdentity = report.caves.resources?.runtimeIdentity
  if (recordedIdentity && inspect?.runtimeIdentity && recordedIdentity !== inspect.runtimeIdentity) return
  if (isInstanceStopping(current.id)) return
  const generation = bumpCavesStartGeneration(current.id, 'caves')
  const stale = () => !isCurrentCavesStartGeneration(current.id, generation)
  report.caves.state = 'failed'
  report.caves.cleanupCompleted = false
  report.caves.diagnosis = { code, message }
  report.status = 'failed'; report.phase = 'failed'; report.updatedAt = at; report.phaseDeadlineAt = null
  report.diagnosis = { code, message }
  const cached = currentStartupTask(current.id)
  if (cached) { cached.snapshot = report; await persistStartupTask(cached) }
  if (stale()) return
  await updateGameInstanceRuntime(current.id, { lastStartupReport: report, runtimeWarning: message, lastError: message, lastErrorPhase: 'runtime' })
  stopShardLogFollow(current.id, 'caves')
  const removed = await stopAndRemoveShard(runtime, ref, current.id, stale)
  if (stale()) return
  report.caves.cleanupCompleted = removed
  if (cached) { cached.snapshot = report; await persistStartupTask(cached) }
  await updateGameInstanceRuntime(current.id, { lastStartupReport: report, lastError: removed ? message : `${message}；洞穴清理未完成，请先停止实例` })
  instanceConsoleLogStore.appendSystem(current.id, `${message}；主世界继续运行${removed ? '' : '；洞穴清理未完成'}`)
  emitPanelEvent({ type: 'instance_exited_unexpectedly', subjectId: current.id, subjectName: current.name, severity: 'critical', at,
    occurrenceKey: `caves:${report.taskId}:${recordedIdentity ?? ref?.id ?? ''}`, message: `实例「${current.name}」${message}；主世界继续运行` })
  app.log.warn({ instanceId: current.id, code, removed }, '洞穴失败已隔离')
}

export async function stopInstanceContainer(instanceId: string, expectedEpoch?: string): Promise<boolean> {
  if (memoryProtectionInFlight.has(instanceId)) throw new Error('内存保护正在清理分片，请稍后再试')
  if (expectedEpoch !== undefined) {
    const observed = await getGameInstanceById(instanceId)
    if (isInstanceStopping(instanceId) || !observed || runtimeMemoryEpoch(observed) !== expectedEpoch || startupIsActive(getStartupSnapshot(instanceId, observed.lastStartupReport))) return false
  }
  // 先作废还在等待中的洞穴启动任务：否则它会在实例停机之后把洞穴拉起来
  const generation = bumpCavesStartGeneration(instanceId)
  // 正常停服的分片消失不能被后台对账当成异常退出，再次递增代号会作废本次停服。
  stoppingInstances.set(instanceId, generation)
  const stale = () => !isCurrentCavesStartGeneration(instanceId, generation)
  stopLogFollow(instanceId)
  try {
    const instance = await getGameInstanceById(instanceId)
    if (stale()) return false
    if (startupIsActive(instance?.lastStartupReport) && !currentStartupTask(instanceId)) {
      const task = createStartupTask(instanceId, instance!.lastStartupReport!)
      cancelStartupTask(instanceId)
      await persistStartupTask(task)
      if (stale()) return false
    }
    const runtime = getContainerRuntime()
    const cavesRef = await resolveCavesContainerRef(instanceId)
    if (stale()) return false
    const masterRef = await resolveInstanceContainerRef(instanceId)
    if (stale()) return false
    const cleanupReport = currentStartupTask(instanceId)?.snapshot ?? instance?.lastStartupReport
    if (cleanupReport && (masterRef || cavesRef)) {
      cleanupReport.master.cleanupCompleted = false
      cleanupReport.caves.cleanupCompleted = false
      if (cleanupReport.protectionStop) cleanupReport.protectionStop.cleanupCompleted = false
      if (cleanupReport.caves.protectionStop) cleanupReport.caves.protectionStop.cleanupCompleted = false
      const cached = currentStartupTask(instanceId)
      if (cached) { cached.snapshot = cleanupReport; await persistStartupTask(cached) }
      if (stale()) return false
      await updateGameInstanceRuntime(instanceId, { lastStartupReport: cleanupReport })
      if (stale()) return false
    }
    if (cavesRef) {
      instanceConsoleLogStore.appendSystem(instanceId, '正在停止并移除洞穴分片以释放内存')
    }
    const cavesRemoved = await stopAndRemoveShard(runtime, cavesRef, instanceId, stale)
    if (stale()) return false
    if (masterRef) {
      instanceConsoleLogStore.appendSystem(instanceId, '正在停止并移除主世界分片以释放内存')
    }
    const masterRemoved = await stopAndRemoveShard(runtime, masterRef, instanceId, stale)
    if (stale()) return false
    if (!cavesRemoved || !masterRemoved) throw new Error('分片清理未完成，请检查运行时连接与控制台后再次停止')
    if (masterRef) {
      instanceConsoleLogStore.appendSystem(instanceId, '实例运行时已删除')
    }
    await runtime.removeShardNetwork(instanceId)
    if (stale()) return false
    const report = currentStartupTask(instanceId)?.snapshot ?? instance?.lastStartupReport
    if (report) {
      report.master.state = 'stopped'; report.master.readyAt = null; report.master.cleanupCompleted = true
      report.caves.state = report.caves.protectionStop ? 'failed' : report.caves.state === 'disabled' ? 'disabled' : 'stopped'
      report.caves.readyAt = null; report.caves.cleanupCompleted = true
      if (report.caves.protectionStop) report.caves.protectionStop.cleanupCompleted = true
      if (report.protectionStop) report.protectionStop.cleanupCompleted = true
      const task = currentStartupTask(instanceId)
      if (task) { task.snapshot = report; await persistStartupTask(task); releaseStartupTask(task) }
      if (stale()) return false
      await updateGameInstanceRuntime(instanceId, { lastStartupReport: report, runtimeReadyAt: null })
    }
    if (!masterRef && !cavesRef) {
      if (instance?.status === 'running' || instance?.runtimeFailureKind === 'memory_protection') {
        await updateGameInstanceRuntime(instanceId, {
          status: 'stopped',
          containerId: null,
          runtimePid: null,
          runtimeStartedAt: null,
          lastError: instance.runtimeFailureKind === 'memory_protection' ? instance.lastError : null,
        })
      }
      return true
    }
    await updateGameInstanceRuntime(instanceId, {
      status: 'stopped',
      containerId: null,
      runtimePid: null,
      runtimeStartedAt: null,
      lastError: instance?.runtimeFailureKind === 'memory_protection' ? instance.lastError : null,
    })
  }
  catch (error) { if (!stale()) throw error; return false }
  finally { if (stoppingInstances.get(instanceId) === generation) stoppingInstances.delete(instanceId) }
  return !stale()
}

export async function removeInstanceContainer(instanceId: string): Promise<void> {
  // 删除实例同样要作废等待中的洞穴启动任务，否则它会把分片又拉回来
  bumpCavesStartGeneration(instanceId)
  stopLogFollow(instanceId)
  const runtime = getContainerRuntime()
  const cavesRef = await resolveCavesContainerRef(instanceId)
  await stopAndRemoveShard(runtime, cavesRef, instanceId)
  const masterRef = await resolveInstanceContainerRef(instanceId)
  if (masterRef) {
    await stopAndRemoveShard(runtime, masterRef, instanceId)
    instanceConsoleLogStore.appendSystem(instanceId, '实例运行时已删除')
  }
  await runtime.removeShardNetwork(instanceId)
}

export async function isCavesContainerRunning(instanceId: string): Promise<boolean> {
  const ref = await resolveCavesContainerRef(instanceId)
  if (!ref) {
    return false
  }
  const runtime = getContainerRuntime()
  const inspect = await runtime.inspect(ref)
  return inspect.running
}

async function resolveConsoleCommandContainerRef(
  instanceId: string,
  shard: ConsoleCommandShard,
): Promise<ContainerRef | undefined> {
  if (shard === 'caves') {
    return resolveCavesContainerRef(instanceId)
  }
  return resolveInstanceContainerRef(instanceId)
}

export async function sendInstanceContainerCommand(
  instanceId: string,
  command: string,
  shard: ConsoleCommandShard = 'master',
  options?: { silent?: boolean },
): Promise<{ ok: boolean, message?: string }> {
  const trimmed = command.trim()
  if (!trimmed) {
    return { ok: false, message: '命令不能为空' }
  }
  const ref = await resolveConsoleCommandContainerRef(instanceId, shard)
  if (!ref) {
    return {
      ok: false,
      message: shard === 'caves' ? '洞穴分片未运行，无法发送命令' : '实例未运行，无法发送命令',
    }
  }
  const runtime = getContainerRuntime()
  const inspect = await runtime.inspect(ref)
  if (!inspect.running) {
    return {
      ok: false,
      message: shard === 'caves' ? '洞穴分片未运行，无法发送命令' : '实例未运行，无法发送命令',
    }
  }
  const result = await runtime.execStdin(ref, trimmed)
  // 面板自己的周期查询（在线人数 / 名单）传 silent：它们不是用户发的命令，
  // 不该每隔几十秒就往控制台里塞一条命令行回显
  if (!options?.silent) {
    instanceConsoleLogStore.appendSystem(instanceId, `> ${trimmed}`, shard)
  }
  if (result.exitCode !== 0) {
    return { ok: false, message: result.output || '命令发送失败' }
  }
  return { ok: true }
}

async function failPreparation(app: FastifyInstance, task: StartupTask, message: string, code = 'prepare_failed') {
  if (!isCurrentStartupTask(task)) return
  task.snapshot.diagnosis = { code, message: code === 'host_memory_pressure' ? '宿主机内存与 swap 余量不足，请按通知中的服务器命令处理后重试' : code === 'probe_unavailable' ? '运行时查询通道不可用，已超过原启动期限' : '启动准备失败，请检查服务器配置与控制台' }
  const keepMaster = task.snapshot.master.state === 'ready'
  task.snapshot[keepMaster ? 'caves' : 'master'].state = 'failed'
  task.snapshot[keepMaster ? 'caves' : 'master'].diagnosis = { code, message }
  task.snapshot[keepMaster ? 'caves' : 'master'].cleanupCompleted ??= true
  await changeStartupPhase(task, 'failed')
  if (!isCurrentStartupTask(task)) return
  instanceConsoleLogStore.appendSystem(task.instanceId, `启动失败：${message}`)
  await updateGameInstanceRuntime(task.instanceId, {
    status: keepMaster ? 'running' : 'error', lastError: message, lastErrorPhase: 'runtime', runtimeWarning: message,
    ...(!keepMaster ? { runtimeReadyAt: null } : {}),
    whereStartupTaskId: task.snapshot.taskId,
  })
  app.log.warn({ instanceId: task.instanceId }, message)
}

async function queuePreparedStartRun(app: FastifyInstance, input: Parameters<typeof startInstanceContainerUnlocked>[1], task: StartupTask, generation: number) {
  try {
    const result = await withInstanceContentActivity(input.instanceId, () => startInstanceContainerUnlocked(app, input, task, generation))
    if (result.ok) await result.completion
    else await failPreparation(app, task, result.message, result.hostMemoryPressure ? 'host_memory_pressure' : 'prepare_failed')
  }
  catch (error) { await failPreparation(app, task, error instanceof Error ? error.message : '启动准备失败') }
}

function queuePreparedStart(app: FastifyInstance, input: Parameters<typeof startInstanceContainerUnlocked>[1], task: StartupTask, generation: number) {
  enqueueStartupTask(task, () => queuePreparedStartRun(app, input, task, generation))
}

const acceptingStarts = new Set<string>()
export async function startInstanceContainer(app: FastifyInstance, input: Parameters<typeof startInstanceContainerUnlocked>[1]) {
  if (acceptingStarts.has(input.instanceId)) return { ok: false as const, message: '该实例正在受理启动请求' }
  acceptingStarts.add(input.instanceId)
  try { return await acceptInstanceStart(app, input) }
  finally { acceptingStarts.delete(input.instanceId) }
}

async function acceptInstanceStart(app: FastifyInstance, input: Parameters<typeof startInstanceContainerUnlocked>[1]): Promise<
  { ok: true, accepted: true, taskId: string } | { ok: false, message: string }
> {
  const observedGeneration = cavesStartGenerations.get(input.instanceId) ?? 0
  const cancelled = () => !isCurrentCavesStartGeneration(input.instanceId, observedGeneration) || memoryProtectionInFlight.has(input.instanceId)
  if (startupIsActive(currentStartupTask(input.instanceId)?.snapshot)) return { ok: false, message: '该实例正在排队或启动中' }
  const instance = await getGameInstanceById(input.instanceId)
  if (!instance) return { ok: false, message: '实例不存在' }
  if (memoryProtectionInFlight.has(input.instanceId)) return { ok: false, message: '内存保护正在清理分片，请稍后再试' }
  if (instance.runtimeFailureKind === 'memory_protection') {
    if (input.source !== 'manual') return { ok: false, message: '内存保护停止后只能由用户手动启动或重启' }
    if (!instance.lastStartupReport?.protectionStop?.cleanupCompleted) return { ok: false, message: '保护停机清理未完成，请先再次停止实例' }
  }
  const target = input.target ?? 'all'
  const runtime = getContainerRuntime()
  const masterRef = await runtime.findByName(buildMasterContainerName(instance.id))
  const masterInspect = masterRef ? await runtime.inspect(masterRef) : null
  if (masterInspect?.probeFailed) return { ok: false, message: '无法确认主世界状态，请稍后重试' }
  const reuseMaster = masterInspect?.running === true && !masterInspect.restarting
  if (target === 'master' && reuseMaster) return { ok: false, message: '主世界已经运行' }
  if (target === 'caves' && !reuseMaster) return { ok: false, message: '请先启动主世界并等待就绪' }
  if (target !== 'master' && !readClusterShardEnabledFromInstall(input.installPath) && target === 'caves') return { ok: false, message: '尚未开启洞穴' }
  const caveLock = instance.lastStartupReport?.caves.protectionStop
  if (instance.lastStartupReport?.master.cleanupCompleted === false) return { ok: false, message: '主世界清理未完成，请先停止实例完成清理' }
  if (target !== 'master' && caveLock && input.source !== 'manual') return { ok: false, message: '洞穴已内存保护停止，只能手动启动或重启' }
  if (target !== 'master' && (caveLock?.cleanupCompleted === false || instance.lastStartupReport?.caves.cleanupCompleted === false)) return { ok: false, message: '洞穴清理未完成，请先停止实例完成清理' }
  if (reuseMaster) {
    await ensureInstanceContainerLogFollow(instance.id)
    const probe = await queryStartupShard(masterRef!, instance.id, 'master', new AbortController().signal)
    if (!probe?.worldReady) return { ok: false, message: '尚未确认主世界就绪，请稍后重试' }
    const cavesRef = await runtime.findByName(buildCavesContainerName(instance.id))
    if (cavesRef) {
      const caveInspect = await runtime.inspect(cavesRef)
      if (caveInspect.probeFailed || caveInspect.running) return { ok: false, message: '洞穴已经运行或状态暂时未知，请刷新后重试' }
      if (cancelled()) return { ok: false, message: '启动请求已取消' }
      if (caveInspect.runtimeIdentity) cavesRef.runtimeIdentity = caveInspect.runtimeIdentity
      if (!await stopAndRemoveShard(runtime, cavesRef, instance.id, cancelled)) return { ok: false, message: '洞穴清理未完成，请先停止实例完成清理' }
    }
  }
  if (instance.runtimeFailureKind === 'memory_protection') {
    if (input.source !== 'manual') return { ok: false, message: '内存保护停止后只能由用户手动启动或重启' }
    if (!instance.lastStartupReport?.protectionStop?.cleanupCompleted) return { ok: false, message: '保护停机清理未完成，请先再次停止实例' }
    const runtime = getContainerRuntime()
    for (const name of [buildMasterContainerName(instance.id), buildCavesContainerName(instance.id)]) {
      const ref = await runtime.findByName(name)
      if (ref) return { ok: false, message: '仍存在上一轮分片，请先停止并完成清理' }
    }
  }
  if (cancelled()) return { ok: false, message: '启动请求已取消' }
  const generation = bumpCavesStartGeneration(input.instanceId, reuseMaster ? 'caves' : target)
  const task = createStartupTask(input.instanceId)
  task.snapshot.target = target
  if (reuseMaster) {
    task.snapshot.master = structuredClone(instance.lastStartupReport?.master ?? { state: 'ready', memoryPeakMb: null })
    task.snapshot.master.state = 'ready'
    task.snapshot.master.startedAt ??= masterInspect?.startedAt ?? instance.runtimeStartedAt ?? task.snapshot.startedAt
    task.snapshot.master.readyAt ??= instance.runtimeReadyAt
    task.snapshot.master.restartBaseline ??= masterInspect?.restarts ?? null
    const resources = await sampleRuntimeResources(runtime, masterRef!)
    if (resources) {
      if (task.snapshot.master.resources?.runtimeIdentity && resources.runtimeIdentity && task.snapshot.master.resources.runtimeIdentity !== resources.runtimeIdentity) {
        task.snapshot.master = { state: 'ready', startedAt: masterInspect?.startedAt ?? task.snapshot.startedAt,
          readyAt: instance.runtimeReadyAt, memoryPeakMb: null, restartBaseline: masterInspect?.restarts ?? null }
      }
      task.snapshot.master.resources = resources
      if (resources.runtimeIdentity !== task.snapshot.master.resourceBaseline?.runtimeIdentity) task.snapshot.master.resourceBaseline = resources
    }
  }
  task.snapshot.planningDemand = {
    masterMb: instance.lastStartupReport?.master.memoryAndSwapPeakMb ?? instance.lastStartupReport?.planningDemand?.masterMb ?? null,
    cavesMb: instance.lastStartupReport?.caves.memoryAndSwapPeakMb ?? instance.lastStartupReport?.planningDemand?.cavesMb ?? null,
  }
  if (instance.runtimeFailureKind === 'memory_protection' && instance.lastStartupReport?.protectionStop) task.snapshot.protectionStop = structuredClone(instance.lastStartupReport.protectionStop)
  task.snapshot.settings = resolveInstanceResourceSettings(instance.resourceConfig)
  task.snapshot.caves.state = readClusterShardEnabledFromInstall(input.installPath) ? target === 'master' ? 'stopped' : 'pending' : 'disabled'
  if (target === 'master' && instance.lastStartupReport?.caves) task.snapshot.caves = structuredClone(instance.lastStartupReport.caves)
  if (caveLock) task.snapshot.caves.protectionStop = structuredClone(caveLock)
  await persistStartupTask(task)
  queuePreparedStart(app, input, task, generation)
  return { ok: true, accepted: true, taskId: task.snapshot.taskId }
}

/** 面板重启后继续原来的等待预算，不能再给卡住的世界一个完整的新超时。 */
export async function recoverInstanceStartup(app: FastifyInstance, instance: NonNullable<Awaited<ReturnType<typeof getGameInstanceById>>>) {
  if (instance.runtimeFailureKind === 'memory_protection' || (instance.lastStartupReport?.caves.protectionStop && instance.lastStartupReport.target !== 'master')) return
  const report = instance.lastStartupReport
  if (currentStartupTask(instance.id) || !startupIsActive(report) || !report || !instance.installPath || instance.nodeId !== LOCAL_NODE_ID) return
  const generation = bumpCavesStartGeneration(instance.id, report.target ?? 'all')
  const task = createStartupTask(instance.id, report)
  task.snapshot.settings ??= resolveInstanceResourceSettings(instance.resourceConfig)
  if (report.phase === 'queued') {
    queuePreparedStart(app, { instanceId: instance.id, gameCode: instance.gameCode, installPath: instance.installPath, instanceName: instance.name, gamePort: instance.gamePort }, task, generation)
    return
  }
  enqueueStartupTask(task, async () => {
    try {
      const runtime = getContainerRuntime()
      const masterRef = await runtime.findByName(buildMasterContainerName(instance.id))
      if (!isCurrentStartupTask(task)) return
      if (report.phase === 'prepare' && !masterRef) {
        // 还未创建主世界的准备任务可以重做；已存在的单元交给监控，不删同名单元。
        await queuePreparedStartRun(app, { instanceId: instance.id, gameCode: instance.gameCode, installPath: instance.installPath!, instanceName: instance.name, gamePort: instance.gamePort }, task, generation)
        return
      }
      if (!masterRef) { task.snapshot.master.state = 'failed'; await failPreparation(app, task, '原启动任务的主世界已不存在'); return }
      if (report.phase === 'prepare') {
        task.snapshot.phase = 'master_loading'
        task.snapshot.phaseStartedAt = instance.runtimeStartedAt ?? report.phaseStartedAt
        task.snapshot.phaseDeadlineAt = new Date(Date.parse(task.snapshot.phaseStartedAt) + task.snapshot.settings!.shardReadyWaitSec * 1000).toISOString()
        await persistStartupTask(task)
      }
      await ensureInstanceContainerLogFollow(instance.id)
      const wantsCaves = report.target !== 'master' && report.caves.state !== 'disabled' && report.caves.state !== 'stopped'
      const cavesRef = wantsCaves ? await runtime.findByName(buildCavesContainerName(instance.id)) : undefined
      const { runtimeMode, gameDstImage, instancesRoot } = getServerContainerConfig()
      const installPath = instance.installPath!
      let containerGameRoot = installPath
      let hostBinds: string[] | undefined
      const needsCavesStart = wantsCaves && !cavesRef
      if (runtimeMode === 'docker' && needsCavesStart) {
        const plan = await resolveInstanceContainerBind(createDockerClient(), installPath, instancesRoot)
        if (plan.error) throw new Error(plan.error)
        containerGameRoot = plan.containerGameRoot
        hostBinds = plan.hostBinds
      }
      const cavesSpec = needsCavesStart ? buildDstCavesShardContainerSpec({
        instanceId: instance.id, hostInstallPath: installPath, containerGameRoot, image: gameDstImage,
        clusterInput: { instanceName: instance.name, gamePort: instance.gamePort },
      }) : undefined
      if (needsCavesStart && !cavesSpec) throw new Error('无法恢复洞穴启动配置')
      if (cavesSpec) {
        cavesSpec.hostBinds = hostBinds
        cavesSpec.networkName = await runtime.ensureShardNetwork(instance.id)
        cavesSpec.resourceLimits = resolveDstContainerResourceLimits({ memory: (task.snapshot.settings!.cavesMemoryMb ?? 0) * 1024 * 1024 }) ?? { memory: 0 }
        const host = await sampleHostResources(runtime)
        const master = await runtime.resourceSnapshot?.(masterRef)
        if (host?.budget.state === 'protected' && master?.memoryParent === DST_MEMORY_SLICE) cavesSpec.memoryParent = DST_MEMORY_SLICE
      }
      await startCavesAfterMasterReady(app, {
        instanceId: instance.id, installPath, masterRef, generation, task, cavesRef,
        baselineRestarts: report.master.restartBaseline ?? report.master.resources?.restarts ?? null,
        startedAt: instance.runtimeStartedAt ?? report.startedAt,
        startCaves: cavesSpec ? async () => {
          const host = await sampleHostResources(runtime)
          const pressure = assessHostMemoryForHeavyOperation('dst-container-start', { shardCount: 1, modCount: await countEnabledInstanceMods(instance.id), memoryCapMb: task.snapshot.settings!.cavesMemoryMb, measuredDemandMb: report.planningDemand?.cavesMb }, host ? hostGuardReading(host) : undefined)
          await recordStartupSwapAdvice(task, host, pressure.requiredMb)
          if (!pressure.ok) return { ok: false as const, message: pressure.detail }
          if (!isCurrentStartupTask(task)) return { ok: false as const, message: '启动已取消' }
          task.snapshot.caves.startedAt = new Date().toISOString()
          await persistStartupTask(task)
          const result = await startSingleShardContainer(runtime, cavesSpec, gameDstImage, runtimeMode)
          if (result.ok && !isCurrentStartupTask(task)) {
            await stopAndRemoveShard(runtime, result.ref, instance.id)
            return { ok: false as const, message: '启动已取消' }
          }
          if (result.ok) startShardLogFollow(instance.id, result.ref, 'caves')
          return result.ok ? { ok: true as const, ref: result.ref, baselineRestarts: result.inspect.probeFailed ? null : result.inspect.restarts ?? 0 } : { ok: false as const, message: result.message, ref: result.ref }
        } : undefined,
      })
    }
    catch (error) {
      // 运行时不可达不代表分片已停；保留原报告，稍后重新恢复查询。
      app.log.warn({ instanceId: instance.id, err: error }, '恢复启动任务时探测不可用')
      if (isCurrentStartupTask(task)) {
        task.snapshot.diagnosis = { code: 'probe_unavailable', message: '运行时探测暂不可用，保留原启动期限' }
        await persistStartupTask(task)
        if (task.snapshot.phaseDeadlineAt && Date.now() >= Date.parse(task.snapshot.phaseDeadlineAt)) {
          await failPreparation(app, task, '已超过原启动期限，运行时查询通道不可用；请恢复运行时后停止或重启实例', 'probe_unavailable')
        }
        else releaseStartupTask(task)
      }
    }
  })
}
