import { EventEmitter } from 'node:events'
import { deserialize } from 'node:v8'
import type { Worker } from 'node:worker_threads'
import { app, utilityProcess, type UtilityProcess } from 'electron'
import type { CrashReportBreadcrumbData } from '../../shared/crash-reporting/crash-reporting'
import { recordDurableCrashBreadcrumb } from '../crash-reporting/breadcrumbs/durable-crash-breadcrumb'
import type {
  UtilityWorkerFailure,
  UtilityWorkerHostEvent,
  UtilityWorkerHostRequest,
  UtilityWorkerSpawn
} from './utility-worker-host-protocol'

/** A host that cannot start in time loses its workers to in-process threads for the session. */
const HOST_READY_TIMEOUT_MS = 4_000
const HOST_IDLE_EXIT_MS = 30_000

/** Electron stops utility processes as it quits; those exits are not failures. */
let quitting = false

export type HostedWorkerThread = Pick<
  Worker,
  'postMessage' | 'ref' | 'unref' | 'terminate' | 'on' | 'once'
>

type Host = {
  child: UtilityProcess
  ready: boolean
  queued: UtilityWorkerHostRequest[]
  workers: Map<number, HostedWorker>
  readyTimer: ReturnType<typeof setTimeout>
  idleTimer?: ReturnType<typeof setTimeout>
  retired: boolean
}

function restoreFailure(failure: UtilityWorkerFailure): Error {
  const error: NodeJS.ErrnoException = new Error(failure.message)
  error.name = failure.name
  if (failure.code) {
    error.code = failure.code
  }
  return error
}

/** A child still launching has no PID to kill yet; kill it once it has one. */
function stopChild(host: Host): void {
  if (!host.child.kill()) {
    host.child.once('spawn', () => host.child.kill())
  }
}

/** Main-side stand-in for a worker thread that runs inside the utility process. */
class HostedWorker extends EventEmitter {
  private local?: Worker
  private chunks: Uint8Array[] = []
  private exitCode?: number
  private readonly exited = Promise.withResolvers<number>()
  /** Posts a worker hasn't answered yet, replayed if it moves in-process. */
  private unheardPosts: unknown[] = []
  /** A worker that has said nothing yet can still move in-process unnoticed. */
  heard = false
  terminating = false
  lastReplyType: string | null = null
  receivedBytes = 0

  constructor(
    readonly id: number,
    readonly spawn: UtilityWorkerSpawn,
    private readonly send: (request: UtilityWorkerHostRequest) => void
  ) {
    super()
  }

  postMessage(message: unknown): void {
    if (this.local) {
      this.local.postMessage(message)
    } else if (this.exitCode === undefined) {
      if (!this.heard) {
        this.unheardPosts.push(message)
      }
      this.send({ type: 'post', id: this.id, message })
    }
  }

  ref(): void {
    this.local?.ref()
  }

  unref(): void {
    this.local?.unref()
  }

  terminate(): Promise<number> {
    this.terminating = true
    if (this.local) {
      return this.local.terminate()
    }
    if (this.exitCode === undefined) {
      this.send({ type: 'terminate', id: this.id })
    }
    return this.exited.promise
  }

  adopt(local: Worker): void {
    this.local = local
    local.on('message', (message: unknown) => this.emit('message', message))
    local.on('error', (error) => this.emit('error', error))
    local.once('exit', (code) => this.finish(code))
    for (const message of this.unheardPosts.splice(0)) {
      local.postMessage(message)
    }
  }

  receive(event: Exclude<UtilityWorkerHostEvent, { type: 'ready' }>): void {
    this.heard = true
    this.unheardPosts = []
    if (event.type === 'error') {
      this.emit('error', restoreFailure(event.error))
    } else if (event.type === 'exit') {
      this.finish(event.code)
    } else {
      this.chunks.push(event.bytes)
      this.receivedBytes += event.bytes.byteLength
      if (event.more) {
        return
      }
      const bytes = this.chunks.length === 1 ? this.chunks[0]! : Buffer.concat(this.chunks)
      this.chunks = []
      let message: unknown
      try {
        message = deserialize(bytes)
      } catch (error) {
        this.emit('error', error)
        return
      }
      this.lastReplyType =
        message && typeof message === 'object' && 'type' in message && typeof message.type === 'string'
          ? message.type
          : null
      this.emit('message', message)
    }
  }

  finish(code: number): void {
    if (this.exitCode !== undefined) {
      return
    }
    this.exitCode = code
    this.chunks = []
    this.emit('exit', code)
    this.exited.resolve(code)
  }
}

/**
 * Runs worker threads in one Electron utility process. Electron main and its worker threads share
 * one V8 pointer cage (~3.25 GB), so a worker's heap there can push main into a fatal OOM; here it
 * has its own, and a host OOM only fails the workers it holds.
 */
export class UtilityWorkerHost {
  private host?: Host
  private unavailable = false
  private nextId = 0

  constructor(
    private readonly serviceName: string,
    private readonly entryPath: () => string,
    private readonly spawnInProcess: (spawn: UtilityWorkerSpawn) => Worker
  ) {
    app.once('will-quit', () => {
      quitting = true
    })
  }

  spawn(spawn: UtilityWorkerSpawn): HostedWorkerThread {
    if (this.unavailable) {
      return this.spawnInProcess(spawn)
    }
    let host: Host
    try {
      host = this.liveHost()
    } catch {
      this.unavailable = true
      return this.spawnInProcess(spawn)
    }
    const id = ++this.nextId
    const worker = new HostedWorker(id, spawn, (request) => this.send(host, request))
    host.workers.set(id, worker)
    clearTimeout(host.idleTimer)
    host.idleTimer = undefined
    this.send(host, { type: 'spawn', id, ...spawn })
    return worker
  }

  private liveHost(): Host {
    if (this.host && !this.host.retired) {
      return this.host
    }
    const child = utilityProcess.fork(this.entryPath(), [], {
      serviceName: this.serviceName,
      stdio: ['ignore', 'ignore', 'pipe']
    })
    const host: Host = {
      child,
      ready: false,
      queued: [],
      workers: new Map(),
      retired: false,
      readyTimer: setTimeout(() => this.giveUp(host, 'ready-timeout'), HOST_READY_TIMEOUT_MS)
    }
    this.host = host
    child.stderr?.on('data', (chunk: Buffer) => {
      console.error(`[${this.serviceName}]`, String(chunk).trimEnd())
    })
    child.on('message', (event: UtilityWorkerHostEvent) => this.receive(host, event))
    // A V8 fatal error is always followed by exit, which reports it; unheard, it would throw in main.
    child.on('error', () => undefined)
    child.on('exit', (code) => this.exited(host, code))
    return host
  }

  private send(host: Host, request: UtilityWorkerHostRequest): void {
    if (host.retired) {
      return
    }
    if (host.ready) {
      host.child.postMessage(request)
    } else {
      host.queued.push(request)
    }
  }

  private receive(host: Host, event: UtilityWorkerHostEvent): void {
    if (host.retired) {
      return
    }
    if (event.type === 'ready') {
      clearTimeout(host.readyTimer)
      host.ready = true
      for (const request of host.queued.splice(0)) {
        host.child.postMessage(request)
      }
      return
    }
    const worker = host.workers.get(event.id)
    if (!worker) {
      return
    }
    // A worker that dies unasked before saying anything could not load here; run it in-process.
    if (!worker.heard && !worker.terminating && event.type !== 'message') {
      this.giveUp(host, 'worker-start-failed')
      return
    }
    if (event.type === 'exit') {
      host.workers.delete(event.id)
      this.scheduleIdleExit(host)
    }
    worker.receive(event)
  }

  private scheduleIdleExit(host: Host): void {
    if (host.workers.size > 0 || host.idleTimer) {
      return
    }
    host.idleTimer = setTimeout(() => {
      if (host.workers.size === 0) {
        this.retire(host)
        stopChild(host)
      }
    }, HOST_IDLE_EXIT_MS)
    host.idleTimer.unref?.()
  }

  /** Workers that never ran here move in-process; the rest fail as if their thread died. */
  private giveUp(
    host: Host,
    reason: 'ready-timeout' | 'exited-before-ready' | 'worker-start-failed'
  ): void {
    if (host.retired) {
      return
    }
    this.unavailable = true
    recordDurableCrashBreadcrumb('utility_worker_host_unavailable', {
      serviceName: this.serviceName,
      reason,
      workers: host.workers.size
    })
    const workers = [...host.workers.values()]
    this.retire(host)
    stopChild(host)
    for (const worker of workers) {
      if (worker.terminating) {
        worker.finish(0)
      } else if (worker.heard) {
        worker.emit('error', new Error(`${this.serviceName} stopped`))
        worker.finish(1)
      } else {
        worker.adopt(this.spawnInProcess(worker.spawn))
      }
    }
  }

  private exited(host: Host, code: number): void {
    if (host.retired) {
      return
    }
    if (!host.ready) {
      this.giveUp(host, 'exited-before-ready')
      return
    }
    const workers = [...host.workers.values()]
    this.retire(host)
    if (!quitting) {
      const data: CrashReportBreadcrumbData = {
        serviceName: this.serviceName,
        code,
        workers: workers.length
      }
      workers.forEach((worker, index) => {
        data[`w${index}_lastReply`] = worker.lastReplyType
        data[`w${index}_receivedMB`] = Math.round(worker.receivedBytes / 2 ** 20)
      })
      recordDurableCrashBreadcrumb('utility_worker_host_exited', data)
    }
    const error = new Error(`${this.serviceName} exited (${code})`)
    for (const worker of workers) {
      worker.emit('error', error)
      worker.finish(code)
    }
  }

  private retire(host: Host): void {
    host.retired = true
    clearTimeout(host.readyTimer)
    clearTimeout(host.idleTimer)
    host.workers.clear()
    if (this.host === host) {
      this.host = undefined
    }
  }
}
