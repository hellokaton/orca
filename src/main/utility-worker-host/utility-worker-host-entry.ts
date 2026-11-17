import { serialize } from 'node:v8'
import type { Worker } from 'node:worker_threads'
import { installFatalSignalListenerGuard } from '../crash-reporting/fatal-signal-listener-guard'
import { spawnWorkerThread } from '../worker-threads/worker-thread-spawn'
import {
  UTILITY_WORKER_MESSAGE_CHUNK_BYTES,
  type UtilityWorkerFailure,
  type UtilityWorkerHostEvent,
  type UtilityWorkerHostRequest
} from './utility-worker-host-protocol'

installFatalSignalListenerGuard()

type UtilityParentPort = {
  on(event: 'message', listener: (event: { data: UtilityWorkerHostRequest }) => void): unknown
  postMessage(message: UtilityWorkerHostEvent): void
}
// Electron types it as always set, but only a utility process has one.
const parent: UtilityParentPort | undefined = process.parentPort
if (!parent) {
  throw new Error('The worker host must run as an Electron utility process')
}
const port = parent
const workers = new Map<number, Worker>()

function failure(error: unknown): UtilityWorkerFailure {
  const value = error instanceof Error ? error : new Error(String(error))
  const code = (value as NodeJS.ErrnoException).code
  return { name: value.name, message: value.message, ...(typeof code === 'string' ? { code } : {}) }
}

function relay(id: number, message: unknown): void {
  let bytes: Buffer
  try {
    bytes = serialize(message)
  } catch (error) {
    port.postMessage({ type: 'error', id, error: failure(error) })
    void workers.get(id)?.terminate()
    return
  }
  for (let start = 0; start < bytes.byteLength; start += UTILITY_WORKER_MESSAGE_CHUNK_BYTES) {
    const end = Math.min(start + UTILITY_WORKER_MESSAGE_CHUNK_BYTES, bytes.byteLength)
    const view = bytes.subarray(start, end)
    // A view clones its whole backing buffer (the shared pool for small ones), so copy partial views.
    const chunk = view.byteOffset === 0 && view.byteLength === view.buffer.byteLength ? view : new Uint8Array(view)
    port.postMessage({ type: 'message', id, bytes: chunk, more: end < bytes.byteLength })
  }
}

port.on('message', ({ data: request }) => {
  if (request.type === 'spawn') {
    const { id } = request
    try {
      const worker = spawnWorkerThread(request.filename, {
        name: request.name,
        workerData: request.workerData,
        execArgv: []
      })
      workers.set(id, worker)
      worker.on('message', (message: unknown) => relay(id, message))
      worker.on('error', (error) => port.postMessage({ type: 'error', id, error: failure(error) }))
      worker.once('exit', (code) => {
        workers.delete(id)
        port.postMessage({ type: 'exit', id, code })
      })
    } catch (error) {
      port.postMessage({ type: 'error', id, error: failure(error) })
      port.postMessage({ type: 'exit', id, code: 1 })
    }
    return
  }
  const worker = workers.get(request.id)
  if (request.type === 'post') {
    worker?.postMessage(request.message)
  } else {
    void worker?.terminate()
  }
})
port.postMessage({ type: 'ready' })
