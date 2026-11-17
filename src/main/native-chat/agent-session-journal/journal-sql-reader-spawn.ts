import type { Worker } from 'node:worker_threads'
import { spawnWorkerThread } from '../../worker-threads/worker-thread-spawn'

export const JOURNAL_SQL_READER_HOST_NAME = 'Orca chat reader'

export type JournalSqlReaderWorker = Pick<
  Worker,
  'postMessage' | 'ref' | 'unref' | 'terminate' | 'on' | 'once'
>
export type JournalSqlReaderWorkerData = { filename: string; originalFilename: string }
export type JournalSqlReaderSpawner = (
  workerPath: string,
  workerData: JournalSqlReaderWorkerData
) => JournalSqlReaderWorker

const spawnJournalSqlReaderThread: JournalSqlReaderSpawner = (workerPath, workerData) =>
  spawnWorkerThread(workerPath, { name: 'orca-journal-sql-reader', workerData, execArgv: [] })

let spawner = spawnJournalSqlReaderThread

/** Electron main hosts readers in a utility process; orcad and tests keep them as threads. */
export function setJournalSqlReaderSpawner(next: JournalSqlReaderSpawner): void {
  spawner = next
}

export function spawnJournalSqlReader(
  workerPath: string,
  workerData: JournalSqlReaderWorkerData
): JournalSqlReaderWorker {
  return spawner(workerPath, workerData)
}
