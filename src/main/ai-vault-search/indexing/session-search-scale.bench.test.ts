/* oxlint-disable */
// Scale benchmark (not committed): real indexer + worker thread on a fixed corpus, fake 20 s timer fired on demand.
import '../worker/session-search-worker.test-fixture'
import { test, expect, vi } from 'vitest'
import { appendFile, mkdir, readFile, rm, stat, utimes, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { performance } from 'node:perf_hooks'
import { SessionSearchIndexer } from './session-search-indexer'
import { isolatedScanRoots } from '../../ai-vault/scanner-fixtures/session-scanner-test-fixtures'

const c = vi.hoisted(() => ({
  stat: 0,
  readdir: 0,
  opens: 0,
  readFiles: 0,
  passes: 0,
  fullPasses: 0,
  discoveries: 0,
  discoveryCpuMs: 0,
  discoveryWallMs: 0,
  purges: 0
}))
vi.mock('../../native-chat/transcript-filesystem/wsl-transcript-fs-access', async (original) => {
  const a = await original<any>()
  return {
    ...a,
    wslGatedStat: (...x: any[]) => (c.stat++, a.wslGatedStat(...x)),
    wslGatedReaddir: (...x: any[]) => (c.readdir++, a.wslGatedReaddir(...x)),
    wslGatedReadFile: (...x: any[]) => (c.readFiles++, a.wslGatedReadFile(...x)),
    openTranscriptReadStream: (...x: any[]) => (c.opens++, a.openTranscriptReadStream(...x))
  }
})
vi.mock('./session-search-pass', async (original) => {
  const a = await original<any>()
  return {
    ...a,
    runSessionSearchPass: (args: any) => {
      c.passes++
      if (args.full) c.fullPasses++
      return a.runSessionSearchPass(args)
    }
  }
})
vi.mock('../discovery/session-search-scan-roots', async (original) => {
  const a = await original<any>()
  const timed =
    (fn: (...x: any[]) => Promise<any>) =>
    async (...x: any[]) => {
      c.discoveries++
      const cpu = process.cpuUsage()
      const wall = performance.now()
      try {
        return await fn(...x)
      } finally {
        const d = process.cpuUsage(cpu)
        c.discoveryCpuMs += (d.user + d.system) / 1000
        c.discoveryWallMs += performance.now() - wall
      }
    }
  const out: any = {
    ...a,
    discoverSessionSearchCandidates: timed(a.discoverSessionSearchCandidates)
  }
  return out
})

const variant = process.env.BENCH_VARIANT ?? 'unknown'
const corpus = process.env.BENCH_CORPUS!
const outDir = process.env.BENCH_OUT!
const benchHome = process.env.BENCH_HOME
// The desktop app always lists Orca's managed Codex home next to the real one.
const extraCodex = process.env.BENCH_EXTRA_CODEX
  ? { additionalCodexSessionsDirs: [process.env.BENCH_EXTRA_CODEX] }
  : {}
const baseRoots = benchHome
  ? {
      ...isolatedScanRoots(corpus),
      codexSessionsDir: join(benchHome, '.codex', 'sessions'),
      claudeProjectsDir: join(benchHome, '.claude', 'projects')
    }
  : isolatedScanRoots(corpus)
const roots = { ...baseRoots, ...extraCodex }

class Clock {
  next = 1
  timers = new Map<number, () => void>()
  now() {
    return Date.now()
  }
  setTimeout(cb: () => void) {
    const id = this.next++
    this.timers.set(id, cb)
    return id
  }
  clearTimeout(id: unknown) {
    this.timers.delete(id as number)
  }
  fire() {
    const first = this.timers.entries().next().value
    if (!first) return false
    this.timers.delete(first[0])
    first[1]()
    return true
  }
}

test(`session search scale ${variant}`, async () => {
  await mkdir(outDir, { recursive: true })
  if (process.env.BENCH_EXTRA_CODEX) await mkdir(process.env.BENCH_EXTRA_CODEX, { recursive: true })
  const databasePath = join(outDir, `${variant}.sqlite`)
  for (const s of ['', '-wal', '-shm']) await rm(databasePath + s, { force: true })
  const clock = new Clock()
  const errors: string[] = []
  const indexer = new SessionSearchIndexer({
    databasePath,
    roots,
    historyDays: null,
    clock,
    resolveRoots: async () => roots,
    reconcileIntervalMs: 20_000,
    passDeadlineMs: 5_000,
    fullSweepEveryCycles: 15,
    onError: (e) => errors.push(String((e as any)?.message ?? e).slice(0, 200))
  } as any)
  await indexer.initialize()
  const out: any = { variant, rows: [] }
  const save = () =>
    writeFile(join(outDir, `${variant}-results.json`), JSON.stringify(out, null, 1))
  const cycle = async (phase: string, run: () => Promise<void>) => {
    const before = { ...c, cpu: process.cpuUsage(), wall: performance.now() }
    await run()
    const cpu = process.cpuUsage(before.cpu)
    const s = indexer.status()
    const row: any = {
      phase,
      cpuMs: Math.round((cpu.user + cpu.system) / 1000),
      wallMs: Math.round(performance.now() - before.wall)
    }
    for (const k of Object.keys(c) as (keyof typeof c)[])
      row[k] = Math.round(((c[k] as number) - (before as any)[k]) * 10) / 10
    Object.assign(row, {
      files: s.filesIndexed,
      due: s.filesDue,
      messages: s.messagesIndexed,
      st: s.phase,
      rssMiB: Math.round(process.memoryUsage().rss / 1048576)
    })
    out.rows.push(row)
    return row
  }
  const tick = async () => {
    if (!clock.fire()) throw new Error('no timer armed')
    await indexer.settled()
  }
  const sum = (rows: any[], k: string) => rows.reduce((t, r) => t + r[k], 0)
  const { readdirSync } = await import('node:fs')
  const firstDir = (dir: string, depth: number): string =>
    depth === 0
      ? dir
      : firstDir(
          join(
            dir,
            readdirSync(dir)
              .filter((n) => /^\d+$/.test(n))
              .sort()[0]
          ),
          depth - 1
        )
  const codexDay = firstDir(roots.codexSessionsDir, 3)
  const restore: Array<() => Promise<void>> = []
  try {
    const started = performance.now()
    await cycle('import', () => indexer.start())
    while (performance.now() - started < 40 * 60_000) {
      const s = indexer.status()
      if (s.phase === 'current' && s.filesDue === 0) break
      await cycle('import', tick)
    }
    const imp = out.rows.filter((r: any) => r.phase === 'import')
    out.import = {
      checks: imp.length,
      cpuMs: sum(imp, 'cpuMs'),
      wallMs: sum(imp, 'wallMs'),
      fullPasses: sum(imp, 'fullPasses'),
      purges: sum(imp, 'purges'),
      status: indexer.status()
    }
    await save()
    for (let k = 0; k < 30; k++) await cycle('unchanged', tick)
    const un = out.rows.filter((r: any) => r.phase === 'unchanged')
    const med = (k: string) =>
      [...un.map((r: any) => r[k])].sort((a, b) => a - b)[Math.floor(un.length / 2)]
    const { Session } = await import('node:inspector/promises')
    const profiler = new Session()
    profiler.connect()
    await profiler.post('Profiler.enable')
    await profiler.post('Profiler.setSamplingInterval', { interval: 200 })
    await profiler.post('Profiler.start')
    for (let k = 0; k < 5; k++) await cycle('sweep', () => indexer.reconcile({ full: true }))
    const { profile } = (await profiler.post('Profiler.stop')) as any
    profiler.disconnect()
    await writeFile(join(outDir, `${variant}-sweeps.cpuprofile`), JSON.stringify(profile))
    const sw = out.rows.filter((r: any) => r.phase === 'sweep')
    out.sweeps = {
      cpuMs: sw.map((r: any) => r.cpuMs),
      stat: sw.map((r: any) => r.stat),
      readdir: sw.map((r: any) => r.readdir)
    }
    out.unchanged = {
      cycles: un.length,
      cpuMsTotal: sum(un, 'cpuMs'),
      cpuMsMedian: med('cpuMs'),
      statMedian: med('stat'),
      readdirMedian: med('readdir'),
      discoveryCpuMedian: med('discoveryCpuMs'),
      opens: sum(un, 'opens'),
      fullPasses: sum(un, 'fullPasses'),
      purges: sum(un, 'purges')
    }
    await save()
    // Freshness: cycles until a phrase written to the filesystem is searchable (by message count).
    const freshness = async (name: string, mutate: () => Promise<void>, added: number) => {
      const beforeMsgs = indexer.status().messagesIndexed
      await mutate()
      await new Promise((r) => setTimeout(r, 1500))
      for (let k = 1; k <= 20; k++) {
        const row = await cycle(`fresh-${name}`, tick)
        if (row.messages >= beforeMsgs + added) return k
      }
      return null
    }
    const line = (text: string) => {
      const ts = new Date().toISOString()
      return (
        JSON.stringify({
          timestamp: ts,
          type: 'response_item',
          payload: { type: 'message', role: 'user', content: [{ type: 'input_text', text }] }
        }) + '\n'
      )
    }
    const target = async (path: string) => {
      const bytes = await readFile(path)
      const st = await stat(path)
      restore.push(async () => {
        await writeFile(path, bytes)
        await utimes(path, st.atime, st.mtime)
      })
    }
    const { readdir } = await import('node:fs/promises')
    const day = (await readdir(codexDay)).filter((f) => f.endsWith('.jsonl')).sort()
    const cold = join(codexDay, day[0])
    await target(cold)
    // Make the cold file genuinely old so recency ordering cannot favour it.
    out.freshness = {}
    out.freshness.coldAppend = await freshness(
      'cold',
      async () => appendFile(cold, line('cold append quetzalcoatl')),
      1
    )
    // An atomic rewrite of a different quiet file changes its directory's mtime.
    const renamed = join(codexDay, day[1])
    await target(renamed)
    out.freshness.coldRename = await freshness(
      'rename',
      async () => {
        const { rename } = await import('node:fs/promises')
        const tmp = `${renamed}.tmp-bench`
        await writeFile(
          tmp,
          Buffer.concat([await readFile(renamed), Buffer.from(line('renamed rewrite okapi'))])
        )
        await rename(tmp, renamed)
      },
      1
    )
    const hot = cold
    out.freshness.hotAppend = await freshness(
      'hot',
      async () => appendFile(hot, line('hot append axolotl')),
      1
    )
    const newDir = join(roots.codexSessionsDir, '2026', '12', '31')
    await mkdir(newDir, { recursive: true })
    restore.push(() =>
      rm(join(roots.codexSessionsDir, '2026', '12'), { recursive: true, force: true })
    )
    const newFile = join(
      newDir,
      'rollout-2026-12-31T00-00-00-019f0000-2222-7222-8333-000000000000.jsonl'
    )
    const meta =
      JSON.stringify({
        timestamp: new Date().toISOString(),
        type: 'session_meta',
        payload: {
          id: '019f0000-2222-7222-8333-000000000000',
          cwd: '/tmp',
          timestamp: new Date().toISOString()
        }
      }) + '\n'
    out.freshness.newFileNewDir = await freshness(
      'newfile',
      () => writeFile(newFile, meta + line('new file pangolin')),
      1
    )
    out.errors = errors
    out.maxRssKiB = process.resourceUsage().maxRSS
    await save()
  } finally {
    await indexer.close()
    for (const r of restore.reverse()) await r()
    await save()
  }
  expect(out.import.status.phase).toBe('current')
}, 3_000_000)
