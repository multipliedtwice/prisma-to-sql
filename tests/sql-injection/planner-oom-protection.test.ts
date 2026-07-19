import { test } from 'vitest'
/**
 * Regression tests for the weak-server OOM protections in the planner
 * stats collector (and the adjacent cache / parser / transaction fixes).
 *
 * Run from the repository root after `npm install`:
 *
 *     npx tsx tests/sql-injection/planner-oom-protection.test.ts
 *
 * No database is required: every test drives the real collector code with
 * a fake executor that records statements and answers catalog queries.
 * The TypeScript sources are bundled on the fly with the repo's esbuild.
 *
 * Covered areas:
 *   1. Stale-low parent row estimates (enumerate tier: random order + n+1
 *      enumeration proof; truncated enumeration is a uniform sample, never
 *      a physical-first read).
 *   2. TABLESAMPLE percentage clamping (first attempt < 100 on the large
 *      tier; escalation only after a demonstrated under-yield).
 *   3. Session-setting capture/restore (a pooled connection is never
 *      returned with collector statement_timeout/lock_timeout/work_mem/...).
 *   4. Non-session-bound executors fail closed (allowUncancelledQueries
 *      escape hatch; SQLite collection rejected).
 *   5. Partition-tree byte totals (relkind 'p' cannot bypass the byte gate).
 *   6. Unknown child size fails closed (no exact GROUP BY scan).
 *   7. Exact path NULL-FK exclusion and bigint max.
 *   8. One deadline shared across all statements of an edge.
 *   9. Benchmark opt-in (SELECT * / json_agg only when explicitly enabled).
 *  10. S3-FIFO capacity invariants + PrismaPromise transaction delegation.
 *  11. Tighten-only resource guards (existing stricter limits are never
 *      raised; per-statement SETs respect the effective ceiling).
 *  12. Guard-installation failures fail closed (capture failure, mandatory
 *      statement_timeout failure; optional-guard failure stays supported).
 *  13. Restoration failure rejects the collection.
 *  14. Non-B-tree FK indexes are rejected for parent sampling.
 *  15. createDatabaseExecutor performs no session mutation (the collector
 *      owns capture/tighten/restore, so stricter server defaults survive).
 *  16. Pre-existing sub-100 ms statement_timeout is preserved exactly.
 *  17. temp_file_limit-only restoration failure rejects the collection.
 *  18. Partitioned parents skip the page-sample tier explicitly.
 */

import { build } from 'esbuild'
import { createRequire } from 'node:module'
import { fileURLToPath } from 'node:url'
import { mkdtempSync, readFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

type QueryRow = Record<string, unknown>
type QueryParams = unknown[]
type SessionSetting = readonly [value: string, unit: string]
type SessionSettingName =
  | 'statement_timeout'
  | 'lock_timeout'
  | 'application_name'
  | 'work_mem'
  | 'max_parallel_workers_per_gather'
  | 'temp_file_limit'

interface TestField {
  name: string
  isId: boolean
  dbName: string | null
  isList: boolean
  type: string
  relationName?: string
  relationFromFields?: string[]
  relationToFields?: string[]
}

interface TestModel {
  name: string
  dbName: string | null
  fields: TestField[]
  [key: string]: unknown
}

interface TestDatamodel {
  models: TestModel[]
}

interface DatamodelOverrides {
  parent?: Partial<TestModel>
  child?: Partial<TestModel>
}

interface QueryLogEntry {
  sql: string
  params: QueryParams
}

interface TestExecutor {
  sessionBound?: boolean
  query(sql: string, params?: QueryParams): Promise<QueryRow[]>
}

interface SessionCapableExecutor extends TestExecutor {
  withSession?<T>(callback: (session: TestExecutor) => Promise<T>): Promise<T>
}

interface ExecutorRoutes {
  delayMs?: (sql: string) => number
  failCapture?: boolean
  sessionSettings?: Partial<Record<SessionSettingName, SessionSetting>>
  failRestoreWhen?: (sql: string) => boolean
  failSet?: (sql: string) => boolean
  modelStatsRows?: QueryRow[]
  catalogRows?: QueryRow[]
  fkIndex?: boolean
  sampledRows?: (sql: string) => QueryRow[]
}

interface ExecutorOptions {
  sessionBound?: boolean
}

interface RelationStats {
  avg: number
  p95?: number
  p99?: number
  max?: number
  coverage?: number
}

interface EdgeTiming {
  failed?: boolean
  sampled?: number
}

interface PlannerArtifacts {
  relationStats: Record<string, Record<string, RelationStats>>
  edgeTimings: Record<string, EdgeTiming>
  modelStats: Record<
    string,
    {
      relBytes?: number
      relationKind?: string
      [key: string]: unknown
    }
  >
  roundtripRowEquivalent: number
  jsonRowFactor: number
}

interface CollectPlannerOptions {
  executor: SessionCapableExecutor
  datamodel: TestDatamodel
  dialect: 'postgres' | 'sqlite'
  mode: 'fast' | 'precise'
  totalBudgetMs?: number
  perEdgeTimeoutMs?: number
  allowUncancelledQueries?: boolean
  benchmarks?: boolean
}

interface PlannerModule {
  collectPlannerArtifacts(
    options: CollectPlannerOptions,
  ): Promise<PlannerArtifacts>
}

interface BoundedCache<K, V> {
  readonly size: number
  set(key: K, value: V): BoundedCache<K, V>
  get(key: K): V | undefined
  delete(key: K): boolean
}

interface ParsedCountSql {
  fromSql: string
  whereSql: string | null
}

function errorMessage(value: unknown): string {
  return value instanceof Error ? value.message : String(value)
}

const repoRoot = fileURLToPath(new URL('../..', import.meta.url))
const outDir = mkdtempSync(join(tmpdir(), 'planner-oom-test-'))

await build({
  entryPoints: [join(repoRoot, 'src/cardinality-planner.ts')],
  bundle: true,
  platform: 'node',
  format: 'cjs',
  outfile: join(outDir, 'cardinality-planner.cjs'),
  // Only resolved lazily inside createDatabaseExecutor (never called here).
  external: ['postgres', 'dotenv'],
  logLevel: 'silent',
})
await build({
  entryPoints: [join(repoRoot, 'src/utils/s3-fifo.ts')],
  bundle: true,
  platform: 'node',
  format: 'cjs',
  outfile: join(outDir, 's3-fifo.cjs'),
  logLevel: 'silent',
})
await build({
  entryPoints: [join(repoRoot, 'src/batch/count-sql-parser.ts')],
  bundle: true,
  platform: 'node',
  format: 'cjs',
  outfile: join(outDir, 'count-sql-parser.cjs'),
  logLevel: 'silent',
})

const require = createRequire(import.meta.url)
const planner = require(
  join(outDir, 'cardinality-planner.cjs'),
) as PlannerModule
const { createBoundedCache } = require(join(outDir, 's3-fifo.cjs')) as {
  createBoundedCache: <K, V>(maxSize: number) => BoundedCache<K, V>
}
const { parseSimpleCountSql } = require(
  join(outDir, 'count-sql-parser.cjs'),
) as {
  parseSimpleCountSql: (sql: string) => ParsedCountSql | null
}

let pass = 0
let fail = 0
function ok(cond: unknown, name: string): void {
  if (cond) {
    pass++
    console.log(`  PASS ${name}`)
  } else {
    fail++
    console.log(`  FAIL ${name}`)
  }
}

for (const e of [
  'PRISMA_SQL_STATS_BENCHMARKS',
  'PRISMA_SQL_STATS_LIGHT',
  'PRISMA_SQL_ANALYZE',
  'PRISMA_SQL_STATS_STRICT',
  'PRISMA_SQL_STATS_ALLOW_UNCANCELLED',
]) {
  delete process.env[e]
}

function dmModels(extra: DatamodelOverrides = {}): TestDatamodel {
  return {
    models: [
      {
        name: 'Parent',
        dbName: null,
        fields: [
          { name: 'id', isId: true, dbName: null, isList: false, type: 'Int' },
          {
            name: 'children',
            isId: false,
            dbName: null,
            relationName: 'ParentToChild',
            isList: true,
            type: 'Child',
          },
        ],
        ...extra.parent,
      },
      {
        name: 'Child',
        dbName: null,
        fields: [
          { name: 'id', isId: true, dbName: null, isList: false, type: 'Int' },
          {
            name: 'parentId',
            isId: false,
            dbName: null,
            isList: false,
            type: 'Int',
          },
          {
            name: 'parent',
            isId: false,
            dbName: null,
            relationName: 'ParentToChild',
            isList: false,
            type: 'Parent',
            relationFromFields: ['parentId'],
            relationToFields: ['id'],
          },
        ],
        ...extra.child,
      },
    ],
  }
}

const DEFAULT_SESSION: Record<SessionSettingName, SessionSetting> = {
  statement_timeout: ['0', 'ms'],
  lock_timeout: ['0', 'ms'],
  application_name: ['psql', ''],
  work_mem: ['65536', 'kB'], // 64 MB -> tightened to the 16 MB cap
  max_parallel_workers_per_gather: ['2', ''],
  temp_file_limit: ['-1', 'kB'],
}

// Fake executor: routes by SQL content and records every statement.
function makeExecutor(
  routes: ExecutorRoutes = {},
  opts: ExecutorOptions = {},
): { executor: TestExecutor; log: QueryLogEntry[] } {
  const log: QueryLogEntry[] = []
  const executor = {
    sessionBound: opts.sessionBound,
    async query(sql: string, params: QueryParams = []): Promise<QueryRow[]> {
      log.push({ sql, params })
      const delayMs = routes.delayMs?.(sql) ?? 0
      if (delayMs > 0) {
        await new Promise<void>((resolve) => setTimeout(resolve, delayMs))
      }
      if (sql.includes('pg_settings')) {
        if (routes.failCapture) throw new Error('pg_settings unreadable')
        const over = routes.sessionSettings ?? {}
        return (
          Object.entries(DEFAULT_SESSION) as Array<
            [SessionSettingName, SessionSetting]
          >
        ).map(([name, def]) => {
          const o = over[name]
          return {
            name,
            setting: o ? o[0] : def[0],
            unit: o ? o[1] : def[1],
          }
        })
      }
      if (sql.includes('set_config')) {
        if (routes.failRestoreWhen && routes.failRestoreWhen(sql)) {
          throw new Error('set_config denied')
        }
        return [{}]
      }
      if (/^SET /.test(sql.trim())) {
        if (routes.failSet && routes.failSet(sql)) {
          throw new Error('42501: permission denied')
        }
        return []
      }
      if (sql.startsWith('ANALYZE')) return []
      if (sql.includes('pg_partition_tree')) return routes.modelStatsRows ?? []
      if (sql.includes('c.reltuples::bigint AS row_count')) {
        return routes.catalogRows ?? []
      }
      if (sql.includes('FROM pg_stats s') || sql.includes('FROM pg_stats\n'))
        return []
      if (sql.includes('FROM pg_stats WHERE')) return [{ sum_width: '50' }]
      if (sql.includes('pg_index')) {
        return routes.fkIndex === false ? [] : [{ first_col: 'parentId' }]
      }
      if (sql.includes('WITH sampled AS')) {
        return routes.sampledRows ? routes.sampledRows(sql) : []
      }
      if (sql.includes('PERCENTILE_CONT')) {
        return [{ avg: '2', max: '5', p95: '3', p99: '4', coverage: '0.8' }]
      }
      if (sql.includes('json_agg')) return [{ rows: '[]' }]
      if (sql.trim() === 'SELECT 1') return [{ '?column?': 1 }]
      if (sql.includes('SELECT * FROM')) {
        return Array.from({ length: 20 }, (_, i) => ({ id: i }))
      }
      if (sql.includes('pg_column_size')) return [{ avg_bytes: '100' }]
      return []
    },
  }
  return { executor, log }
}

const PARENT_KNOWN = {
  schema_name: 'public',
  table_name: 'Parent',
  reltuples: '1000',
  live_tup: '1000',
  relkind: 'r',
  rel_bytes: '10485760',
}
const CHILD_HUGE = {
  schema_name: 'public',
  table_name: 'Child',
  reltuples: '10000000',
  live_tup: '10000000',
  relkind: 'r',
  rel_bytes: '10737418240',
}

async function testStaleLowParentEstimates() {
  console.log('test: stale-low parent estimates (random order + n+1 proof)')
  // reltuples says 3000 (<= n); the table actually has more (fake returns n+1)
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [
        { ...PARENT_KNOWN, reltuples: '3000', live_tup: '3000' },
        CHILD_HUGE,
      ],
      sampledRows: () => Array.from({ length: 5001 }, () => ({ cnt: '3' })),
    },
    { sessionBound: true },
  )
  const res = await planner.collectPlannerArtifacts({
    executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const sampleQ = log.filter((l) => l.sql.includes('WITH sampled AS'))
  ok(sampleQ.length === 1, 'one sampling statement')
  ok(
    sampleQ[0].sql.includes('ORDER BY random() LIMIT 5001'),
    'enumerate tier: random order, n+1 limit (never physical-first)',
  )
  ok(!sampleQ[0].sql.includes('TABLESAMPLE'), 'enumerate tier: no tablesample')
  ok(
    res.relationStats.Parent.children.avg === 3,
    'stats from truncated sample (uniform, not physical-first)',
  )
  ok(
    res.edgeTimings['Parent.children'].sampled === 5000,
    'truncated to n=5000 sampled parents',
  )

  // Proven enumeration: 60 rows total -> 60 < 5001 proves completeness and
  // is accepted even though 60 < MIN_PARENT_SAMPLE.
  const e2 = makeExecutor(
    {
      modelStatsRows: [
        {
          ...PARENT_KNOWN,
          reltuples: '60',
          live_tup: '60',
          rel_bytes: '1048576',
        },
        CHILD_HUGE,
      ],
      sampledRows: () => Array.from({ length: 60 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  const res2 = await planner.collectPlannerArtifacts({
    executor: e2.executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  ok(
    res2.relationStats.Parent.children.avg === 2,
    'tiny table: proven enumeration accepted below MIN_PARENT_SAMPLE',
  )
  ok(!res2.edgeTimings['Parent.children'].failed, 'tiny table: edge not failed')

  // Contrast: a 60-row result on the random-sort tier is NOT a proven
  // enumeration and is rejected as noise.
  const e3 = makeExecutor(
    {
      modelStatsRows: [
        { ...PARENT_KNOWN, reltuples: '30000', live_tup: '30000' },
        CHILD_HUGE,
      ],
      sampledRows: () => Array.from({ length: 60 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  const res3 = await planner.collectPlannerArtifacts({
    executor: e3.executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const q3 = e3.log.filter((l) => l.sql.includes('WITH sampled AS'))
  ok(
    q3.length === 1 &&
      q3[0].sql.includes('ORDER BY random() LIMIT 5000') &&
      !q3[0].sql.includes('5001'),
    'random-sort tier: plain n limit',
  )
  ok(
    res3.relationStats.Parent.children.avg === 1 &&
      res3.edgeTimings['Parent.children'].failed,
    'small non-enumerated sample rejected -> fallback',
  )
}

async function testTablesampleClamping() {
  console.log('test: TABLESAMPLE percentage clamping')
  let sampleCalls = 0
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [
        // reltuples stale-low (10k) but huge heap -> large tier via bytes
        {
          ...PARENT_KNOWN,
          reltuples: '10000',
          live_tup: '10000',
          rel_bytes: '314572800',
        },
        CHILD_HUGE,
      ],
      sampledRows: () => {
        sampleCalls++
        return sampleCalls === 1
          ? Array.from({ length: 50 }, () => ({ cnt: '2' })) // under-yield
          : Array.from({ length: 5000 }, () => ({ cnt: '2' })) // retry succeeds
      },
    },
    { sessionBound: true },
  )
  const res = await planner.collectPlannerArtifacts({
    executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const ts = log.filter((l) => l.sql.includes('TABLESAMPLE'))
  ok(ts.length === 2, 'under-yield triggered exactly one retry')
  ok(
    ts[0].sql.includes('SYSTEM (90.0000)'),
    'first attempt capped at 90 (stale-low estimate cannot reach 100)',
  )
  ok(
    ts[1].sql.includes('SYSTEM (100.0000)'),
    'retry escalated to 100 after demonstrated under-yield',
  )
  ok(res.relationStats.Parent.children.avg === 2, 'retry produced stats')

  // Healthy sample -> single attempt, pct derived from the estimate.
  const e2 = makeExecutor(
    {
      modelStatsRows: [
        {
          ...PARENT_KNOWN,
          reltuples: '10000000',
          live_tup: '10000000',
          rel_bytes: '1073741824',
        },
        { ...CHILD_HUGE, reltuples: '100000000', live_tup: '100000000' },
      ],
      sampledRows: () => Array.from({ length: 5000 }, () => ({ cnt: '4' })),
    },
    { sessionBound: true },
  )
  await planner.collectPlannerArtifacts({
    executor: e2.executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const ts2 = e2.log.filter((l) => l.sql.includes('TABLESAMPLE'))
  ok(ts2.length === 1, 'healthy sample -> no retry')
  ok(
    /SYSTEM \(0\.15(00)?\)/.test(ts2[0].sql),
    'pct derived from estimate, well below cap',
  )
}

async function testSessionRestoration() {
  console.log('test: session-setting restoration')
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [
        {
          ...PARENT_KNOWN,
          reltuples: '100',
          live_tup: '100',
          rel_bytes: '1048576',
        },
      ],
    },
    { sessionBound: true },
  )
  await planner.collectPlannerArtifacts({
    executor,
    datamodel: { models: [dmModels().models[0]] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 30000,
  })
  ok(
    log[0].sql.includes('pg_settings'),
    'settings captured BEFORE any mutation',
  )
  const setIdx = log.findIndex((l) => /^SET /.test(l.sql.trim()))
  ok(setIdx > 0, 'SETs come after capture')
  const setText = log
    .filter((l) => /^SET /.test(l.sql.trim()))
    .map((l) => l.sql)
    .join('\n')
  ok(
    setText.includes('SET statement_timeout = 15000'),
    'statement_timeout set to cap (existing 0 = unlimited)',
  )
  ok(setText.includes('SET lock_timeout = 5000'), 'lock_timeout set')
  ok(
    setText.includes("SET application_name = 'prisma-sql-planner'"),
    'application_name set',
  )
  ok(
    setText.includes("SET work_mem = '16384kB'"),
    'work_mem tightened 64MB -> 16MB cap',
  )
  ok(
    setText.includes('SET max_parallel_workers_per_gather = 0'),
    'parallel gather disabled',
  )
  ok(
    setText.includes('SET temp_file_limit = 1048576'),
    'temp_file_limit applied (existing -1 = unlimited)',
  )
  const restoreIdx = log.findIndex((l) => l.sql.includes('set_config'))
  ok(restoreIdx > setIdx, 'restore comes after SETs')
  const restore = log[restoreIdx]
  ok(
    restore.params &&
      restore.params[0] === '0ms' &&
      restore.params[1] === '0ms' &&
      restore.params[2] === 'psql' &&
      restore.params[3] === '65536kB' &&
      restore.params[4] === '2',
    'core five restored to captured originals (unit round-trip) via bound params',
  )
  const tfl = log.find(
    (l) =>
      l.sql.includes("set_config('temp_file_limit'") &&
      l.sql.trim().startsWith('SELECT'),
  )
  ok(
    tfl && tfl.params && tfl.params[0] === '-1',
    'temp_file_limit restored separately (SUSET)',
  )
  const lastCollectionIdx = log.reduce(
    (acc, l, i) => (l.sql.includes('pg_partition_tree') ? i : acc),
    -1,
  )
  ok(
    restoreIdx > lastCollectionIdx,
    'restore is the last session activity (finally)',
  )

  // withSession: restore must happen INSIDE the checkout, before the
  // connection goes back to the pool.
  const inner = makeExecutor({ modelStatsRows: [] }, {})
  let sessionReturned = false
  const pooled: SessionCapableExecutor = {
    async query(): Promise<QueryRow[]> {
      throw new Error('pooled executor must not be queried directly')
    },
    async withSession<T>(
      cb: (session: TestExecutor) => Promise<T>,
    ): Promise<T> {
      const sessionExecutor = { query: inner.executor.query }
      const result = await cb(sessionExecutor)
      sessionReturned = true
      const last = inner.log[inner.log.length - 1]
      ok(
        last && last.sql.includes('set_config'),
        'settings restored BEFORE withSession returns connection to pool',
      )
      return result
    },
  }
  await planner.collectPlannerArtifacts({
    executor: pooled,
    datamodel: { models: [dmModels().models[0]] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 30000,
  })
  ok(sessionReturned, 'withSession path completed')
}

async function testTightenOnlyGuards() {
  console.log('test: existing stricter limits are never increased')
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [PARENT_KNOWN, CHILD_HUGE],
      sessionSettings: {
        statement_timeout: ['5000', 'ms'], // stricter than the 15s cap
        lock_timeout: ['1000', 'ms'], // stricter than the 5s cap
        work_mem: ['4096', 'kB'], // 4 MB, stricter than the 16 MB cap
        temp_file_limit: ['102400', 'kB'], // 100 MB, stricter than 1 GB
        max_parallel_workers_per_gather: ['2', ''],
        application_name: ['myapp', ''],
      },
      sampledRows: () => Array.from({ length: 1000 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  await planner.collectPlannerArtifacts({
    executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const setStatements = log
    .filter((l) => /^SET /.test(l.sql.trim()))
    .map((l) => l.sql)
  const stValues = setStatements
    .filter((s) => s.startsWith('SET statement_timeout = '))
    .map((s) => Number(s.replace('SET statement_timeout = ', '')))
  ok(stValues.length >= 2, 'guard install + per-statement timeouts present')
  ok(
    stValues.every((v) => v <= 5000),
    'every statement_timeout SET (install AND per-statement) respects the stricter 5s existing limit',
  )
  ok(
    setStatements.includes('SET lock_timeout = 1000'),
    'lock_timeout tightened to existing 1s, not raised to 5s',
  )
  ok(
    !setStatements.some((s) => s.includes('work_mem')),
    'work_mem untouched (existing 4MB < 16MB cap — never increased)',
  )
  ok(
    !setStatements.some((s) => s.includes('temp_file_limit')),
    'temp_file_limit untouched (existing 100MB < 1GB cap)',
  )
  ok(
    setStatements.includes('SET max_parallel_workers_per_gather = 0'),
    'parallelism disabled (tighten-only by construction)',
  )
  const restore = log.find((l) => l.sql.includes('set_config'))
  ok(
    restore && restore.params[0] === '5000ms' && restore.params[3] === '4096kB',
    'originals restored after collection',
  )
}

async function testGuardFailureFailClosed() {
  console.log('test: guard-installation failures fail closed')

  // (a) capture failure
  const e1 = makeExecutor({ failCapture: true }, { sessionBound: true })
  let threw1: unknown = null
  try {
    await planner.collectPlannerArtifacts({
      executor: e1.executor,
      datamodel: { models: [] },
      dialect: 'postgres',
      mode: 'fast',
      totalBudgetMs: 5000,
    })
  } catch (e) {
    threw1 = e
  }
  ok(
    threw1 && /Refusing to collect planner stats/.test(errorMessage(threw1)),
    'capture failure: collection refuses by default',
  )
  const r1 = await planner.collectPlannerArtifacts({
    executor: e1.executor,
    datamodel: { models: [] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 5000,
    allowUncancelledQueries: true,
  })
  ok(
    r1 && typeof r1.roundtripRowEquivalent === 'number',
    'capture failure: explicit opt-in proceeds',
  )

  // (b) mandatory statement_timeout installation failure
  const e2 = makeExecutor(
    { failSet: (sql) => sql.startsWith('SET statement_timeout') },
    { sessionBound: true },
  )
  let threw2: unknown = null
  try {
    await planner.collectPlannerArtifacts({
      executor: e2.executor,
      datamodel: { models: [] },
      dialect: 'postgres',
      mode: 'fast',
      totalBudgetMs: 5000,
    })
  } catch (e) {
    threw2 = e
  }
  ok(
    threw2 &&
      /mandatory server-side statement_timeout/.test(errorMessage(threw2)),
    'mandatory SET failure: collection refuses by default',
  )
  const logLenBeforeOptIn = e2.log.length
  const r2 = await planner.collectPlannerArtifacts({
    executor: e2.executor,
    datamodel: { models: [] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 5000,
    allowUncancelledQueries: true,
  })
  ok(
    r2 && typeof r2.roundtripRowEquivalent === 'number',
    'mandatory SET failure: explicit opt-in proceeds',
  )
  const optInSets = e2.log
    .slice(logLenBeforeOptIn)
    .filter((l) => /^SET statement_timeout/.test(l.sql.trim()))
  ok(
    optInSets.length === 1,
    'opt-in without server support: only the failed install attempt, no per-statement SETs',
  )

  // (c) optional-guard failure does NOT disable a valid timeout
  const e3 = makeExecutor(
    {
      modelStatsRows: [PARENT_KNOWN],
      failSet: (sql) =>
        sql.includes('work_mem') || sql.includes('lock_timeout'),
    },
    { sessionBound: true },
  )
  const r3 = await planner.collectPlannerArtifacts({
    executor: e3.executor,
    datamodel: { models: [dmModels().models[0]] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 30000,
  })
  ok(
    r3 && typeof r3.roundtripRowEquivalent === 'number',
    'optional failure: collection still resolves',
  )
  const stSets = e3.log.filter((l) =>
    /^SET statement_timeout/.test(l.sql.trim()),
  )
  ok(
    stSets.length >= 2,
    'optional failure: serverTimeoutSupported stays true (per-statement SETs continue)',
  )
}

async function testRestorationFailureRejects() {
  console.log('test: restoration failure rejects collection')
  const { executor } = makeExecutor(
    { modelStatsRows: [PARENT_KNOWN], failRestoreWhen: () => true },
    { sessionBound: true },
  )
  let threw: unknown = null
  try {
    await planner.collectPlannerArtifacts({
      executor,
      datamodel: { models: [dmModels().models[0]] },
      dialect: 'postgres',
      mode: 'fast',
      totalBudgetMs: 30000,
    })
  } catch (e) {
    threw = e
  }
  ok(
    threw && /set_config denied/.test(errorMessage(threw)),
    'failed restore rejects a successful collection (mutated connection never silently pooled)',
  )
}

async function testNonBtreeIndexRejected() {
  console.log('test: non-B-tree FK indexes rejected for sampling')
  const e1 = makeExecutor(
    {
      modelStatsRows: [PARENT_KNOWN, CHILD_HUGE],
      sampledRows: () => Array.from({ length: 1000 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  await planner.collectPlannerArtifacts({
    executor: e1.executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const idxQuery = e1.log.find((l) => l.sql.includes('pg_index'))
  ok(
    idxQuery && idxQuery.sql.includes("am.amname = 'btree'"),
    'index catalog query restricts to B-tree access method',
  )

  // BRIN-only database: the server-side filter finds no usable index.
  const e2 = makeExecutor(
    { modelStatsRows: [PARENT_KNOWN, CHILD_HUGE], fkIndex: false },
    { sessionBound: true },
  )
  const res2 = await planner.collectPlannerArtifacts({
    executor: e2.executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  ok(
    !e2.log.some((l) => l.sql.includes('WITH sampled AS')),
    'no usable B-tree -> sampling never attempted',
  )
  ok(
    res2.relationStats.Parent.children.avg === 1 &&
      res2.edgeTimings['Parent.children'].failed,
    'fallback stats used, edge marked failed for retry',
  )
}

async function testCreateDatabaseExecutorNoMutation() {
  console.log(
    'test: createDatabaseExecutor performs no session mutation (source contract)',
  )
  const src = readFileSync(join(repoRoot, 'src/cardinality-planner.ts'), 'utf8')
  const start = src.indexOf('export async function createDatabaseExecutor')
  const end = src.indexOf(
    'createDatabaseExecutor does not support dialect',
    start,
  )
  ok(start > 0 && end > start, 'factory located')
  const body = src.slice(start, end)
  ok(
    !body.includes('SET statement_timeout') &&
      !body.includes('SET lock_timeout') &&
      !body.includes('SET application_name'),
    'no SET at connect time — stricter server defaults survive capture',
  )
  ok(
    !/sql\.unsafe\(`SET /.test(body),
    'no session mutation of any kind in the factory',
  )
}

async function testSubHundredMsTimeoutPreserved() {
  console.log(
    'test: pre-existing sub-100ms statement_timeout preserved exactly',
  )
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [PARENT_KNOWN, CHILD_HUGE],
      sessionSettings: { statement_timeout: ['50', 'ms'] },
      sampledRows: () => Array.from({ length: 1000 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  await planner.collectPlannerArtifacts({
    executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const stValues = log
    .filter((l) => /^SET statement_timeout = /.test(l.sql.trim()))
    .map((l) => Number(l.sql.trim().replace('SET statement_timeout = ', '')))
  ok(stValues.length >= 2, 'install + per-statement SETs present')
  ok(
    stValues.every((v) => v <= 50),
    'no SET raises the 50ms existing limit (no 100ms floor)',
  )
  ok(stValues[0] === 50, 'install preserves the existing 50ms')
  const restore = log.find((l) => l.sql.includes('set_config'))
  ok(restore && restore.params[0] === '50ms', 'original 50ms restored')
}

async function testTempFileLimitRestoreFailureRejects() {
  console.log('test: temp_file_limit-only restoration failure rejects')
  const { executor } = makeExecutor(
    {
      modelStatsRows: [PARENT_KNOWN],
      failRestoreWhen: (sql) => sql.includes('temp_file_limit'),
    },
    { sessionBound: true },
  )
  let threw: unknown = null
  try {
    await planner.collectPlannerArtifacts({
      executor,
      datamodel: { models: [dmModels().models[0]] },
      dialect: 'postgres',
      mode: 'fast',
      totalBudgetMs: 30000,
    })
  } catch (e) {
    threw = e
  }
  ok(
    threw && /set_config denied/.test(errorMessage(threw)),
    'temp_file_limit restore failure rejects a successful collection',
  )
}

async function testPartitionedParentSampling() {
  console.log('test: partitioned parents skip the page-sample tier explicitly')
  // Large partitioned parent -> page tier would be chosen -> explicit skip
  const e1 = makeExecutor(
    {
      modelStatsRows: [
        {
          schema_name: 'public',
          table_name: 'Parent',
          reltuples: '10000000',
          live_tup: '10000000',
          relkind: 'p',
          rel_bytes: '1073741824',
        },
        CHILD_HUGE,
      ],
      sampledRows: () => Array.from({ length: 5000 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  const res1 = await planner.collectPlannerArtifacts({
    executor: e1.executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  ok(
    !e1.log.some((l) => l.sql.includes('WITH sampled AS')),
    'no TABLESAMPLE attempted on a partitioned root (would error server-side)',
  )
  ok(
    res1.relationStats.Parent.children.avg === 1 &&
      res1.edgeTimings['Parent.children'].failed,
    'fallback stats used, edge marked failed for retry',
  )

  // Small partitioned parent -> enumerate tier is a plain SELECT and works
  const e2 = makeExecutor(
    {
      modelStatsRows: [
        {
          schema_name: 'public',
          table_name: 'Parent',
          reltuples: '3000',
          live_tup: '3000',
          relkind: 'p',
          rel_bytes: '10485760',
        },
        CHILD_HUGE,
      ],
      sampledRows: () => Array.from({ length: 3000 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  const res2 = await planner.collectPlannerArtifacts({
    executor: e2.executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const q2 = e2.log.filter((l) => l.sql.includes('WITH sampled AS'))
  ok(
    q2.length === 1 &&
      q2[0].sql.includes('ORDER BY random()') &&
      !q2[0].sql.includes('TABLESAMPLE'),
    'enumerate tier runs on partitioned roots (plain SELECT)',
  )
  ok(
    res2.relationStats.Parent.children.avg === 2,
    'partitioned parent stats collected via enumerate tier',
  )
}

async function testFailClosedExecutor() {
  console.log('test: non-session-bound rejection (+ sqlite rejection)')
  const { executor } = makeExecutor({ modelStatsRows: [] }, {})
  let threw: unknown = null
  try {
    await planner.collectPlannerArtifacts({
      executor,
      datamodel: { models: [] },
      dialect: 'postgres',
      mode: 'fast',
      totalBudgetMs: 5000,
    })
  } catch (e) {
    threw = e
  }
  ok(
    threw && /session-bound PostgreSQL executor/.test(errorMessage(threw)),
    'non-session-bound executor throws by default',
  )

  const res = await planner.collectPlannerArtifacts({
    executor,
    datamodel: { models: [] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 5000,
    allowUncancelledQueries: true,
  })
  ok(
    res && typeof res.roundtripRowEquivalent === 'number',
    'explicit allowUncancelledQueries opt-in proceeds',
  )

  let threwSqlite: unknown = null
  try {
    await planner.collectPlannerArtifacts({
      executor,
      datamodel: { models: [] },
      dialect: 'sqlite',
      mode: 'fast',
      totalBudgetMs: 5000,
    })
  } catch (e) {
    threwSqlite = e
  }
  ok(
    threwSqlite && /requires PostgreSQL/.test(errorMessage(threwSqlite)),
    'sqlite collection explicitly rejected',
  )
}

async function testPartitionTreeBytes() {
  console.log('test: partition-tree byte totals')
  const { executor } = makeExecutor(
    {
      modelStatsRows: [
        {
          schema_name: 'public',
          table_name: 'Parent',
          reltuples: '100',
          live_tup: '100',
          relkind: 'p',
          rel_bytes: '40960000',
        },
      ],
    },
    { sessionBound: true },
  )
  const res = await planner.collectPlannerArtifacts({
    executor,
    datamodel: { models: [dmModels().models[0]] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 30000,
  })
  ok(
    res.modelStats.Parent.relBytes === 40960000,
    'relBytes from partition tree sum',
  )
  ok(
    res.modelStats.Parent.relationKind === 'partitioned',
    'relationKind recorded',
  )
}

async function testUnknownChildFailClosed() {
  console.log('test: unknown child size fails closed')
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [
        PARENT_KNOWN,
        // reltuples -1 + live_tup 0 -> unknown even after the ANALYZE retry
        {
          schema_name: 'public',
          table_name: 'Child',
          reltuples: '-1',
          live_tup: '0',
          relkind: 'r',
          rel_bytes: '0',
        },
      ],
      sampledRows: () => Array.from({ length: 1000 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  const res = await planner.collectPlannerArtifacts({
    executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  ok(
    !log.some((l) => l.sql.includes('PERCENTILE_CONT')),
    'no exact scan issued for unknown-size child',
  )
  ok(
    log.some((l) => l.sql.startsWith('ANALYZE "public"."Child"')),
    'guarded ANALYZE attempted for unknown table',
  )
  ok(
    res.relationStats.Parent.children.avg === 2,
    'bounded parent sampling used instead',
  )
}

async function testNullFkExclusion() {
  console.log('test: exact path NULL-FK exclusion')
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [
        PARENT_KNOWN,
        {
          schema_name: 'public',
          table_name: 'Child',
          reltuples: '5000',
          live_tup: '5000',
          relkind: 'r',
          rel_bytes: '20971520',
        },
      ],
    },
    { sessionBound: true },
  )
  const res = await planner.collectPlannerArtifacts({
    executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    totalBudgetMs: 30000,
  })
  const exact = log.find((l) => l.sql.includes('PERCENTILE_CONT'))
  ok(
    exact &&
      exact.sql.includes('"parentId" IS NOT NULL') &&
      exact.sql.includes('GROUP BY'),
    'exact scan has IS NOT NULL + GROUP BY',
  )
  ok(exact && exact.sql.includes('MAX(cnt)::bigint'), 'max is bigint-cast')
  ok(
    res.relationStats.Parent.children.avg === 2,
    'exact stats used for small child',
  )
}

async function testSharedEdgeDeadline() {
  console.log('test: one deadline shared across edge statements')
  const { executor, log } = makeExecutor(
    {
      modelStatsRows: [PARENT_KNOWN, CHILD_HUGE],
      // index discovery eats most of the 400ms edge budget
      delayMs: (sql) => (sql.includes('pg_index') ? 350 : 0),
      sampledRows: () => Array.from({ length: 1000 }, () => ({ cnt: '2' })),
    },
    { sessionBound: true },
  )
  const res = await planner.collectPlannerArtifacts({
    executor,
    datamodel: dmModels(),
    dialect: 'postgres',
    mode: 'precise',
    perEdgeTimeoutMs: 400,
    totalBudgetMs: 30000,
  })
  ok(
    log.some((l) => l.sql.includes('pg_index')),
    'index discovery ran within edge deadline',
  )
  ok(
    !log.some((l) => l.sql.includes('WITH sampled AS')),
    'sampling refused: <250ms left of the SAME edge deadline',
  )
  ok(
    res.edgeTimings['Parent.children'].failed === true,
    'edge marked failed for retry next run',
  )
}

async function testBenchmarkOptIn() {
  console.log('test: benchmark opt-in')
  const rows = [PARENT_KNOWN]
  const e1 = makeExecutor({ modelStatsRows: rows }, { sessionBound: true })
  const r1 = await planner.collectPlannerArtifacts({
    executor: e1.executor,
    datamodel: { models: [dmModels().models[0]] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 30000,
  })
  ok(
    !e1.log.some(
      (l) => l.sql.includes('SELECT * FROM') || l.sql.includes('json_agg'),
    ),
    'default: no benchmark queries',
  )
  ok(
    r1.roundtripRowEquivalent === 50 && r1.jsonRowFactor === 1.5,
    'default: benchmark defaults used',
  )

  const e2 = makeExecutor({ modelStatsRows: rows }, { sessionBound: true })
  await planner.collectPlannerArtifacts({
    executor: e2.executor,
    datamodel: { models: [dmModels().models[0]] },
    dialect: 'postgres',
    mode: 'fast',
    totalBudgetMs: 30000,
    benchmarks: true,
  })
  ok(
    e2.log.some((l) => l.sql.includes('SELECT * FROM')),
    'opt-in: SELECT * benchmark ran',
  )
  ok(
    e2.log.some((l) => l.sql.includes('json_agg')),
    'opt-in: json_agg benchmark ran',
  )
}

function testS3FifoInvariants() {
  console.log('test: S3-FIFO capacity invariants (fuzz)')
  let seed = 1234567
  const rnd = () =>
    (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff
  for (const maxSize of [1, 3, 10, 100]) {
    const cache = createBoundedCache<number, number>(maxSize)
    let invariantHeld = true
    for (let i = 0; i < 50000; i++) {
      const k = Math.floor(rnd() * maxSize * 5)
      const op = rnd()
      if (op < 0.55) cache.set(k, k)
      else if (op < 0.85) cache.get(k)
      else cache.delete(k)
      if (cache.size > maxSize) {
        invariantHeld = false
        break
      }
    }
    ok(
      invariantHeld,
      `maxSize=${maxSize}: size never exceeds limit over 50k ops`,
    )
  }
}

function testTransactionDelegation() {
  console.log('test: PrismaPromise transaction delegation (source contract)')
  const src = readFileSync(join(repoRoot, 'src/code-emitter.ts'), 'utf8')
  ok(
    src.includes('return originalTransaction(queriesOrFn, options)'),
    'array-form delegates to original $transaction (atomicity preserved)',
  )
  ok(
    !/return\s+Promise\.all\(queries\)/.test(src),
    'no eager Promise.all over PrismaPromises',
  )
  const tx = readFileSync(join(repoRoot, 'src/transaction.ts'), 'utf8')
  ok(
    /SET LOCAL statement_timeout = \$\{validatedTimeout\}/.test(tx),
    'SET LOCAL inlines validated int (no bind param in utility command)',
  )
}

function testCountParserFallback() {
  console.log('test: count parser subquery fallback')
  ok(
    parseSimpleCountSql('SELECT COUNT(*) AS "count" FROM "User"') !== null,
    'simple count parses',
  )
  ok(
    parseSimpleCountSql(
      'SELECT COUNT(*) AS "count" FROM (SELECT * FROM "User" WHERE "x" = 1) t WHERE "y" = 2',
    ) === null,
    'subquery FROM rejected (falls back safely)',
  )
}

test('planner OOM protections', async () => {
  pass = 0
  fail = 0

  await testStaleLowParentEstimates()
  await testTablesampleClamping()
  await testSessionRestoration()
  await testTightenOnlyGuards()
  await testGuardFailureFailClosed()
  await testRestorationFailureRejects()
  await testNonBtreeIndexRejected()
  await testCreateDatabaseExecutorNoMutation()
  await testSubHundredMsTimeoutPreserved()
  await testTempFileLimitRestoreFailureRejects()
  await testPartitionedParentSampling()
  await testFailClosedExecutor()
  await testPartitionTreeBytes()
  await testUnknownChildFailClosed()
  await testNullFkExclusion()
  await testSharedEdgeDeadline()
  await testBenchmarkOptIn()
  testS3FifoInvariants()
  testTransactionDelegation()
  testCountParserFallback()

  console.log(`\n${pass} passed, ${fail} failed`)

  if (fail > 0) {
    throw new Error(`${fail} planner OOM protection assertion(s) failed`)
  }
})
