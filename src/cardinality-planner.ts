import { DMMF } from '@prisma/generator-helper'
import {
  toNumberOrZero,
  clampStatsMonotonic,
  normalizeStats,
  stableJson,
  stripPrismaParams,
} from './utils/pure-utils'
import { SqlDialect } from './sql-builder-dialect'
import {
  setRelationStats,
  setRoundtripRowEquivalent,
  setJsonRowFactor,
  setModelStats,
  type ModelStats,
  type ModelStatsMap,
} from './builder/select/strategy-estimator'

type Executor = {
  query: (
    sql: string,
    params?: unknown[],
  ) => Promise<Array<Record<string, unknown>>>
  /**
   * True when every query runs on ONE server session (e.g. a max:1 pool),
   * so SET statement_timeout applies to all subsequent statements. Server
   * cancellation is only guaranteed for session-bound executors — three
   * SETs on a pooled executor may each land on a different connection.
   */
  sessionBound?: boolean
  /**
   * Set by applySessionGuards: false when installing server-side timeouts
   * failed, so guarded queries skip the per-statement SET instead of
   * failing every statement. Undefined = not yet attempted.
   */
  serverTimeoutSupported?: boolean
  /**
   * Set by applySessionGuards: true only when the best-effort SUSET
   * temp_file_limit SET succeeded — restoreSessionSettings restores it only
   * then (a non-superuser set_config would fail and abort the whole
   * restore batch).
   */
  tempFileLimitApplied?: boolean
  /**
   * Set by applySessionGuards to the EFFECTIVE session statement_timeout
   * (min of the pre-existing finite timeout and the collector cap).
   * runGuardedQuery clamps every per-statement SET to this ceiling so a
   * stricter pre-existing limit is never raised mid-collection.
   */
  statementTimeoutCeilingMs?: number
}

interface DatabaseExecutor {
  query: (
    sql: string,
    params?: unknown[],
  ) => Promise<Array<Record<string, unknown>>>
  sessionBound?: boolean
  /**
   * Optional session checkout for pooled executors: runs the callback with
   * an executor pinned to a single connection.
   */
  withSession?: <T>(callback: (session: Executor) => Promise<T>) => Promise<T>
}

/** Absolute wall-clock deadline shared by every collector statement. */
type Deadline = {
  at: number
  left: () => number
  passed: () => boolean
}

function makeDeadline(at?: number): Deadline | undefined {
  if (at === undefined) return undefined
  return {
    at,
    left: () => at - Date.now(),
    passed: () => Date.now() >= at,
  }
}

export type RelStats = {
  avg: number
  p95: number
  p99: number
  max: number
  coverage: number
}

export type RelationStatsMap = Record<string, Record<string, RelStats>>

export type { ModelStats, ModelStatsMap }

export type EdgeTiming = {
  ms: number
  measuredAt: number
  /**
   * True when the edge measurement failed (timeout/error). Failed edges are
   * NOT eligible for the slow-edge skip window: a failure must be retried on
   * the next run instead of pinning default stats for `staleEdgeHours`.
   */
  failed?: boolean
  /** Actual number of parents sampled (parent-sampling path only). */
  sampled?: number
}

// ---------------------------------------------------------------------------
// Resource guards (weak-server protection)
// ---------------------------------------------------------------------------
//
// Stats collection runs GROUP BY + PERCENTILE_CONT over every one-to-many
// edge and pulls full-width row samples for the roundtrip/json benchmarks.
// On small servers (little Node heap, small DB box) this is the classic OOM
// profile. The guards below bound: (1) server-side query time, (2) bytes
// pulled into the Node process per measurement query, (3) total wall-clock
// time for the whole collection, and (4) per-edge scan size via TABLESAMPLE.

/** Server-side per-statement cap for the collector connection. */
const DEFAULT_STATEMENT_TIMEOUT_MS = 15000
/** Session lock_timeout so ANALYZE etc. cannot queue behind locks forever. */
const DEFAULT_LOCK_TIMEOUT_MS = 5000
/**
 * Max bytes a single benchmark query may pull into Node
 * (SELECT * samples / json_agg payloads). 16 MiB is safe for tiny heaps.
 * Enforced via a conservative per-row width estimate (see
 * ROW_WIDTH_SAFETY_FACTOR) — NOT a hard wire limit, so benchmarks are
 * skipped entirely when even a few rows could exceed the budget.
 */
const DEFAULT_SAMPLE_BYTE_BUDGET_BYTES = 16 * 1024 * 1024
/** Byte budget used in light mode. */
const LIGHT_SAMPLE_BYTE_BUDGET_BYTES = 8 * 1024 * 1024
/**
 * pg_column_size reports STORED size (possibly TOAST-compressed) and says
 * nothing about wire encoding, JSON serialization, JS object overhead or
 * parse-time allocations. We multiply the stored-width estimate by this
 * factor before comparing it with the byte budget.
 */
const ROW_WIDTH_SAFETY_FACTOR = 8
/**
 * Exact GROUP BY fanout stats are only allowed for child tables at or below
 * this many rows. Above it we sample PARENT KEYS and count their children
 * through the FK index — statistically unbiased for parent-level fanout and
 * bounded in cost. Child-row TABLESAMPLE was considered and rejected: it
 * scales per-parent counts by the sample rate, which badly underestimates
 * fanout and overestimates coverage.
 */
const EXACT_MAX_CHILD_ROWS = 500_000
/** Light-mode ceiling for exact per-edge aggregation. */
const LIGHT_EXACT_MAX_CHILD_ROWS = 100_000
/**
 * Physical-size gate for exact aggregation (pg_relation_size — always
 * current, unlike reltuples which can be stale or -1). Exact GROUP BY runs
 * only when BOTH the row estimate and the physical size are under their
 * ceilings, so a stale "500k rows" estimate cannot authorize scanning a
 * 40 GB table. Both ceilings are defaults only — override via params or
 * PRISMA_SQL_STATS_EXACT_MAX_CHILD_ROWS / _BYTES.
 */
const EXACT_MAX_CHILD_BYTES = 128 * 1024 * 1024
/** Light-mode physical ceiling for exact aggregation. */
const LIGHT_EXACT_MAX_CHILD_BYTES = 32 * 1024 * 1024
/** Number of parent keys sampled per large edge. */
const PARENT_SAMPLE_SIZE = 5000
/** Parent tables up to this size use ORDER BY random() instead of page
 * sampling (bounded sort of narrow keys; avoids TABLESAMPLE's
 * first-N-physical-rows bias when pct would clamp to 100). */
const RANDOM_SORT_MAX_PARENTS = 50_000
/** Physical gate for the enumerate-all parent tier (catalog row estimates
 * can be stale-low; bytes cannot lie about scan cost). */
const ENUMERATE_MAX_PARENT_BYTES = 32 * 1024 * 1024
/** Physical gate for the random-sort parent tier. */
const RANDOM_SORT_MAX_PARENT_BYTES = 256 * 1024 * 1024
/** Hard cap on rows taken from a page sample before the random re-order —
 * bounds the sort input even when the row estimate behind pct was stale. */
const SAMPLED_PAGE_HARD_CAP = 50_000
/** Below this many sampled parents the sample is rejected as noise. */
const MIN_PARENT_SAMPLE = 100
/**
 * First-attempt cap for TABLESAMPLE percentages on the large-parent tier.
 * pct = 100 on a large-tier table makes the "sample" the whole table, after
 * which the downstream hard cap silently keeps the first
 * SAMPLED_PAGE_HARD_CAP physical rows — physically biased. Retries may
 * escalate past this cap only after a demonstrated under-yield (<
 * MIN_PARENT_SAMPLE rows), which at high pct implies a small table where
 * the hard cap cannot truncate.
 */
const PAGE_SAMPLE_MAX_FIRST_PCT = 90
/** Max page-sample attempts per edge (initial + escalation retries). */
const PAGE_SAMPLE_MAX_ATTEMPTS = 3
/**
 * Ceiling for the collector session's work_mem, in kB (16 MB). Applied
 * TIGHTEN-ONLY: an existing smaller work_mem is never raised. work_mem is
 * granted PER sort/hash node, so the exact fanout query (GROUP BY +
 * PERCENTILE_CONT) can allocate it several times over — raising it on a
 * weak server is the last thing an OOM guard should do.
 */
const SESSION_WORK_MEM_CAP_KB = 16 * 1024
/** Disable parallel gather on collector queries: predictable, small plans.
 * Tighten-only by construction (0 is the minimum possible value). */
const SESSION_MAX_PARALLEL_WORKERS = 0
/**
 * Ceiling for the collector session's temp_file_limit in kB (= 1 GiB).
 * SUSET on PostgreSQL, so it is applied best-effort and only restored when
 * the SET actually succeeded. Applied tighten-only (-1 = unlimited).
 */
const SESSION_TEMP_FILE_LIMIT_KB = 1024 * 1024
/** Statements are never STARTED with less remaining budget than this — a
 * strict "1 ms left means zero new statements" guarantee, not a floor that
 * lets a statement overrun the deadline. */
const MIN_STATEMENT_BUDGET_MS = 250
/** Benchmarks are skipped when the byte budget allows fewer rows than this. */
const MIN_BENCHMARK_ROWS = 10
/** Per-row header added to pg_stats width estimates. */
const ROW_HEADER_BYTES = 24
/** Default total wall-clock budget for the whole collection. Enforced at
 * statement granularity: every collector statement checks the deadline first
 * and every client watchdog is clamped to the remaining budget; the session
 * statement_timeout bounds each individual statement server-side. */
const DEFAULT_TOTAL_BUDGET_MS = 60000
/** Do not start a new measurement phase with less budget than this left. */
const MIN_PHASE_BUDGET_MS = 5000
/** Hosts with less RAM than this get light mode automatically. */
const LIGHT_MODE_TOTAL_MEM_BYTES = 2 * 1024 * 1024 * 1024

function getEnvNumber(name: string): number | undefined {
  const raw = process.env[name]
  if (!raw) return undefined
  const parsed = Number(raw)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined
}

/**
 * Light mode trades measurement fidelity for a hard resource ceiling:
 * benchmark phases are skipped (defaults used), per-edge scans are sampled
 * aggressively, and byte budgets shrink. Enabled explicitly via
 * PRISMA_SQL_STATS_LIGHT=1 or automatically on hosts with < 2 GiB RAM.
 */
function resolveLightMode(explicit: boolean | undefined): boolean {
  if (explicit !== undefined) return explicit
  if (
    process.env.PRISMA_SQL_STATS_LIGHT === '1' ||
    process.env.PRISMA_SQL_STATS_LIGHT === 'true'
  ) {
    return true
  }
  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const os = require('os') as typeof import('os')
    return os.totalmem() < LIGHT_MODE_TOTAL_MEM_BYTES
  } catch {
    return false
  }
}

function resolveSampleByteBudget(
  explicit: number | undefined,
  light: boolean,
): number {
  if (explicit !== undefined && Number.isFinite(explicit) && explicit > 0) {
    return Math.floor(explicit)
  }
  const fromEnv = getEnvNumber('PRISMA_SQL_SAMPLE_BYTE_BUDGET')
  if (fromEnv !== undefined) return Math.floor(fromEnv)
  return light
    ? LIGHT_SAMPLE_BYTE_BUDGET_BYTES
    : DEFAULT_SAMPLE_BYTE_BUDGET_BYTES
}

/**
 * Conservative average row-width estimate for benchmark budgeting.
 * Prefers PostgreSQL's own ANALYZE-derived per-column widths (pg_stats
 * avg_width, based on default_statistics_target rows — a far larger sample
 * than any probe we could afford, and zero table access). Falls back to a
 * bounded 32-row physical probe when pg_stats has no rows.
 *
 * NOTE: any average-based estimate can be defeated by payload skew (small
 * first rows, huge later rows). That is why benchmarks are OPT-IN and the
 * estimate is inflated by ROW_WIDTH_SAFETY_FACTOR — this is a heuristic
 * gate, not a hard wire limit.
 */
async function estimateRowWidthBytes(
  executor: Executor,
  schema: string,
  table: string,
  tableRef: string,
  deadline?: Deadline,
): Promise<number> {
  try {
    const rows = await runGuardedQuery(
      executor,
      `SELECT COALESCE(SUM(avg_width), 0)::float AS sum_width
       FROM pg_stats WHERE schemaname = $1 AND tablename = $2`,
      [schema, table],
      5000,
      deadline,
    )
    const w = toNumberOrZero(rows[0]?.sum_width)
    if (w > 0) return w + ROW_HEADER_BYTES
  } catch {
    /* fall through to the probe */
  }

  try {
    const rows = await runGuardedQuery(
      executor,
      `SELECT COALESCE(AVG(sz), 1024)::float AS avg_bytes
       FROM (SELECT pg_column_size(t) AS sz FROM ${tableRef} t LIMIT 32) s`,
      [],
      5000,
      deadline,
    )
    const v = toNumberOrZero(rows[0]?.avg_bytes)
    return v > 0 ? v : 1024
  } catch {
    return 1024
  }
}

/**
 * Cap a LIMIT so that rows * conservativeRowBytes stays within byteBudget.
 * Returns 0 when even MIN_BENCHMARK_ROWS rows could exceed the budget —
 * callers must then skip the benchmark (never "fetch at least one row",
 * which could itself exceed the budget).
 */
function computeRowLimit(
  conservativeRowBytes: number,
  hardCap: number,
  byteBudget: number,
): number {
  const safeBytes = Math.max(1, Math.floor(conservativeRowBytes))
  const byBudget = Math.floor(byteBudget / safeBytes)
  if (byBudget < MIN_BENCHMARK_ROWS) return 0
  return Math.min(hardCap, byBudget)
}

/**
 * Client-side watchdog for a single collector statement. The deadline
 * clamp makes the client give up around the same time the server-side
 * statement_timeout aborts the query; the timer is ALWAYS cleared so fast
 * queries never leave dangling handles. Server-side statement_timeout is
 * the real cancellation; this only frees the client.
 */
async function runBoundedQuery(
  executor: Executor,
  sql: string,
  params: unknown[],
  timeoutMs: number,
): Promise<Record<string, unknown>[]> {
  const bounded = Math.max(MIN_STATEMENT_BUDGET_MS, Math.floor(timeoutMs))
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      executor.query(sql, params),
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`collector statement exceeded ${bounded}ms`)),
          bounded,
        )
        if (typeof timer === 'object' && 'unref' in timer) timer.unref()
      }),
    ])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

/**
 * Run one collector statement with BOTH sides of the timeout aligned:
 * the client watchdog and the server statement_timeout are clamped to the
 * same budget (statement cap AND remaining global deadline), so the client
 * never gives up seconds before the server aborts. The per-statement
 * server timeout requires a session-bound executor; otherwise only the
 * client watchdog applies (already warned about upstream).
 */
async function runGuardedQuery(
  executor: Executor,
  sql: string,
  params: unknown[],
  capMs: number,
  deadline?: Deadline,
): Promise<Record<string, unknown>[]> {
  let bounded = Math.max(MIN_STATEMENT_BUDGET_MS, Math.floor(capMs))
  if (deadline) {
    const left = deadline.left()
    // Strict start threshold: never begin a statement that cannot have at
    // least MIN_STATEMENT_BUDGET_MS of budget — no overrun beyond it.
    if (left < MIN_STATEMENT_BUDGET_MS) {
      throw new DeadlineExhaustedError()
    }
    bounded = Math.min(bounded, Math.floor(left))
  }
  // Per-statement server timeout only when the session guards were
  // actually installed; otherwise the SET itself would fail and the query
  // would never run.
  if (executor.sessionBound && executor.serverTimeoutSupported !== false) {
    // Never raise above the effective ceiling established when the guards
    // were installed: the session may have had a STRICTER timeout than the
    // collector cap, and the tighten-only rule applies to every statement.
    const ceiling = executor.statementTimeoutCeilingMs
    const effective =
      ceiling !== undefined && ceiling > 0
        ? Math.min(bounded, ceiling)
        : bounded
    // No 100ms floor here: a pre-existing sub-100ms timeout must be
    // preserved exactly (floor of 1ms — the server still cancels first;
    // the client watchdog keeps its own 250ms minimum, which only means
    // the client waits a little longer than the server needs).
    await executor.query(
      `SET statement_timeout = ${Math.max(1, Math.floor(effective))}`,
    )
  }
  return runBoundedQuery(executor, sql, params, bounded)
}

/** Throw if the deadline has passed (checked before every statement). */
function checkDeadline(deadline?: Deadline): void {
  if (deadline?.passed()) throw new Error('collector deadline exhausted')
}

/** Clamp a statement cap to the remaining deadline. */
function deadlineCap(capMs: number, deadline?: Deadline): number {
  const left = deadline ? deadline.left() : capMs
  if (deadline && left <= 0) throw new DeadlineExhaustedError()
  return Math.max(250, Math.min(capMs, left))
}

/** Thrown when a statement is refused because the deadline is (nearly) up;
 * phases catch this and degrade to defaults instead of crashing collection. */
class DeadlineExhaustedError extends Error {
  constructor() {
    super('collector deadline exhausted')
    this.name = 'DeadlineExhaustedError'
  }
}

function isDeadlineExhausted(err: unknown): boolean {
  return (
    err instanceof DeadlineExhaustedError ||
    (err instanceof Error && err.message === 'collector deadline exhausted')
  )
}

/**
 * Session-level settings the collector mutates. Every one of them is
 * captured before collection and restored in a finally afterwards: a pooled
 * connection must NEVER be returned to its pool with a 15s
 * statement_timeout, a 5s lock_timeout, a collector application_name, a
 * shrunken work_mem or disabled parallelism — later application queries on
 * that connection would silently inherit them.
 */
const SESSION_GUARD_SETTINGS = [
  'statement_timeout',
  'lock_timeout',
  'application_name',
  'work_mem',
  'max_parallel_workers_per_gather',
  'temp_file_limit',
] as const

/**
 * One captured session setting. `restore` is a string set_config() accepts
 * (numeric settings keep their unit suffix so the exact value round-trips);
 * `numeric` is the plain pg_settings value in its native unit (ms for the
 * timeouts, kB for work_mem/temp_file_limit) used by the tighten-only
 * min() computations. null numeric = unparseable, treated as unlimited.
 */
type CapturedSetting = { restore: string; numeric: number | null }

type SessionSettings = Partial<
  Record<(typeof SESSION_GUARD_SETTINGS)[number], CapturedSetting>
>

/**
 * Capture the current values of every session guard setting. Returns null
 * when the capture fails — callers must then NOT mutate the session at all
 * (mutating a pooled session without the ability to restore is the
 * production regression this guard exists to prevent).
 *
 * Values come from pg_settings (numeric setting + unit) rather than
 * current_setting()'s display format, which pretty-prints ('5s', '1min
 * 30s', '4MB') and cannot be reliably parsed for the tighten-only min().
 */
async function captureSessionSettings(
  executor: Executor,
): Promise<SessionSettings | null> {
  try {
    const rows = await executor.query(
      `SELECT name, setting, unit FROM pg_settings WHERE name IN (${SESSION_GUARD_SETTINGS.map(
        (n) => `'${n}'`,
      ).join(', ')})`,
    )
    if (rows.length === 0) throw new Error('pg_settings returned no rows')
    const out: SessionSettings = {}
    for (const row of rows) {
      const name = String(row.name) as (typeof SESSION_GUARD_SETTINGS)[number]
      const setting = String(row.setting ?? '')
      const unit =
        row.unit === null || row.unit === undefined ? '' : String(row.unit)
      const parsed = Number(setting)
      out[name] = {
        restore:
          unit && !setting.startsWith('-') ? `${setting}${unit}` : setting,
        numeric: Number.isFinite(parsed) ? parsed : null,
      }
    }
    return out
  } catch (err) {
    console.warn(
      '[planner] Could not capture session settings:',
      err instanceof Error ? err.message : err,
    )
    return null
  }
}

/**
 * Restore previously captured session settings. set_config() accepts bound
 * parameters (SET does not) and is_local=false restores at session level.
 * The USERSET settings are restored in one statement; temp_file_limit is
 * restored separately and only when its SET succeeded (it is SUSET — a
 * non-superuser restore attempt would otherwise abort the batch).
 *
 * EVERY restoration failure propagates: a swallowed error here would let a
 * pooled connection return to its pool with collector settings — the
 * production session-leak this whole mechanism exists to prevent. The
 * separate temp_file_limit statement is no exception.
 */
async function restoreSessionSettings(
  executor: Executor,
  saved: SessionSettings,
): Promise<void> {
  const core = SESSION_GUARD_SETTINGS.filter((n) => n !== 'temp_file_limit')
  await executor.query(
    `SELECT ${core
      .map((n, i) => `set_config('${n}', $${i + 1}, false)`)
      .join(', ')}`,
    core.map((n) => saved[n]?.restore ?? ''),
  )
  if (executor.tempFileLimitApplied) {
    await executor.query(`SELECT set_config('temp_file_limit', $1, false)`, [
      saved['temp_file_limit']?.restore ?? '-1',
    ])
  }
}

/**
 * Tighten-only rule for timeout settings (0 = unlimited): the collector may
 * only make an existing limit STRICTER, never weaker. Unknown (unparseable)
 * is treated as unlimited.
 */
function tightenMs(existing: number | null, capMs: number): number {
  if (existing === null || existing <= 0) return capMs
  return Math.min(existing, capMs)
}

/** Tighten-only rule for kB-denominated settings (-1 = unlimited). */
function tightenKb(existing: number | null, capKb: number): number {
  if (existing === null || existing < 0) return capKb
  return Math.min(existing, capKb)
}

/**
 * Should a tighten-only kB guard actually be SET? True when the existing
 * value is unknown (null), unlimited (negative sentinel) or strictly above
 * the effective value — never when it is already at or below the cap.
 */
function shouldApplyKb(existing: number | null, effectiveKb: number): boolean {
  if (existing === null || existing < 0) return true
  return effectiveKb < existing
}

/**
 * Install server-side guards on the session-bound executor (any executor
 * reaching this point — including one from createDatabaseExecutor, which
 * deliberately performs no session mutation of its own).
 *
 * Guard taxonomy:
 *  - MANDATORY: statement_timeout — the server-side cancellation guarantee.
 *    Its installation failure throws (the caller applies the fail-closed /
 *    allowUncancelledQueries rule).
 *  - OPTIONAL: everything else. Each failure is logged and ignored, and
 *    must NOT flip serverTimeoutSupported off — the mandatory timeout that
 *    actually cancels runaway queries is already installed.
 *
 * Every limit is applied TIGHTEN-ONLY (see tightenMs/tightenKb): a session
 * that already has a stricter statement_timeout / lock_timeout / work_mem /
 * temp_file_limit keeps it, and runGuardedQuery clamps its per-statement
 * SETs to the same effective ceiling via statementTimeoutCeilingMs.
 *
 * Every setting installed here MUST be restored via restoreSessionSettings
 * before a pooled session is returned to its pool.
 */
async function applySessionGuards(
  executor: Executor,
  statementTimeoutMs: number,
  saved: SessionSettings,
): Promise<void> {
  const requested = Math.max(
    1000,
    Math.floor(Math.abs(statementTimeoutMs)) || DEFAULT_STATEMENT_TIMEOUT_MS,
  )

  // MANDATORY. SET does not accept bind parameters; the value is a strictly
  // validated integer. Tighten-only: never raise a stricter existing limit.
  const effectiveTimeout = tightenMs(
    saved['statement_timeout']?.numeric ?? null,
    requested,
  )
  await executor.query(`SET statement_timeout = ${effectiveTimeout}`)
  executor.serverTimeoutSupported = true
  executor.statementTimeoutCeilingMs = effectiveTimeout

  const optional = async (label: string, sql: string): Promise<boolean> => {
    try {
      await executor.query(sql)
      return true
    } catch (err) {
      console.warn(
        `[planner] Optional session guard ${label} could not be installed ` +
          '(continuing without it):',
        err instanceof Error ? err.message : err,
      )
      return false
    }
  }

  await optional(
    'lock_timeout',
    `SET lock_timeout = ${tightenMs(saved['lock_timeout']?.numeric ?? null, DEFAULT_LOCK_TIMEOUT_MS)}`,
  )
  await optional(
    'application_name',
    `SET application_name = 'prisma-sql-planner'`,
  )

  // work_mem: CAP ONLY. An existing smaller work_mem is never raised — the
  // collector's GROUP BY / PERCENTILE_CONT may allocate work_mem per
  // sort/hash node, so increasing it would multiply query memory.
  const existingWorkMemKb = saved['work_mem']?.numeric ?? null
  const effectiveWorkMemKb = tightenKb(
    existingWorkMemKb,
    SESSION_WORK_MEM_CAP_KB,
  )
  if (shouldApplyKb(existingWorkMemKb, effectiveWorkMemKb)) {
    await optional('work_mem', `SET work_mem = '${effectiveWorkMemKb}kB'`)
  }

  await optional(
    'max_parallel_workers_per_gather',
    `SET max_parallel_workers_per_gather = ${SESSION_MAX_PARALLEL_WORKERS}`,
  )

  // temp_file_limit is SUSET on PostgreSQL — best-effort by design, and
  // restored only when actually applied (see restoreSessionSettings).
  const existingTflKb = saved['temp_file_limit']?.numeric ?? null
  const effectiveTflKb = tightenKb(existingTflKb, SESSION_TEMP_FILE_LIMIT_KB)
  if (shouldApplyKb(existingTflKb, effectiveTflKb)) {
    executor.tempFileLimitApplied = await optional(
      'temp_file_limit',
      `SET temp_file_limit = ${effectiveTflKb}`,
    )
  } else {
    executor.tempFileLimitApplied = false
  }
}

export type GeneratePlannerArtifacts = {
  relationStats: RelationStatsMap
  modelStats: ModelStatsMap
  roundtripRowEquivalent: number
  jsonRowFactor: number
  collectedAt: number
  edgeTimings: Record<string, EdgeTiming>
}

type RelEdge = {
  parentModel: string
  relName: string
  childModel: string
  parentTable: string
  childTable: string
  parentSchema?: string
  childSchema?: string
  parentPkColumns: string[]
  childFkColumns: string[]
  isMany: boolean
}

function edgeKey(edge: RelEdge): string {
  return `${edge.parentModel}.${edge.relName}`
}

function quoteIdent(dialect: SqlDialect, ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`
}

function tableRefFor(
  dialect: SqlDialect,
  schemaName: string | undefined,
  tableName: string,
): string {
  if (dialect === 'postgres' && schemaName) {
    return `${quoteIdent('postgres', schemaName)}.${quoteIdent('postgres', tableName)}`
  }
  return quoteIdent(dialect, tableName)
}

export async function createDatabaseExecutor(options: {
  databaseUrl: string
  dialect: 'postgres' | 'sqlite'
  connectTimeoutMs?: number
  /**
   * IGNORED here — kept only for API compatibility. This factory must NOT
   * mutate the session: any SET issued at connect time would be captured by
   * collectPlannerArtifacts as the "pre-existing" value, hiding a stricter
   * server default (e.g. statement_timeout = 5s in postgresql.conf) from
   * the tighten-only min() and permanently weakening it for the session.
   * collectPlannerArtifacts exclusively owns capture, tighten-only
   * mutation and restoration. Pass the timeout there instead.
   */
  statementTimeoutMs?: number
}): Promise<{ executor: DatabaseExecutor; cleanup: () => Promise<void> }> {
  const { databaseUrl, dialect, connectTimeoutMs = 30000 } = options

  if (dialect === 'postgres') {
    const postgres = await import('postgres')
    const sql = postgres.default(stripPrismaParams(databaseUrl), {
      connect_timeout: Math.ceil(connectTimeoutMs / 1000),
      max: 1,
    })

    // No session mutation on purpose (see the statementTimeoutMs note):
    // the max:1 pool makes every collection query run on one session, and
    // collectPlannerArtifacts installs tighten-only guards only after
    // capturing the session's own (possibly stricter) settings.
    return {
      executor: {
        // max: 1 pool => every statement runs on this one server session.
        sessionBound: true,
        query: async (q: string, params?: unknown[]) => {
          return await sql.unsafe(q, (params ?? []) as any[])
        },
      },
      cleanup: async () => {
        await sql.end()
      },
    }
  }

  throw new Error(`createDatabaseExecutor does not support dialect: ${dialect}`)
}

function extractMeasurableOneToManyEdges(datamodel: DMMF.Datamodel): RelEdge[] {
  const modelByName = new Map(datamodel.models.map((m) => [m.name, m]))
  const edges: RelEdge[] = []

  for (const parent of datamodel.models) {
    const pkFields = parent.fields.filter((f) => f.isId)
    if (pkFields.length === 0) continue

    const parentTable = parent.dbName || parent.name
    const parentSchema =
      (parent as { schema?: string | null }).schema || undefined

    for (const f of parent.fields) {
      if (!f.relationName) continue
      if (!f.isList) continue

      const child = modelByName.get(f.type)
      if (!child) continue

      const childRelField = child.fields.find(
        (cf) => cf.relationName === f.relationName && cf.type === parent.name,
      )
      if (!childRelField) continue

      const fkFieldNames = childRelField.relationFromFields || []
      if (fkFieldNames.length === 0) continue

      const fkFields = fkFieldNames.map((name) => {
        const fld = child.fields.find((x) => x.name === name)
        return fld ? fld.dbName || fld.name : name
      })

      const refFieldNames = childRelField.relationToFields || []
      if (refFieldNames.length === 0) continue

      const references = refFieldNames.map((name) => {
        const fld = parent.fields.find((x) => x.name === name)
        return fld ? fld.dbName || fld.name : name
      })

      if (fkFields.length !== references.length) continue

      const childTable = child.dbName || child.name
      const childSchema =
        (child as { schema?: string | null }).schema || undefined

      edges.push({
        parentModel: parent.name,
        relName: f.name,
        childModel: child.name,
        parentTable,
        childTable,
        parentSchema,
        childSchema,
        parentPkColumns: references,
        childFkColumns: fkFields,
        isMany: true,
      })
    }
  }

  return edges
}

interface FanoutSqlOptions {
  /**
   * Catalog-estimated parent row count (pg_class.reltuples), used for the
   * coverage denominator. Exact parent COUNT(*) is never run: it is a full
   * sequential scan per edge, and planner hints do not need exactness.
   * When omitted, coverage is reported as NULL (-> 0, conservative).
   */
  parentTotal?: number
}

function buildPostgresStatsSql(
  edge: RelEdge,
  options: FanoutSqlOptions = {},
): string {
  const childTable = tableRefFor('postgres', edge.childSchema, edge.childTable)
  const groupCols = edge.childFkColumns
    .map((c) => quoteIdent('postgres', c))
    .join(', ')
  // Exclude orphan rows (NULL FK): they belong to no parent and would
  // otherwise form one giant spurious group inflating avg/max/coverage.
  const notNullCond = edge.childFkColumns
    .map((c) => `${quoteIdent('postgres', c)} IS NOT NULL`)
    .join(' AND ')

  const hasParentEstimate =
    options.parentTotal !== undefined &&
    Number.isFinite(options.parentTotal) &&
    options.parentTotal > 0
  const coverageExpr = hasParentEstimate
    ? `(SELECT COUNT(*) FROM counts)::float / GREATEST(1, ${Math.floor(options.parentTotal as number)})`
    : 'NULL'

  return `
WITH counts AS (
  SELECT ${groupCols}, COUNT(*) AS cnt
  FROM ${childTable}
  WHERE ${notNullCond}
  GROUP BY ${groupCols}
)
SELECT
  AVG(cnt)::float AS avg,
  MAX(cnt)::bigint AS max,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cnt)::float AS p95,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY cnt)::float AS p99,
  ${coverageExpr} AS coverage
FROM counts
`.trim()
}

function buildSqliteStatsSql(
  edge: RelEdge,
  options: FanoutSqlOptions = {},
): string {
  const childTable = tableRefFor('sqlite', undefined, edge.childTable)
  const groupCols = edge.childFkColumns
    .map((c) => quoteIdent('sqlite', c))
    .join(', ')
  const notNullCond = edge.childFkColumns
    .map((c) => `${quoteIdent('sqlite', c)} IS NOT NULL`)
    .join(' AND ')

  // Exact parent COUNT(*) is never embedded: use the catalog estimate or a
  // cheap pre-measured count; without one, coverage degrades to NULL -> 0.
  const hasParentEstimate =
    options.parentTotal !== undefined &&
    Number.isFinite(options.parentTotal) &&
    options.parentTotal > 0
  const parentTotalExpr = hasParentEstimate
    ? Math.floor(options.parentTotal as number).toString()
    : 'NULL'

  return `
WITH counts AS (
  SELECT ${groupCols}, COUNT(*) AS cnt
  FROM ${childTable}
  WHERE ${notNullCond}
  GROUP BY ${groupCols}
),
n AS (
  SELECT COUNT(*) AS total FROM counts
),
parent_n AS (
  SELECT ${parentTotalExpr} AS total
),
ordered AS (
  SELECT cnt
  FROM counts
  ORDER BY cnt
)
SELECT
  (SELECT AVG(cnt) FROM counts) AS avg,
  (SELECT MAX(cnt) FROM counts) AS max,
  (
    SELECT cnt
    FROM ordered
    LIMIT 1
    OFFSET (
      SELECT
        CASE
          WHEN total <= 1 THEN 0
          ELSE CAST((0.95 * (total - 1)) AS INT)
        END
      FROM n
    )
  ) AS p95,
  (
    SELECT cnt
    FROM ordered
    LIMIT 1
    OFFSET (
      SELECT
        CASE
          WHEN total <= 1 THEN 0
          ELSE CAST((0.99 * (total - 1)) AS INT)
        END
      FROM n
    )
  ) AS p99,
  CAST((SELECT total FROM n) AS FLOAT) / MAX(1, (SELECT total FROM parent_n)) AS coverage
`.trim()
}

function buildFanoutStatsSql(
  dialect: SqlDialect,
  edge: RelEdge,
  options: FanoutSqlOptions = {},
): string {
  return dialect === 'postgres'
    ? buildPostgresStatsSql(edge, options)
    : buildSqliteStatsSql(edge, options)
}

const POSTGRES_STATS_QUERY = `
  SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.reltuples::bigint AS reltuples,
    COALESCE(s.n_live_tup, 0)::bigint AS live_tup,
    c.relkind,
    COALESCE(pt.tree_bytes, pg_relation_size(c.oid))::bigint AS rel_bytes
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
  LEFT JOIN LATERAL (
    SELECT SUM(pg_relation_size(t.relid))::bigint AS tree_bytes
    FROM pg_partition_tree(c.oid) t
  ) pt ON c.relkind = 'p'
  WHERE c.relkind IN ('r', 'p')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema')
`

interface TableRef {
  schema: string
  table: string
}

function bestRowCount(row: Record<string, unknown>): number {
  const reltuples = toNumberOrZero(row.reltuples)
  const liveTup = toNumberOrZero(row.live_tup)

  if (reltuples >= 0) return Math.max(reltuples, liveTup)
  if (liveTup > 0) return liveTup
  return -1
}

async function populatePostgresModelStats(
  executor: Executor,
  tableToModel: Map<string, string>,
  out: ModelStatsMap,
  deadline?: Deadline,
): Promise<TableRef[]> {
  const rows = await runGuardedQuery(
    executor,
    POSTGRES_STATS_QUERY,
    [],
    DEFAULT_STATEMENT_TIMEOUT_MS,
    deadline,
  )
  const unknown: TableRef[] = []

  for (const row of rows) {
    const schemaName = String(row.schema_name)
    const tableName = String(row.table_name)
    const modelName = tableToModel.get(`${schemaName}.${tableName}`)
    if (!modelName) continue

    // Physical size is always recorded (needs no ANALYZE), even when the
    // row estimate is unknown. For partitioned tables this is the summed
    // size of all leaf partitions (the partitioned root has no own heap),
    // so the byte gate cannot be bypassed via relkind 'p'.
    out[modelName].relBytes = toNumberOrZero(row.rel_bytes)
    out[modelName].relationKind =
      String(row.relkind ?? '') === 'p' ? 'partitioned' : 'table'

    const count = bestRowCount(row)
    if (count < 0) {
      unknown.push({ schema: schemaName, table: tableName })
      continue
    }

    out[modelName].rowCount = count
    out[modelName].known = true
  }

  return unknown
}

async function analyzePostgresTables(
  executor: Executor,
  tables: readonly TableRef[],
  deadline?: Deadline,
): Promise<void> {
  for (const { schema, table } of tables) {
    if (deadline?.passed()) {
      console.warn('[planner] Budget exhausted; skipping remaining ANALYZE')
      return
    }
    try {
      // Guarded like every other collector statement: server-side timeout
      // aligned to the remaining budget, not just a client watchdog.
      await runGuardedQuery(
        executor,
        `ANALYZE ${tableRefFor('postgres', schema, table)}`,
        [],
        DEFAULT_STATEMENT_TIMEOUT_MS,
        deadline,
      )
    } catch (_) {}
  }
}

async function collectModelStatsPostgres(
  executor: Executor,
  datamodel: DMMF.Datamodel,
  deadlineMs?: number,
): Promise<ModelStatsMap> {
  const out: ModelStatsMap = {}
  const tableToModel = new Map<string, string>()

  for (const model of datamodel.models) {
    const schema = (model as { schema?: string | null }).schema || 'public'
    const tableName = model.dbName || model.name
    tableToModel.set(`${schema}.${tableName}`, model.name)
    out[model.name] = {
      rowCount: 0,
      tableName,
      schemaName: (model as { schema?: string | null }).schema || undefined,
      known: false,
    }
  }

  const forceAnalyze = process.env.PRISMA_SQL_ANALYZE === '1'
  const deadline = makeDeadline(deadlineMs)

  if (forceAnalyze) {
    const allTables: TableRef[] = datamodel.models.map((m) => ({
      schema: (m as { schema?: string | null }).schema || 'public',
      table: m.dbName || m.name,
    }))
    await analyzePostgresTables(executor, allTables, deadline)
  }

  let unknown = await populatePostgresModelStats(
    executor,
    tableToModel,
    out,
    deadline,
  )

  if (unknown.length > 0 && !forceAnalyze) {
    await analyzePostgresTables(executor, unknown, deadline)
    unknown = await populatePostgresModelStats(
      executor,
      tableToModel,
      out,
      deadline,
    )
  }

  const unknownNames: string[] = []
  for (const model of datamodel.models) {
    if (!out[model.name].known) {
      unknownNames.push(model.name)
    }
  }

  if (unknownNames.length > 0) {
    console.warn(
      `[planner] ${unknownNames.length} model(s) have unknown row counts ` +
        `(table not analyzed or not found): ${unknownNames.join(', ')}. ` +
        `Pathological-query guard will be inactive for these models.`,
    )
  }

  return out
}

async function collectModelStatsSqlite(
  executor: Executor,
  datamodel: DMMF.Datamodel,
  previousArtifacts?: GeneratePlannerArtifacts,
): Promise<ModelStatsMap> {
  const out: ModelStatsMap = {}

  for (const model of datamodel.models) {
    const tableName = model.dbName || model.name

    const cached = previousArtifacts?.modelStats?.[model.name]
    if (cached) {
      out[model.name] = cached
      continue
    }

    try {
      const table = quoteIdent('sqlite', tableName)
      const rows = await executor.query(`SELECT COUNT(*) AS cnt FROM ${table}`)
      const rowCount = toNumberOrZero(rows[0]?.cnt)
      out[model.name] = { rowCount, tableName, known: true }
    } catch (_) {
      out[model.name] = { rowCount: 0, tableName, known: false }
    }
  }

  // Physical per-table sizes via the dbstat virtual table (may not be
  // compiled in). Without it relBytes stays undefined and the exact-scan
  // gate fails closed — same contract as on postgres.
  try {
    const sizeRows = await executor.query(
      `SELECT name, SUM(pgsize) AS bytes FROM dbstat GROUP BY name`,
    )
    const bytesByTable = new Map<string, number>()
    for (const row of sizeRows) {
      bytesByTable.set(String(row.name), toNumberOrZero(row.bytes))
    }
    for (const model of datamodel.models) {
      const tableName = model.dbName || model.name
      const bytes = bytesByTable.get(tableName)
      if (bytes !== undefined && out[model.name]) {
        out[model.name].relBytes = bytes
      }
    }
  } catch (_) {
    // dbstat unavailable: byte gate stays unknown -> fail closed
  }

  return out
}

async function collectModelStats(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  previousArtifacts?: GeneratePlannerArtifacts
  deadlineMs?: number
}): Promise<ModelStatsMap> {
  const { executor, datamodel, dialect, previousArtifacts, deadlineMs } = params

  if (dialect === 'postgres') {
    return collectModelStatsPostgres(executor, datamodel, deadlineMs)
  }

  return collectModelStatsSqlite(executor, datamodel, previousArtifacts)
}

function findLargestTable(args: {
  modelStats: ModelStatsMap
  dialect: SqlDialect
}): {
  tableRef: string
  rowCount: number
  schemaName: string
  tableName: string
} | null {
  const { modelStats, dialect } = args
  let best: {
    tableRef: string
    rowCount: number
    schemaName: string
    tableName: string
  } | null = null

  for (const stats of Object.values(modelStats)) {
    if (stats.known === false) continue
    if (!best || stats.rowCount > best.rowCount) {
      best = {
        tableRef: tableRefFor(dialect, stats.schemaName, stats.tableName),
        rowCount: stats.rowCount,
        schemaName: stats.schemaName || 'public',
        tableName: stats.tableName,
      }
    }
  }

  return best
}

async function measureRoundtripCost(params: {
  executor: Executor
  modelStats: ModelStatsMap
  dialect: SqlDialect
  byteBudget?: number
  deadlineMs?: number
}): Promise<number> {
  const { executor, modelStats, dialect, deadlineMs } = params
  const byteBudget = params.byteBudget ?? DEFAULT_SAMPLE_BYTE_BUDGET_BYTES
  const WARMUP = 5
  const SAMPLES = 15
  const deadline = makeDeadline(deadlineMs)
  const pastDeadline = () => deadline?.passed() ?? false

  for (let i = 0; i < WARMUP && !pastDeadline(); i++) {
    await runGuardedQuery(executor, 'SELECT 1', [], 5000, deadline)
  }

  const roundtripTimes: number[] = []
  for (let i = 0; i < SAMPLES && !pastDeadline(); i++) {
    const start = performance.now()
    await runGuardedQuery(executor, 'SELECT 1', [], 5000, deadline)
    roundtripTimes.push(performance.now() - start)
  }
  if (roundtripTimes.length === 0) return 50
  roundtripTimes.sort((a, b) => a - b)
  const medianRoundtrip = roundtripTimes[Math.floor(roundtripTimes.length / 2)]

  console.log(
    `  [roundtrip] SELECT 1 times (ms): min=${roundtripTimes[0].toFixed(3)} median=${medianRoundtrip.toFixed(3)} max=${roundtripTimes[roundtripTimes.length - 1].toFixed(3)}`,
  )

  const largest = findLargestTable({ modelStats, dialect })

  if (!largest || largest.rowCount < 50) {
    console.log(
      `  [roundtrip] Largest table: ${largest?.tableRef ?? 'none'} (${largest?.rowCount ?? 0} rows) — too small, using default 50`,
    )
    return 50
  }

  console.log(
    `  [roundtrip] Using table ${largest.tableRef} (${largest.rowCount} rows)`,
  )

  return estimateFromQueryPairRatio({
    executor,
    tableRef: largest.tableRef,
    schemaName: largest.schemaName,
    tableName: largest.tableName,
    medianRoundtrip,
    tableRowCount: largest.rowCount,
    byteBudget,
    deadlineMs,
  })
}

async function estimateFromQueryPairRatio(params: {
  executor: Executor
  tableRef: string
  schemaName: string
  tableName: string
  medianRoundtrip: number
  tableRowCount: number
  byteBudget?: number
  deadlineMs?: number
}): Promise<number> {
  const {
    executor,
    tableRef,
    schemaName,
    tableName,
    medianRoundtrip,
    tableRowCount,
    deadlineMs,
  } = params
  const byteBudget = params.byteBudget ?? DEFAULT_SAMPLE_BYTE_BUDGET_BYTES
  const WARMUP = 5
  const SAMPLES = 10
  const deadline = makeDeadline(deadlineMs)
  const pastDeadline = () => deadline?.passed() ?? false

  const smallLimit = 1
  // Bound the sample by bytes, not just rows: SELECT * pulls full-width rows
  // (including TOASTed jsonb/text/bytea) into the Node heap. The width
  // estimate is inflated by ROW_WIDTH_SAFETY_FACTOR and when even a few
  // rows could exceed the budget the benchmark is skipped entirely.
  const avgRowBytes = await estimateRowWidthBytes(
    executor,
    schemaName,
    tableName,
    tableRef,
    deadline,
  )
  const conservativeBytes = avgRowBytes * ROW_WIDTH_SAFETY_FACTOR
  const largeLimit = computeRowLimit(
    conservativeBytes,
    Math.min(1000, tableRowCount),
    byteBudget,
  )
  if (largeLimit === 0) {
    console.warn(
      `  [roundtrip] rows too wide for a safe sample ` +
        `(est. ~${Math.round(conservativeBytes)}B/row vs ` +
        `${(byteBudget / 1048576).toFixed(0)} MiB budget) — using default 50`,
    )
    return 50
  }
  console.log(
    `  [roundtrip] est. row width ~${Math.round(conservativeBytes)}B -> sample limit ${largeLimit} rows (budget ${(byteBudget / 1048576).toFixed(0)} MiB)`,
  )

  for (let i = 0; i < WARMUP && !pastDeadline(); i++) {
    await runGuardedQuery(
      executor,
      `SELECT * FROM ${tableRef} LIMIT ${largeLimit}`,
      [],
      DEFAULT_STATEMENT_TIMEOUT_MS,
      deadline,
    )
  }

  const smallTimes: number[] = []
  for (let i = 0; i < SAMPLES && !pastDeadline(); i++) {
    const start = performance.now()
    await runGuardedQuery(
      executor,
      `SELECT * FROM ${tableRef} LIMIT ${smallLimit}`,
      [],
      DEFAULT_STATEMENT_TIMEOUT_MS,
      deadline,
    )
    smallTimes.push(performance.now() - start)
  }
  if (smallTimes.length === 0) return 50
  smallTimes.sort((a, b) => a - b)
  const medianSmall = smallTimes[Math.floor(smallTimes.length / 2)]

  const largeTimes: number[] = []
  let actualLargeRows = 0
  for (let i = 0; i < SAMPLES && !pastDeadline(); i++) {
    const start = performance.now()
    const rows = await runGuardedQuery(
      executor,
      `SELECT * FROM ${tableRef} LIMIT ${largeLimit}`,
      [],
      DEFAULT_STATEMENT_TIMEOUT_MS,
      deadline,
    )
    largeTimes.push(performance.now() - start)
    actualLargeRows = rows.length
  }
  if (largeTimes.length === 0) return 50
  largeTimes.sort((a, b) => a - b)
  const medianLarge = largeTimes[Math.floor(largeTimes.length / 2)]

  const rowDiff = actualLargeRows - smallLimit
  const timeDiff = medianLarge - medianSmall

  console.log(
    `  [roundtrip] LIMIT ${smallLimit}: median=${medianSmall.toFixed(3)}ms`,
  )
  console.log(
    `  [roundtrip] LIMIT ${largeLimit} (got ${actualLargeRows}): median=${medianLarge.toFixed(3)}ms`,
  )
  console.log(
    `  [roundtrip] Time diff: ${timeDiff.toFixed(3)}ms for ${rowDiff} rows`,
  )

  if (rowDiff < 50 || timeDiff <= 0.05) {
    console.log(
      `  [roundtrip] Insufficient signal (need ≥50 row diff and >0.05ms time diff), defaulting to 50`,
    )
    return 50
  }

  const perRow = timeDiff / rowDiff

  const sequentialTimes: number[] = []
  for (let i = 0; i < SAMPLES && !pastDeadline(); i++) {
    const start = performance.now()
    for (let j = 0; j < 3; j++) {
      await runGuardedQuery(
        executor,
        `SELECT * FROM ${tableRef} LIMIT ${smallLimit}`,
        [],
        DEFAULT_STATEMENT_TIMEOUT_MS,
        deadline,
      )
    }
    sequentialTimes.push(performance.now() - start)
  }
  if (sequentialTimes.length === 0) return 50
  sequentialTimes.sort((a, b) => a - b)
  const median3Sequential =
    sequentialTimes[Math.floor(sequentialTimes.length / 2)]

  const marginalQueryCost = (median3Sequential - medianSmall) / 2

  console.log(
    `  [roundtrip] 3x sequential LIMIT 1: median=${median3Sequential.toFixed(3)}ms`,
  )
  console.log(`  [roundtrip] Single query: ${medianSmall.toFixed(3)}ms`)
  console.log(
    `  [roundtrip] Marginal query cost: ${marginalQueryCost.toFixed(3)}ms`,
  )
  console.log(`  [roundtrip] Per-row cost: ${perRow.toFixed(4)}ms`)

  const equivalent = Math.round(marginalQueryCost / perRow)

  console.log(`  [roundtrip] Raw equivalent: ${equivalent} rows`)

  const clamped = Math.max(10, Math.min(500, equivalent))
  console.log(`  [roundtrip] Final (clamped): ${clamped} rows`)

  return clamped
}

async function measureJsonOverhead(params: {
  executor: Executor
  tableRef: string
  schemaName: string
  tableName: string
  tableRowCount: number
  byteBudget?: number
  deadlineMs?: number
}): Promise<number> {
  const {
    executor,
    tableRef,
    schemaName,
    tableName,
    tableRowCount,
    deadlineMs,
  } = params
  const byteBudget = params.byteBudget ?? DEFAULT_SAMPLE_BYTE_BUDGET_BYTES
  const WARMUP = 3
  const SAMPLES = 10
  const deadline = makeDeadline(deadlineMs)
  const pastDeadline = () => deadline?.passed() ?? false

  // json_agg materializes the whole sample as ONE json value server-side and
  // transfers it as a single string — same OOM vector as SELECT *, but worse.
  const avgRowBytes = await estimateRowWidthBytes(
    executor,
    schemaName,
    tableName,
    tableRef,
    deadline,
  )
  const conservativeBytes = avgRowBytes * ROW_WIDTH_SAFETY_FACTOR
  const limit = computeRowLimit(
    conservativeBytes,
    Math.min(500, tableRowCount),
    byteBudget,
  )
  if (limit === 0) {
    console.warn(
      `  [json] rows too wide for a safe sample ` +
        `(est. ~${Math.round(conservativeBytes)}B/row) — using default 1.5`,
    )
    return 1.5
  }
  console.log(
    `  [json] est. row width ~${Math.round(conservativeBytes)}B -> sample limit ${limit} rows`,
  )

  const rawSql = `SELECT * FROM ${tableRef} LIMIT ${limit}`

  const aggSql = `
    WITH sample AS (
      SELECT * FROM ${tableRef} LIMIT ${limit}
    )
    SELECT COALESCE(json_agg(sample), '[]'::json) AS rows
    FROM sample
  `.trim()

  for (let i = 0; i < WARMUP && !pastDeadline(); i++) {
    await runGuardedQuery(
      executor,
      rawSql,
      [],
      DEFAULT_STATEMENT_TIMEOUT_MS,
      deadline,
    )
    await runGuardedQuery(
      executor,
      aggSql,
      [],
      DEFAULT_STATEMENT_TIMEOUT_MS,
      deadline,
    )
  }

  const rawTimes: number[] = []
  for (let i = 0; i < SAMPLES && !pastDeadline(); i++) {
    const start = performance.now()
    await runGuardedQuery(
      executor,
      rawSql,
      [],
      DEFAULT_STATEMENT_TIMEOUT_MS,
      deadline,
    )
    rawTimes.push(performance.now() - start)
  }
  if (rawTimes.length === 0) return 1.5
  rawTimes.sort((a, b) => a - b)
  const medianRaw = rawTimes[Math.floor(rawTimes.length / 2)]

  const aggTimes: number[] = []
  for (let i = 0; i < SAMPLES && !pastDeadline(); i++) {
    const start = performance.now()
    await runGuardedQuery(
      executor,
      aggSql,
      [],
      DEFAULT_STATEMENT_TIMEOUT_MS,
      deadline,
    )
    aggTimes.push(performance.now() - start)
  }
  if (aggTimes.length === 0) return 1.5
  aggTimes.sort((a, b) => a - b)
  const medianAgg = aggTimes[Math.floor(aggTimes.length / 2)]

  const factor = medianRaw > 0.01 ? medianAgg / medianRaw : 3.0

  console.log(`  [json] Raw ${limit} rows: ${medianRaw.toFixed(3)}ms`)
  console.log(`  [json] json_agg ${limit} rows: ${medianAgg.toFixed(3)}ms`)
  console.log(`  [json] Overhead factor: ${factor.toFixed(2)}x`)

  return Math.max(1.5, Math.min(8.0, factor))
}

async function collectPostgresStatsFromCatalog(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  deadlineMs?: number
}): Promise<{ stats: RelationStatsMap; timings: Record<string, EdgeTiming> }> {
  const { executor, datamodel, deadlineMs } = params
  const deadline = makeDeadline(deadlineMs)
  const edges = extractMeasurableOneToManyEdges(datamodel)
  const out: RelationStatsMap = {}
  const timings: Record<string, EdgeTiming> = {}
  const now = Date.now()

  const tablesToAnalyze = new Map<string, { schema: string; table: string }>()
  for (const edge of edges) {
    const parentSchema = edge.parentSchema || 'public'
    const childSchema = edge.childSchema || 'public'
    tablesToAnalyze.set(`${parentSchema}.${edge.parentTable}`, {
      schema: parentSchema,
      table: edge.parentTable,
    })
    tablesToAnalyze.set(`${childSchema}.${edge.childTable}`, {
      schema: childSchema,
      table: edge.childTable,
    })
  }

  const shouldAnalyze = process.env.PRISMA_SQL_ANALYZE === '1'
  if (shouldAnalyze) {
    for (const { schema, table } of tablesToAnalyze.values()) {
      if (deadline?.passed()) {
        console.warn('[planner] Budget exhausted; skipping remaining ANALYZE')
        break
      }
      try {
        await runGuardedQuery(
          executor,
          `ANALYZE ${tableRefFor('postgres', schema, table)}`,
          [],
          DEFAULT_STATEMENT_TIMEOUT_MS,
          deadline,
        )
      } catch (_) {}
    }
  }

  const tableStatsQuery = `
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      c.reltuples::bigint AS row_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  `

  const tableStats = await runGuardedQuery(
    executor,
    tableStatsQuery,
    [],
    DEFAULT_STATEMENT_TIMEOUT_MS,
    deadline,
  )
  const rowCounts = new Map<string, number>()

  for (const row of tableStats) {
    const schemaName = String(row.schema_name)
    const tableName = String(row.table_name)
    const count = toNumberOrZero(row.row_count)
    rowCounts.set(`${schemaName}.${tableName}`, count)
  }

  for (const edge of edges) {
    if (deadline?.passed()) {
      console.warn('[planner] Budget exhausted; remaining edges use defaults')
      break
    }
    const key = edgeKey(edge)
    const start = performance.now()
    const parentSchema = edge.parentSchema || 'public'
    const childSchema = edge.childSchema || 'public'
    const parentRows = rowCounts.get(`${parentSchema}.${edge.parentTable}`) || 0
    const childRows = rowCounts.get(`${childSchema}.${edge.childTable}`) || 0

    if (parentRows === 0 || childRows === 0) {
      if (!out[edge.parentModel]) out[edge.parentModel] = {}
      out[edge.parentModel][edge.relName] = {
        avg: 1,
        p95: 1,
        p99: 1,
        max: 1,
        coverage: 0,
      }
      timings[key] = { ms: performance.now() - start, measuredAt: now }
      continue
    }

    const fkColumn = edge.childFkColumns[0]

    const statsQuery = `
      SELECT
        s.n_distinct,
        s.correlation,
        (s.most_common_freqs)[1] as max_freq
      FROM pg_stats s
      WHERE s.schemaname = $1
        AND s.tablename = $2
        AND s.attname = $3
    `

    const statsRows = await runGuardedQuery(
      executor,
      statsQuery,
      [childSchema, edge.childTable, fkColumn],
      5000,
      deadline,
    )

    let avg: number
    let p95: number
    let p99: number
    let max: number
    let coverage: number

    if (statsRows.length > 0) {
      const stats = statsRows[0]
      const nDistinct = toNumberOrZero(stats.n_distinct)
      const correlation =
        stats.correlation !== null ? Number(stats.correlation) : 0
      const maxFreq = stats.max_freq !== null ? Number(stats.max_freq) : null

      const distinctCount =
        nDistinct < 0
          ? Math.abs(nDistinct) * childRows
          : nDistinct > 0
            ? nDistinct
            : parentRows

      avg =
        distinctCount > 0 ? childRows / distinctCount : childRows / parentRows
      coverage = Math.min(1, distinctCount / parentRows)

      const skewFactor = Math.abs(correlation) > 0.5 ? 2.5 : 1.5
      p95 = avg * skewFactor
      p99 = avg * (skewFactor * 1.3)

      max = maxFreq ? Math.ceil(childRows * maxFreq) : Math.ceil(p99 * 1.5)
    } else {
      avg = childRows / parentRows
      coverage = 1
      p95 = avg * 2
      p99 = avg * 3
      max = avg * 5
    }

    if (!out[edge.parentModel]) out[edge.parentModel] = {}
    out[edge.parentModel][edge.relName] = clampStatsMonotonic(
      Math.ceil(avg),
      Math.ceil(p95),
      Math.ceil(p99),
      Math.ceil(max),
      coverage,
    )
    timings[key] = { ms: performance.now() - start, measuredAt: now }
  }

  return { stats: out, timings }
}

/**
 * Does the child table have a USABLE index whose leading key is the FK
 * column? Required for parent-key sampling. Validated through catalogs, not
 * indexdef text: expression indexes (indkey[0] = 0 -> no attribute row),
 * partial indexes (indpred), and invalid/not-ready indexes are all excluded.
 * Only B-tree qualifies: a BRIN (or hash/gin/...) index whose leading key
 * is the FK would technically resolve the correlated equality counts but
 * can degenerate into repeated block-range scans across 5,000 correlated
 * subqueries — exactly the database load this collector must not create.
 */
async function hasUsableFkIndex(
  executor: Executor,
  cache: Map<string, boolean>,
  schema: string,
  table: string,
  column: string,
  deadline?: Deadline,
): Promise<boolean> {
  const cacheKey = `${schema}.${table}.${column}`
  const cached = cache.get(cacheKey)
  if (cached !== undefined) return cached

  let ok = false
  try {
    const rows = await runGuardedQuery(
      executor,
      `SELECT a.attname AS first_col
       FROM pg_index i
       JOIN pg_class t ON t.oid = i.indrelid
       JOIN pg_namespace n ON n.oid = t.relnamespace
       JOIN pg_class ix ON ix.oid = i.indexrelid
       JOIN pg_am am ON am.oid = ix.relam
       LEFT JOIN pg_attribute a
         ON a.attrelid = t.oid AND a.attnum = i.indkey[0]
       WHERE n.nspname = $1 AND t.relname = $2
         AND i.indisvalid AND i.indisready
         AND i.indpred IS NULL AND i.indexprs IS NULL
         AND am.amname = 'btree'`,
      [schema, table],
      5000,
      deadline,
    )
    ok = rows.some(
      (row) => (row as { first_col?: unknown }).first_col === column,
    )
  } catch {
    ok = false
  }
  cache.set(cacheKey, ok)
  return ok
}

/**
 * Fanout stats for large child tables by sampling PARENT KEYS instead of
 * scanning the child table. One server-side statement: the sampled CTE
 * materializes the parent keys (strategy depends on parent size), then a
 * correlated subquery counts each parent's children through the FK index.
 * Keys never leave the server, so there is no typed-key stitching problem.
 *
 * Parent-sample strategy (catalog row AND physical-byte gates; every
 * branch has a hard row cap — a stale estimate can never make the
 * collector read more than `sampleSize` parent keys or sort more than
 * SAMPLED_PAGE_HARD_CAP rows):
 *  - rows <= n  AND bytes <= 32MB:   enumerate tier — ORDER BY random()
 *    LIMIT n+1. Fewer than n+1 rows back PROVES complete enumeration
 *    (a LIMIT n+1 query on a table with > n rows always returns n+1);
 *    a stale-low catalog estimate therefore cannot silently turn a
 *    truncated physical-first read into a fake "enumeration", and the
 *    random order makes the truncated case a uniform sample.
 *  - rows <= 50k AND bytes <= 256MB: ORDER BY random() LIMIT n.
 *  - larger (rows known):            TABLESAMPLE page sample, hard-capped,
 *    then ORDER BY random() LIMIT n — page sampling first (cost bound),
 *    random ordering second (bias removal). The first attempt is capped
 *    at PAGE_SAMPLE_MAX_FIRST_PCT (< 100): pct = 100 would make the
 *    "sample" the whole table and the hard cap would then keep only the
 *    first SAMPLED_PAGE_HARD_CAP physical rows. Escalation past the cap
 *    happens only after a demonstrated under-yield (< MIN_PARENT_SAMPLE
 *    rows), which at high pct implies a small table where the hard cap
 *    cannot truncate.
 *  - rows unknown / gates exceeded:  measurement refused (fallback stats).
 *  - partitioned parent at page tier: refused explicitly with a diagnostic
 *    (PostgreSQL has no TABLESAMPLE on partitioned roots); the enumerate
 *    and random-sort tiers are plain SELECTs and work on partitioned roots.
 *
 * Stats contract matches the exact/catalog collectors: avg/p95/p99/max are
 * computed over parents WITH at least one child; coverage is the fraction
 * of sampled parents that have at least one child.
 *
 * Known limitation (documented, accepted): TABLESAMPLE SYSTEM samples
 * BLOCKS, not independent rows. Randomizing rows within the sampled pages
 * removes first-physical-row bias, but fanout correlated with physical
 * clustering can still skew the estimate. That is acceptable for planner
 * hints — the stats are periodically refreshed and self-correcting.
 */
async function collectParentSampledStats(params: {
  executor: Executor
  edge: RelEdge
  rowCounts: Map<string, number>
  tableBytes?: Map<string, number>
  /** schema.table keys of partitioned parents (page sampling unsupported). */
  partitionedTables?: ReadonlySet<string>
  indexCache: Map<string, boolean>
  sampleSize: number
  timeoutMs: number
  deadline?: Deadline
}): Promise<{ stats: RelStats; sampled: number } | null> {
  const {
    executor,
    edge,
    rowCounts,
    tableBytes,
    partitionedTables,
    indexCache,
    sampleSize,
    timeoutMs,
    deadline,
  } = params

  if (edge.childFkColumns.length !== 1 || edge.parentPkColumns.length !== 1) {
    return null // composite keys: skip sampling (exact-if-small or fallback)
  }

  const parentKey = `${edge.parentSchema || 'public'}.${edge.parentTable}`
  const parentRows = rowCounts.get(parentKey)
  if (!parentRows || parentRows <= 0) return null
  const parentBytes = tableBytes?.get(parentKey)

  const indexed = await hasUsableFkIndex(
    executor,
    indexCache,
    edge.childSchema || 'public',
    edge.childTable,
    edge.childFkColumns[0],
    deadline,
  )
  if (!indexed) return null

  const parentTable = tableRefFor(
    'postgres',
    edge.parentSchema,
    edge.parentTable,
  )
  const childTable = tableRefFor('postgres', edge.childSchema, edge.childTable)
  const pkCol = quoteIdent('postgres', edge.parentPkColumns[0])
  const fkCol = quoteIdent('postgres', edge.childFkColumns[0])
  const n = Math.floor(sampleSize)

  type ParentTier = 'enumerate' | 'random-sort' | 'page-sample'
  let tier: ParentTier
  let sampledCte: string
  let pct = 0

  const pageSampleCte = (p: number): string =>
    `SELECT pk FROM (` +
    `  SELECT ${pkCol} AS pk FROM ${parentTable} TABLESAMPLE SYSTEM (${p.toFixed(4)}) LIMIT ${SAMPLED_PAGE_HARD_CAP}` +
    `) page_sample ORDER BY random() LIMIT ${n}`

  if (
    parentRows <= n &&
    parentBytes !== undefined &&
    parentBytes <= ENUMERATE_MAX_PARENT_BYTES
  ) {
    // Catalog says few parents AND the heap is provably small. ORDER BY
    // random() over at most 32 MB of narrow keys is cheap and makes the
    // truncated case (stale-low catalog estimate) a uniform sample instead
    // of the first-N-physical rows. The n+1 LIMIT turns "enumerated all"
    // into a PROVEN fact (see below) instead of an estimate-based claim.
    tier = 'enumerate'
    sampledCte = `SELECT ${pkCol} AS pk FROM ${parentTable} ORDER BY random() LIMIT ${n + 1}`
  } else if (
    parentRows <= RANDOM_SORT_MAX_PARENTS &&
    parentBytes !== undefined &&
    parentBytes <= RANDOM_SORT_MAX_PARENT_BYTES
  ) {
    // TABLESAMPLE at pct=100 would return the first N PHYSICAL rows —
    // biased when fanout correlates with insertion order. A bounded sort
    // of narrow keys over a provably small table is cheap and truly random.
    tier = 'random-sort'
    sampledCte = `SELECT ${pkCol} AS pk FROM ${parentTable} ORDER BY random() LIMIT ${n}`
  } else {
    if (partitionedTables?.has(parentKey)) {
      // PostgreSQL rejects TABLESAMPLE on partitioned-table roots (only
      // plain tables and matviews support it), so the page tier cannot
      // run here. The enumerate/random tiers above use plain SELECT and
      // work fine on partitioned roots; proportional leaf sampling is out
      // of scope. Skip EXPLICITLY with a diagnostic (the edge falls back
      // to previous/default stats and is marked failed for retry) rather
      // than failing the sampling query at runtime.
      console.warn(
        `  ⚠ ${edgeKey(edge)}: parent ${parentKey} is partitioned and too ` +
          'large for the enumerate/random tiers; TABLESAMPLE is unsupported ' +
          'on partitioned roots — skipping parent sampling (fallback stats)',
      )
      return null
    }
    // Page sample first (server-side cost is bounded by pct regardless of
    // table size), hard-capped so a stale estimate cannot inflate the
    // sort, then randomly ordered so the final LIMIT is not just the
    // first-N-physical rows of the sampled pages. The first attempt never
    // reaches 100% (see PAGE_SAMPLE_MAX_FIRST_PCT).
    tier = 'page-sample'
    pct = Math.min(
      PAGE_SAMPLE_MAX_FIRST_PCT,
      Math.max(0.01, ((n * 3) / parentRows) * 100),
    )
    sampledCte = pageSampleCte(pct)
  }

  const countSql = (cte: string): string =>
    `WITH sampled AS (${cte})
     SELECT (SELECT COUNT(*)::bigint FROM ${childTable} c WHERE c.${fkCol} = s.pk) AS cnt
     FROM sampled s`

  let rows: Record<string, unknown>[] = []
  let enumeratedAll = false

  for (let attempt = 0; attempt < PAGE_SAMPLE_MAX_ATTEMPTS; attempt++) {
    rows = await runGuardedQuery(
      executor,
      countSql(sampledCte),
      [],
      timeoutMs,
      deadline,
    )

    if (tier === 'enumerate') {
      // n+1 rows came back => the table has MORE than n rows: the
      // enumeration claim is disproved, but the random order makes the
      // first n rows a uniform sample. Fewer than n+1 rows PROVES the
      // whole parent population was enumerated (LIMIT n+1 on a larger
      // table would have returned exactly n+1).
      if (rows.length > n) {
        rows = rows.slice(0, n)
      } else {
        enumeratedAll = true
      }
      break
    }

    if (
      tier !== 'page-sample' ||
      rows.length >= MIN_PARENT_SAMPLE ||
      pct >= 100
    ) {
      break
    }
    // Under-yield: the page sample returned too few parents to be
    // meaningful. Escalating to a larger percentage is bias-safe here —
    // at a high percentage an under-yield means the table is small, so
    // the hard cap cannot truncate a large population.
    pct = Math.min(100, pct * 4)
    sampledCte = pageSampleCte(pct)
  }

  const sampled = rows.length
  if (sampled === 0) return null
  if (!enumeratedAll && sampled < MIN_PARENT_SAMPLE) {
    return null // sample too small to be meaningful
  }

  const counts = rows.map((r) => toNumberOrZero((r as { cnt?: unknown }).cnt))
  const nonzero = counts.filter((c) => c > 0)
  const coverage = nonzero.length / sampled

  if (nonzero.length === 0) {
    return {
      stats: { avg: 1, p95: 1, p99: 1, max: 1, coverage: 0 },
      sampled,
    }
  }

  // avg/percentiles over nonzero fanouts only — the exact and catalog
  // collectors use the same contract (avg = fanout given >= 1 child).
  const sorted = [...nonzero].sort((a, b) => a - b)
  const m = sorted.length
  const nearestRank = (q: number): number =>
    sorted[Math.min(m - 1, Math.max(0, Math.ceil(q * m) - 1))]
  const sum = nonzero.reduce((acc, c) => acc + c, 0)

  return {
    stats: {
      avg: sum / m,
      p95: nearestRank(0.95),
      p99: nearestRank(0.99),
      max: sorted[m - 1],
      coverage,
    },
    sampled,
  }
}

async function collectPreciseCardinalities(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  previousArtifacts?: GeneratePlannerArtifacts
  slowEdgeThresholdMs?: number
  perEdgeTimeoutMs?: number
  staleEdgeHours?: number
  /** Absolute wall-clock deadline (Date.now() ms). No new edge starts past it. */
  deadlineMs?: number
  /** schema.table -> catalog-estimated row counts (pg_class.reltuples). */
  rowCounts?: Map<string, number>
  /** schema.table -> physical heap bytes (pg_relation_size, always current). */
  tableBytes?: Map<string, number>
  /** schema.table keys of partitioned parents (page sampling unsupported). */
  partitionedTables?: ReadonlySet<string>
  light?: boolean
  /** Overrides for the exact-aggregation ceilings (fail-closed gates). */
  exactMaxChildRows?: number
  exactMaxChildBytes?: number
}): Promise<{ stats: RelationStatsMap; timings: Record<string, EdgeTiming> }> {
  const {
    executor,
    datamodel,
    dialect,
    previousArtifacts,
    slowEdgeThresholdMs = 10000,
    perEdgeTimeoutMs = 30000,
    staleEdgeHours = 168,
    deadlineMs,
    rowCounts,
    tableBytes,
    light = false,
  } = params
  const edges = extractMeasurableOneToManyEdges(datamodel)
  const deadline = makeDeadline(deadlineMs)
  const out: RelationStatsMap = {}
  const timings: Record<string, EdgeTiming> = {}
  const now = Date.now()

  const indexCache = new Map<string, boolean>()
  const exactMaxChildRows =
    params.exactMaxChildRows ??
    (light ? LIGHT_EXACT_MAX_CHILD_ROWS : EXACT_MAX_CHILD_ROWS)
  const exactMaxChildBytes =
    params.exactMaxChildBytes ??
    (light ? LIGHT_EXACT_MAX_CHILD_BYTES : EXACT_MAX_CHILD_BYTES)

  const fallbackStatsFor = (
    edge: RelEdge,
  ): { stats: RelStats; usedPrevious: boolean } => {
    const prev =
      previousArtifacts?.relationStats[edge.parentModel]?.[edge.relName]
    if (prev) return { stats: prev, usedPrevious: true }
    return {
      stats: { avg: 1, p95: 1, p99: 1, max: 1, coverage: 0 },
      usedPrevious: false,
    }
  }

  const catalogRows = (
    schema: string | undefined,
    table: string,
  ): number | undefined => rowCounts?.get(`${schema || 'public'}.${table}`)

  const catalogBytes = (
    schema: string | undefined,
    table: string,
  ): number | undefined => tableBytes?.get(`${schema || 'public'}.${table}`)

  for (const edge of edges) {
    const key = edgeKey(edge)
    const prevTiming = previousArtifacts?.edgeTimings[key]
    const prevStats =
      previousArtifacts?.relationStats[edge.parentModel]?.[edge.relName]

    if (prevTiming && prevStats && !prevTiming.failed) {
      const edgeAgeHours = (now - prevTiming.measuredAt) / (3600 * 1000)
      const wasSlow = prevTiming.ms > slowEdgeThresholdMs

      if (wasSlow && edgeAgeHours < staleEdgeHours) {
        if (!out[edge.parentModel]) out[edge.parentModel] = {}
        out[edge.parentModel][edge.relName] = prevStats
        timings[key] = prevTiming
        console.log(
          `  ⏭ ${key} (took ${(prevTiming.ms / 1000).toFixed(1)}s last run, ${edgeAgeHours.toFixed(0)}h old < ${staleEdgeHours}h cap)`,
        )
        continue
      }
    }

    // Total budget: stop launching heavy edge queries past the deadline and
    // fill the remainder from previous stats/defaults.
    if (deadline?.passed()) {
      const { stats, usedPrevious } = fallbackStatsFor(edge)
      if (!out[edge.parentModel]) out[edge.parentModel] = {}
      out[edge.parentModel][edge.relName] = stats
      timings[key] = { ms: 0, measuredAt: now, failed: !usedPrevious }
      continue
    }

    const start = performance.now()

    try {
      // Cost decision per edge — FAIL CLOSED on unknowns:
      //  - exact GROUP BY only when BOTH the row estimate and the physical
      //    heap size are known and under their ceilings. Unknown size never
      //    selects exact: that is exactly when the table may be huge (e.g.
      //    ANALYZE timed out, stale reltuples).
      //  - otherwise sample parent keys through a verified FK index.
      //  - sampling impossible (composite key / no index / no catalog
      //    estimates): fall back to previous stats/defaults — never scan.
      const childRows = catalogRows(edge.childSchema, edge.childTable)
      const childBytes = catalogBytes(edge.childSchema, edge.childTable)
      const useExact =
        childRows !== undefined &&
        childRows <= exactMaxChildRows &&
        childBytes !== undefined &&
        childBytes <= exactMaxChildBytes

      let stats: RelStats | null = null
      let sampledParents = 0

      // perEdgeTimeoutMs is a real per-EDGE budget: index discovery,
      // sampling and counting all share this single deadline (combined
      // with the global one), instead of each statement getting its own
      // full timeout.
      const edgeDeadline = makeDeadline(
        Math.min(
          deadline?.at ?? Number.POSITIVE_INFINITY,
          Date.now() + perEdgeTimeoutMs,
        ),
      )

      if (useExact) {
        const sql = buildFanoutStatsSql(dialect, edge, {
          parentTotal: catalogRows(edge.parentSchema, edge.parentTable),
        })
        const rows = await runGuardedQuery(
          executor,
          sql,
          [],
          perEdgeTimeoutMs,
          edgeDeadline,
        )
        stats = normalizeStats(rows[0] || {})
      } else if (dialect === 'postgres' && rowCounts) {
        const sampledResult = await collectParentSampledStats({
          executor,
          edge,
          rowCounts,
          tableBytes,
          partitionedTables: params.partitionedTables,
          indexCache,
          sampleSize: PARENT_SAMPLE_SIZE,
          timeoutMs: perEdgeTimeoutMs,
          deadline: edgeDeadline,
        })
        if (sampledResult) {
          stats = sampledResult.stats
          sampledParents = sampledResult.sampled
        }
      }

      const elapsed = performance.now() - start

      if (!stats) {
        // Large edge we cannot measure safely.
        const fallback = fallbackStatsFor(edge)
        stats = fallback.stats
        timings[key] = { ms: elapsed, measuredAt: now, failed: true }
        console.warn(
          `  ⚠ ${key}: child table too large/unknown for exact stats and ` +
            'parent sampling unavailable (no FK index or catalog estimates); ' +
            `using ${fallback.usedPrevious ? 'previous' : 'default'} stats`,
        )
      } else {
        timings[key] = {
          ms: elapsed,
          measuredAt: now,
          ...(sampledParents > 0 ? { sampled: sampledParents } : {}),
        }
        if (elapsed > 5000) {
          console.log(
            `  ⚠ ${key}: ${(elapsed / 1000).toFixed(1)}s` +
              (sampledParents ? ` (sampled ${sampledParents} parents)` : ''),
          )
        }
      }

      if (!out[edge.parentModel]) out[edge.parentModel] = {}
      out[edge.parentModel][edge.relName] = stats
    } catch (err) {
      const elapsed = performance.now() - start

      if (!out[edge.parentModel]) out[edge.parentModel] = {}

      if (prevStats) {
        out[edge.parentModel][edge.relName] = prevStats
        console.warn(
          `  ⚠ ${key} failed (${(elapsed / 1000).toFixed(1)}s), reusing previous: ${err instanceof Error ? err.message : err}`,
        )
      } else {
        out[edge.parentModel][edge.relName] = {
          avg: 1,
          p95: 1,
          p99: 1,
          max: 1,
          coverage: 0,
        }
        console.warn(
          `  ⚠ ${key} failed (${(elapsed / 1000).toFixed(1)}s), using defaults: ${err instanceof Error ? err.message : err}`,
        )
      }

      // Mark as failed so the next run retries instead of treating this edge
      // as "slow but valid" for the whole staleEdgeHours window.
      timings[key] = { ms: elapsed, measuredAt: now, failed: true }
    }
  }

  return { stats: out, timings }
}

async function collectRelationCardinalities(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  mode?: 'fast' | 'precise'
  previousArtifacts?: GeneratePlannerArtifacts
  slowEdgeThresholdMs?: number
  perEdgeTimeoutMs?: number
  staleEdgeHours?: number
  deadlineMs?: number
  rowCounts?: Map<string, number>
  tableBytes?: Map<string, number>
  /** schema.table keys of partitioned parents (page sampling unsupported). */
  partitionedTables?: ReadonlySet<string>
  light?: boolean
  exactMaxChildRows?: number
  exactMaxChildBytes?: number
}): Promise<{ stats: RelationStatsMap; timings: Record<string, EdgeTiming> }> {
  const {
    executor,
    datamodel,
    dialect,
    mode = 'fast',
    previousArtifacts,
    slowEdgeThresholdMs,
    perEdgeTimeoutMs,
    staleEdgeHours,
    deadlineMs,
    rowCounts,
    tableBytes,
    light = false,
  } = params

  if (dialect === 'postgres' && mode === 'fast') {
    const result = await collectPostgresStatsFromCatalog({
      executor,
      datamodel,
      deadlineMs,
    })

    let allTrivial = true
    for (const model of Object.values(result.stats)) {
      for (const rel of Object.values(model)) {
        if (rel.avg > 1 || rel.coverage > 0.5) {
          allTrivial = false
          break
        }
      }
      if (!allTrivial) break
    }

    // Fast mode is catalog-only. It NEVER escalates to precise aggregation:
    // on large or busy databases a silent full GROUP BY over every edge is
    // exactly the weak-server failure this collector must avoid. Precise
    // collection is an explicit opt-in (PRISMA_SQL_STATS_MODE=precise).
    if (allTrivial && Object.keys(result.stats).length > 0) {
      console.warn(
        '⚠ Catalog stats look stale (fresh or empty database?). ' +
          'Using catalog-derived stats as-is; rerun with ' +
          'PRISMA_SQL_STATS_MODE=precise to measure edges exactly ' +
          '(bounded by row ceilings / parent sampling).',
      )
    }

    return result
  }

  return collectPreciseCardinalities({
    executor,
    datamodel,
    dialect,
    previousArtifacts,
    slowEdgeThresholdMs,
    perEdgeTimeoutMs,
    staleEdgeHours,
    deadlineMs,
    rowCounts,
    tableBytes,
    partitionedTables: params.partitionedTables,
    light,
    exactMaxChildRows: params.exactMaxChildRows,
    exactMaxChildBytes: params.exactMaxChildBytes,
  })
}

export async function collectPlannerArtifacts(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  mode?: 'fast' | 'precise'
  previousArtifacts?: GeneratePlannerArtifacts
  slowEdgeThresholdMs?: number
  perEdgeTimeoutMs?: number
  staleEdgeHours?: number
  /**
   * Total wall-clock budget for the whole collection. Enforced at statement
   * granularity: every collector statement checks the deadline first and
   * every client watchdog is clamped to the remaining budget; the session
   * statement_timeout bounds each individual statement server-side.
   * Default 60s, env PRISMA_SQL_STATS_BUDGET_MS.
   */
  totalBudgetMs?: number
  /** Per-query byte cap for the SELECT-star / json_agg benchmark queries. */
  byteBudget?: number
  /** Force light mode (skip benchmarks, sample aggressively). */
  light?: boolean
  /** Session statement_timeout to (re)assert on the given executor. */
  statementTimeoutMs?: number
  /**
   * Explicitly enable/disable the full-row benchmarks. Default: disabled
   * unless PRISMA_SQL_STATS_BENCHMARKS=1 (opt-in for weak-server safety).
   */
  benchmarks?: boolean
  /**
   * DANGEROUS escape hatch: allow collection on a non-session-bound
   * PostgreSQL executor, where an expired client watchdog CANNOT cancel
   * the server-side query (the original weak-server failure mode).
   * Refused by default; env equivalent PRISMA_SQL_STATS_ALLOW_UNCANCELLED=1.
   */
  allowUncancelledQueries?: boolean
  /**
   * Overrides for the exact-aggregation ceilings. The defaults are
   * intentionally conservative for weak servers; raise only with headroom.
   * Env equivalents: PRISMA_SQL_STATS_EXACT_MAX_CHILD_ROWS / _BYTES.
   */
  exactMaxChildRows?: number
  exactMaxChildBytes?: number
}): Promise<GeneratePlannerArtifacts> {
  const {
    executor,
    datamodel,
    dialect,
    mode = 'fast',
    previousArtifacts,
    slowEdgeThresholdMs,
    perEdgeTimeoutMs,
    staleEdgeHours,
  } = params

  const light = resolveLightMode(params.light)
  const byteBudget = resolveSampleByteBudget(params.byteBudget, light)
  const totalBudgetMs =
    params.totalBudgetMs ??
    getEnvNumber('PRISMA_SQL_STATS_BUDGET_MS') ??
    DEFAULT_TOTAL_BUDGET_MS
  const deadlineMs = Date.now() + totalBudgetMs
  const budgetLeft = () => deadlineMs - Date.now()

  // SQLite is refused outright: there is no server-side query cancellation
  // (a client timeout cannot abort a synchronous sqlite COUNT(*)/dbstat
  // read), so collection there would run unprotected — exactly what this
  // collector must not do. The runtime estimator's sqlite paths are
  // unaffected; this concerns only the collection CLI/generator path.
  if (dialect === 'sqlite') {
    throw new Error(
      '[planner] Planner stats collection requires PostgreSQL; SQLite ' +
        'collection is not supported (no server-side query cancellation).',
    )
  }

  const runCollection = async (): Promise<GeneratePlannerArtifacts> => {
    // Benchmarks (SELECT * / json_agg) are the only collector queries that
    // move table data into the Node heap, and any average-based width
    // estimate can be defeated by payload skew. They are therefore OPT-IN:
    // enable with PRISMA_SQL_STATS_BENCHMARKS=1.
    const benchmarksEnabled =
      params.benchmarks ??
      (process.env.PRISMA_SQL_STATS_BENCHMARKS === '1' ||
        process.env.PRISMA_SQL_STATS_BENCHMARKS === 'true')

    if (light) {
      console.log(
        '[planner] Light mode: benchmarks skipped, reduced exact-scan ceilings ' +
          '(PRISMA_SQL_STATS_LIGHT=1 or < 2 GiB RAM detected). Note: light ' +
          'mode reflects Node host memory, not remote database capacity — ' +
          'use PRISMA_SQL_STATS_LIGHT=1 explicitly for weak database servers.',
      )
    }

    console.log('📊 Collecting model row counts...')
    let modelStats: ModelStatsMap
    try {
      modelStats = await collectModelStats({
        executor,
        datamodel,
        dialect,
        previousArtifacts,
        deadlineMs,
      })
    } catch (err) {
      if (!isDeadlineExhausted(err)) throw err
      console.warn(
        '[planner] Budget exhausted during model stats; all edges will use ' +
          'previous/default stats',
      )
      modelStats = {}
    }

    const largest = findLargestTable({ modelStats, dialect })

    const rowCounts = new Map<string, number>()
    const tableBytes = new Map<string, number>()
    const partitionedTables = new Set<string>()
    for (const stats of Object.values(modelStats)) {
      const key = `${stats.schemaName || 'public'}.${stats.tableName}`
      if (stats.known !== false) {
        rowCounts.set(key, stats.rowCount)
      }
      // Physical size is recorded even for tables with unknown row counts.
      if (stats.relBytes !== undefined) {
        tableBytes.set(key, stats.relBytes)
      }
      if (stats.relationKind === 'partitioned') {
        partitionedTables.add(key)
      }
    }

    // NOTE: these phases used to run via Promise.all over a max:1 connection.
    // That serialized the queries anyway while interleaving their in-flight
    // row buffers (higher peak memory) and contaminating the latency
    // benchmarks with each other's workload. Sequential phases give a strict
    // peak-memory ceiling AND meaningful timings.
    let cardinalityResult: {
      stats: RelationStatsMap
      timings: Record<string, EdgeTiming>
    }
    try {
      cardinalityResult = await collectRelationCardinalities({
        executor,
        datamodel,
        dialect,
        mode,
        previousArtifacts,
        slowEdgeThresholdMs,
        perEdgeTimeoutMs,
        staleEdgeHours,
        deadlineMs,
        rowCounts,
        tableBytes,
        partitionedTables,
        light,
        exactMaxChildRows:
          params.exactMaxChildRows ??
          getEnvNumber('PRISMA_SQL_STATS_EXACT_MAX_CHILD_ROWS'),
        exactMaxChildBytes:
          params.exactMaxChildBytes ??
          getEnvNumber('PRISMA_SQL_STATS_EXACT_MAX_CHILD_BYTES'),
      })
    } catch (err) {
      if (!isDeadlineExhausted(err)) throw err
      console.warn(
        '[planner] Budget exhausted during cardinality collection; ' +
          'remaining edges use previous/default stats',
      )
      cardinalityResult = { stats: {}, timings: {} }
    }

    let roundtripRowEquivalent = 50
    if (!light && benchmarksEnabled && budgetLeft() > MIN_PHASE_BUDGET_MS) {
      try {
        roundtripRowEquivalent = await measureRoundtripCost({
          executor,
          modelStats,
          dialect,
          byteBudget,
          deadlineMs,
        })
      } catch (err) {
        if (!isDeadlineExhausted(err)) throw err
        console.warn(
          '[planner] Budget exhausted; using default roundtrip cost (50)',
        )
      }
    } else if (!light && benchmarksEnabled) {
      console.warn(
        '[planner] Budget exhausted; using default roundtrip cost (50)',
      )
    }

    let jsonRowFactor = 1.5
    if (
      !light &&
      benchmarksEnabled &&
      largest &&
      largest.rowCount >= 50 &&
      dialect === 'postgres' &&
      budgetLeft() > MIN_PHASE_BUDGET_MS
    ) {
      try {
        jsonRowFactor = await measureJsonOverhead({
          executor,
          tableRef: largest.tableRef,
          schemaName: largest.schemaName,
          tableName: largest.tableName,
          tableRowCount: largest.rowCount,
          byteBudget,
          deadlineMs,
        })
      } catch (err) {
        if (!isDeadlineExhausted(err)) throw err
        console.warn(
          '[planner] Budget exhausted; using default JSON factor (1.5)',
        )
      }
    } else if (
      !light &&
      benchmarksEnabled &&
      largest &&
      dialect === 'postgres'
    ) {
      console.warn(
        '[planner] Budget exhausted; using default JSON factor (1.5)',
      )
    }

    console.log(`  Roundtrip cost: ~${roundtripRowEquivalent} row equivalents`)
    console.log(`  JSON overhead factor: ${jsonRowFactor.toFixed(2)}x`)

    const slowEdges = Object.entries(cardinalityResult.timings)
      .filter(([, t]) => t.ms > 5000)
      .sort((a, b) => b[1].ms - a[1].ms)

    if (slowEdges.length > 0) {
      console.log(`  Slow edges:`)
      for (const [key, t] of slowEdges) {
        console.log(`    ${key}: ${(t.ms / 1000).toFixed(1)}s`)
      }
    }

    const failedEdges = Object.entries(cardinalityResult.timings).filter(
      ([, t]) => t.failed,
    )
    if (failedEdges.length > 0) {
      console.warn(
        `[planner] ${failedEdges.length} edge(s) failed or were budget-skipped ` +
          'and will be retried on the next run',
      )
    }

    return {
      relationStats: cardinalityResult.stats,
      modelStats,
      roundtripRowEquivalent,
      jsonRowFactor,
      collectedAt: Date.now(),
      edgeTimings: cardinalityResult.timings,
    }
  }

  // Server-side cancellation is only guaranteed on a session-bound
  // executor. We do NOT infer session affinity from SET succeeding — on a
  // pooled executor each SET may land on a different connection. Callers
  // must either pass a sessionBound executor (createDatabaseExecutor) or a
  // withSession checkout.
  if (dialect === 'postgres') {
    const pooled = executor as DatabaseExecutor
    if (!executor.sessionBound && typeof pooled.withSession === 'function') {
      // Wrap explicitly instead of spreading: spread drops prototype
      // methods/getters and any non-enumerable state the session executor
      // may rely on. The recursive call captures/applies/restores the
      // session settings INSIDE the checkout, so the connection returns to
      // its pool with its original settings.
      return pooled.withSession((session) =>
        collectPlannerArtifacts({
          ...params,
          executor: {
            sessionBound: true,
            query: session.query.bind(session),
          },
        }),
      )
    }
    // ONE fail-closed rule everywhere server-side cancellation is
    // unavailable — no session affinity, settings capture failure, or
    // mandatory statement_timeout installation failure. A client-side
    // watchdog that cannot cancel the server-side query is the original
    // weak-server failure mode: the watchdog expires, the query keeps
    // running, later queries queue behind it. Unsafe operation always
    // requires the explicit opt-in (PRISMA_SQL_STATS_STRICT is no longer
    // consulted: the default IS the strict behavior).
    const allowUncancelled =
      params.allowUncancelledQueries === true ||
      process.env.PRISMA_SQL_STATS_ALLOW_UNCANCELLED === '1' ||
      process.env.PRISMA_SQL_STATS_ALLOW_UNCANCELLED === 'true'
    const proceedWithoutCancellation = (msg: string, cause?: unknown): void => {
      if (!allowUncancelled) {
        throw new Error(
          `${msg}. Refusing to collect planner stats: an expired client ` +
            'watchdog cannot cancel a running server query. Pass ' +
            'allowUncancelledQueries: true (or set ' +
            'PRISMA_SQL_STATS_ALLOW_UNCANCELLED=1) to run with client-side ' +
            'watchdogs only.' +
            (cause !== undefined
              ? ` (cause: ${cause instanceof Error ? cause.message : cause})`
              : ''),
        )
      }
      console.warn(
        `${msg}; continuing with client-side watchdogs only ` +
          '(allowUncancelledQueries):',
        cause instanceof Error ? cause.message : cause ?? '',
      )
    }

    if (!executor.sessionBound) {
      // Without session affinity the per-statement SET statement_timeout
      // may land on a different pooled connection and an expired client
      // watchdog leaves the query running on the server.
      proceedWithoutCancellation(
        '[planner] Planner collection requires a session-bound ' +
          'PostgreSQL executor (sessionBound or withSession)',
      )
      return runCollection()
    }

    // Session-bound: mutate ONLY with a guaranteed restore. When the
    // current settings cannot be captured, no guard is installed at all —
    // a pooled session must never be returned with collector settings.
    const savedSettings = await captureSessionSettings(executor)
    if (!savedSettings) {
      executor.serverTimeoutSupported = false
      proceedWithoutCancellation(
        '[planner] Could not capture session settings; server-side ' +
          'cancellation cannot be guaranteed on an unrestorable session',
      )
      return runCollection()
    }

    // Restoration failure must NOT be swallowed: when collection itself
    // succeeded, a failed restore rejects the whole operation rather than
    // silently letting withSession return a mutated connection to its
    // pool. When collection ALSO failed, the original error wins and the
    // restore failure is logged as an error alongside it. (Whether the
    // pool discards the connection on a thrown error depends on the pool
    // implementation — throwing is still strictly safer than resolving.)
    let collectionError: unknown
    try {
      try {
        await applySessionGuards(
          executor,
          params.statementTimeoutMs ?? DEFAULT_STATEMENT_TIMEOUT_MS,
          savedSettings,
        )
      } catch (guardError) {
        // Mandatory guard failed: server-side cancellation is unavailable.
        executor.serverTimeoutSupported = false
        proceedWithoutCancellation(
          '[planner] Could not install the mandatory server-side ' +
            'statement_timeout on this session',
          guardError,
        )
      }
      return await runCollection()
    } catch (error) {
      collectionError = error
      throw error
    } finally {
      try {
        await restoreSessionSettings(executor, savedSettings)
      } catch (restoreError) {
        if (!collectionError) throw restoreError
        console.error(
          '[planner] Collection and session restoration both failed; the ' +
            'connection may retain collector settings:',
          restoreError instanceof Error ? restoreError.message : restoreError,
        )
      }
    }
  }

  return runCollection()
}

export function emitPlannerGeneratedModule(
  artifacts: GeneratePlannerArtifacts,
): string {
  return [
    `export const RELATION_STATS = ${stableJson(artifacts.relationStats)} as const`,
    ``,
    `export type RelationStats = typeof RELATION_STATS`,
    ``,
    `export const MODEL_STATS = ${stableJson(artifacts.modelStats)} as const`,
    ``,
    `export type ModelStats = typeof MODEL_STATS`,
    ``,
    `export const ROUNDTRIP_ROW_EQUIVALENT = ${artifacts.roundtripRowEquivalent}`,
    ``,
    `export const JSON_ROW_FACTOR = ${artifacts.jsonRowFactor.toFixed(2)}`,
    ``,
    `export const COLLECTED_AT = ${artifacts.collectedAt}`,
    ``,
    `export const EDGE_TIMINGS = ${stableJson(artifacts.edgeTimings)}`,
    ``,
  ].join('\n')
}

export function parsePreviousArtifacts(
  moduleExports: Record<string, unknown>,
): GeneratePlannerArtifacts | null {
  const relationStats = moduleExports.RELATION_STATS
  const modelStats = moduleExports.MODEL_STATS
  const roundtrip = moduleExports.ROUNDTRIP_ROW_EQUIVALENT
  const jsonFactor = moduleExports.JSON_ROW_FACTOR
  const collectedAt = moduleExports.COLLECTED_AT
  const edgeTimings = moduleExports.EDGE_TIMINGS

  if (
    !relationStats ||
    typeof relationStats !== 'object' ||
    typeof roundtrip !== 'number' ||
    typeof jsonFactor !== 'number' ||
    typeof collectedAt !== 'number'
  ) {
    return null
  }

  return {
    relationStats: relationStats as RelationStatsMap,
    modelStats:
      modelStats && typeof modelStats === 'object'
        ? (modelStats as ModelStatsMap)
        : {},
    roundtripRowEquivalent: roundtrip,
    jsonRowFactor: jsonFactor,
    collectedAt,
    edgeTimings:
      edgeTimings && typeof edgeTimings === 'object'
        ? (edgeTimings as Record<string, EdgeTiming>)
        : {},
  }
}

export function loadExternalPlannerStats(filePath: string): boolean {
  try {
    delete require.cache[require.resolve(filePath)]
    const mod = require(filePath)

    if (mod.RELATION_STATS && typeof mod.RELATION_STATS === 'object') {
      setRelationStats(mod.RELATION_STATS)
    }
    if (mod.MODEL_STATS && typeof mod.MODEL_STATS === 'object') {
      setModelStats(mod.MODEL_STATS)
    }
    if (typeof mod.ROUNDTRIP_ROW_EQUIVALENT === 'number') {
      setRoundtripRowEquivalent(mod.ROUNDTRIP_ROW_EQUIVALENT)
    }
    if (typeof mod.JSON_ROW_FACTOR === 'number') {
      setJsonRowFactor(mod.JSON_ROW_FACTOR)
    }

    return true
  } catch {
    return false
  }
}
