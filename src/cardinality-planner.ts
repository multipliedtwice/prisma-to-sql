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
}

interface DatabaseExecutor {
  query: (
    sql: string,
    params?: unknown[],
  ) => Promise<Array<Record<string, unknown>>>
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
}): Promise<{ executor: DatabaseExecutor; cleanup: () => Promise<void> }> {
  const { databaseUrl, dialect, connectTimeoutMs = 30000 } = options

  if (dialect === 'postgres') {
    const postgres = await import('postgres')
    const sql = postgres.default(stripPrismaParams(databaseUrl), {
      connect_timeout: Math.ceil(connectTimeoutMs / 1000),
      max: 1,
    })

    return {
      executor: {
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

function buildPostgresStatsSql(edge: RelEdge): string {
  const childTable = tableRefFor('postgres', edge.childSchema, edge.childTable)
  const parentTable = tableRefFor(
    'postgres',
    edge.parentSchema,
    edge.parentTable,
  )
  const groupCols = edge.childFkColumns
    .map((c) => quoteIdent('postgres', c))
    .join(', ')

  return `
WITH counts AS (
  SELECT ${groupCols}, COUNT(*) AS cnt
  FROM ${childTable}
  GROUP BY ${groupCols}
),
total_parents AS (
  SELECT COUNT(*) AS total FROM ${parentTable}
)
SELECT
  AVG(cnt)::float AS avg,
  MAX(cnt)::int AS max,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY cnt)::float AS p95,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY cnt)::float AS p99,
  (SELECT COUNT(*) FROM counts)::float / GREATEST(1, (SELECT total FROM total_parents)) AS coverage
FROM counts
`.trim()
}

function buildSqliteStatsSql(edge: RelEdge): string {
  const childTable = tableRefFor('sqlite', undefined, edge.childTable)
  const parentTable = tableRefFor('sqlite', undefined, edge.parentTable)
  const groupCols = edge.childFkColumns
    .map((c) => quoteIdent('sqlite', c))
    .join(', ')

  return `
WITH counts AS (
  SELECT ${groupCols}, COUNT(*) AS cnt
  FROM ${childTable}
  GROUP BY ${groupCols}
),
n AS (
  SELECT COUNT(*) AS total FROM counts
),
parent_n AS (
  SELECT COUNT(*) AS total FROM ${parentTable}
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

function buildFanoutStatsSql(dialect: SqlDialect, edge: RelEdge): string {
  return dialect === 'postgres'
    ? buildPostgresStatsSql(edge)
    : buildSqliteStatsSql(edge)
}

async function collectModelStatsPostgres(
  executor: Executor,
  datamodel: DMMF.Datamodel,
): Promise<ModelStatsMap> {
  const out: ModelStatsMap = {}
  const tableToModel = new Map<string, string>()
  const unknownModels: string[] = []

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

  const shouldAnalyze = process.env.PRISMA_SQL_ANALYZE === '1'
  if (shouldAnalyze) {
    for (const model of datamodel.models) {
      const schema = (model as { schema?: string | null }).schema || 'public'
      const tableName = model.dbName || model.name
      const ref = tableRefFor('postgres', schema, tableName)
      try {
        await executor.query(`ANALYZE ${ref}`)
      } catch (_) {}
    }
  }

  const rows = await executor.query(
    `
    SELECT
      n.nspname AS schema_name,
      c.relname AS table_name,
      c.reltuples::bigint AS row_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p')
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  `,
    [],
  )

  const seenModels = new Set<string>()

  for (const row of rows) {
    const schemaName = String(row.schema_name)
    const tableName = String(row.table_name)
    const modelName = tableToModel.get(`${schemaName}.${tableName}`)
    if (!modelName) continue

    seenModels.add(modelName)
    const rawCount = toNumberOrZero(row.row_count)

    if (rawCount < 0) {
      unknownModels.push(modelName)
      continue
    }

    out[modelName].rowCount = rawCount
    out[modelName].known = true
  }

  for (const model of datamodel.models) {
    if (!seenModels.has(model.name)) {
      unknownModels.push(model.name)
    }
  }

  if (unknownModels.length > 0) {
    console.warn(
      `[planner] ${unknownModels.length} model(s) have unknown row counts ` +
        `(table not analyzed or not found): ${unknownModels.join(', ')}. ` +
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

  return out
}

async function collectModelStats(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  previousArtifacts?: GeneratePlannerArtifacts
}): Promise<ModelStatsMap> {
  const { executor, datamodel, dialect, previousArtifacts } = params

  if (dialect === 'postgres') {
    return collectModelStatsPostgres(executor, datamodel)
  }

  return collectModelStatsSqlite(executor, datamodel, previousArtifacts)
}

function findLargestTable(args: {
  modelStats: ModelStatsMap
  dialect: SqlDialect
}): { tableRef: string; rowCount: number } | null {
  const { modelStats, dialect } = args
  let best: { tableRef: string; rowCount: number } | null = null

  for (const stats of Object.values(modelStats)) {
    if (stats.known === false) continue
    if (!best || stats.rowCount > best.rowCount) {
      best = {
        tableRef: tableRefFor(dialect, stats.schemaName, stats.tableName),
        rowCount: stats.rowCount,
      }
    }
  }

  return best
}

async function measureRoundtripCost(params: {
  executor: Executor
  modelStats: ModelStatsMap
  dialect: SqlDialect
}): Promise<number> {
  const { executor, modelStats, dialect } = params
  const WARMUP = 5
  const SAMPLES = 15

  for (let i = 0; i < WARMUP; i++) {
    await executor.query('SELECT 1')
  }

  const roundtripTimes: number[] = []
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    await executor.query('SELECT 1')
    roundtripTimes.push(performance.now() - start)
  }
  roundtripTimes.sort((a, b) => a - b)
  const medianRoundtrip = roundtripTimes[Math.floor(SAMPLES / 2)]

  console.log(
    `  [roundtrip] SELECT 1 times (ms): min=${roundtripTimes[0].toFixed(3)} median=${medianRoundtrip.toFixed(3)} max=${roundtripTimes[SAMPLES - 1].toFixed(3)}`,
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
    medianRoundtrip,
    tableRowCount: largest.rowCount,
  })
}

async function estimateFromQueryPairRatio(params: {
  executor: Executor
  tableRef: string
  medianRoundtrip: number
  tableRowCount: number
}): Promise<number> {
  const { executor, tableRef, medianRoundtrip, tableRowCount } = params
  const WARMUP = 5
  const SAMPLES = 10

  const smallLimit = 1
  const largeLimit = Math.min(1000, tableRowCount)

  for (let i = 0; i < WARMUP; i++) {
    await executor.query(`SELECT * FROM ${tableRef} LIMIT ${largeLimit}`)
  }

  const smallTimes: number[] = []
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    await executor.query(`SELECT * FROM ${tableRef} LIMIT ${smallLimit}`)
    smallTimes.push(performance.now() - start)
  }
  smallTimes.sort((a, b) => a - b)
  const medianSmall = smallTimes[Math.floor(SAMPLES / 2)]

  const largeTimes: number[] = []
  let actualLargeRows = 0
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    const rows = await executor.query(
      `SELECT * FROM ${tableRef} LIMIT ${largeLimit}`,
    )
    largeTimes.push(performance.now() - start)
    actualLargeRows = rows.length
  }
  largeTimes.sort((a, b) => a - b)
  const medianLarge = largeTimes[Math.floor(SAMPLES / 2)]

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
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    await executor.query(`SELECT * FROM ${tableRef} LIMIT ${smallLimit}`)
    await executor.query(`SELECT * FROM ${tableRef} LIMIT ${smallLimit}`)
    await executor.query(`SELECT * FROM ${tableRef} LIMIT ${smallLimit}`)
    sequentialTimes.push(performance.now() - start)
  }
  sequentialTimes.sort((a, b) => a - b)
  const median3Sequential = sequentialTimes[Math.floor(SAMPLES / 2)]

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
  tableRowCount: number
}): Promise<number> {
  const { executor, tableRef, tableRowCount } = params
  const WARMUP = 3
  const SAMPLES = 10
  const limit = Math.min(500, tableRowCount)

  const rawSql = `SELECT * FROM ${tableRef} LIMIT ${limit}`

  const aggSql = `
    WITH sample AS (
      SELECT * FROM ${tableRef} LIMIT ${limit}
    )
    SELECT COALESCE(json_agg(sample), '[]'::json) AS rows
    FROM sample
  `.trim()

  for (let i = 0; i < WARMUP; i++) {
    await executor.query(rawSql)
    await executor.query(aggSql)
  }

  const rawTimes: number[] = []
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    await executor.query(rawSql)
    rawTimes.push(performance.now() - start)
  }
  rawTimes.sort((a, b) => a - b)
  const medianRaw = rawTimes[Math.floor(SAMPLES / 2)]

  const aggTimes: number[] = []
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    await executor.query(aggSql)
    aggTimes.push(performance.now() - start)
  }
  aggTimes.sort((a, b) => a - b)
  const medianAgg = aggTimes[Math.floor(SAMPLES / 2)]

  const factor = medianRaw > 0.01 ? medianAgg / medianRaw : 3.0

  console.log(`  [json] Raw ${limit} rows: ${medianRaw.toFixed(3)}ms`)
  console.log(`  [json] json_agg ${limit} rows: ${medianAgg.toFixed(3)}ms`)
  console.log(`  [json] Overhead factor: ${factor.toFixed(2)}x`)

  return Math.max(1.5, Math.min(8.0, factor))
}

async function collectPostgresStatsFromCatalog(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
}): Promise<{ stats: RelationStatsMap; timings: Record<string, EdgeTiming> }> {
  const { executor, datamodel } = params
  const edges = extractMeasurableOneToManyEdges(datamodel)
  const out: RelationStatsMap = {}
  const timings: Record<string, EdgeTiming> = {}
  const now = Date.now()

  const tablesToAnalyze = new Map<
    string,
    { schema: string; table: string }
  >()
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
      try {
        await executor.query(
          `ANALYZE ${tableRefFor('postgres', schema, table)}`,
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

  const tableStats = await executor.query(tableStatsQuery, [])
  const rowCounts = new Map<string, number>()

  for (const row of tableStats) {
    const schemaName = String(row.schema_name)
    const tableName = String(row.table_name)
    const count = toNumberOrZero(row.row_count)
    rowCounts.set(`${schemaName}.${tableName}`, count)
  }

  for (const edge of edges) {
    const key = edgeKey(edge)
    const start = performance.now()
    const parentSchema = edge.parentSchema || 'public'
    const childSchema = edge.childSchema || 'public'
    const parentRows =
      rowCounts.get(`${parentSchema}.${edge.parentTable}`) || 0
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

    const statsRows = await executor.query(statsQuery, [
      childSchema,
      edge.childTable,
      fkColumn,
    ])

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

async function collectPreciseCardinalities(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  previousArtifacts?: GeneratePlannerArtifacts
  slowEdgeThresholdMs?: number
  perEdgeTimeoutMs?: number
  staleEdgeHours?: number
}): Promise<{ stats: RelationStatsMap; timings: Record<string, EdgeTiming> }> {
  const {
    executor,
    datamodel,
    dialect,
    previousArtifacts,
    slowEdgeThresholdMs = 10000,
    perEdgeTimeoutMs = 30000,
    staleEdgeHours = 168,
  } = params
  const edges = extractMeasurableOneToManyEdges(datamodel)
  const out: RelationStatsMap = {}
  const timings: Record<string, EdgeTiming> = {}
  const now = Date.now()

  for (const edge of edges) {
    const key = edgeKey(edge)
    const prevTiming = previousArtifacts?.edgeTimings[key]
    const prevStats =
      previousArtifacts?.relationStats[edge.parentModel]?.[edge.relName]

    if (prevTiming && prevStats) {
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

    const sql = buildFanoutStatsSql(dialect, edge)
    const start = performance.now()

    try {
      const rows = await Promise.race([
        executor.query(sql, []),
        new Promise<never>((_, reject) => {
          const id = setTimeout(
            () => reject(new Error('timeout')),
            perEdgeTimeoutMs,
          )
          if (typeof id === 'object' && 'unref' in id) id.unref()
        }),
      ])

      const elapsed = performance.now() - start
      timings[key] = { ms: elapsed, measuredAt: now }

      const row = rows[0] || {}
      const stats = normalizeStats(row)

      if (!out[edge.parentModel]) out[edge.parentModel] = {}
      out[edge.parentModel][edge.relName] = stats

      if (elapsed > 5000) {
        console.log(`  ⚠ ${key}: ${(elapsed / 1000).toFixed(1)}s`)
      }
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

      timings[key] = { ms: elapsed, measuredAt: now }
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
  } = params

  if (dialect === 'postgres' && mode === 'fast') {
    const result = await collectPostgresStatsFromCatalog({
      executor,
      datamodel,
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

    if (allTrivial && Object.keys(result.stats).length > 0) {
      console.warn('⚠ Catalog stats look stale, falling back to precise mode')
      return collectPreciseCardinalities({
        executor,
        datamodel,
        dialect,
        previousArtifacts,
        slowEdgeThresholdMs,
        perEdgeTimeoutMs,
        staleEdgeHours,
      })
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

  console.log('📊 Collecting model row counts...')
  const modelStats = await collectModelStats({
    executor,
    datamodel,
    dialect,
    previousArtifacts,
  })

  const largest = findLargestTable({ modelStats, dialect })

  const [cardinalityResult, roundtripRowEquivalent, jsonRowFactor] =
    await Promise.all([
      collectRelationCardinalities({
        executor,
        datamodel,
        dialect,
        mode,
        previousArtifacts,
        slowEdgeThresholdMs,
        perEdgeTimeoutMs,
        staleEdgeHours,
      }),
      measureRoundtripCost({ executor, modelStats, dialect }),
      largest && largest.rowCount >= 50 && dialect === 'postgres'
        ? measureJsonOverhead({
            executor,
            tableRef: largest.tableRef,
            tableRowCount: largest.rowCount,
          })
        : Promise.resolve(1.5),
    ])

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

  return {
    relationStats: cardinalityResult.stats,
    modelStats,
    roundtripRowEquivalent,
    jsonRowFactor,
    collectedAt: Date.now(),
    edgeTimings: cardinalityResult.timings,
  }
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

/**
 * Load planner stats from an external file path at runtime.
 * Applies RELATION_STATS, MODEL_STATS, ROUNDTRIP_ROW_EQUIVALENT, and JSON_ROW_FACTOR
 * to the global strategy estimator. Returns true if loaded successfully.
 */
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
