import { DMMF } from '@prisma/generator-helper'
import {
  toNumberOrZero,
  clampStatsMonotonic,
  normalizeStats,
  stableJson,
  cleanDatabaseUrl,
} from './utils/pure-utils'
import { SqlDialect } from './sql-builder-dialect'

type Executor = {
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

export type GeneratePlannerArtifacts = {
  relationStats: RelationStatsMap
  roundtripRowEquivalent: number
  jsonRowFactor: number
}

type RelEdge = {
  parentModel: string
  relName: string
  childModel: string
  parentTable: string
  childTable: string
  parentPkColumns: string[]
  childFkColumns: string[]
  isMany: boolean
}

function quoteIdent(dialect: SqlDialect, ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`
}

export async function createDatabaseExecutor(params: {
  databaseUrl: string
  dialect: SqlDialect
}): Promise<{ executor: Executor; cleanup: () => Promise<void> }> {
  const { databaseUrl, dialect } = params

  if (dialect === 'postgres') {
    const postgres = await import('postgres')
    const cleanUrl = cleanDatabaseUrl(databaseUrl)
    const sql = postgres.default(databaseUrl, { max: 1 })

    return {
      executor: {
        query: async (sqlStr: string, params?: unknown[]) => {
          return await sql.unsafe(sqlStr, (params || []) as any[])
        },
      },
      cleanup: async () => {
        await sql.end()
      },
    }
  }

  throw new Error(`Dialect ${dialect} not supported for stats collection`)
}

function extractMeasurableOneToManyEdges(datamodel: DMMF.Datamodel): RelEdge[] {
  const modelByName = new Map(datamodel.models.map((m) => [m.name, m]))
  const edges: RelEdge[] = []

  for (const parent of datamodel.models) {
    const pkFields = parent.fields.filter((f) => f.isId)
    if (pkFields.length === 0) continue

    const parentPk = pkFields.map((f) => f.dbName || f.name)
    const parentTable = parent.dbName || parent.name

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

      edges.push({
        parentModel: parent.name,
        relName: f.name,
        childModel: child.name,
        parentTable,
        childTable,
        parentPkColumns: references,
        childFkColumns: fkFields,
        isMany: true,
      })
    }
  }

  return edges
}

function buildPostgresStatsSql(edge: RelEdge): string {
  const childTable = quoteIdent('postgres', edge.childTable)
  const parentTable = quoteIdent('postgres', edge.parentTable)
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
  const childTable = quoteIdent('sqlite', edge.childTable)
  const parentTable = quoteIdent('sqlite', edge.parentTable)
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

async function findLargestTable(params: {
  executor: Executor
  dialect: SqlDialect
  datamodel: DMMF.Datamodel
}): Promise<{ tableName: string; rowCount: number } | null> {
  const { executor, dialect, datamodel } = params

  let best: { tableName: string; rowCount: number } | null = null

  for (const model of datamodel.models) {
    const table = quoteIdent(dialect, model.dbName || model.name)
    try {
      const rows = await executor.query(`SELECT COUNT(*) AS cnt FROM ${table}`)
      const count = toNumberOrZero(rows[0]?.cnt)
      if (!best || count > best.rowCount) {
        best = { tableName: table, rowCount: count }
      }
    } catch (_) {}
  }

  return best
}

async function measureRoundtripCost(params: {
  executor: Executor
  dialect: SqlDialect
  datamodel: DMMF.Datamodel
}): Promise<number> {
  const { executor, dialect, datamodel } = params
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

  const largest = await findLargestTable({ executor, dialect, datamodel })

  if (!largest || largest.rowCount < 50) {
    console.log(
      `  [roundtrip] Largest table: ${largest?.tableName ?? 'none'} (${largest?.rowCount ?? 0} rows) — too small, using default 50`,
    )
    return 50
  }

  console.log(
    `  [roundtrip] Using table ${largest.tableName} (${largest.rowCount} rows)`,
  )

  return estimateFromQueryPairRatio({
    executor,
    tableName: largest.tableName,
    medianRoundtrip,
    tableRowCount: largest.rowCount,
  })
}

async function estimateFromQueryPairRatio(params: {
  executor: Executor
  tableName: string
  medianRoundtrip: number
  tableRowCount: number
}): Promise<number> {
  const { executor, tableName, medianRoundtrip, tableRowCount } = params
  const WARMUP = 5
  const SAMPLES = 10

  const smallLimit = 1
  const largeLimit = Math.min(1000, tableRowCount)

  for (let i = 0; i < WARMUP; i++) {
    await executor.query(`SELECT * FROM ${tableName} LIMIT ${largeLimit}`)
  }

  const smallTimes: number[] = []
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    await executor.query(`SELECT * FROM ${tableName} LIMIT ${smallLimit}`)
    smallTimes.push(performance.now() - start)
  }
  smallTimes.sort((a, b) => a - b)
  const medianSmall = smallTimes[Math.floor(SAMPLES / 2)]

  const largeTimes: number[] = []
  let actualLargeRows = 0
  for (let i = 0; i < SAMPLES; i++) {
    const start = performance.now()
    const rows = await executor.query(
      `SELECT * FROM ${tableName} LIMIT ${largeLimit}`,
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
    await executor.query(`SELECT * FROM ${tableName} LIMIT ${smallLimit}`)
    await executor.query(`SELECT * FROM ${tableName} LIMIT ${smallLimit}`)
    await executor.query(`SELECT * FROM ${tableName} LIMIT ${smallLimit}`)
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
  tableName: string
  tableRowCount: number
}): Promise<number> {
  const { executor, tableName, tableRowCount } = params
  const WARMUP = 3
  const SAMPLES = 10
  const limit = Math.min(500, tableRowCount)

  const rawSql = `SELECT * FROM ${tableName} LIMIT ${limit}`

  const colsResult = await executor.query(
    `SELECT column_name FROM information_schema.columns WHERE table_name = ${tableName.replace(/"/g, "'")} LIMIT 10`,
  )

  let aggSql: string
  if (colsResult.length >= 3) {
    const cols = colsResult.slice(0, 6).map((r) => `"${r.column_name}"`)
    const aggExprs = cols.map((c) => `array_agg(${c})`).join(', ')
    const groupCol = cols[0]
    aggSql = `SELECT ${groupCol}, ${aggExprs} FROM ${tableName} GROUP BY ${groupCol} LIMIT ${limit}`
  } else {
    aggSql = `SELECT json_agg(t) FROM (SELECT * FROM ${tableName} LIMIT ${limit}) t`
  }

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
  console.log(`  [json] array_agg grouped: ${medianAgg.toFixed(3)}ms`)
  console.log(`  [json] Overhead factor: ${factor.toFixed(2)}x`)

  return Math.max(1.5, Math.min(8.0, factor))
}

async function collectPostgresStatsFromCatalog(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
}): Promise<RelationStatsMap> {
  const { executor, datamodel } = params
  const edges = extractMeasurableOneToManyEdges(datamodel)
  const out: RelationStatsMap = {}

  const tablesToAnalyze = new Set<string>()
  for (const edge of edges) {
    tablesToAnalyze.add(edge.parentTable)
    tablesToAnalyze.add(edge.childTable)
  }

  for (const table of tablesToAnalyze) {
    try {
      await executor.query(`ANALYZE ${quoteIdent('postgres', table)}`)
    } catch (_) {}
  }

  const tableStatsQuery = `
    SELECT
      c.relname as table_name,
      c.reltuples::bigint as row_count
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  `

  const tableStats = await executor.query(tableStatsQuery, [])
  const rowCounts = new Map<string, number>()

  for (const row of tableStats) {
    const tableName = String(row.table_name)
    const count = toNumberOrZero(row.row_count)
    rowCounts.set(tableName, count)
  }

  for (const edge of edges) {
    const parentRows = rowCounts.get(edge.parentTable) || 0
    const childRows = rowCounts.get(edge.childTable) || 0

    if (parentRows === 0 || childRows === 0) {
      if (!out[edge.parentModel]) out[edge.parentModel] = {}
      out[edge.parentModel][edge.relName] = {
        avg: 1,
        p95: 1,
        p99: 1,
        max: 1,
        coverage: 0,
      }
      continue
    }

    const fkColumn = edge.childFkColumns[0]

    const statsQuery = `
      SELECT
        s.n_distinct,
        s.correlation,
        (s.most_common_freqs)[1] as max_freq
      FROM pg_stats s
      WHERE s.tablename = $1
        AND s.attname = $2
        AND s.schemaname NOT IN ('pg_catalog', 'information_schema')
    `

    const statsRows = await executor.query(statsQuery, [
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
  }

  return out
}

async function collectPreciseCardinalities(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
}): Promise<RelationStatsMap> {
  const { executor, datamodel, dialect } = params
  const edges = extractMeasurableOneToManyEdges(datamodel)
  const out: RelationStatsMap = {}

  for (const edge of edges) {
    const sql = buildFanoutStatsSql(dialect, edge)
    const rows = await executor.query(sql, [])
    const row = rows[0] || {}
    const stats = normalizeStats(row)

    if (!out[edge.parentModel]) out[edge.parentModel] = {}
    out[edge.parentModel][edge.relName] = stats
  }

  return out
}

async function collectRelationCardinalities(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  mode?: 'fast' | 'precise'
}): Promise<RelationStatsMap> {
  const { executor, datamodel, dialect, mode = 'precise' } = params

  if (dialect === 'postgres' && mode === 'fast') {
    const stats = await collectPostgresStatsFromCatalog({ executor, datamodel })

    let allTrivial = true
    for (const model of Object.values(stats)) {
      for (const rel of Object.values(model)) {
        if (rel.avg > 1 || rel.coverage > 0.5) {
          allTrivial = false
          break
        }
      }
      if (!allTrivial) break
    }

    if (allTrivial && Object.keys(stats).length > 0) {
      console.warn('⚠ Catalog stats look stale, falling back to precise mode')
      return collectPreciseCardinalities({ executor, datamodel, dialect })
    }

    return stats
  }

  return collectPreciseCardinalities({ executor, datamodel, dialect })
}

export async function collectPlannerArtifacts(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: SqlDialect
  mode?: 'fast' | 'precise'
}): Promise<GeneratePlannerArtifacts> {
  const { executor, datamodel, dialect, mode } = params

  const largest = await findLargestTable({ executor, dialect, datamodel })

  const [relationStats, roundtripRowEquivalent, jsonRowFactor] =
    await Promise.all([
      collectRelationCardinalities({ executor, datamodel, dialect, mode }),
      measureRoundtripCost({ executor, dialect, datamodel }),
      largest && largest.rowCount >= 50 && dialect === 'postgres'
        ? measureJsonOverhead({
            executor,
            tableName: largest.tableName,
            tableRowCount: largest.rowCount,
          })
        : Promise.resolve(1.5),
    ])

  console.log(`  Roundtrip cost: ~${roundtripRowEquivalent} row equivalents`)
  console.log(`  JSON overhead factor: ${jsonRowFactor.toFixed(2)}x`)

  return { relationStats, roundtripRowEquivalent, jsonRowFactor }
}

export function emitPlannerGeneratedModule(
  artifacts: GeneratePlannerArtifacts,
): string {
  return [
    `export const RELATION_STATS = ${stableJson(artifacts.relationStats)} as const`,
    ``,
    `export type RelationStats = typeof RELATION_STATS`,
    ``,
    `export const ROUNDTRIP_ROW_EQUIVALENT = ${artifacts.roundtripRowEquivalent}`,
    ``,
    `export const JSON_ROW_FACTOR = ${artifacts.jsonRowFactor.toFixed(2)}`,
    ``,
  ].join('\n')
}
