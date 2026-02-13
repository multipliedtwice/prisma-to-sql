import { DMMF } from '@prisma/generator-helper'
import {
  toNumberOrZero,
  clampStatsMonotonic,
  normalizeStats,
  stableJson,
  cleanDatabaseUrl,
} from './utils/pure-utils'

export type Dialect = 'postgres' | 'sqlite'

export type Executor = {
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

function quoteIdent(dialect: Dialect, ident: string): string {
  return `"${ident.replace(/"/g, '""')}"`
}

export async function createDatabaseExecutor(params: {
  databaseUrl: string
  dialect: Dialect
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

export function extractMeasurableOneToManyEdges(
  datamodel: DMMF.Datamodel,
): RelEdge[] {
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

export function buildFanoutStatsSql(dialect: Dialect, edge: RelEdge): string {
  return dialect === 'postgres'
    ? buildPostgresStatsSql(edge)
    : buildSqliteStatsSql(edge)
}

async function collectPostgresStatsFromCatalog(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
}): Promise<RelationStatsMap> {
  const { executor, datamodel } = params
  const edges = extractMeasurableOneToManyEdges(datamodel)
  const out: RelationStatsMap = {}

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
  dialect: Dialect
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

export async function collectRelationCardinalities(params: {
  executor: Executor
  datamodel: DMMF.Datamodel
  dialect: Dialect
  mode?: 'fast' | 'precise'
}): Promise<RelationStatsMap> {
  const { executor, datamodel, dialect, mode = 'fast' } = params

  if (dialect === 'postgres' && mode === 'fast') {
    return collectPostgresStatsFromCatalog({ executor, datamodel })
  }

  return collectPreciseCardinalities({ executor, datamodel, dialect })
}

export function emitPlannerGeneratedModule(
  artifacts: GeneratePlannerArtifacts,
): string {
  return [
    `export const RELATION_STATS = ${stableJson(artifacts.relationStats)} as const`,
    ``,
    `export type RelationStats = typeof RELATION_STATS`,
    ``,
  ].join('\n')
}
