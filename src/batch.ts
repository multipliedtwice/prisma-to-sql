import type { Model } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import { buildWhereClause } from './builder/where'
import { buildTableReference } from './builder/shared/sql-utils'
import { SQL_TEMPLATES, SQL_RESERVED_WORDS } from './builder/shared/constants'
import { isValidWhereClause } from './builder/shared/validators/sql-validators'

export interface BatchCountQuery {
  model: string
  method: 'count'
  args?: { where?: Record<string, unknown> }
}

export interface BatchResult {
  sql: string
  params: unknown[]
}

function makeAlias(name: string): string {
  const base = name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 50)
  const safe = /^[a-z_]/.test(base) ? base : `_${base}`
  return SQL_RESERVED_WORDS.has(safe) ? `${safe}_t` : safe
}

function reindexWhereClause(
  clause: string,
  whereParams: readonly unknown[],
  paramOffset: number,
): { reindexedClause: string; reindexedParams: unknown[] } {
  const reindexedParams: unknown[] = []
  const usedOriginalIndices = new Map<number, number>()

  const reindexedClause = clause.replace(/\$(\d+)/g, (_match, num) => {
    const originalIndex = parseInt(num, 10) - 1

    if (usedOriginalIndices.has(originalIndex)) {
      return `$${usedOriginalIndices.get(originalIndex)}`
    }

    const newIndex = paramOffset + reindexedParams.length + 1
    usedOriginalIndices.set(originalIndex, newIndex)
    reindexedParams.push(whereParams[originalIndex])
    return `$${newIndex}`
  })

  return { reindexedClause, reindexedParams }
}

export function buildBatchCountSql(
  queries: BatchCountQuery[],
  modelMap: Map<string, Model>,
  models: Model[],
  dialect: SqlDialect,
): BatchResult {
  if (queries.length === 0) {
    throw new Error('buildBatchCountSql requires at least one query')
  }

  if (dialect !== 'postgres') {
    throw new Error('Batch count is only supported for postgres dialect')
  }

  const allParams: unknown[] = []
  const subqueries: string[] = []

  for (let i = 0; i < queries.length; i++) {
    const q = queries[i]

    if (q.method !== 'count') {
      throw new Error(
        `Batch currently only supports count queries, got: ${q.method}`,
      )
    }

    const model = modelMap.get(q.model)
    if (!model) {
      throw new Error(
        `Model '${q.model}' not found. Available: ${[...modelMap.keys()].join(', ')}`,
      )
    }

    const where = q.args?.where ?? {}
    const tableName = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      model.tableName,
      dialect,
    )
    const alias = makeAlias(model.tableName)

    const whereResult = buildWhereClause(where, {
      alias,
      schemaModels: models,
      model,
      path: ['where'],
      isSubquery: false,
      dialect,
    })

    if (isValidWhereClause(whereResult.clause)) {
      const { reindexedClause, reindexedParams } = reindexWhereClause(
        whereResult.clause,
        whereResult.params,
        allParams.length,
      )
      allParams.push(...reindexedParams)
      subqueries.push(
        `(SELECT COUNT(*) FROM ${tableName} ${alias} WHERE ${reindexedClause}) AS "${i}"`,
      )
    } else {
      subqueries.push(`(SELECT COUNT(*) FROM ${tableName} ${alias}) AS "${i}"`)
    }
  }

  const sql = `SELECT ${subqueries.join(', ')}`

  return { sql, params: allParams }
}

export function parseBatchCountResults(
  row: Record<string, unknown>,
  queryCount: number,
): number[] {
  const results: number[] = []
  for (let i = 0; i < queryCount; i++) {
    const val = row[String(i)]
    if (typeof val === 'string') {
      results.push(parseInt(val, 10))
    } else if (typeof val === 'number') {
      results.push(val)
    } else if (typeof val === 'bigint') {
      results.push(Number(val))
    } else {
      results.push(0)
    }
  }
  return results
}
