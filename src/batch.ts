import type { Model } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import type { PrismaMethod } from './result-transformers'
import { buildSQLWithCache } from './query-cache'
import { transformQueryResults } from './result-transformers'

export interface BatchQuery {
  model: string
  method: PrismaMethod
  args?: Record<string, unknown>
}

export interface BatchCountQuery {
  model: string
  method: 'count'
  args?: { where?: Record<string, unknown> }
}

export interface BatchResult {
  sql: string
  params: unknown[]
}

function assertNoControlChars(label: string, s: string): void {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i)
    if (c <= 31 || c === 127) {
      throw new Error(`${label} contains control characters`)
    }
  }
}

function quoteIdent(id: string): string {
  const raw = String(id)
  if (raw.length === 0) throw new Error('Identifier cannot be empty')
  assertNoControlChars('Identifier', raw)
  return `"${raw.replace(/"/g, '""')}"`
}

function reindexParams(
  sql: string,
  params: readonly unknown[],
  offset: number,
): { sql: string; params: unknown[] } {
  if (!Number.isInteger(offset) || offset < 0) {
    throw new Error(`Invalid param offset: ${offset}`)
  }

  const newParams: unknown[] = []
  const paramMap = new Map<number, number>()

  const reindexed = sql.replace(/\$(\d+)/g, (_match, num) => {
    const oldIndex = Number(num)
    if (!Number.isInteger(oldIndex) || oldIndex < 1) {
      throw new Error(`Invalid param placeholder: $${num}`)
    }

    const existing = paramMap.get(oldIndex)
    if (existing !== undefined) return `$${existing}`

    const pos = oldIndex - 1
    if (pos >= params.length) {
      throw new Error(
        `Param placeholder $${oldIndex} exceeds params length (${params.length})`,
      )
    }

    const newIndex = offset + newParams.length + 1
    paramMap.set(oldIndex, newIndex)
    newParams.push(params[pos])
    return `$${newIndex}`
  })

  return { sql: reindexed, params: newParams }
}

function wrapQueryForMethod(
  method: PrismaMethod,
  cteName: string,
  resultKey: string,
): string {
  const outKey = quoteIdent(resultKey)

  switch (method) {
    case 'findMany':
    case 'groupBy':
      return `(SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM ${cteName} t) AS ${outKey}`

    case 'findFirst':
    case 'findUnique':
      return `(SELECT row_to_json(t) FROM ${cteName} t LIMIT 1) AS ${outKey}`

    case 'count':
      return `(SELECT * FROM ${cteName}) AS ${outKey}`

    case 'aggregate':
      return `(SELECT row_to_json(t) FROM ${cteName} t) AS ${outKey}`

    default:
      throw new Error(`Unsupported batch method: ${method}`)
  }
}

export function buildBatchSql(
  queries: Record<string, BatchQuery>,
  modelMap: Map<string, Model>,
  models: Model[],
  dialect: SqlDialect,
): BatchResult & { keys: string[] } {
  const keys = Object.keys(queries)

  if (keys.length === 0) {
    throw new Error('buildBatchSql requires at least one query')
  }

  if (dialect !== 'postgres') {
    throw new Error('Batch queries are only supported for postgres dialect')
  }

  const ctes: string[] = new Array(keys.length)
  const selects: string[] = new Array(keys.length)
  const allParams: unknown[] = []

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    const query = queries[key]

    const model = modelMap.get(query.model)
    if (!model) {
      throw new Error(
        `Model '${query.model}' not found. Available: ${[...modelMap.keys()].join(', ')}`,
      )
    }

    const { sql: querySql, params: queryParams } = buildSQLWithCache(
      model,
      models,
      query.method,
      (query.args || {}) as Record<string, unknown>,
      dialect,
    )

    const { sql: reindexedSql, params: reindexedParams } = reindexParams(
      querySql,
      queryParams,
      allParams.length,
    )

    for (let p = 0; p < reindexedParams.length; p++) {
      allParams.push(reindexedParams[p])
    }

    const cteName = `batch_${i}`
    ctes[i] = `${cteName} AS (${reindexedSql})`
    selects[i] = wrapQueryForMethod(query.method, cteName, key)
  }

  const sql = `WITH ${ctes.join(', ')} SELECT ${selects.join(', ')}`

  return { sql, params: allParams, keys }
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
    throw new Error(
      'Batch count queries are only supported for postgres dialect',
    )
  }

  const ctes: string[] = new Array(queries.length)
  const selects: string[] = new Array(queries.length)
  const allParams: unknown[] = []

  for (let i = 0; i < queries.length; i++) {
    const query = queries[i]

    const model = modelMap.get(query.model)
    if (!model) {
      throw new Error(
        `Model '${query.model}' not found. Available: ${[...modelMap.keys()].join(', ')}`,
      )
    }

    const { sql: querySql, params: queryParams } = buildSQLWithCache(
      model,
      models,
      'count',
      (query.args || {}) as Record<string, unknown>,
      dialect,
    )

    const { sql: reindexedSql, params: reindexedParams } = reindexParams(
      querySql,
      queryParams,
      allParams.length,
    )

    for (let p = 0; p < reindexedParams.length; p++) {
      allParams.push(reindexedParams[p])
    }

    const cteName = `count_${i}`
    const resultKey = `count_${i}`
    ctes[i] = `${cteName} AS (${reindexedSql})`
    selects[i] = `(SELECT * FROM ${cteName}) AS ${quoteIdent(resultKey)}`
  }

  const sql = `WITH ${ctes.join(', ')} SELECT ${selects.join(', ')}`

  return { sql, params: allParams }
}

function looksLikeJsonString(s: string): boolean {
  const t = s.trim()
  if (t.length === 0) return false
  const c0 = t.charCodeAt(0)
  const cN = t.charCodeAt(t.length - 1)
  if (c0 === 123 && cN === 125) return true
  if (c0 === 91 && cN === 93) return true
  if (t === 'null' || t === 'true' || t === 'false') return true
  return false
}

function parseJsonValue(value: unknown): unknown {
  if (typeof value !== 'string') return value
  if (!looksLikeJsonString(value)) return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

function parseCountValue(value: unknown): number {
  if (value === null || value === undefined) return 0
  if (typeof value === 'number') return value
  if (typeof value === 'string') {
    const n = Number.parseInt(value, 10)
    return Number.isFinite(n) ? n : 0
  }
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>
    const countKey = Object.prototype.hasOwnProperty.call(obj, 'count')
      ? 'count'
      : Object.prototype.hasOwnProperty.call(obj, '_count')
        ? '_count'
        : Object.keys(obj).find((k) => k.endsWith('_count'))

    if (countKey !== undefined) {
      const v = obj[countKey]
      if (typeof v === 'number') return v
      if (typeof v === 'string') {
        const n = Number.parseInt(v, 10)
        return Number.isFinite(n) ? n : 0
      }
    }
  }
  return 0
}

export function parseBatchCountResults(
  row: Record<string, unknown>,
  count: number,
): number[] {
  const results: number[] = []

  for (let i = 0; i < count; i++) {
    const key = `count_${i}`
    const value = row[key]
    results.push(parseCountValue(value))
  }

  return results
}

export function parseBatchResults(
  row: Record<string, unknown>,
  keys: string[],
  queries: Record<string, BatchQuery>,
): Record<string, unknown> {
  const results: Record<string, unknown> = {}

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    const rawValue = row[key]
    const query = queries[key]

    switch (query.method) {
      case 'findMany': {
        const parsed = parseJsonValue(rawValue)
        results[key] = Array.isArray(parsed) ? parsed : []
        break
      }

      case 'findFirst':
      case 'findUnique': {
        const parsed = parseJsonValue(rawValue)
        results[key] = parsed ?? null
        break
      }

      case 'count': {
        results[key] = parseCountValue(rawValue)
        break
      }

      case 'aggregate': {
        const parsed = parseJsonValue(rawValue)
        const obj = (parsed ?? {}) as Record<string, unknown>
        results[key] = transformQueryResults('aggregate', [obj])
        break
      }

      case 'groupBy': {
        const parsed = parseJsonValue(rawValue)
        const arr = Array.isArray(parsed) ? parsed : []
        results[key] = transformQueryResults('groupBy', arr)
        break
      }

      default:
        results[key] = rawValue
    }
  }

  return results
}
