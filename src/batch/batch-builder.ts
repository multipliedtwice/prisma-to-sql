import type { Model, PrismaMethod } from '../types'
import type { SqlDialect } from '../sql-builder-dialect'

import { buildSQLWithCache } from '../query-cache'
import { assertSafeAlias } from '../builder/shared/sql-utils'
import {
  reindexPlaceholders,
  containsPlaceholder,
} from '../builder/shared/sql-param-scanner'
import { parseSimpleCountSql } from './count-sql-parser'

const BATCH_ALIAS_PREFIX = 'k'
const BATCH_CTE_PREFIX = 'batch_'
const COUNT_CTE_PREFIX = 'count_'
const MODEL_SUBQUERY_PREFIX = 'm_'
const CTE_ROW_ALIAS = 't'
const BATCH_ORD_ALIAS = '__tp_s'
const BATCH_ORD_COL = 'n'
const BATCH_ROW_COL = 'r'

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

function quoteBatchIdent(id: string): string {
  const raw = String(id)
  assertSafeAlias(raw)
  return `"${raw.replace(/"/g, '""')}"`
}

function makeBatchAlias(i: number): string {
  return `${BATCH_ALIAS_PREFIX}${i}`
}

function resolveModelOrThrow(
  modelName: string,
  modelMap: Map<string, Model>,
): Model {
  const model = modelMap.get(modelName)
  if (!model) {
    throw new Error(
      `Model '${modelName}' not found. Available: ${[...modelMap.keys()].join(', ')}`,
    )
  }
  return model
}

function buildAndReindexQuery(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
  paramOffset: number,
): { sql: string; params: unknown[] } {
  const built = buildSQLWithCache(model, models, method, args, dialect)
  return reindexPlaceholders(built.sql, built.params, paramOffset)
}

function appendParams(target: unknown[], source: unknown[]): void {
  for (const p of source) {
    target.push(p)
  }
}

function wrapOrderedAgg(cteName: string, resultAlias: string): string {
  const outKey = quoteBatchIdent(resultAlias)
  return (
    `(SELECT COALESCE(json_agg(${BATCH_ORD_ALIAS}.${BATCH_ROW_COL} ORDER BY ${BATCH_ORD_ALIAS}.${BATCH_ORD_COL}), '[]'::json) ` +
    `FROM (SELECT row_to_json(${CTE_ROW_ALIAS}) AS ${BATCH_ROW_COL}, ROW_NUMBER() OVER () AS ${BATCH_ORD_COL} ` +
    `FROM ${cteName} ${CTE_ROW_ALIAS}) ${BATCH_ORD_ALIAS}) AS ${outKey}`
  )
}

function wrapQueryForMethod(
  method: PrismaMethod,
  cteName: string,
  resultAlias: string,
): string {
  const outKey = quoteBatchIdent(resultAlias)

  switch (method) {
    case 'findMany':
    case 'groupBy':
      return wrapOrderedAgg(cteName, resultAlias)
    case 'findFirst':
    case 'findUnique':
      return `(SELECT row_to_json(${CTE_ROW_ALIAS}) FROM ${cteName} ${CTE_ROW_ALIAS} LIMIT 1) AS ${outKey}`
    case 'count':
      return `(SELECT * FROM ${cteName}) AS ${outKey}`
    case 'aggregate':
      return `(SELECT row_to_json(${CTE_ROW_ALIAS}) FROM ${cteName} ${CTE_ROW_ALIAS}) AS ${outKey}`
    default:
      throw new Error(`Unsupported batch method: ${method}`)
  }
}

function isAllCountQueries(
  queries: Record<string, BatchQuery>,
  keys: string[],
) {
  for (const key of keys) {
    if (queries[key]?.method !== 'count') return false
  }
  return true
}

interface CountQueryItem {
  key: string
  alias: string
  args: Record<string, unknown>
}

interface CountSubquery {
  alias: string
  sql: string
  params: unknown[]
  keys: string[]
  aliases: string[]
}

function processCountQuery(
  item: CountQueryItem,
  model: Model,
  models: Model[],
  dialect: SqlDialect,
  sharedFrom: string | null,
  localParams: unknown[],
): {
  expression: string
  reindexedParams: unknown[]
  sharedFrom: string
} | null {
  const built = buildSQLWithCache(model, models, 'count', item.args, dialect)
  const parsed = parseSimpleCountSql(built.sql)

  if (!parsed) return null
  if (containsPlaceholder(parsed.fromSql)) return null

  const currentFrom = parsed.fromSql
  if (sharedFrom !== null && sharedFrom !== currentFrom) return null

  if (!parsed.whereSql) {
    if (built.params.length > 0) return null
    return {
      expression: `count(*) AS ${quoteBatchIdent(item.alias)}`,
      reindexedParams: [],
      sharedFrom: currentFrom,
    }
  }

  const re = reindexPlaceholders(
    parsed.whereSql,
    built.params,
    localParams.length,
  )
  return {
    expression: `count(*) FILTER (WHERE ${re.sql}) AS ${quoteBatchIdent(item.alias)}`,
    reindexedParams: re.params,
    sharedFrom: currentFrom,
  }
}

function buildCountSubqueriesForModel(
  items: CountQueryItem[],
  model: Model,
  models: Model[],
  dialect: SqlDialect,
  aliasIndex: number,
): CountSubquery | null {
  let sharedFrom: string | null = null
  const expressions: string[] = []
  const localParams: unknown[] = []
  const localKeys: string[] = []
  const localAliases: string[] = []

  for (const item of items) {
    const result = processCountQuery(
      item,
      model,
      models,
      dialect,
      sharedFrom,
      localParams,
    )

    if (!result) return null

    sharedFrom = result.sharedFrom
    expressions.push(result.expression)
    for (const param of result.reindexedParams) {
      localParams.push(param)
    }
    localKeys.push(item.key)
    localAliases.push(item.alias)
  }

  if (!sharedFrom) return null

  const alias = `${MODEL_SUBQUERY_PREFIX}${aliasIndex}`
  const subSql = `(SELECT ${expressions.join(', ')} FROM ${sharedFrom}) ${alias}`

  return {
    alias,
    sql: subSql,
    params: localParams,
    keys: localKeys,
    aliases: localAliases,
  }
}

function groupQueriesByModel(
  queries: Record<string, BatchQuery>,
  keys: string[],
  aliasesByKey: Map<string, string>,
): Map<string, CountQueryItem[]> | null {
  const modelGroups = new Map<string, CountQueryItem[]>()

  for (const key of keys) {
    const q = queries[key]
    const alias = aliasesByKey.get(key)
    if (!alias) return null

    if (!modelGroups.has(q.model)) {
      modelGroups.set(q.model, [])
    }

    const items = modelGroups.get(q.model)
    if (items) {
      items.push({
        key,
        alias,
        args: q.args || {},
      })
    }
  }

  return modelGroups
}

function buildSubqueriesFromGroups(
  modelGroups: Map<string, CountQueryItem[]>,
  modelMap: Map<string, Model>,
  models: Model[],
  dialect: SqlDialect,
): CountSubquery[] | null {
  const subqueries: CountSubquery[] = []
  let aliasIndex = 0

  for (const [modelName, items] of modelGroups) {
    const model = modelMap.get(modelName)
    if (!model) return null

    const subquery = buildCountSubqueriesForModel(
      items,
      model,
      models,
      dialect,
      aliasIndex++,
    )

    if (!subquery) return null
    subqueries.push(subquery)
  }

  return subqueries.length > 0 ? subqueries : null
}

function reindexSubqueries(subqueries: CountSubquery[]): {
  sql: string[]
  params: unknown[]
} {
  let offset = 0
  const rewrittenSubs: string[] = []
  const finalParams: unknown[] = []

  for (const sq of subqueries) {
    const re = reindexPlaceholders(sq.sql, sq.params, offset)
    offset += re.params.length
    rewrittenSubs.push(re.sql)
    for (const p of re.params) {
      finalParams.push(p)
    }
  }

  return { sql: rewrittenSubs, params: finalParams }
}

function buildSelectParts(subqueries: CountSubquery[]): string[] {
  const selectParts: string[] = []

  for (const sq of subqueries) {
    for (const outAlias of sq.aliases) {
      selectParts.push(
        `${sq.alias}.${quoteBatchIdent(outAlias)} AS ${quoteBatchIdent(outAlias)}`,
      )
    }
  }

  return selectParts
}

function buildMergedCountBatchSql(
  queries: Record<string, BatchQuery>,
  keys: string[],
  aliasesByKey: Map<string, string>,
  modelMap: Map<string, Model>,
  models: Model[],
  dialect: SqlDialect,
): (BatchResult & { keys: string[]; aliases: string[] }) | null {
  const modelGroups = groupQueriesByModel(queries, keys, aliasesByKey)
  if (!modelGroups) return null

  const subqueries = buildSubqueriesFromGroups(
    modelGroups,
    modelMap,
    models,
    dialect,
  )
  if (!subqueries) return null

  const { sql: rewrittenSubs, params: finalParams } =
    reindexSubqueries(subqueries)
  const selectParts = buildSelectParts(subqueries)

  const fromSql = rewrittenSubs.join(' CROSS JOIN ')
  const sql = `SELECT ${selectParts.join(', ')} FROM ${fromSql}`
  const aliases = keys.map((k) => aliasesByKey.get(k) ?? '')

  return { sql, params: finalParams, keys, aliases }
}

function buildAliasesForKeys(keys: string[]): {
  aliases: string[]
  aliasesByKey: Map<string, string>
} {
  const aliases = new Array(keys.length)
  const aliasesByKey = new Map<string, string>()

  for (let i = 0; i < keys.length; i++) {
    const a = makeBatchAlias(i)
    aliases[i] = a
    aliasesByKey.set(keys[i], a)
  }

  return { aliases, aliasesByKey }
}

function buildRegularBatchQueries(
  queries: Record<string, BatchQuery>,
  keys: string[],
  aliases: string[],
  modelMap: Map<string, Model>,
  models: Model[],
  dialect: SqlDialect,
): { sql: string; params: unknown[] } {
  const ctes: string[] = new Array(keys.length)
  const selects: string[] = new Array(keys.length)
  const allParams: unknown[] = []

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    const query = queries[key]
    const model = resolveModelOrThrow(query.model, modelMap)

    const { sql: reindexedSql, params: reindexedParams } = buildAndReindexQuery(
      model,
      models,
      query.method,
      query.args || {},
      dialect,
      allParams.length,
    )

    appendParams(allParams, reindexedParams)

    const cteName = `${BATCH_CTE_PREFIX}${i}`
    ctes[i] = `${cteName} AS (${reindexedSql})`
    selects[i] = wrapQueryForMethod(query.method, cteName, aliases[i])
  }

  const sql = `WITH ${ctes.join(', ')} SELECT ${selects.join(', ')}`
  return { sql, params: allParams }
}

export function buildBatchSql(
  queries: Record<string, BatchQuery>,
  modelMap: Map<string, Model>,
  models: Model[],
  dialect: SqlDialect,
): BatchResult & { keys: string[]; aliases: string[] } {
  const keys = Object.keys(queries)

  if (keys.length === 0) {
    throw new Error('buildBatchSql requires at least one query')
  }

  if (dialect !== 'postgres') {
    throw new Error('Batch queries are only supported for postgres dialect')
  }

  const { aliases, aliasesByKey } = buildAliasesForKeys(keys)

  if (isAllCountQueries(queries, keys)) {
    const merged = buildMergedCountBatchSql(
      queries,
      keys,
      aliasesByKey,
      modelMap,
      models,
      dialect,
    )
    if (merged) return merged
  }

  const result = buildRegularBatchQueries(
    queries,
    keys,
    aliases,
    modelMap,
    models,
    dialect,
  )

  return { ...result, keys, aliases }
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
    const model = resolveModelOrThrow(query.model, modelMap)

    const { sql: reindexedSql, params: reindexedParams } = buildAndReindexQuery(
      model,
      models,
      'count',
      (query.args || {}) as Record<string, unknown>,
      dialect,
      allParams.length,
    )

    appendParams(allParams, reindexedParams)

    const cteName = `${COUNT_CTE_PREFIX}${i}`
    ctes[i] = `${cteName} AS (${reindexedSql})`
    selects[i] = `(SELECT * FROM ${cteName}) AS ${quoteBatchIdent(cteName)}`
  }

  const sql = `WITH ${ctes.join(', ')} SELECT ${selects.join(', ')}`
  return { sql, params: allParams }
}
