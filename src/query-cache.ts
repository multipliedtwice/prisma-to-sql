import type { Model, PrismaMethod } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import type { ParamMap } from '@dee-wan/schema-parser'
import type { LateralRelationMeta } from './builder/select/lateral-join'
import { buildWhereClause } from './builder/where'
import { buildSelectSql } from './builder/select'
import type { SqlBuildOptions } from './builder/select'
import {
  buildAggregateSql,
  buildCountSql,
  buildGroupBySql,
} from './builder/aggregates'
import { buildTableReference } from './builder/shared/sql-utils'
import {
  SQL_TEMPLATES,
  SQL_RESERVED_WORDS,
  LIMITS,
} from './builder/shared/constants'
import { createBoundedCache } from './utils/s3-fifo'
import { tryFastPath } from './fast-path'
import { pgToSqlitePlaceholders } from './builder/shared/sql-param-scanner'

interface SqlResult {
  sql: string
  params: unknown[]
  paramMappings?: readonly ParamMap[]
  requiresReduction?: boolean
  includeSpec?: Record<string, any>
  isLateral?: boolean
  lateralMeta?: LateralRelationMeta[]
  skipWhereIn?: boolean
}

interface CacheStats {
  hits: number
  misses: number
  size: number
}

class QueryCacheStats {
  #hits = 0
  #misses = 0
  hit(): void {
    this.#hits++
  }
  miss(): void {
    this.#misses++
  }
  reset(): void {
    this.#hits = 0
    this.#misses = 0
  }
  get snapshot(): CacheStats {
    return Object.freeze({
      hits: this.#hits,
      misses: this.#misses,
      size: queryCache.size,
    })
  }
}

let queryCache = createBoundedCache<string, SqlResult>(LIMITS.QUERY_CACHE_SIZE)
const queryCacheStats = new QueryCacheStats()

/**
 * Recreate the query cache with the current LIMITS.QUERY_CACHE_SIZE.
 * Call after setLimits({ QUERY_CACHE_SIZE: N }) to apply the new size.
 */
export function rebuildQueryCache(): void {
  queryCache = createBoundedCache<string, SqlResult>(LIMITS.QUERY_CACHE_SIZE)
  queryCacheStats.reset()
}

export function makeAlias(name: string): string {
  const base = name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 50)
  const safe = /^[a-z_]/.test(base) ? base : `_${base}`
  return SQL_RESERVED_WORDS.has(safe) ? `${safe}_t` : safe
}

function bigintReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return `__bigint__${value.toString()}`
  return value
}

function optionsKeyFragment(options?: SqlBuildOptions): string {
  if (!options) return ''
  const mode = options.orRewrite ?? 'default'
  return mode === 'default' ? '' : `:or=${mode}`
}

function canonicalizeQuery(
  modelName: string,
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
  options?: SqlBuildOptions,
): string {
  const optsFragment = optionsKeyFragment(options)
  if (!args) return `${dialect}:${modelName}:${method}:{}${optsFragment}`
  return `${dialect}:${modelName}:${method}:${JSON.stringify(args, bigintReplacer)}${optsFragment}`
}

function buildSQLFull(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
  options?: SqlBuildOptions,
): SqlResult {
  const tableName = buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    model.tableName,
    dialect,
  )
  const alias = makeAlias(model.tableName)
  const whereResult = buildWhereClause(
    (args.where || {}) as Record<string, unknown>,
    {
      alias,
      schemaModels: models,
      model,
      path: ['where'],
      isSubquery: false,
      dialect,
    },
  )
  const withMethod = { ...args, method }
  let result: {
    sql: string
    params: readonly unknown[]
    paramMappings?: readonly ParamMap[]
    requiresReduction?: boolean
    includeSpec?: Record<string, any>
    isLateral?: boolean
    lateralMeta?: LateralRelationMeta[]
    skipWhereIn?: boolean
  }
  switch (method) {
    case 'aggregate':
      result = buildAggregateSql(
        withMethod,
        whereResult,
        tableName,
        alias,
        model,
        dialect,
      )
      break
    case 'groupBy':
      result = buildGroupBySql(
        withMethod,
        whereResult,
        tableName,
        alias,
        model,
        dialect,
      )
      break
    case 'count':
      result = buildCountSql(
        whereResult,
        tableName,
        alias,
        args,
        dialect,
        model,
        models,
      )
      break
    default:
      result = buildSelectSql({
        method,
        args: withMethod,
        model,
        schemas: models,
        from: { tableName, alias },
        whereResult,
        dialect,
        options,
      })
  }
  const needsPlaceholderConversion =
    dialect === 'sqlite' && result.sql.includes('$')
  const sqlResult = needsPlaceholderConversion
    ? pgToSqlitePlaceholders(result.sql, result.params)
    : { sql: result.sql, params: [...result.params] }
  return {
    ...sqlResult,
    paramMappings: result.paramMappings,
    requiresReduction: result.requiresReduction,
    includeSpec: result.includeSpec,
    isLateral: result.isLateral,
    lateralMeta: result.lateralMeta,
    skipWhereIn: result.skipWhereIn,
  }
}

export function buildSQLWithCache(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
  options?: SqlBuildOptions,
): SqlResult {
  const cacheKey = canonicalizeQuery(model.name, method, args, dialect, options)
  const cached = queryCache.get(cacheKey)
  if (cached) {
    queryCacheStats.hit()
    return {
      sql: cached.sql,
      params: [...cached.params],
      paramMappings: cached.paramMappings,
      requiresReduction: cached.requiresReduction,
      includeSpec: cached.includeSpec,
      isLateral: cached.isLateral,
      lateralMeta: cached.lateralMeta,
      skipWhereIn: cached.skipWhereIn,
    }
  }
  queryCacheStats.miss()
  const useOrRewrite = options?.orRewrite === 'union-of-ids'
  if (!useOrRewrite) {
    const fastResult = tryFastPath(model, method, args, dialect)
    if (fastResult) {
      queryCache.set(cacheKey, {
        sql: fastResult.sql,
        params: [...fastResult.params],
      })
      return fastResult
    }
  }
  const result = buildSQLFull(model, models, method, args, dialect, options)
  queryCache.set(cacheKey, {
    sql: result.sql,
    params: [...result.params],
    paramMappings: result.paramMappings,
    requiresReduction: result.requiresReduction,
    includeSpec: result.includeSpec,
    isLateral: result.isLateral,
    lateralMeta: result.lateralMeta,
    skipWhereIn: result.skipWhereIn,
  })
  return result
}
