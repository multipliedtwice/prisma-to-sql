import { ParamMap, isDynamicParameter } from '@dee-wan/schema-parser'
import type { Model } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import { buildWhereClause } from './builder/where'
import { buildSelectSql } from './builder/select'
import {
  buildAggregateSql,
  buildCountSql,
  buildGroupBySql,
} from './builder/aggregates'
import { buildTableReference } from './builder/shared/sql-utils'
import { SQL_TEMPLATES, SQL_RESERVED_WORDS } from './builder/shared/constants'
import { createBoundedCache } from './utils/s3-fifo'

export type PrismaMethod =
  | 'findMany'
  | 'findFirst'
  | 'findUnique'
  | 'count'
  | 'aggregate'
  | 'groupBy'

interface SqlResult {
  sql: string
  params: unknown[]
}

interface CacheStats {
  hits: number
  misses: number
  size: number
}

const queryCache = createBoundedCache<string, string>(1000)

export const queryCacheStats: CacheStats = { hits: 0, misses: 0, size: 0 }

function makeAlias(name: string): string {
  const base = name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 50)
  const safe = /^[a-z_]/.test(base) ? base : `_${base}`
  return SQL_RESERVED_WORDS.has(safe) ? `${safe}_t` : safe
}

function toSqliteParams(sql: string, params: readonly unknown[]): SqlResult {
  const reorderedParams: unknown[] = []
  let lastIndex = 0
  const parts: string[] = []

  for (let i = 0; i < sql.length; i++) {
    if (sql[i] === '$' && i + 1 < sql.length) {
      let num = 0
      let j = i + 1
      while (j < sql.length && sql[j] >= '0' && sql[j] <= '9') {
        num = num * 10 + (sql.charCodeAt(j) - 48)
        j++
      }

      if (j > i + 1) {
        parts.push(sql.substring(lastIndex, i))
        parts.push('?')
        reorderedParams.push(params[num - 1])
        i = j - 1
        lastIndex = j
      }
    }
  }

  parts.push(sql.substring(lastIndex))
  return { sql: parts.join(''), params: reorderedParams }
}

function canonicalizeQuery(
  modelName: string,
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): string {
  function normalize(obj: any): any {
    if (obj === null || obj === undefined) return obj

    if (obj instanceof Date) {
      return '__DATE_PARAM__'
    }

    if (Array.isArray(obj)) {
      return obj.map(normalize)
    }

    if (typeof obj === 'object') {
      const sorted: any = {}
      for (const key of Object.keys(obj).sort()) {
        const value = obj[key]
        sorted[key] = isDynamicParameter(value) ? '__DYN__' : normalize(value)
      }
      return sorted
    }

    return obj
  }

  const canonical = normalize(args)
  return `${dialect}:${modelName}:${method}:${JSON.stringify(canonical)}`
}

function buildSQLFull(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
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
  let result: { sql: string; params: readonly unknown[] }

  switch (method) {
    case 'aggregate':
      result = buildAggregateSql(
        withMethod,
        whereResult,
        tableName,
        alias,
        model,
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
        args.skip as number,
        dialect,
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
      })
  }

  return dialect === 'sqlite'
    ? toSqliteParams(result.sql, result.params)
    : { sql: result.sql, params: [...result.params] }
}

export function buildSQLWithCache(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult {
  const cacheKey = canonicalizeQuery(model.name, method, args, dialect)
  const cachedSql = queryCache.get(cacheKey)

  if (cachedSql) {
    queryCacheStats.hits++
    const result = buildSQLFull(model, models, method, args, dialect)
    return { sql: cachedSql, params: result.params }
  }

  queryCacheStats.misses++
  const result = buildSQLFull(model, models, method, args, dialect)
  queryCache.set(cacheKey, result.sql)
  queryCacheStats.size = queryCache.size

  return result
}

export { queryCache }
