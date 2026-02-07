import type { Model, PrismaMethod } from './types'
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

interface SqlResult {
  sql: string
  params: unknown[]
}

interface CacheStats {
  hits: number
  misses: number
  size: number
}

const queryCache = createBoundedCache<string, SqlResult>(1000)

export const queryCacheStats: CacheStats = { hits: 0, misses: 0, size: 0 }

function makeAlias(name: string): string {
  const base = name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 50)
  const safe = /^[a-z_]/.test(base) ? base : `_${base}`
  return SQL_RESERVED_WORDS.has(safe) ? `${safe}_t` : safe
}

type SqliteScanMode = 'normal' | 'single' | 'double'

function toSqliteParams(sql: string, params: readonly unknown[]): SqlResult {
  const reorderedParams: unknown[] = []
  const n = sql.length
  let i = 0
  let out = ''
  let mode: SqliteScanMode = 'normal'

  while (i < n) {
    const ch = sql.charCodeAt(i)

    if (mode === 'normal') {
      if (ch === 39) {
        out += sql[i]
        mode = 'single'
        i++
        continue
      }

      if (ch === 34) {
        out += sql[i]
        mode = 'double'
        i++
        continue
      }

      if (ch === 36) {
        let j = i + 1
        let num = 0
        let hasDigit = false
        while (j < n) {
          const d = sql.charCodeAt(j)
          if (d >= 48 && d <= 57) {
            num = num * 10 + (d - 48)
            hasDigit = true
            j++
          } else {
            break
          }
        }
        if (hasDigit && num >= 1) {
          out += '?'
          reorderedParams.push(params[num - 1])
          i = j
          continue
        }
      }

      out += sql[i]
      i++
      continue
    }

    if (mode === 'single') {
      out += sql[i]
      if (ch === 39) {
        if (i + 1 < n && sql.charCodeAt(i + 1) === 39) {
          out += sql[i + 1]
          i += 2
          continue
        }
        mode = 'normal'
      }
      i++
      continue
    }

    if (mode === 'double') {
      out += sql[i]
      if (ch === 34) {
        if (i + 1 < n && sql.charCodeAt(i + 1) === 34) {
          out += sql[i + 1]
          i += 2
          continue
        }
        mode = 'normal'
      }
      i++
      continue
    }

    out += sql[i]
    i++
  }

  return { sql: out, params: reorderedParams }
}

function canonicalizeReplacer(key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return `__bigint__${value.toString()}`
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>
    const sorted: Record<string, unknown> = {}
    for (const k of Object.keys(obj).sort()) {
      const v = obj[k]
      if (
        v &&
        typeof v === 'object' &&
        !Array.isArray(v) &&
        Object.keys(v).length === 0
      ) {
        continue
      }
      sorted[k] = v
    }
    return sorted
  }
  return value
}

function canonicalizeQuery(
  modelName: string,
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): string {
  if (!args) return `${dialect}:${modelName}:${method}:{}`
  return `${dialect}:${modelName}:${method}:${JSON.stringify(args, canonicalizeReplacer)}`
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

  const cached = queryCache.get(cacheKey)
  if (cached) {
    queryCacheStats.hits++
    return { sql: cached.sql, params: [...cached.params] }
  }

  queryCacheStats.misses++

  const result = buildSQLFull(model, models, method, args, dialect)

  queryCache.set(cacheKey, result)
  queryCacheStats.size = queryCache.size

  return result
}

export { queryCache }
