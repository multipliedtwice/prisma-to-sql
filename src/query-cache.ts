import { ParamMap, isDynamicParameter } from '@dee-wan/schema-parser'
import type { Model } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import { buildWhereClause } from './builder/where'
import { buildSelectSql } from './builder/select'
import { buildAggregateSql, buildCountSql, buildGroupBySql } from './builder/aggregates'
import { buildTableReference } from './builder/shared/sql-utils'
import { SQL_TEMPLATES, SQL_RESERVED_WORDS } from './builder/shared/constants'
import { createBoundedCache } from './utils/s3-fifo'

export type PrismaMethod = 'findMany' | 'findFirst' | 'findUnique' | 'count' | 'aggregate' | 'groupBy'

interface CachedQuery {
  sql: string
  paramMappings: readonly ParamMap[]
}

interface SqlResult {
  sql: string
  params: unknown[]
}

const queryCache = createBoundedCache<string, CachedQuery>(1000)

function makeAlias(name: string): string {
  const base = name.toLowerCase().replace(/[^a-z0-9_]/g, '_').slice(0, 50)
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
  args: Record<string, unknown>
): string {
  function normalize(obj: any, path: string[] = []): any {
    if (obj === null || typeof obj !== 'object') return obj
    if (Array.isArray(obj)) return obj.map((v, i) => normalize(v, [...path, String(i)]))
    
    const sorted: any = {}
    const keys = Object.keys(obj).sort()
    
    for (const key of keys) {
      const fullPath = [...path, key].join('.')
      const value = obj[key]
      
      if (isDynamicParameter(value)) {
        sorted[key] = `__DYN:${fullPath}__`
      } else {
        sorted[key] = normalize(value, [...path, key])
      }
    }
    
    return sorted
  }
  
  const canonical = normalize(args)
  return `${modelName}:${method}:${JSON.stringify(canonical)}`
}

function extractValueFromPath(obj: any, path: string): unknown {
  const parts = path.split('.')
  let current = obj
  
  for (const part of parts) {
    if (current === undefined || current === null) return undefined
    current = current[part]
  }
  
  return current
}

function rebuildParams(
  mappings: readonly ParamMap[],
  args: Record<string, unknown>
): unknown[] {
  const params: unknown[] = new Array(mappings.length)
  
  for (let i = 0; i < mappings.length; i++) {
    const mapping = mappings[i]
    
    if (mapping.dynamicName !== undefined) {
      // Extract path from scoped name like "root.pagination.skip" → "skip"
      // or "where.status" → "where.status"
      const parts = mapping.dynamicName.split(':')
      const path = parts.length === 2 ? parts[1] : mapping.dynamicName
      
      params[i] = extractValueFromPath(args, path)
    } else {
      params[i] = mapping.value
    }
  }
  
  return params
}

function buildSQLFull(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult & { paramMappings: readonly ParamMap[] } {
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
  let result: { sql: string; params: readonly unknown[]; paramMappings: readonly ParamMap[] }

  switch (method) {
    case 'aggregate':
      result = buildAggregateSql(withMethod, whereResult, tableName, alias, model)
      break
    case 'groupBy':
      result = buildGroupBySql(withMethod, whereResult, tableName, alias, model, dialect)
      break
    case 'count':
      result = buildCountSql(whereResult, tableName, alias, args.skip as number, dialect)
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

  const finalResult = dialect === 'sqlite'
    ? toSqliteParams(result.sql, result.params)
    : { sql: result.sql, params: [...result.params] }

  return {
    ...finalResult,
    paramMappings: result.paramMappings
  }
}

export function buildSQLWithCache(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult {
  const cacheKey = canonicalizeQuery(model.name, method, args)
  const cached = queryCache.get(cacheKey)
  
  if (cached) {
    const params = rebuildParams(cached.paramMappings, args)
    return { sql: cached.sql, params }
  }
  
  const result = buildSQLFull(model, models, method, args, dialect)
  
  queryCache.set(cacheKey, {
    sql: result.sql,
    paramMappings: result.paramMappings
  })
  
  return { sql: result.sql, params: result.params }
}

export { queryCache }