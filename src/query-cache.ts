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
import { tryFastPath } from './fast-path'

interface SqlResult {
  sql: string
  params: unknown[]
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

export const queryCache = createBoundedCache<string, SqlResult>(1000)

export const queryCacheStats = new QueryCacheStats()

export function makeAlias(name: string): string {
  const base = name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 50)
  const safe = /^[a-z_]/.test(base) ? base : `_${base}`
  return SQL_RESERVED_WORDS.has(safe) ? `${safe}_t` : safe
}

type SqliteScanMode = 'normal' | 'single' | 'double'

interface ScanState {
  mode: SqliteScanMode
  position: number
  output: string
  reorderedParams: unknown[]
}

function handleSingleQuote(sql: string, state: ScanState): ScanState {
  const n = sql.length
  let i = state.position
  let out = state.output + sql[i]
  i++

  while (i < n) {
    out += sql[i]
    if (sql.charCodeAt(i) === 39) {
      if (i + 1 < n && sql.charCodeAt(i + 1) === 39) {
        out += sql[i + 1]
        i += 2
        continue
      }
      return { ...state, mode: 'normal', position: i + 1, output: out }
    }
    i++
  }

  return { ...state, position: i, output: out }
}

function handleDoubleQuote(sql: string, state: ScanState): ScanState {
  const n = sql.length
  let i = state.position
  let out = state.output + sql[i]
  i++

  while (i < n) {
    out += sql[i]
    if (sql.charCodeAt(i) === 34) {
      if (i + 1 < n && sql.charCodeAt(i + 1) === 34) {
        out += sql[i + 1]
        i += 2
        continue
      }
      return { ...state, mode: 'normal', position: i + 1, output: out }
    }
    i++
  }

  return { ...state, position: i, output: out }
}

function extractParameterNumber(
  sql: string,
  startPos: number,
): { num: number; nextPos: number } | null {
  const n = sql.length
  let j = startPos + 1
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
    return { num, nextPos: j }
  }

  return null
}

function handleParameterSubstitution(
  sql: string,
  params: readonly unknown[],
  state: ScanState,
): ScanState {
  const result = extractParameterNumber(sql, state.position)

  if (result) {
    return {
      ...state,
      position: result.nextPos,
      output: state.output + '?',
      reorderedParams: [...state.reorderedParams, params[result.num - 1]],
    }
  }

  return {
    ...state,
    position: state.position + 1,
    output: state.output + sql[state.position],
  }
}

function toSqliteParams(sql: string, params: readonly unknown[]): SqlResult {
  const n = sql.length
  let state: ScanState = {
    mode: 'normal',
    position: 0,
    output: '',
    reorderedParams: [],
  }

  while (state.position < n) {
    const ch = sql.charCodeAt(state.position)

    if (state.mode === 'single') {
      state = handleSingleQuote(sql, state)
      continue
    }

    if (state.mode === 'double') {
      state = handleDoubleQuote(sql, state)
      continue
    }

    if (ch === 39) {
      state = { ...state, mode: 'single' }
      continue
    }

    if (ch === 34) {
      state = { ...state, mode: 'double' }
      continue
    }

    if (ch === 36) {
      state = handleParameterSubstitution(sql, params, state)
      continue
    }

    state = {
      ...state,
      position: state.position + 1,
      output: state.output + sql[state.position],
    }
  }

  return { sql: state.output, params: state.reorderedParams }
}

function canonicalizeReplacer(key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return `__bigint__${value.toString()}`
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>
    const sorted: Record<string, unknown> = {}
    for (const k of Object.keys(obj).sort()) {
      sorted[k] = obj[k]
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
    queryCacheStats.hit()
    return { sql: cached.sql, params: [...cached.params] }
  }

  queryCacheStats.miss()

  const fastResult = tryFastPath(model, method, args, dialect)
  if (fastResult) {
    queryCache.set(cacheKey, {
      sql: fastResult.sql,
      params: [...fastResult.params],
    })
    return fastResult
  }

  const result = buildSQLFull(model, models, method, args, dialect)

  queryCache.set(cacheKey, { sql: result.sql, params: [...result.params] })

  return result
}
