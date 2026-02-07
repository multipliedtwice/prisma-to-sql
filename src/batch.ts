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

function assertSafeIdentifier(label: string, s: string): void {
  const raw = String(s)
  if (raw.trim() !== raw) {
    throw new Error(`${label} must not contain leading/trailing whitespace`)
  }
  if (raw.length === 0) throw new Error(`${label} cannot be empty`)
  assertNoControlChars(label, raw)

  if (/[ \t\r\n]/.test(raw)) {
    throw new Error(`${label} must not contain whitespace`)
  }
  if (raw.includes(';')) {
    throw new Error(`${label} must not contain semicolons`)
  }
  if (raw.includes('--') || raw.includes('/*') || raw.includes('*/')) {
    throw new Error(`${label} must not contain SQL comment tokens`)
  }
}

function quoteIdent(id: string): string {
  const raw = String(id)
  assertSafeIdentifier('Identifier', raw)
  return `"${raw.replace(/"/g, '""')}"`
}

function makeBatchAlias(i: number): string {
  return `k${i}`
}

type ScanMode =
  | 'normal'
  | 'single'
  | 'double'
  | 'lineComment'
  | 'blockComment'
  | 'dollar'

function replacePgPlaceholders(
  sql: string,
  replace: (oldIndex: number) => string,
): string {
  const s = String(sql)
  const n = s.length
  let i = 0
  let mode: ScanMode = 'normal'
  let dollarTag: string | null = null
  let out = ''

  const startsWith = (pos: number, lit: string): boolean =>
    s.slice(pos, pos + lit.length) === lit

  const readDollarTag = (pos: number): string | null => {
    if (s.charCodeAt(pos) !== 36) return null
    let j = pos + 1
    while (j < n) {
      const c = s.charCodeAt(j)
      if (
        (c >= 48 && c <= 57) ||
        (c >= 65 && c <= 90) ||
        (c >= 97 && c <= 122) ||
        c === 95
      ) {
        j++
        continue
      }
      break
    }
    if (j < n && s.charCodeAt(j) === 36 && j > pos) {
      return s.slice(pos, j + 1)
    }
    if (pos + 1 < n && s.charCodeAt(pos + 1) === 36) {
      return '$$'
    }
    return null
  }

  while (i < n) {
    const ch = s.charCodeAt(i)

    if (mode === 'normal') {
      if (ch === 39) {
        out += s[i]
        mode = 'single'
        i++
        continue
      }

      if (ch === 34) {
        out += s[i]
        mode = 'double'
        i++
        continue
      }

      if (ch === 45 && i + 1 < n && s.charCodeAt(i + 1) === 45) {
        out += '--'
        mode = 'lineComment'
        i += 2
        continue
      }

      if (ch === 47 && i + 1 < n && s.charCodeAt(i + 1) === 42) {
        out += '/*'
        mode = 'blockComment'
        i += 2
        continue
      }

      if (ch === 36) {
        const tag = readDollarTag(i)
        if (tag) {
          out += tag
          mode = 'dollar'
          dollarTag = tag
          i += tag.length
          continue
        }

        let j = i + 1
        if (j < n) {
          const c1 = s.charCodeAt(j)
          if (c1 >= 48 && c1 <= 57) {
            while (j < n) {
              const cj = s.charCodeAt(j)
              if (cj >= 48 && cj <= 57) {
                j++
                continue
              }
              break
            }
            const numStr = s.slice(i + 1, j)
            const oldIndex = Number(numStr)
            if (!Number.isInteger(oldIndex) || oldIndex < 1) {
              throw new Error(`Invalid param placeholder: $${numStr}`)
            }
            out += replace(oldIndex)
            i = j
            continue
          }
        }
      }

      out += s[i]
      i++
      continue
    }

    if (mode === 'single') {
      out += s[i]
      if (ch === 39) {
        if (i + 1 < n && s.charCodeAt(i + 1) === 39) {
          out += s[i + 1]
          i += 2
          continue
        }
        mode = 'normal'
        i++
        continue
      }
      i++
      continue
    }

    if (mode === 'double') {
      out += s[i]
      if (ch === 34) {
        if (i + 1 < n && s.charCodeAt(i + 1) === 34) {
          out += s[i + 1]
          i += 2
          continue
        }
        mode = 'normal'
        i++
        continue
      }
      i++
      continue
    }

    if (mode === 'lineComment') {
      out += s[i]
      if (ch === 10) {
        mode = 'normal'
      }
      i++
      continue
    }

    if (mode === 'blockComment') {
      if (ch === 42 && i + 1 < n && s.charCodeAt(i + 1) === 47) {
        out += '*/'
        i += 2
        mode = 'normal'
        continue
      }
      out += s[i]
      i++
      continue
    }

    if (mode === 'dollar') {
      if (dollarTag && startsWith(i, dollarTag)) {
        out += dollarTag
        i += dollarTag.length
        mode = 'normal'
        dollarTag = null
        continue
      }
      out += s[i]
      i++
      continue
    }

    out += s[i]
    i++
  }

  return out
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

  const reindexed = replacePgPlaceholders(sql, (oldIndex) => {
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
  resultAlias: string,
): string {
  const outKey = quoteIdent(resultAlias)

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

function isAllCountQueries(
  queries: Record<string, BatchQuery>,
  keys: string[],
) {
  for (let i = 0; i < keys.length; i++) {
    if (queries[keys[i]]?.method !== 'count') return false
  }
  return true
}

function looksTooComplexForFilter(sql: string): boolean {
  const s = sql.toLowerCase()
  if (s.includes(' group by ')) return true
  if (s.includes(' having ')) return true
  if (s.includes(' union ')) return true
  if (s.includes(' intersect ')) return true
  if (s.includes(' except ')) return true
  if (s.includes(' window ')) return true
  if (s.includes(' distinct ')) return true
  return false
}

function containsPgPlaceholder(sql: string): boolean {
  return /\$[1-9][0-9]*/.test(sql)
}

function parseSimpleCountSql(
  sql: string,
): { fromSql: string; whereSql: string | null } | null {
  const trimmed = sql.trim().replace(/;$/, '').trim()
  const lower = trimmed.toLowerCase()
  if (!lower.startsWith('select')) return null
  if (!lower.includes('count(*)')) return null
  if (looksTooComplexForFilter(trimmed)) return null

  const match = trimmed.match(
    /^select\s+count\(\*\)(?:\s*::\s*[a-zA-Z0-9_\."]+)?\s+as\s+("[^"]+"|[a-zA-Z_][a-zA-Z0-9_\.]*)\s+from\s+([\s\S]+?)(?:\s+where\s+([\s\S]+))?$/i,
  )
  if (!match) return null

  const fromSql = match[2].trim()
  const whereSql = match[3] ? match[3].trim() : null

  if (!fromSql) return null
  return { fromSql, whereSql }
}

function buildMergedCountBatchSql(
  queries: Record<string, BatchQuery>,
  keys: string[],
  aliasesByKey: Map<string, string>,
  modelMap: Map<string, Model>,
  models: Model[],
  dialect: SqlDialect,
): (BatchResult & { keys: string[]; aliases: string[] }) | null {
  const modelGroups = new Map<
    string,
    Array<{ key: string; alias: string; args: Record<string, unknown> }>
  >()

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    const q = queries[key]
    const alias = aliasesByKey.get(key)
    if (!alias) return null
    if (!modelGroups.has(q.model)) modelGroups.set(q.model, [])
    modelGroups
      .get(q.model)!
      .push({ key, alias, args: (q.args || {}) as Record<string, unknown> })
  }

  const subqueries: Array<{
    alias: string
    sql: string
    params: unknown[]
    keys: string[]
    aliases: string[]
  }> = []

  let aliasIndex = 0

  for (const [modelName, items] of modelGroups) {
    const model = modelMap.get(modelName)
    if (!model) return null

    let sharedFrom: string | null = null
    const expressions: string[] = []
    const localParams: unknown[] = []
    const localKeys: string[] = []
    const localAliases: string[] = []

    for (let i = 0; i < items.length; i++) {
      const { key, alias, args } = items[i]
      const built = buildSQLWithCache(model, models, 'count', args, dialect)
      const parsed = parseSimpleCountSql(built.sql)
      if (!parsed) return null

      if (containsPgPlaceholder(parsed.fromSql)) {
        return null
      }

      if (!parsed.whereSql) {
        if (built.params.length > 0) return null

        if (sharedFrom === null) sharedFrom = parsed.fromSql
        if (sharedFrom !== parsed.fromSql) return null

        expressions.push(`count(*) AS ${quoteIdent(alias)}`)
        localKeys.push(key)
        localAliases.push(alias)
        continue
      }

      if (sharedFrom === null) sharedFrom = parsed.fromSql
      if (sharedFrom !== parsed.fromSql) return null

      const re = reindexParams(
        parsed.whereSql,
        built.params,
        localParams.length,
      )
      for (let p = 0; p < re.params.length; p++) localParams.push(re.params[p])

      expressions.push(
        `count(*) FILTER (WHERE ${re.sql}) AS ${quoteIdent(alias)}`,
      )
      localKeys.push(key)
      localAliases.push(alias)
    }

    if (!sharedFrom) return null

    const alias = `m_${aliasIndex++}`
    const subSql = `(SELECT ${expressions.join(', ')} FROM ${sharedFrom}) ${alias}`

    subqueries.push({
      alias,
      sql: subSql,
      params: localParams,
      keys: localKeys,
      aliases: localAliases,
    })
  }

  if (subqueries.length === 0) return null

  let offset = 0
  const rewrittenSubs: string[] = []
  const finalParams: unknown[] = []

  for (let i = 0; i < subqueries.length; i++) {
    const sq = subqueries[i]
    const re = reindexParams(sq.sql, sq.params, offset)
    offset += re.params.length
    rewrittenSubs.push(re.sql)
    for (let p = 0; p < re.params.length; p++) finalParams.push(re.params[p])
  }

  const selectParts: string[] = []
  for (let i = 0; i < subqueries.length; i++) {
    const sq = subqueries[i]
    for (let k = 0; k < sq.aliases.length; k++) {
      const outAlias = sq.aliases[k]
      selectParts.push(
        `${sq.alias}.${quoteIdent(outAlias)} AS ${quoteIdent(outAlias)}`,
      )
    }
  }

  const fromSql = rewrittenSubs.join(' CROSS JOIN ')
  const sql = `SELECT ${selectParts.join(', ')} FROM ${fromSql}`

  const aliases = keys.map((k) => aliasesByKey.get(k)!)
  return { sql, params: finalParams, keys, aliases }
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

  const aliases = new Array(keys.length)
  const aliasesByKey = new Map<string, string>()
  for (let i = 0; i < keys.length; i++) {
    const a = makeBatchAlias(i)
    aliases[i] = a
    aliasesByKey.set(keys[i], a)
  }

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
    selects[i] = wrapQueryForMethod(query.method, cteName, aliases[i])
  }

  const sql = `WITH ${ctes.join(', ')} SELECT ${selects.join(', ')}`

  return { sql, params: allParams, keys, aliases }
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
  if (typeof value === 'bigint') {
    const n = Number(value)
    return Number.isSafeInteger(n) ? n : 0
  }
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
      if (typeof v === 'bigint') {
        const n = Number(v)
        return Number.isSafeInteger(n) ? n : 0
      }
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
  aliases?: string[],
): Record<string, unknown> {
  const results: Record<string, unknown> = {}

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    const columnKey = aliases && aliases[i] ? aliases[i] : key
    const rawValue = row[columnKey]
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
