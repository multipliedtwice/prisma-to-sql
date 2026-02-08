import type { Model, PrismaMethod } from './types'
import type { SqlDialect } from './sql-builder-dialect'

import { buildSQLWithCache } from './query-cache'
import { transformQueryResults } from './result-transformers'
import { assertSafeAlias } from './builder/shared/sql-utils'

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
  return `k${i}`
}

type ScanMode =
  | 'normal'
  | 'single'
  | 'double'
  | 'lineComment'
  | 'blockComment'
  | 'dollar'

interface ScanState {
  mode: ScanMode
  dollarTag: string | null
}

interface ProcessResult {
  consumed: number
  output: string
  newState?: ScanState
  shouldExitMode?: boolean
}

function isDigit(charCode: number): boolean {
  return charCode >= 48 && charCode <= 57
}

function isAlphaNumericOrUnderscore(charCode: number): boolean {
  return (
    isDigit(charCode) ||
    (charCode >= 65 && charCode <= 90) ||
    (charCode >= 97 && charCode <= 122) ||
    charCode === 95
  )
}

function readDollarTag(s: string, pos: number, n: number): string | null {
  if (s.charCodeAt(pos) !== 36) return null
  let j = pos + 1
  while (j < n && isAlphaNumericOrUnderscore(s.charCodeAt(j))) {
    j++
  }
  if (j < n && s.charCodeAt(j) === 36 && j > pos) {
    return s.slice(pos, j + 1)
  }
  if (pos + 1 < n && s.charCodeAt(pos + 1) === 36) {
    return '$$'
  }
  return null
}

function parseParamPlaceholder(
  s: string,
  i: number,
  n: number,
  replace: (oldIndex: number) => string,
): { consumed: number; output: string } {
  let j = i + 1
  if (j >= n) {
    return { consumed: 1, output: s[i] }
  }

  const c1 = s.charCodeAt(j)
  if (!isDigit(c1)) {
    return { consumed: 1, output: s[i] }
  }

  while (j < n && isDigit(s.charCodeAt(j))) {
    j++
  }

  const numStr = s.slice(i + 1, j)
  const oldIndex = Number(numStr)
  if (!Number.isInteger(oldIndex) || oldIndex < 1) {
    throw new Error(`Invalid param placeholder: $${numStr}`)
  }

  return { consumed: j - i, output: replace(oldIndex) }
}

function handleDollarInNormalMode(
  s: string,
  i: number,
  n: number,
  state: ScanState,
  replace: (oldIndex: number) => string,
): ProcessResult {
  const tag = readDollarTag(s, i, n)
  if (tag) {
    return {
      consumed: tag.length,
      output: tag,
      newState: { mode: 'dollar', dollarTag: tag },
    }
  }

  const placeholder = parseParamPlaceholder(s, i, n, replace)
  return { ...placeholder, newState: state }
}

function handleCommentStart(
  s: string,
  i: number,
  n: number,
  ch: number,
): ProcessResult | null {
  if (ch === 45 && i + 1 < n && s.charCodeAt(i + 1) === 45) {
    return {
      consumed: 2,
      output: '--',
      newState: { mode: 'lineComment', dollarTag: null },
    }
  }

  if (ch === 47 && i + 1 < n && s.charCodeAt(i + 1) === 42) {
    return {
      consumed: 2,
      output: '/*',
      newState: { mode: 'blockComment', dollarTag: null },
    }
  }

  return null
}

function processNormalMode(
  s: string,
  i: number,
  n: number,
  state: ScanState,
  replace: (oldIndex: number) => string,
): ProcessResult {
  const ch = s.charCodeAt(i)

  if (ch === 39) {
    return {
      consumed: 1,
      output: s[i],
      newState: { mode: 'single', dollarTag: null },
    }
  }

  if (ch === 34) {
    return {
      consumed: 1,
      output: s[i],
      newState: { mode: 'double', dollarTag: null },
    }
  }

  const commentResult = handleCommentStart(s, i, n, ch)
  if (commentResult) return commentResult

  if (ch === 36) {
    return handleDollarInNormalMode(s, i, n, state, replace)
  }

  return { consumed: 1, output: s[i], newState: state }
}

function processQuoteMode(
  s: string,
  i: number,
  n: number,
  quoteChar: number,
): ProcessResult {
  const ch = s.charCodeAt(i)

  if (ch === quoteChar) {
    if (i + 1 < n && s.charCodeAt(i + 1) === quoteChar) {
      return { consumed: 2, output: s[i] + s[i + 1], shouldExitMode: false }
    }
    return { consumed: 1, output: s[i], shouldExitMode: true }
  }

  return { consumed: 1, output: s[i], shouldExitMode: false }
}

function processBlockCommentMode(
  s: string,
  i: number,
  n: number,
): ProcessResult {
  const ch = s.charCodeAt(i)

  if (ch === 42 && i + 1 < n && s.charCodeAt(i + 1) === 47) {
    return { consumed: 2, output: '*/', shouldExitMode: true }
  }

  return { consumed: 1, output: s[i], shouldExitMode: false }
}

function processDollarMode(
  s: string,
  i: number,
  dollarTag: string,
): ProcessResult {
  if (s.slice(i, i + dollarTag.length) === dollarTag) {
    return {
      consumed: dollarTag.length,
      output: dollarTag,
      shouldExitMode: true,
    }
  }

  return { consumed: 1, output: s[i], shouldExitMode: false }
}

function processLineCommentMode(ch: number): { shouldExitMode: boolean } {
  return { shouldExitMode: ch === 10 }
}

function processCharacter(
  s: string,
  i: number,
  n: number,
  state: ScanState,
  replace: (oldIndex: number) => string,
): ProcessResult {
  const ch = s.charCodeAt(i)

  switch (state.mode) {
    case 'normal':
      return processNormalMode(s, i, n, state, replace)
    case 'single':
      return processQuoteMode(s, i, n, 39)
    case 'double':
      return processQuoteMode(s, i, n, 34)
    case 'lineComment': {
      const result = processLineCommentMode(ch)
      return {
        consumed: 1,
        output: s[i],
        shouldExitMode: result.shouldExitMode,
      }
    }
    case 'blockComment':
      return processBlockCommentMode(s, i, n)
    case 'dollar':
      return state.dollarTag
        ? processDollarMode(s, i, state.dollarTag)
        : { consumed: 1, output: s[i], shouldExitMode: false }
    default:
      return { consumed: 1, output: s[i], shouldExitMode: false }
  }
}

function updateStateAfterProcessing(
  currentState: ScanState,
  result: ProcessResult,
): ScanState {
  if (result.newState) {
    return result.newState
  }

  if (result.shouldExitMode) {
    return { mode: 'normal', dollarTag: null }
  }

  return currentState
}

function replacePgPlaceholders(
  sql: string,
  replace: (oldIndex: number) => string,
): string {
  const s = String(sql)
  const n = s.length
  let i = 0
  let state: ScanState = { mode: 'normal', dollarTag: null }
  let out = ''

  while (i < n) {
    const result = processCharacter(s, i, n, state, replace)
    out += result.output
    i += result.consumed
    state = updateStateAfterProcessing(state, result)
  }

  return out
}

function containsPgPlaceholder(sql: string): boolean {
  let found = false
  replacePgPlaceholders(sql, (oldIndex) => {
    found = true
    return `$${oldIndex}`
  })
  return found
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
  const outKey = quoteBatchIdent(resultAlias)

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
  for (const key of keys) {
    if (queries[key]?.method !== 'count') return false
  }
  return true
}

function looksTooComplexForFilter(sql: string): boolean {
  const s = sql.toLowerCase()
  const complexKeywords = [
    ' group by ',
    ' having ',
    ' union ',
    ' intersect ',
    ' except ',
    ' window ',
    ' distinct ',
  ]
  return complexKeywords.some((keyword) => s.includes(keyword))
}

function skipWhitespace(s: string, i: number): number {
  while (i < s.length && /\s/.test(s[i])) {
    i++
  }
  return i
}

function matchKeyword(s: string, i: number, keyword: string): number {
  const lower = s.slice(i).toLowerCase()
  if (!lower.startsWith(keyword)) return -1
  const endPos = i + keyword.length
  if (endPos < s.length && /[a-z0-9_]/i.test(s[endPos])) return -1
  return endPos
}

function parseQuotedIdentifier(
  s: string,
  i: number,
): { value: string; endPos: number } | null {
  let j = i + 1
  while (j < s.length) {
    if (s[j] === '"') {
      if (j + 1 < s.length && s[j + 1] === '"') {
        j += 2
        continue
      }
      return { value: s.slice(i, j + 1), endPos: j + 1 }
    }
    j++
  }
  return null
}

function parseUnquotedIdentifier(
  s: string,
  i: number,
): { value: string; endPos: number } | null {
  if (!/[a-z_]/i.test(s[i])) return null

  let j = i
  while (j < s.length && /[a-z0-9_.]/i.test(s[j])) {
    j++
  }
  return { value: s.slice(i, j), endPos: j }
}

function parseIdentifier(
  s: string,
  i: number,
): { value: string; endPos: number } | null {
  if (i >= s.length) return null

  if (s[i] === '"') {
    return parseQuotedIdentifier(s, i)
  }

  return parseUnquotedIdentifier(s, i)
}

function findFromClauseEnd(s: string, fromStart: number): number {
  const lower = s.slice(fromStart).toLowerCase()
  const whereIdx = lower.indexOf(' where ')
  return whereIdx === -1 ? s.length : fromStart + whereIdx
}

function parseSimpleCountSql(
  sql: string,
): { fromSql: string; whereSql: string | null } | null {
  const trimmed = sql.trim().replace(/;$/, '').trim()
  const lower = trimmed.toLowerCase()

  if (!lower.startsWith('select')) return null
  if (!lower.includes('count(*)')) return null
  if (looksTooComplexForFilter(trimmed)) return null

  let pos = matchKeyword(trimmed, 0, 'select')
  if (pos === -1) return null

  pos = skipWhitespace(trimmed, pos)

  const countMatch = trimmed.slice(pos).match(/^count\(\*\)/i)
  if (!countMatch) return null
  pos += countMatch[0].length

  pos = skipWhitespace(trimmed, pos)

  const castMatch = trimmed.slice(pos).match(/^::\s*\w+/)
  if (castMatch) {
    pos += castMatch[0].length
    pos = skipWhitespace(trimmed, pos)
  }

  const asPos = matchKeyword(trimmed, pos, 'as')
  if (asPos === -1) return null
  pos = skipWhitespace(trimmed, asPos)

  const ident = parseIdentifier(trimmed, pos)
  if (!ident) return null
  pos = skipWhitespace(trimmed, ident.endPos)

  const fromPos = matchKeyword(trimmed, pos, 'from')
  if (fromPos === -1) return null
  pos = skipWhitespace(trimmed, fromPos)

  const fromEnd = findFromClauseEnd(trimmed, pos)
  const fromSql = trimmed.slice(pos, fromEnd).trim()
  if (!fromSql) return null

  let whereSql: string | null = null
  if (fromEnd < trimmed.length) {
    const adjustedPos = skipWhitespace(trimmed, fromEnd)
    const wherePos = matchKeyword(trimmed, adjustedPos, 'where')
    if (wherePos !== -1) {
      whereSql = trimmed.slice(skipWhitespace(trimmed, wherePos)).trim()
    }
  }

  return { fromSql, whereSql }
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
  if (containsPgPlaceholder(parsed.fromSql)) return null

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

  const re = reindexParams(parsed.whereSql, built.params, localParams.length)
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

  const alias = `m_${aliasIndex}`
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
    const re = reindexParams(sq.sql, sq.params, offset)
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
      query.args || {},
      dialect,
    )

    const { sql: reindexedSql, params: reindexedParams } = reindexParams(
      querySql,
      queryParams,
      allParams.length,
    )

    for (const p of reindexedParams) {
      allParams.push(p)
    }

    const cteName = `batch_${i}`
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

    for (const p of reindexedParams) {
      allParams.push(p)
    }

    const cteName = `count_${i}`
    const resultKey = `count_${i}`
    ctes[i] = `${cteName} AS (${reindexedSql})`
    selects[i] = `(SELECT * FROM ${cteName}) AS ${quoteBatchIdent(resultKey)}`
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

function findCountKey(obj: Record<string, unknown>): string | undefined {
  if (Object.prototype.hasOwnProperty.call(obj, 'count')) {
    return 'count'
  }
  if (Object.prototype.hasOwnProperty.call(obj, '_count')) {
    return '_count'
  }
  return Object.keys(obj).find((k) => k.endsWith('_count'))
}

function extractNumericValue(value: unknown): number {
  if (typeof value === 'number') return value

  if (typeof value === 'bigint') {
    const n = Number(value)
    return Number.isSafeInteger(n) ? n : 0
  }

  if (typeof value === 'string') {
    const n = Number.parseInt(value, 10)
    return Number.isFinite(n) ? n : 0
  }

  return 0
}

function parseCountValue(value: unknown): number {
  if (value === null || value === undefined) return 0
  if (typeof value === 'number') return value
  if (typeof value === 'bigint' || typeof value === 'string') {
    return extractNumericValue(value)
  }

  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>
    const countKey = findCountKey(obj)
    if (countKey !== undefined) {
      return extractNumericValue(obj[countKey])
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

function isDateTimeFieldType(type: string): boolean {
  const base = type.replace(/\[\]|\?/g, '')
  return base === 'DateTime'
}

function coerceDateTime(value: unknown): unknown {
  if (typeof value !== 'string') return value
  const d = new Date(value)
  if (Number.isFinite(d.getTime())) return d
  return value
}

function coerceFieldValue(
  value: unknown,
  field: {
    name: string
    type: string
    isRelation?: boolean
    relatedModel?: string
  },
  modelMap: Map<string, Model>,
): { value: unknown; changed: boolean } {
  if (field.isRelation && field.relatedModel) {
    const relModel = modelMap.get(field.relatedModel)
    if (relModel) {
      const coerced = coerceBatchRowTypes(value, relModel, modelMap)
      return { value: coerced, changed: coerced !== value }
    }
    return { value, changed: false }
  }

  if (isDateTimeFieldType(field.type)) {
    const coerced = coerceDateTime(value)
    return { value: coerced, changed: coerced !== value }
  }

  return { value, changed: false }
}

function coerceBatchRowTypes(
  obj: unknown,
  model: Model | undefined,
  modelMap: Map<string, Model>,
): unknown {
  if (!model || obj === null || obj === undefined) return obj

  if (Array.isArray(obj)) {
    return obj.map((item) => coerceBatchRowTypes(item, model, modelMap))
  }

  if (typeof obj !== 'object') return obj

  const record = obj as Record<string, unknown>
  const result: Record<string, unknown> = {}
  let changed = false

  for (const key in record) {
    if (!Object.prototype.hasOwnProperty.call(record, key)) continue

    const val = record[key]
    const field = model.fields.find((f) => f.name === key)

    if (!field) {
      result[key] = val
      continue
    }

    const coerced = coerceFieldValue(val, field, modelMap)
    result[key] = coerced.value
    if (coerced.changed) changed = true
  }

  return changed ? result : obj
}

function coerceBatchValue(
  value: unknown,
  method: PrismaMethod,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  if (!modelMap || method === 'count') return value

  const model = modelMap.get(modelName)
  if (!model) return value

  if (Array.isArray(value)) {
    return value.map((item) => coerceBatchRowTypes(item, model, modelMap))
  }

  if (value !== null && typeof value === 'object') {
    return coerceBatchRowTypes(value, model, modelMap)
  }

  return value
}

function coerceAggSubFields(
  inner: Record<string, unknown>,
  model: Model,
): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  let changed = false

  for (const fieldName in inner) {
    if (!Object.prototype.hasOwnProperty.call(inner, fieldName)) continue
    const fieldVal = inner[fieldName]
    const field = model.fields.find((f) => f.name === fieldName)

    if (field && isDateTimeFieldType(field.type)) {
      const coerced = coerceDateTime(fieldVal)
      result[fieldName] = coerced
      if (coerced !== fieldVal) changed = true
    } else {
      result[fieldName] = fieldVal
    }
  }

  return changed ? result : inner
}

function coerceAggregateResult(
  obj: unknown,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  if (!modelMap || obj === null || obj === undefined || typeof obj !== 'object')
    return obj

  const model = modelMap.get(modelName)
  if (!model) return obj

  const record = obj as Record<string, unknown>
  const result: Record<string, unknown> = {}
  let changed = false

  for (const key in record) {
    if (!Object.prototype.hasOwnProperty.call(record, key)) continue
    const val = record[key]

    if (
      (key === '_min' || key === '_max') &&
      val !== null &&
      val !== undefined &&
      typeof val === 'object'
    ) {
      const coerced = coerceAggSubFields(val as Record<string, unknown>, model)
      result[key] = coerced
      if (coerced !== val) changed = true
    } else {
      result[key] = val
    }
  }

  return changed ? result : obj
}

function coerceGroupByResults(
  arr: unknown,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  if (!modelMap || !Array.isArray(arr)) return arr

  const model = modelMap.get(modelName)
  if (!model) return arr

  return arr.map((item) => {
    if (item === null || item === undefined || typeof item !== 'object')
      return item

    const record = item as Record<string, unknown>
    const result: Record<string, unknown> = {}
    let changed = false

    for (const key in record) {
      if (!Object.prototype.hasOwnProperty.call(record, key)) continue
      const val = record[key]

      if (
        (key === '_min' || key === '_max') &&
        val !== null &&
        val !== undefined &&
        typeof val === 'object'
      ) {
        const coerced = coerceAggSubFields(
          val as Record<string, unknown>,
          model,
        )
        result[key] = coerced
        if (coerced !== val) changed = true
      } else {
        const field = model.fields.find((f) => f.name === key)
        if (field && isDateTimeFieldType(field.type)) {
          const coerced = coerceDateTime(val)
          result[key] = coerced
          if (coerced !== val) changed = true
        } else {
          result[key] = val
        }
      }
    }

    return changed ? result : item
  })
}

function parseBatchValue(
  rawValue: unknown,
  method: PrismaMethod,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  switch (method) {
    case 'findMany': {
      const parsed = parseJsonValue(rawValue)
      const arr = Array.isArray(parsed) ? parsed : []
      return coerceBatchValue(arr, 'findMany', modelName, modelMap)
    }
    case 'findFirst':
    case 'findUnique': {
      const parsed = parseJsonValue(rawValue)
      const val = parsed ?? null
      return val === null
        ? null
        : coerceBatchValue(val, method, modelName, modelMap)
    }
    case 'count': {
      return parseCountValue(rawValue)
    }
    case 'aggregate': {
      const parsed = parseJsonValue(rawValue)
      const obj = (parsed ?? {}) as Record<string, unknown>
      const transformed = transformQueryResults('aggregate', [obj])
      return coerceAggregateResult(transformed, modelName, modelMap)
    }
    case 'groupBy': {
      const parsed = parseJsonValue(rawValue)
      const arr = Array.isArray(parsed) ? parsed : []
      const transformed = transformQueryResults('groupBy', arr)
      return coerceGroupByResults(transformed, modelName, modelMap)
    }
    default:
      return rawValue
  }
}

export function parseBatchResults(
  row: Record<string, unknown>,
  keys: string[],
  queries: Record<string, BatchQuery>,
  aliases?: string[],
  modelMap?: Map<string, Model>,
): Record<string, unknown> {
  const results: Record<string, unknown> = {}

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    const columnKey = aliases?.[i] ?? key
    const rawValue = row[columnKey]
    const query = queries[key]

    results[key] = parseBatchValue(
      rawValue,
      query.method,
      query.model,
      modelMap,
    )
  }

  return results
}
