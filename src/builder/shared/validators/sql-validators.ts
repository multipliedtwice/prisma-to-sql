import { DEFAULT_WHERE_CLAUSE, REGEX_CACHE, SQL_KEYWORDS } from '../constants'
import {
  isNotNullish,
  isNonEmptyString,
  hasValidContent,
  hasRequiredKeywords,
} from './type-guards'
import { SqlDialect } from '../../../sql-builder-dialect'

export function isValidWhereClause(clause: string): boolean {
  return (
    isNotNullish(clause) &&
    clause.trim().length > 0 &&
    clause !== DEFAULT_WHERE_CLAUSE
  )
}

export function isEmptyWhere(
  where: Record<string, unknown> | null | undefined,
): boolean {
  if (!isNotNullish(where)) return true
  return Object.keys(where).length === 0
}

export function validateSelectQuery(sql: string): void {
  if (!hasValidContent(sql)) {
    throw new Error('CRITICAL: Generated empty SQL query')
  }

  if (!hasRequiredKeywords(sql)) {
    throw new Error(
      `CRITICAL: Invalid SQL structure. SQL: ${sql.substring(0, 100)}...`,
    )
  }
}

function sqlPreview(sql: string): string {
  return `${sql.substring(0, 100)}...`
}

type PlaceholderScan = {
  count: number
  min: number
  max: number
  seen: Uint8Array
}

type ParseResult = {
  next: number
  num: number
  ok: boolean
}

function parseDollarNumber(sql: string, start: number, n: number): ParseResult {
  let i = start
  let num = 0
  let hasDigit = false

  while (i < n) {
    const c = sql.charCodeAt(i)
    if (c < 48 || c > 57) break
    hasDigit = true
    num = num * 10 + (c - 48)
    i++
  }

  if (!hasDigit || num <= 0) return { next: i, num: 0, ok: false }
  return { next: i, num, ok: true }
}

function scanDollarPlaceholders(
  sql: string,
  markUpTo: number,
): PlaceholderScan {
  const seen = new Uint8Array(markUpTo + 1)
  let count = 0
  let min = Number.POSITIVE_INFINITY
  let max = 0

  const n = sql.length
  let i = 0

  while (i < n) {
    if (sql.charCodeAt(i) !== 36) {
      i++
      continue
    }

    const { next, num, ok } = parseDollarNumber(sql, i + 1, n)
    i = next
    if (!ok) continue

    count++
    if (num < min) min = num
    if (num > max) max = num
    if (num <= markUpTo) seen[num] = 1
  }

  return { count, min, max, seen }
}

function assertNoGapsDollar(
  scan: PlaceholderScan,
  rangeMin: number,
  rangeMax: number,
  sql: string,
): void {
  for (let k = rangeMin; k <= rangeMax; k++) {
    if (scan.seen[k] !== 1) {
      throw new Error(
        `CRITICAL: Parameter mismatch - SQL is missing placeholder $${k}. ` +
          `Placeholders must cover ${rangeMin}..${rangeMax} with no gaps. SQL: ${sqlPreview(sql)}`,
      )
    }
  }
}

export function validateParamConsistency(
  sql: string,
  params: readonly unknown[],
): void {
  const paramLen = params.length

  if (paramLen === 0) {
    if (sql.indexOf('$') === -1) return
  }

  const scan = scanDollarPlaceholders(sql, paramLen)

  if (scan.count === 0) {
    if (paramLen !== 0) {
      throw new Error(
        `CRITICAL: Parameter mismatch - SQL has no placeholders but ${paramLen} params provided.`,
      )
    }
    return
  }

  if (scan.max !== paramLen) {
    throw new Error(
      `CRITICAL: Parameter mismatch - SQL max placeholder is $${scan.max} but ${paramLen} params provided. ` +
        `This will cause SQL execution to fail. SQL: ${sqlPreview(sql)}`,
    )
  }

  assertNoGapsDollar(scan, 1, scan.max, sql)
}

export function needsQuoting(id: string): boolean {
  if (!isNonEmptyString(id)) return true

  const isKeyword = SQL_KEYWORDS.has(id.toLowerCase())
  if (isKeyword) return true

  const isValidIdentifier = REGEX_CACHE.VALID_IDENTIFIER.test(id)
  return !isValidIdentifier
}

export function validateParamConsistencyFragment(
  sql: string,
  params: readonly unknown[],
): void {
  const paramLen = params.length
  const scan = scanDollarPlaceholders(sql, paramLen)

  if (scan.max === 0) return

  if (scan.max > paramLen) {
    throw new Error(
      `CRITICAL: Parameter mismatch - SQL references $${scan.max} but only ${paramLen} params provided. SQL: ${sqlPreview(sql)}`,
    )
  }

  assertNoGapsDollar(scan, scan.min, scan.max, sql)
}

function assertOrThrow(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

function parseSqlitePlaceholderIndices(sql: string): {
  indices: number[]
  sawNumbered: boolean
  sawAnonymous: boolean
} {
  const re = /\?(?:(\d+))?/g
  const indices: number[] = []
  let anonCount = 0
  let sawNumbered = false
  let sawAnonymous = false

  for (const m of sql.matchAll(re)) {
    const n = m[1]
    if (n) {
      sawNumbered = true
      indices.push(parseInt(n, 10))
    } else {
      sawAnonymous = true
      anonCount += 1
      indices.push(anonCount)
    }
  }

  return { indices, sawNumbered, sawAnonymous }
}

function parseDollarPlaceholderIndices(sql: string): number[] {
  const re = /\$(\d+)/g
  const indices: number[] = []
  for (const m of sql.matchAll(re)) indices.push(parseInt(m[1], 10))
  return indices
}

function maxIndex(indices: readonly number[]): number {
  return indices.length > 0 ? Math.max(...indices) : 0
}

function ensureSequentialIndices(
  seen: ReadonlySet<number>,
  max: number,
  prefix: string,
): void {
  for (let i = 1; i <= max; i++) {
    assertOrThrow(
      seen.has(i),
      `CRITICAL: Missing SQL placeholder ${prefix}${i} - placeholders must be sequential 1..${max}.`,
    )
  }
}

function validateSqlitePlaceholders(
  sql: string,
  params: readonly unknown[],
): void {
  const paramLen = params.length
  const { indices, sawNumbered, sawAnonymous } =
    parseSqlitePlaceholderIndices(sql)

  if (indices.length === 0) {
    if (paramLen !== 0) {
      throw new Error(
        `CRITICAL: Parameter mismatch - SQL has no sqlite placeholders but ${paramLen} params provided. SQL: ${sqlPreview(sql)}`,
      )
    }
    return
  }

  assertOrThrow(
    !(sawNumbered && sawAnonymous),
    `CRITICAL: Mixed sqlite placeholders ('?' and '?NNN') are not supported.`,
  )

  const max = maxIndex(indices)
  assertOrThrow(
    max === paramLen,
    `CRITICAL: SQL placeholder max mismatch - max is ?${max}, but params length is ${paramLen}. SQL: ${sqlPreview(sql)}`,
  )

  const set = new Set(indices)
  ensureSequentialIndices(set, max, '?')
}

function validateDollarPlaceholders(
  sql: string,
  params: readonly unknown[],
): void {
  validateParamConsistency(sql, params)
}

function detectPlaceholderStyle(sql: string): {
  hasDollar: boolean
  hasSqliteQ: boolean
} {
  const hasDollar = /\$\d+/.test(sql)
  const hasSqliteQ = /\?(?:\d+)?/.test(sql)
  return { hasDollar, hasSqliteQ }
}

/**
 * Dialect-aware consistency validator.
 * - postgres: enforces $1..$N (existing behavior)
 * - sqlite: supports either $1..$N OR ?/?NNN, but rejects mixing.
 */
export function validateParamConsistencyByDialect(
  sql: string,
  params: readonly unknown[],
  dialect: SqlDialect,
): void {
  const { hasDollar, hasSqliteQ } = detectPlaceholderStyle(sql)

  if (dialect !== 'sqlite') {
    if (hasSqliteQ && !hasDollar) {
      throw new Error(
        `CRITICAL: Non-sqlite dialect query contains sqlite '?' placeholders. SQL: ${sqlPreview(sql)}`,
      )
    }
    return validateDollarPlaceholders(sql, params)
  }

  if (hasDollar && hasSqliteQ) {
    throw new Error(
      `CRITICAL: Mixed placeholder styles ($N and ?/ ?NNN) are not supported. SQL: ${sqlPreview(sql)}`,
    )
  }

  if (hasSqliteQ) return validateSqlitePlaceholders(sql, params)
  return validateDollarPlaceholders(sql, params)
}
