import {
  DEFAULT_WHERE_CLAUSE,
  REGEX_CACHE,
  SQL_KEYWORDS,
  IS_PRODUCTION,
} from '../constants'
import {
  isNotNullish,
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

function sqlPreview(sql: string): string {
  const s = String(sql)
  if (s.length <= 160) return s
  return `${s.slice(0, 160)}...`
}

export function validateSelectQuery(sql: string): void {
  if (IS_PRODUCTION) return

  if (!hasValidContent(sql)) {
    throw new Error('CRITICAL: Generated empty SQL query')
  }

  if (!hasRequiredKeywords(sql)) {
    throw new Error(`CRITICAL: Invalid SQL structure. SQL: ${sqlPreview(sql)}`)
  }
}

type DollarScan = {
  min: number
  max: number
  seen: Uint8Array
  sawAny: boolean
}

function parseDollarNumber(
  sql: string,
  start: number,
): { next: number; num: number } {
  const n = sql.length
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

  if (!hasDigit || num <= 0) return { next: i, num: 0 }
  return { next: i, num }
}

function scanDollarPlaceholders(sql: string, markUpTo: number): DollarScan {
  const seen = new Uint8Array(markUpTo + 1)
  let min = Number.POSITIVE_INFINITY
  let max = 0
  let sawAny = false

  const n = sql.length
  let i = 0

  while (i < n) {
    if (sql.charCodeAt(i) !== 36) {
      i++
      continue
    }

    const parsed = parseDollarNumber(sql, i + 1)
    i = parsed.next

    const num = parsed.num
    if (num === 0) continue

    sawAny = true
    if (num < min) min = num
    if (num > max) max = num
    if (num <= markUpTo) seen[num] = 1
  }

  if (!sawAny) {
    return { min: 0, max: 0, seen, sawAny: false }
  }

  return { min, max, seen, sawAny: true }
}

function assertNoGapsDollar(
  scan: DollarScan,
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
  if (IS_PRODUCTION) return

  const paramLen = params.length
  const scan = scanDollarPlaceholders(sql, paramLen)

  if (paramLen === 0) {
    if (scan.sawAny) {
      throw new Error(
        `CRITICAL: SQL contains placeholders but params is empty. SQL: ${sqlPreview(sql)}`,
      )
    }
    return
  }

  if (!scan.sawAny) {
    throw new Error(
      `CRITICAL: SQL is missing placeholders ($1..$${paramLen}) but params has length ${paramLen}. SQL: ${sqlPreview(sql)}`,
    )
  }

  if (scan.min !== 1) {
    throw new Error(
      `CRITICAL: Placeholder range must start at $1, got min=$${scan.min}. SQL: ${sqlPreview(sql)}`,
    )
  }

  if (scan.max !== paramLen) {
    throw new Error(
      `CRITICAL: Placeholder max must match params length. max=$${scan.max}, params=${paramLen}. SQL: ${sqlPreview(sql)}`,
    )
  }

  assertNoGapsDollar(scan, 1, paramLen, sql)
}

function countQuestionMarkPlaceholders(sql: string): number {
  const s = String(sql)
  let count = 0
  for (let i = 0; i < s.length; i++) {
    if (s.charCodeAt(i) === 63) count++
  }
  return count
}

function validateQuestionMarkConsistency(
  sql: string,
  params: readonly unknown[],
): void {
  if (IS_PRODUCTION) return

  const expected = params.length
  const found = countQuestionMarkPlaceholders(sql)

  if (expected !== found) {
    throw new Error(
      `CRITICAL: Parameter mismatch - expected ${expected} '?' placeholders, found ${found}. SQL: ${sqlPreview(sql)}`,
    )
  }
}

export function validateParamConsistencyByDialect(
  sql: string,
  params: readonly unknown[],
  dialect: SqlDialect,
): void {
  if (IS_PRODUCTION) return

  if (dialect === 'postgres') {
    validateParamConsistency(sql, params)
    return
  }

  if (dialect === 'sqlite') {
    validateQuestionMarkConsistency(sql, params)
    return
  }

  if ((dialect as any) === 'mysql' || (dialect as any) === 'mariadb') {
    validateQuestionMarkConsistency(sql, params)
    return
  }

  validateParamConsistency(sql, params)
}

export function needsQuoting(identifier: string): boolean {
  const s = String(identifier)

  if (!REGEX_CACHE.VALID_IDENTIFIER.test(s)) return true

  const lowered = s.toLowerCase()
  if (SQL_KEYWORDS.has(lowered)) return true

  return false
}
