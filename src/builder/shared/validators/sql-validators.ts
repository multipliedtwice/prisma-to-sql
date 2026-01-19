import { DEFAULT_WHERE_CLAUSE, REGEX_CACHE, SQL_KEYWORDS } from '../constants'
import {
  isNotNullish,
  isNonEmptyString,
  hasValidContent,
  hasRequiredKeywords,
} from './type-guards'
import { SqlDialect } from '../../../sql-builder-dialect'
import { ParamMap } from '@dee-wan/schema-parser'

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

function assertNoGaps(
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

  assertNoGaps(scan, 1, scan.max, sql)
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

  assertNoGaps(scan, scan.min, scan.max, sql)
}

function assertOrThrow(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message)
}

function dialectPlaceholderPrefix(dialect: SqlDialect): string {
  return dialect === 'sqlite' ? '?' : '$'
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

function getPlaceholderIndices(
  sql: string,
  dialect: SqlDialect,
): { indices: number[]; sawNumbered: boolean; sawAnonymous: boolean } {
  if (dialect === 'sqlite') return parseSqlitePlaceholderIndices(sql)
  return {
    indices: parseDollarPlaceholderIndices(sql),
    sawNumbered: false,
    sawAnonymous: false,
  }
}

function maxIndex(indices: readonly number[]): number {
  return indices.length > 0 ? Math.max(...indices) : 0
}

function ensureNoMixedSqlitePlaceholders(
  sawNumbered: boolean,
  sawAnonymous: boolean,
): void {
  assertOrThrow(
    !(sawNumbered && sawAnonymous),
    `CRITICAL: Mixed sqlite placeholders ('?' and '?NNN') are not supported.`,
  )
}

function ensurePlaceholderMaxMatchesMappingsLength(
  max: number,
  mappingsLength: number,
  dialect: SqlDialect,
): void {
  assertOrThrow(
    max === mappingsLength,
    `CRITICAL: SQL placeholder max mismatch - max is ${dialectPlaceholderPrefix(dialect)}${max}, but mappings length is ${mappingsLength}.`,
  )
}

function ensureSequentialPlaceholders(
  placeholders: ReadonlySet<number>,
  max: number,
  dialect: SqlDialect,
): void {
  const prefix = dialectPlaceholderPrefix(dialect)
  for (let i = 1; i <= max; i++) {
    assertOrThrow(
      placeholders.has(i),
      `CRITICAL: Missing SQL placeholder ${prefix}${i} - placeholders must be sequential 1..${max}.`,
    )
  }
}

function validateMappingIndex(mapping: ParamMap, max: number): void {
  assertOrThrow(
    Number.isInteger(mapping.index) &&
      mapping.index >= 1 &&
      mapping.index <= max,
    `CRITICAL: ParamMapping index ${mapping.index} out of range 1..${max}.`,
  )
}

function ensureUniqueMappingIndex(
  mappingIndices: Set<number>,
  index: number,
  dialect: SqlDialect,
): void {
  assertOrThrow(
    !mappingIndices.has(index),
    `CRITICAL: Duplicate ParamMapping index ${index} - each placeholder index must map to exactly one ParamMap.`,
  )
  mappingIndices.add(index)
}

function ensureMappingIndexExistsInSql(
  placeholders: ReadonlySet<number>,
  index: number,
): void {
  assertOrThrow(
    placeholders.has(index),
    `CRITICAL: ParamMapping index ${index} not found in SQL placeholders.`,
  )
}

function validateMappingValueShape(mapping: ParamMap): void {
  assertOrThrow(
    !(mapping.dynamicName !== undefined && mapping.value !== undefined),
    `CRITICAL: ParamMap ${mapping.index} has both dynamicName and value`,
  )

  assertOrThrow(
    !(mapping.dynamicName === undefined && mapping.value === undefined),
    `CRITICAL: ParamMap ${mapping.index} has neither dynamicName nor value`,
  )
}

function ensureMappingsCoverAllIndices(
  mappingIndices: ReadonlySet<number>,
  max: number,
  dialect: SqlDialect,
): void {
  const prefix = dialectPlaceholderPrefix(dialect)
  for (let i = 1; i <= max; i++) {
    assertOrThrow(
      mappingIndices.has(i),
      `CRITICAL: Missing ParamMap for placeholder ${prefix}${i} - mappings must cover 1..${max} with no gaps.`,
    )
  }
}

function validateMappingsAgainstPlaceholders(
  mappings: readonly ParamMap[],
  placeholders: ReadonlySet<number>,
  max: number,
  dialect: SqlDialect,
): void {
  const mappingIndices = new Set<number>()

  for (const mapping of mappings) {
    validateMappingIndex(mapping, max)
    ensureUniqueMappingIndex(mappingIndices, mapping.index, dialect)
    ensureMappingIndexExistsInSql(placeholders, mapping.index)
    validateMappingValueShape(mapping)
  }

  ensureMappingsCoverAllIndices(mappingIndices, max, dialect)
}

export function validateSqlPositions(
  sql: string,
  mappings: readonly ParamMap[],
  dialect: SqlDialect,
): void {
  const { indices, sawNumbered, sawAnonymous } = getPlaceholderIndices(
    sql,
    dialect,
  )

  if (dialect === 'sqlite') {
    ensureNoMixedSqlitePlaceholders(sawNumbered, sawAnonymous)
  }

  const placeholders = new Set(indices)

  if (placeholders.size === 0 && mappings.length === 0) return

  const max = maxIndex(indices)

  ensurePlaceholderMaxMatchesMappingsLength(max, mappings.length, dialect)
  ensureSequentialPlaceholders(placeholders, max, dialect)
  validateMappingsAgainstPlaceholders(mappings, placeholders, max, dialect)
}
