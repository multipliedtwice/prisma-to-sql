import { SqlDialect } from '../../sql-builder-dialect'
import { needsQuoting } from './validators/sql-validators'
import { isEmptyString, isNotNullish } from './validators/type-guards'
import type { Model } from '../../types'
import { getColumnMap, getQuotedColumn } from './model-field-cache'
import { ALIAS_FORBIDDEN_KEYWORDS } from './constants'

const COL_EXPR_CACHE = new WeakMap<Model, Map<string, string>>()
const COL_WITH_ALIAS_CACHE = new WeakMap<Model, Map<string, string>>()

function containsControlChars(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const code = s.charCodeAt(i)
    if ((code >= 0 && code <= 31) || code === 127) {
      return true
    }
  }
  return false
}

function assertNoControlChars(label: string, s: string): void {
  if (containsControlChars(s)) {
    throw new Error(
      `${label} contains invalid characters: ${JSON.stringify(s)}`,
    )
  }
}

function quoteRawIdent(id: string): string {
  return `"${id.replace(/"/g, '""')}"`
}

function isIdentCharCode(c: number): boolean {
  return (
    (c >= 48 && c <= 57) ||
    (c >= 65 && c <= 90) ||
    (c >= 97 && c <= 122) ||
    c === 95
  )
}

function isIdentStartCharCode(c: number): boolean {
  return (c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c === 95
}

const MAX_PARSE_ITERATIONS = 10000

function parseQuotedPart(input: string, start: number): number {
  const n = input.length
  let i = start + 1
  let sawAny = false
  let iterations = 0

  while (i < n) {
    if (++iterations > MAX_PARSE_ITERATIONS) {
      throw new Error('Table name parsing exceeded complexity limit')
    }

    const c = input.charCodeAt(i)
    if (c === 34) {
      const next = i + 1
      if (next < n && input.charCodeAt(next) === 34) {
        sawAny = true
        i += 2
        continue
      }
      if (!sawAny) {
        throw new Error(
          `qualified name has empty quoted identifier part: ${JSON.stringify(input)}`,
        )
      }
      return i + 1
    }
    if (c === 10 || c === 13 || c === 0) {
      throw new Error(
        `qualified name contains invalid characters: ${JSON.stringify(input)}`,
      )
    }
    sawAny = true
    i++
  }

  throw new Error(
    `qualified name has unterminated quoted identifier: ${JSON.stringify(input)}`,
  )
}

function parseUnquotedPart(input: string, start: number): number {
  const n = input.length
  let i = start

  if (i >= n) {
    throw new Error(`qualified name is invalid: ${JSON.stringify(input)}`)
  }

  const c0 = input.charCodeAt(i)
  if (!isIdentStartCharCode(c0)) {
    throw new Error(
      `qualified name must use identifiers (or quoted identifiers). Got: ${JSON.stringify(input)}`,
    )
  }
  i++

  while (i < n) {
    const c = input.charCodeAt(i)
    if (c === 46) break
    if (!isIdentCharCode(c)) {
      throw new Error(
        `qualified name contains invalid identifier characters: ${JSON.stringify(input)}`,
      )
    }
    i++
  }

  return i
}

function validateQualifiedNameFormat(trimmed: string): void {
  for (let i = 0; i < trimmed.length; i++) {
    const c = trimmed.charCodeAt(i)
    if (c === 9 || c === 11 || c === 12 || c === 32) {
      throw new Error(
        `tableName/tableRef must not contain whitespace: ${JSON.stringify(trimmed)}`,
      )
    }
    if (c === 59) {
      throw new Error(
        `tableName/tableRef must not contain ';': ${JSON.stringify(trimmed)}`,
      )
    }
    if (c === 40 || c === 41) {
      throw new Error(
        `tableName/tableRef must not contain parentheses: ${JSON.stringify(trimmed)}`,
      )
    }
  }
}

function parseQualifiedNameParts(trimmed: string): void {
  let i = 0
  const n = trimmed.length
  let parts = 0

  while (i < n) {
    const c = trimmed.charCodeAt(i)
    if (c === 46) {
      throw new Error(
        `tableName/tableRef has empty identifier part: ${JSON.stringify(trimmed)}`,
      )
    }

    if (c === 34) {
      i = parseQuotedPart(trimmed, i)
    } else {
      i = parseUnquotedPart(trimmed, i)
    }

    parts++
    if (parts > 2) {
      throw new Error(
        `tableName/tableRef must be 'table' or 'schema.table' (max 2 parts). Got: ${JSON.stringify(trimmed)}`,
      )
    }

    if (i === n) break

    if (trimmed.charCodeAt(i) !== 46) {
      throw new Error(
        `tableName/tableRef is invalid: ${JSON.stringify(trimmed)}`,
      )
    }
    i++

    if (i === n) {
      throw new Error(
        `tableName/tableRef cannot end with '.': ${JSON.stringify(trimmed)}`,
      )
    }
  }
}

function assertSafeQualifiedName(input: string): void {
  const raw = String(input)
  const trimmed = raw.trim()

  if (trimmed.length === 0) {
    throw new Error('tableName/tableRef is required and cannot be empty')
  }

  if (raw !== trimmed) {
    throw new Error(
      `tableName/tableRef must not contain leading/trailing whitespace: ${JSON.stringify(raw)}`,
    )
  }

  assertNoControlChars('tableName/tableRef', trimmed)
  validateQualifiedNameFormat(trimmed)
  parseQualifiedNameParts(trimmed)
}

export function quote(id: string): string {
  if (isEmptyString(id)) {
    throw new Error('quote: identifier is required and cannot be empty')
  }

  if (containsControlChars(id)) {
    throw new Error(
      `quote: identifier contains invalid characters: ${JSON.stringify(id)}`,
    )
  }

  if (needsQuoting(id)) {
    return quoteRawIdent(id)
  }

  return id
}

export function quoteColumn(
  model: Model | undefined,
  fieldName: string,
): string {
  if (!model) return quote(fieldName)
  const cached = getQuotedColumn(model, fieldName)
  return cached || quote(fieldName)
}

function getOrCreateCache<K extends object, V>(
  weakMap: WeakMap<K, Map<string, V>>,
  key: K,
): Map<string, V> {
  let cache = weakMap.get(key)
  if (!cache) {
    cache = new Map()
    weakMap.set(key, cache)
  }
  return cache
}

export function col(alias: string, field: string, model?: Model): string {
  if (!model) return `${alias}.${quote(field)}`

  const cache = getOrCreateCache(COL_EXPR_CACHE, model)
  const cacheKey = `${alias}.${field}`

  let cached = cache.get(cacheKey)
  if (cached) return cached

  const quotedCol = getQuotedColumn(model, field) || quote(field)
  cached = `${alias}.${quotedCol}`
  cache.set(cacheKey, cached)
  return cached
}

export function colWithAlias(
  alias: string,
  field: string,
  model?: Model,
): string {
  if (isEmptyString(alias)) {
    throw new Error('colWithAlias: alias is required and cannot be empty')
  }

  if (isEmptyString(field)) {
    throw new Error('colWithAlias: field is required and cannot be empty')
  }

  if (!model) {
    return `${alias}.${quote(field)} AS ${quote(field)}`
  }

  const cache = getOrCreateCache(COL_WITH_ALIAS_CACHE, model)
  const cacheKey = `${alias}.${field}`

  let cached = cache.get(cacheKey)
  if (cached) return cached

  const columnMap = getColumnMap(model)
  const columnName = columnMap.get(field) || field
  const quotedCol = getQuotedColumn(model, field) || quote(columnName)
  const columnRef = `${alias}.${quotedCol}`

  if (columnName !== field) {
    cached = `${columnRef} AS ${quote(field)}`
  } else {
    cached = columnRef
  }

  cache.set(cacheKey, cached)
  return cached
}

export function sqlStringLiteral(value: string): string {
  if (containsControlChars(value)) {
    throw new Error('sqlStringLiteral: value contains invalid characters')
  }
  return `'${value.replace(/'/g, "''")}'`
}

export function buildTableReference(
  schemaName: string,
  tableName: string,
  dialect?: SqlDialect,
): string {
  if (isEmptyString(tableName)) {
    throw new Error(
      'buildTableReference: tableName is required and cannot be empty',
    )
  }

  if (containsControlChars(tableName)) {
    throw new Error(
      'buildTableReference: tableName contains invalid characters',
    )
  }

  const d = dialect ?? 'postgres'
  if (d === 'sqlite') {
    return quote(tableName)
  }

  if (isEmptyString(schemaName)) {
    throw new Error(
      'buildTableReference: schemaName is required and cannot be empty',
    )
  }

  if (containsControlChars(schemaName)) {
    throw new Error(
      'buildTableReference: schemaName contains invalid characters',
    )
  }

  const safeSchema = schemaName.replace(/"/g, '""')
  const safeTable = tableName.replace(/"/g, '""')
  return `"${safeSchema}"."${safeTable}"`
}

export function assertSafeAlias(alias: string): void {
  if (typeof alias !== 'string') {
    throw new Error(`Invalid alias: expected string, got ${typeof alias}`)
  }

  const a = alias.trim()

  if (a.length === 0) {
    throw new Error('Invalid alias: required and cannot be empty')
  }

  if (a !== alias) {
    throw new Error('Invalid alias: leading/trailing whitespace')
  }

  if (containsControlChars(a)) {
    throw new Error(
      'Invalid alias: contains unsafe characters (control characters)',
    )
  }

  if (a.includes('"') || a.includes("'") || a.includes('`')) {
    throw new Error('Invalid alias: contains unsafe characters (quotes)')
  }

  if (a.includes(';')) {
    throw new Error('Invalid alias: contains unsafe characters (semicolon)')
  }

  if (a.includes('--') || a.includes('/*') || a.includes('*/')) {
    throw new Error(
      'Invalid alias: contains unsafe characters (SQL comment tokens)',
    )
  }

  if (/\s/.test(a)) {
    throw new Error(
      'Invalid alias: must be a simple identifier without whitespace',
    )
  }

  if (!/^[A-Za-z_]\w*$/.test(a)) {
    throw new Error(
      `Invalid alias: must be a simple identifier (alphanumeric with underscores): "${alias}"`,
    )
  }

  const lowered = a.toLowerCase()
  if (ALIAS_FORBIDDEN_KEYWORDS.has(lowered)) {
    throw new Error(
      `Invalid alias: '${alias}' is a SQL keyword that would break query parsing. ` +
        `Forbidden aliases: ${[...ALIAS_FORBIDDEN_KEYWORDS].join(', ')}`,
    )
  }
}

export function assertSafeTableRef(tableRef: string): void {
  assertSafeQualifiedName(tableRef)
}

export function normalizeKeyList(input: unknown): string[] {
  if (!isNotNullish(input)) return []

  if (Array.isArray(input)) {
    const out: string[] = []
    for (const v of input) {
      const s = String(v).trim()
      if (s.length > 0) out.push(s)
    }
    return out
  }

  if (typeof input === 'string') {
    const raw = input.trim()
    if (raw.length === 0) return []
    if (raw.includes(',')) {
      return raw
        .split(',')
        .map((s) => s.trim())
        .filter((s) => s.length > 0)
    }
    return [raw]
  }

  const s = String(input).trim()
  return s.length > 0 ? [s] : []
}
