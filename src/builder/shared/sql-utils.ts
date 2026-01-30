import { SqlDialect } from '../../sql-builder-dialect'
import { needsQuoting } from './validators/sql-validators'
import { isEmptyString, isNotNullish } from './validators/type-guards'
import type { Model } from '../../types'

const NUL = String.fromCharCode(0)

function containsControlChars(s: string): boolean {
  return s.includes(NUL) || s.includes('\n') || s.includes('\r')
}

function assertNoControlChars(label: string, s: string): void {
  if (containsControlChars(s)) {
    throw new Error(
      `${label} contains invalid characters: ${JSON.stringify(s)}`,
    )
  }
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

function parseQuotedPart(input: string, start: number): number {
  const n = input.length
  let i = start + 1
  let sawAny = false

  while (i < n) {
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
          `tableName/tableRef has empty quoted identifier part: ${JSON.stringify(input)}`,
        )
      }
      return i + 1
    }
    if (c === 10 || c === 13 || c === 0) {
      throw new Error(
        `tableName/tableRef contains invalid characters: ${JSON.stringify(input)}`,
      )
    }
    sawAny = true
    i++
  }

  throw new Error(
    `tableName/tableRef has unterminated quoted identifier: ${JSON.stringify(input)}`,
  )
}

function parseUnquotedPart(input: string, start: number): number {
  const n = input.length
  let i = start

  if (i >= n) {
    throw new Error(`tableName/tableRef is invalid: ${JSON.stringify(input)}`)
  }

  const c0 = input.charCodeAt(i)
  if (!isIdentStartCharCode(c0)) {
    throw new Error(
      `tableName/tableRef must use identifiers (or quoted identifiers). Got: ${JSON.stringify(input)}`,
    )
  }
  i++

  while (i < n) {
    const c = input.charCodeAt(i)
    if (c === 46) break
    if (!isIdentCharCode(c)) {
      throw new Error(
        `tableName/tableRef contains invalid identifier characters: ${JSON.stringify(input)}`,
      )
    }
    i++
  }

  return i
}

function assertSafeQualifiedName(tableRef: string): void {
  const raw = String(tableRef)
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
    return `"${id.replace(/"/g, '""')}"`
  }

  return id
}

function pickDbFieldName(field: any): string | undefined {
  const candidates = [
    field?.dbName,
    field?.columnName,
    field?.databaseName,
    field?.mappedName,
  ]
  for (const c of candidates) {
    if (typeof c === 'string') {
      const s = c.trim()
      if (s.length > 0) return s
    }
  }
  return undefined
}

export function resolveColumnName(
  model: Model | undefined,
  fieldName: string,
): string {
  if (!model) return fieldName
  const f = (model.fields as any[]).find((x) => x?.name === fieldName)
  if (!f) return fieldName
  return pickDbFieldName(f) ?? fieldName
}

export function quoteColumn(
  model: Model | undefined,
  fieldName: string,
): string {
  return quote(resolveColumnName(model, fieldName))
}

export function col(alias: string, field: string, model?: Model): string {
  if (isEmptyString(alias)) {
    throw new Error('col: alias is required and cannot be empty')
  }

  if (isEmptyString(field)) {
    throw new Error('col: field is required and cannot be empty')
  }

  const columnName = resolveColumnName(model, field)
  return `${alias}.${quote(columnName)}`
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

  const columnName = resolveColumnName(model, field)
  const columnRef = `${alias}.${quote(columnName)}`

  if (columnName !== field) {
    return `${columnRef} AS ${quote(field)}`
  }

  return columnRef
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
  const a = String(alias)
  if (a.trim().length === 0) {
    throw new Error('alias is required and cannot be empty')
  }
  if (containsControlChars(a) || a.includes(';')) {
    throw new Error(`alias contains unsafe characters: ${JSON.stringify(a)}`)
  }
  if (!/^[A-Za-z_]\w*$/.test(a)) {
    throw new Error(
      `alias must be a simple identifier, got: ${JSON.stringify(a)}`,
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
