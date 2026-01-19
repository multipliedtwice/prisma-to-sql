import { SqlDialect } from '../../sql-builder-dialect'
import { needsQuoting } from './validators/sql-validators'
import { isEmptyString, isNotNullish } from './validators/type-guards'

const NUL = String.fromCharCode(0)

function containsControlChars(s: string): boolean {
  return s.includes(NUL) || s.includes('\n') || s.includes('\r')
}

function containsUnsafeSqlFragmentChars(s: string): boolean {
  return containsControlChars(s) || s.includes(';')
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

export function col(alias: string, field: string): string {
  if (isEmptyString(alias)) {
    throw new Error('col: alias is required and cannot be empty')
  }

  if (isEmptyString(field)) {
    throw new Error('col: field is required and cannot be empty')
  }

  return `${alias}.${quote(field)}`
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
  if (containsUnsafeSqlFragmentChars(a)) {
    throw new Error(`alias contains unsafe characters: ${JSON.stringify(a)}`)
  }
  if (!/^[A-Za-z_]\w*$/.test(a)) {
    throw new Error(
      `alias must be a simple identifier, got: ${JSON.stringify(a)}`,
    )
  }
}

export function assertSafeTableRef(tableRef: string): void {
  const t = String(tableRef)
  if (t.trim().length === 0) {
    throw new Error('tableName/tableRef is required and cannot be empty')
  }
  if (containsUnsafeSqlFragmentChars(t)) {
    throw new Error(
      `tableName/tableRef contains unsafe characters: ${JSON.stringify(t)}`,
    )
  }
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
