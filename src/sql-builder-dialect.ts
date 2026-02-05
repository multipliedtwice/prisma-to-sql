import { normalizeValue } from './utils/normalize-value'

export type SqlDialect = 'postgres' | 'sqlite'

let globalDialect: SqlDialect = 'postgres'

export function setGlobalDialect(dialect: SqlDialect): void {
  if (dialect !== 'postgres' && dialect !== 'sqlite') {
    throw new Error(
      `Invalid dialect: ${dialect}. Must be 'postgres' or 'sqlite'`,
    )
  }
  globalDialect = dialect
}

export function getGlobalDialect(): SqlDialect {
  return globalDialect
}

function assertNonEmpty(value: string, name: string): void {
  if (!value || value.trim().length === 0) {
    throw new Error(`${name} is required and cannot be empty`)
  }
}

export function arrayContains(
  column: string,
  value: string,
  arrayType: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'arrayContains column')
  assertNonEmpty(value, 'arrayContains value')

  if (dialect === 'postgres') {
    return `${column} @> ARRAY[${value}]::${arrayType}`
  }

  return `EXISTS (SELECT 1 FROM json_each(${column}) WHERE value = ${value})`
}

export function arrayOverlaps(
  column: string,
  value: string,
  arrayType: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'arrayOverlaps column')
  assertNonEmpty(value, 'arrayOverlaps value')

  if (dialect === 'postgres') {
    return `${column} && ${value}::${arrayType}`
  }

  return `EXISTS (
    SELECT 1 FROM json_each(${column}) AS col
    JOIN json_each(${value}) AS val
    WHERE col.value = val.value
  )`
}

export function arrayContainsAll(
  column: string,
  value: string,
  arrayType: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'arrayContainsAll column')
  assertNonEmpty(value, 'arrayContainsAll value')

  if (dialect === 'postgres') {
    return `${column} @> ${value}::${arrayType}`
  }

  return `NOT EXISTS (
    SELECT 1 FROM json_each(${value}) AS val
    WHERE NOT EXISTS (
      SELECT 1 FROM json_each(${column}) AS col
      WHERE col.value = val.value
    )
  )`
}

export function arrayIsEmpty(column: string, dialect: SqlDialect): string {
  assertNonEmpty(column, 'arrayIsEmpty column')

  if (dialect === 'postgres') {
    return `(${column} IS NULL OR array_length(${column}, 1) IS NULL)`
  }
  return `(${column} IS NULL OR json_array_length(${column}) = 0)`
}

export function arrayIsNotEmpty(column: string, dialect: SqlDialect): string {
  assertNonEmpty(column, 'arrayIsNotEmpty column')

  if (dialect === 'postgres') {
    return `(${column} IS NOT NULL AND array_length(${column}, 1) IS NOT NULL)`
  }
  return `(${column} IS NOT NULL AND json_array_length(${column}) > 0)`
}

export function arrayEquals(
  column: string,
  value: string,
  arrayType: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'arrayEquals column')
  assertNonEmpty(value, 'arrayEquals value')

  if (dialect === 'postgres') {
    return `${column} = ${value}::${arrayType}`
  }

  return `json(${column}) = json(${value})`
}

export function caseInsensitiveLike(
  column: string,
  pattern: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'caseInsensitiveLike column')
  assertNonEmpty(pattern, 'caseInsensitiveLike pattern')

  if (dialect === 'postgres') {
    return `${column} ILIKE ${pattern}`
  }

  return `LOWER(${column}) LIKE LOWER(${pattern})`
}

export function caseInsensitiveEquals(
  column: string,
  value: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'caseInsensitiveEquals column')
  assertNonEmpty(value, 'caseInsensitiveEquals value')

  return `LOWER(${column}) = LOWER(${value})`
}

export function jsonExtractText(
  column: string,
  path: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'jsonExtractText column')
  assertNonEmpty(path, 'jsonExtractText path')

  if (dialect === 'postgres') {
    const p = String(path).trim()
    const pathExpr = /^\$\d+$/.test(p) ? `${p}::text[]` : p
    return `${column}#>>${pathExpr}`
  }

  return `json_extract(${column}, ${path})`
}

export function jsonExtractNumeric(
  column: string,
  path: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'jsonExtractNumeric column')
  assertNonEmpty(path, 'jsonExtractNumeric path')

  if (dialect === 'postgres') {
    const p = String(path).trim()
    const pathExpr = /^\$\d+$/.test(p) ? `${p}::text[]` : p
    return `(${column}#>>${pathExpr})::numeric`
  }
  return `CAST(json_extract(${column}, ${path}) AS REAL)`
}

export function jsonToText(column: string, dialect: SqlDialect): string {
  assertNonEmpty(column, 'jsonToText column')

  if (dialect === 'postgres') {
    return `${column}::text`
  }
  return column
}

export function inArray(
  column: string,
  value: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'inArray column')
  assertNonEmpty(value, 'inArray value')

  if (dialect === 'postgres') {
    return `${column} = ANY(${value})`
  }

  return `${column} IN (SELECT value FROM json_each(${value}))`
}

export function notInArray(
  column: string,
  value: string,
  dialect: SqlDialect,
): string {
  assertNonEmpty(column, 'notInArray column')
  assertNonEmpty(value, 'notInArray value')

  if (dialect === 'postgres') {
    return `${column} != ALL(${value})`
  }

  return `${column} NOT IN (SELECT value FROM json_each(${value}))`
}

export function getArrayType(prismaType: string, dialect: SqlDialect): string {
  if (!prismaType || prismaType.length === 0) {
    return dialect === 'sqlite' ? 'TEXT' : 'text[]'
  }

  if (dialect === 'sqlite') {
    return 'TEXT'
  }

  const baseType = prismaType.replace(/\[\]|\?/g, '')

  switch (baseType) {
    case 'String':
      return 'text[]'
    case 'Int':
      return 'integer[]'
    case 'Float':
      return 'double precision[]'
    case 'Decimal':
      return 'numeric[]'
    case 'Boolean':
      return 'boolean[]'
    case 'BigInt':
      return 'bigint[]'
    case 'DateTime':
      return 'timestamptz[]'
    default:
      return `"${baseType}"[]`
  }
}

export function jsonAgg(content: string, dialect: SqlDialect): string {
  assertNonEmpty(content, 'jsonAgg content')

  if (dialect === 'postgres') {
    return `json_agg(${content})`
  }

  return `json_group_array(${content})`
}

export function jsonBuildObject(pairs: string, dialect: SqlDialect): string {
  const safePairs = (pairs ?? '').trim()

  if (dialect === 'postgres') {
    return safePairs.length > 0
      ? `json_build_object(${safePairs})`
      : `json_build_object()`
  }

  return safePairs.length > 0 ? `json_object(${safePairs})` : `json_object()`
}

export function prepareArrayParam(
  value: unknown[],
  dialect: SqlDialect,
): unknown {
  if (!Array.isArray(value)) {
    throw new Error('prepareArrayParam requires array value')
  }

  if (dialect === 'postgres') {
    return value.map((v) => normalizeValue(v))
  }
  return JSON.stringify(value)
}
