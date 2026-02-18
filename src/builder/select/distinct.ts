import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { col, quote } from '../shared/sql-utils'
import { SelectQuerySpec } from '../shared/types'
import {
  isNotNullish,
  isNonEmptyArray,
  isNonEmptyString,
} from '../shared/validators/type-guards'
import { parseOrderByValue, buildOrderByFragment } from '../pagination'
import {
  normalizeOrderByInput,
  OrderBySortObject,
} from '../shared/order-by-utils'
import { SqlDialect } from '../../sql-builder-dialect'

const DISTINCT_ROW_NUMBER_COLUMN = '__tp_rn'
const DISTINCT_WRAPPER_ALIAS = '__tp_distinct'
const DISTINCT_FIRST_ROW = 1
const DEFAULT_PRIMARY_KEY = 'id'
export const COUNT_SELECT_KEY = '_count'

export type OrderByEntry = {
  field: string
  direction: 'asc' | 'desc'
  nulls?: 'first' | 'last'
}

const SELECT_FIELD_REGEX =
  /^\s*("(?:[^"]|"")+"|[a-z_][a-z0-9_]*)\s*\.\s*("(?:[^"]|"")+"|[a-z_][a-z0-9_]*)(?:\s+AS\s+("(?:[^"]|"")+"|[a-z_][a-z0-9_]*))?\s*$/i

function unquoteIdent(s: string): string {
  if (s.startsWith('"') && s.endsWith('"')) {
    return s.slice(1, -1).replace(/""/g, '"')
  }
  return s
}

function parseSelectField(p: string, fromAlias: string): string {
  const match = SELECT_FIELD_REGEX.exec(p)
  if (!match) {
    throw new Error(
      `SQLite distinct emulation requires simple column references.\n` +
        `Complex expressions, functions, and computed fields are not supported.\n` +
        `Got: ${p}\n\n` +
        `Hint: When using distinct with SQLite:\n` +
        `  - Use only direct field selections (e.g., { id: true, name: true })\n` +
        `  - Remove computed fields from select\n` +
        `  - Avoid selecting relation counts or aggregates\n` +
        `  - Or switch to PostgreSQL which supports DISTINCT ON with expressions`,
    )
  }

  const [, alias, column, outputAlias] = match

  const actualAlias = unquoteIdent(alias)
  if (actualAlias.toLowerCase() !== fromAlias.toLowerCase()) {
    throw new Error(
      `Expected alias '${fromAlias}', got '${actualAlias}' in: ${p}`,
    )
  }

  if (outputAlias) {
    return unquoteIdent(outputAlias)
  }

  return unquoteIdent(column)
}

function parseSimpleScalarSelect(select: string, fromAlias: string): string[] {
  const raw = select.trim()
  if (raw.length === 0) return []

  const parts = raw.split(SQL_SEPARATORS.FIELD_LIST)
  const names: string[] = []

  for (const p of parts) {
    const trimmed = p.trim()
    if (trimmed.length === 0) continue
    names.push(parseSelectField(trimmed, fromAlias))
  }

  return names
}

function buildDistinctColumns(
  distinct: readonly string[],
  fromAlias: string,
  model?: any,
): string {
  return distinct
    .map((f) => col(fromAlias, f, model))
    .join(SQL_SEPARATORS.FIELD_LIST)
}

function buildOutputColumns(
  scalarNames: string[],
  includeNames: string[],
  hasCount: boolean,
): string {
  const outputCols = hasCount
    ? [...scalarNames, ...includeNames, COUNT_SELECT_KEY]
    : [...scalarNames, ...includeNames]

  const formatted = outputCols
    .map((n) => quote(n))
    .join(SQL_SEPARATORS.FIELD_LIST)
  if (!isNonEmptyString(formatted)) {
    throw new Error('distinct emulation requires at least one output column')
  }
  return formatted
}

export function getOrderByEntries(spec: SelectQuerySpec): OrderByEntry[] {
  if (!isNotNullish(spec.args.orderBy)) return []
  const normalized = normalizeOrderByInput(spec.args.orderBy, parseOrderByValue)
  const entries: OrderByEntry[] = []
  for (const item of normalized) {
    for (const field in item) {
      if (!Object.prototype.hasOwnProperty.call(item, field)) continue
      const value = item[field]
      if (typeof value === 'string') {
        entries.push({ field, direction: value as 'asc' | 'desc' })
        continue
      }
      const obj = value as OrderBySortObject
      entries.push({ field, direction: obj.direction, nulls: obj.nulls })
    }
  }
  return entries
}

export function renderOrderBySql(
  entries: OrderByEntry[],
  alias: string,
  dialect: SqlDialect,
  model?: any,
): string {
  return buildOrderByFragment(entries, alias, dialect, model)
}

function renderOrderBySimple(entries: OrderByEntry[], alias: string): string {
  if (entries.length === 0) return ''
  const out: string[] = []
  for (const e of entries) {
    const dir = e.direction.toUpperCase()
    const c = `${alias}.${quote(e.field)}`
    if (isNotNullish(e.nulls)) {
      const isNullExpr = `(${c} IS NULL)`
      const nullRankDir = e.nulls === 'first' ? 'DESC' : 'ASC'
      out.push(isNullExpr + ' ' + nullRankDir)
      out.push(c + ' ' + dir)
    } else {
      out.push(c + ' ' + dir)
    }
  }
  return out.join(SQL_SEPARATORS.ORDER_BY)
}

export function ensureIdTiebreakerEntries(
  entries: OrderByEntry[],
  model: any,
): OrderByEntry[] {
  const idField = model?.fields?.find?.(
    (f: any) => f.name === DEFAULT_PRIMARY_KEY && !f.isRelation,
  )
  if (!idField) return entries
  if (entries.some((e) => e.field === DEFAULT_PRIMARY_KEY)) return entries
  return [...entries, { field: DEFAULT_PRIMARY_KEY, direction: 'asc' }]
}

export function ensurePostgresDistinctOrderEntries(args: {
  entries: OrderByEntry[]
  distinct: readonly string[]
  model: any
}): OrderByEntry[] {
  const { entries, distinct, model } = args

  const distinctEntries: OrderByEntry[] = [...distinct].map((f) => ({
    field: f,
    direction: 'asc' as const,
  }))

  const canKeepAsIs =
    entries.length >= distinctEntries.length &&
    distinctEntries.every((de, i) => entries[i].field === de.field)

  const merged = canKeepAsIs ? entries : [...distinctEntries, ...entries]

  return ensureIdTiebreakerEntries(merged, model)
}

function extractDistinctOrderEntries(spec: SelectQuerySpec): OrderByEntry[] {
  const entries = getOrderByEntries(spec)
  if (entries.length > 0) return entries

  if (isNotNullish(spec.distinct) && isNonEmptyArray(spec.distinct)) {
    return [...spec.distinct].map((f) => ({
      field: f,
      direction: 'asc' as const,
    }))
  }

  return []
}

function buildWhereSql(conditions: readonly string[]): string {
  if (!isNonEmptyArray(conditions)) return ''
  return (
    ' ' +
    SQL_TEMPLATES.WHERE +
    ' ' +
    conditions.join(SQL_SEPARATORS.CONDITION_AND)
  )
}

function buildJoinsSql(
  ...joinGroups: Array<readonly string[] | undefined>
): string {
  const all: string[] = []
  for (const g of joinGroups) {
    if (isNonEmptyArray(g)) {
      for (const j of g) all.push(j)
    }
  }
  return all.length > 0 ? ' ' + all.join(' ') : ''
}

export function buildSqliteDistinctQuery(
  spec: SelectQuerySpec,
  selectWithIncludes: string,
  countJoins?: string[],
): string {
  const { includes, from, whereClause, whereJoins, distinct, model } = spec
  if (!isNotNullish(distinct) || !isNonEmptyArray(distinct)) {
    throw new Error('buildSqliteDistinctQuery requires distinct fields')
  }

  const scalarNames = parseSimpleScalarSelect(spec.select, from.alias)
  const includeNames = includes.map((i) => i.name)
  const hasCount = Boolean(spec.args?.select?.[COUNT_SELECT_KEY])

  const outerSelectCols = buildOutputColumns(
    scalarNames,
    includeNames,
    hasCount,
  )
  const distinctCols = buildDistinctColumns([...distinct], from.alias, model)

  const baseEntries = getOrderByEntries(spec)
  const fallbackEntries: OrderByEntry[] = [...distinct].map((f) => ({
    field: f,
    direction: 'asc' as const,
  }))

  const resolvedEntries = baseEntries.length > 0 ? baseEntries : fallbackEntries

  const windowEntries = ensureIdTiebreakerEntries(resolvedEntries, model)
  const windowOrder = renderOrderBySql(
    windowEntries,
    from.alias,
    'sqlite',
    model,
  )

  const outerEntries = extractDistinctOrderEntries(spec)
  const outerOrder = renderOrderBySimple(
    outerEntries,
    `"${DISTINCT_WRAPPER_ALIAS}"`,
  )

  const joins = buildJoinsSql(whereJoins, countJoins)

  const conditions: string[] = []
  if (whereClause && whereClause !== '1=1') conditions.push(whereClause)
  const whereSql = buildWhereSql(conditions)

  const innerSelectList = selectWithIncludes.trim()
  const innerComma = innerSelectList.length > 0 ? SQL_SEPARATORS.FIELD_LIST : ''

  const innerParts: string[] = [
    SQL_TEMPLATES.SELECT,
    innerSelectList + innerComma,
    'ROW_NUMBER() OVER (PARTITION BY ' +
      distinctCols +
      ' ORDER BY ' +
      windowOrder +
      ')',
    SQL_TEMPLATES.AS,
    `"${DISTINCT_ROW_NUMBER_COLUMN}"`,
    SQL_TEMPLATES.FROM,
    from.table,
    from.alias,
  ]
  if (joins) innerParts.push(joins)
  if (whereSql) innerParts.push(whereSql)
  const inner = innerParts.join(' ')

  const outerParts: string[] = [
    SQL_TEMPLATES.SELECT,
    outerSelectCols,
    SQL_TEMPLATES.FROM,
    '(' + inner + ')',
    SQL_TEMPLATES.AS,
    `"${DISTINCT_WRAPPER_ALIAS}"`,
    SQL_TEMPLATES.WHERE,
    `"${DISTINCT_ROW_NUMBER_COLUMN}" = ${DISTINCT_FIRST_ROW}`,
  ]
  if (isNonEmptyString(outerOrder)) {
    outerParts.push(SQL_TEMPLATES.ORDER_BY, outerOrder)
  }
  return outerParts.join(' ')
}

export function buildPostgresDistinctOnClause(
  fromAlias: string,
  distinct?: readonly string[],
  model?: any,
): string | null {
  if (!isNonEmptyArray(distinct)) return null
  const distinctCols = buildDistinctColumns([...distinct], fromAlias, model)
  return SQL_TEMPLATES.DISTINCT_ON + ' (' + distinctCols + ')'
}
