import { isDynamicParameter } from '@dee-wan/schema-parser'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { col, quote } from '../shared/sql-utils'
import { SelectQuerySpec, SqlResult } from '../shared/types'
import {
  validateSelectQuery,
  validateParamConsistencyByDialect,
} from '../shared/validators/sql-validators'
import {
  isNotNullish,
  isNonEmptyArray,
  isNonEmptyString,
  isPlainObject,
} from '../shared/validators/type-guards'
import { addAutoScoped } from '../shared/dynamic-params'
import { jsonBuildObject } from '../../sql-builder-dialect'
import { buildRelationCountSql } from './includes'

// ✅ MEDIUM-PRIORITY FIX: Remove per-alias cache, use single compiled pattern
const ALIAS_CAPTURE = '([A-Za-z_][A-Za-z0-9_]*)'
const COLUMN_PART = '(?:"([^"]+)"|([a-z_][a-z0-9_]*))'
const AS_PART = `(?:\\s+AS\\s+${COLUMN_PART})?`
const SIMPLE_COLUMN_PATTERN = `^${ALIAS_CAPTURE}\\.${COLUMN_PART}${AS_PART}$`
const SIMPLE_COLUMN_RE = new RegExp(SIMPLE_COLUMN_PATTERN, 'i')

function joinNonEmpty(parts: string[], sep: string): string {
  return parts.filter((s) => s.trim().length > 0).join(sep)
}

function buildWhereSql(conditions: readonly string[]): string {
  if (!isNonEmptyArray(conditions)) return ''
  const parts = [
    SQL_TEMPLATES.WHERE,
    conditions.join(SQL_SEPARATORS.CONDITION_AND),
  ]
  return ` ${parts.join(' ')}`
}

function buildJoinsSql(
  ...joinGroups: Array<readonly string[] | undefined>
): string {
  const all: string[] = []
  for (const g of joinGroups) {
    if (isNonEmptyArray(g)) all.push(...g)
  }
  return all.length > 0 ? ` ${all.join(' ')}` : ''
}

function buildSelectList(baseSelect: string, extraCols: string): string {
  const base = baseSelect.trim()
  const extra = extraCols.trim()
  if (base && extra) return `${base}${SQL_SEPARATORS.FIELD_LIST}${extra}`
  return base || extra
}

function finalizeSql(
  sql: string,
  params: SelectQuerySpec['params'],
  dialect: SelectQuerySpec['dialect'],
): SqlResult {
  const snapshot = params.snapshot()
  validateSelectQuery(sql)
  validateParamConsistencyByDialect(sql, snapshot.params, dialect)
  return Object.freeze({
    sql,
    params: snapshot.params,
    paramMappings: Object.freeze(snapshot.mappings),
  })
}

function parseSimpleScalarSelect(select: string, fromAlias: string): string[] {
  const raw = select.trim()
  if (raw.length === 0) return []

  const parts = raw.split(SQL_SEPARATORS.FIELD_LIST)
  const names: string[] = []

  for (const part of parts) {
    const p = part.trim()
    const m = p.match(SIMPLE_COLUMN_RE)

    if (!m) {
      throw new Error(
        `sqlite distinct emulation requires scalar select fields to be simple columns (optionally with AS). Got: ${p}`,
      )
    }

    // ✅ Validate alias matches expected
    const actualAlias = m[1]
    if (actualAlias !== fromAlias) {
      throw new Error(
        `Expected alias '${fromAlias}', got '${actualAlias}' in: ${p}`,
      )
    }

    const columnName = (m[2] ?? m[3] ?? '').trim()
    const outAlias = (m[4] ?? m[5] ?? '').trim()
    const name = outAlias.length > 0 ? outAlias : columnName

    if (name.length === 0) {
      throw new Error(`Failed to parse selected column name from: ${p}`)
    }

    names.push(name)
  }

  return names
}

function replaceOrderByAlias(
  orderBy: string,
  fromAlias: string,
  outerAlias: string,
): string {
  const needle = `${fromAlias}.`
  const replacement = `${outerAlias}.`
  return orderBy.split(needle).join(replacement)
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
  const outputCols = [...scalarNames, ...includeNames]
  if (hasCount) outputCols.push('_count')
  const formatted = outputCols
    .map((n) => quote(n))
    .join(SQL_SEPARATORS.FIELD_LIST)
  if (!isNonEmptyString(formatted)) {
    throw new Error('distinct emulation requires at least one output column')
  }
  return formatted
}

function buildWindowOrder(args: {
  baseOrder: string
  idField: any
  fromAlias: string
  model?: any
}): string {
  const { baseOrder, idField, fromAlias, model } = args
  const orderFields = baseOrder
    .split(SQL_SEPARATORS.ORDER_BY)
    .map((s) => s.trim().toLowerCase())
  const hasIdInOrder = orderFields.some(
    (f) =>
      f.startsWith(`${fromAlias}.id `) || f.startsWith(`${fromAlias}."id" `),
  )
  if (hasIdInOrder) return baseOrder
  const idTiebreaker = idField ? `, ${col(fromAlias, 'id', model)} ASC` : ''
  return `${baseOrder}${idTiebreaker}`
}

function buildSqliteDistinctQuery(
  spec: SelectQuerySpec,
  selectWithIncludes: string,
  countJoins?: string[],
): string {
  const { includes, from, whereClause, whereJoins, orderBy, distinct, model } =
    spec
  if (!isNotNullish(distinct) || !isNonEmptyArray(distinct)) {
    throw new Error('buildSqliteDistinctQuery requires distinct fields')
  }
  const scalarNames = parseSimpleScalarSelect(spec.select, from.alias)
  const includeNames = includes.map((i) => i.name)
  const hasCount = Boolean(spec.args?.select?._count)
  const outerSelectCols = buildOutputColumns(
    scalarNames,
    includeNames,
    hasCount,
  )
  const distinctCols = buildDistinctColumns([...distinct], from.alias, model)
  const fallbackOrder = [...distinct]
    .map((f) => `${col(from.alias, f, model)} ASC`)
    .join(SQL_SEPARATORS.FIELD_LIST)
  const idField = model.fields.find(
    (f: any) => f.name === 'id' && !f.isRelation,
  )
  const baseOrder = isNonEmptyString(orderBy) ? orderBy : fallbackOrder
  const windowOrder = buildWindowOrder({
    baseOrder,
    idField,
    fromAlias: from.alias,
    model,
  })
  const outerOrder = isNonEmptyString(orderBy)
    ? replaceOrderByAlias(orderBy, from.alias, `"__tp_distinct"`)
    : replaceOrderByAlias(fallbackOrder, from.alias, `"__tp_distinct"`)
  const joins = buildJoinsSql(whereJoins, countJoins)
  const conditions: string[] = []
  if (whereClause && whereClause !== '1=1') conditions.push(whereClause)
  const whereSql = buildWhereSql(conditions)
  const innerSelectList = selectWithIncludes.trim()
  const innerComma = innerSelectList.length > 0 ? SQL_SEPARATORS.FIELD_LIST : ''

  const innerParts = [
    SQL_TEMPLATES.SELECT,
    innerSelectList + innerComma,
    `ROW_NUMBER() OVER (PARTITION BY ${distinctCols} ORDER BY ${windowOrder})`,
    SQL_TEMPLATES.AS,
    '"__tp_rn"',
    SQL_TEMPLATES.FROM,
    from.table,
    from.alias,
  ]
  if (joins) innerParts.push(joins)
  if (whereSql) innerParts.push(whereSql)
  const inner = innerParts.filter(Boolean).join(' ')

  const outerParts = [
    SQL_TEMPLATES.SELECT,
    outerSelectCols,
    SQL_TEMPLATES.FROM,
    `(${inner})`,
    SQL_TEMPLATES.AS,
    '"__tp_distinct"',
    SQL_TEMPLATES.WHERE,
    '"__tp_rn" = 1',
  ]
  if (isNonEmptyString(outerOrder)) {
    outerParts.push(SQL_TEMPLATES.ORDER_BY, outerOrder)
  }
  return outerParts.filter(Boolean).join(' ')
}

function buildIncludeColumns(spec: SelectQuerySpec): {
  includeCols: string
  selectWithIncludes: string
  countJoins: string[]
} {
  const { select, includes, dialect, model, schemas, from, params } = spec
  const baseSelect = (select ?? '').trim()
  let countCols = ''
  let countJoins: string[] = []
  const countSelect = spec.args?.select?._count
  if (countSelect) {
    if (isPlainObject(countSelect) && 'select' in countSelect) {
      const countBuild = buildRelationCountSql(
        (countSelect as any).select,
        model,
        schemas,
        from.alias,
        params,
        dialect,
      )
      if (countBuild.jsonPairs) {
        countCols = `${jsonBuildObject(countBuild.jsonPairs, dialect)} ${SQL_TEMPLATES.AS} ${quote('_count')}`
      }
      countJoins = countBuild.joins
    }
  }
  const hasIncludes = isNonEmptyArray(includes)
  const hasCountCols = isNonEmptyString(countCols)
  if (!hasIncludes && !hasCountCols) {
    return { includeCols: '', selectWithIncludes: baseSelect, countJoins: [] }
  }
  const emptyJson = dialect === 'postgres' ? `'[]'::json` : `json('[]')`
  const includeCols = hasIncludes
    ? includes
        .map((inc) => {
          const expr = inc.isOneToOne
            ? `(${inc.sql})`
            : `COALESCE((${inc.sql}), ${emptyJson})`
          return `${expr} ${SQL_TEMPLATES.AS} ${quote(inc.name)}`
        })
        .join(SQL_SEPARATORS.FIELD_LIST)
    : ''
  const allCols = joinNonEmpty(
    [includeCols, countCols],
    SQL_SEPARATORS.FIELD_LIST,
  )
  const selectWithIncludes = buildSelectList(baseSelect, allCols)
  return { includeCols: allCols, selectWithIncludes, countJoins }
}

function appendPagination(sql: string, spec: SelectQuerySpec): string {
  const { method, pagination, params } = spec
  const isFindUniqueOrFirst = method === 'findUnique' || method === 'findFirst'
  if (isFindUniqueOrFirst) {
    const parts: string[] = [sql, SQL_TEMPLATES.LIMIT, '1']
    const hasSkip =
      isNotNullish(pagination.skip) &&
      (isDynamicParameter(pagination.skip) ||
        (typeof pagination.skip === 'number' && pagination.skip > 0)) &&
      method === 'findFirst'
    if (hasSkip) {
      const placeholder = addAutoScoped(
        params,
        pagination.skip,
        'root.pagination.skip',
      )
      parts.push(SQL_TEMPLATES.OFFSET, placeholder)
    }
    return parts.join(' ')
  }
  const parts: string[] = [sql]
  if (isNotNullish(pagination.take)) {
    const placeholder = addAutoScoped(
      params,
      pagination.take,
      'root.pagination.take',
    )
    parts.push(SQL_TEMPLATES.LIMIT, placeholder)
  }
  if (isNotNullish(pagination.skip)) {
    const placeholder = addAutoScoped(
      params,
      pagination.skip,
      'root.pagination.skip',
    )
    parts.push(SQL_TEMPLATES.OFFSET, placeholder)
  }
  return parts.join(' ')
}

function hasWindowDistinct(spec: SelectQuerySpec): boolean {
  const d = spec.distinct
  return isNotNullish(d) && isNonEmptyArray(d)
}

function assertDistinctAllowed(
  method: SelectQuerySpec['method'],
  enabled: boolean,
): void {
  if (enabled && method !== 'findMany') {
    throw new Error(
      'distinct is only supported for findMany in this SQL builder',
    )
  }
}

function assertHasSelectFields(baseSelect: string, includeCols: string): void {
  if (!isNonEmptyString(baseSelect) && !isNonEmptyString(includeCols)) {
    throw new Error('SELECT requires at least one selected field or include')
  }
}

function withCountJoins(
  spec: SelectQuerySpec,
  countJoins: string[],
  whereJoins?: readonly string[],
): SelectQuerySpec {
  return {
    ...spec,
    whereJoins: [...(whereJoins || []), ...(countJoins || [])],
  }
}

function buildPostgresDistinctOnClause(
  fromAlias: string,
  distinct?: readonly string[],
  model?: any,
): string | null {
  if (!isNonEmptyArray(distinct)) return null
  const distinctCols = buildDistinctColumns([...distinct], fromAlias, model)
  return `${SQL_TEMPLATES.DISTINCT_ON} (${distinctCols})`
}

function pushJoinGroups(
  parts: string[],
  ...groups: Array<readonly string[] | undefined>
): void {
  for (const g of groups) {
    if (isNonEmptyArray(g)) parts.push(g.join(' '))
  }
}

function buildConditions(
  whereClause?: string,
  cursorClause?: string,
): string[] {
  const conditions: string[] = []
  if (whereClause && whereClause !== '1=1') conditions.push(whereClause)
  if (isNotNullish(cursorClause) && isNonEmptyString(cursorClause))
    conditions.push(cursorClause)
  return conditions
}

function pushWhere(parts: string[], conditions: string[]): void {
  if (!isNonEmptyArray(conditions)) return
  parts.push(SQL_TEMPLATES.WHERE, conditions.join(SQL_SEPARATORS.CONDITION_AND))
}

export function constructFinalSql(spec: SelectQuerySpec): SqlResult {
  const {
    select,
    from,
    whereClause,
    whereJoins,
    orderBy,
    distinct,
    method,
    cursorCte,
    cursorClause,
    params,
    dialect,
    model,
  } = spec
  const useWindowDistinct = hasWindowDistinct(spec)
  assertDistinctAllowed(method, useWindowDistinct)
  const { includeCols, selectWithIncludes, countJoins } =
    buildIncludeColumns(spec)
  if (useWindowDistinct) {
    const baseSelect = (select ?? '').trim()
    assertHasSelectFields(baseSelect, includeCols)
    const spec2 = withCountJoins(spec, countJoins, whereJoins)
    let sql = buildSqliteDistinctQuery(spec2, selectWithIncludes).trim()
    sql = appendPagination(sql, spec)
    return finalizeSql(sql, params, dialect)
  }
  const parts: string[] = []
  if (cursorCte) {
    parts.push(`WITH ${cursorCte}`)
  }
  parts.push(SQL_TEMPLATES.SELECT)
  const distinctOn =
    dialect === 'postgres'
      ? buildPostgresDistinctOnClause(from.alias, distinct, model)
      : null
  if (distinctOn) parts.push(distinctOn)
  const baseSelect = (select ?? '').trim()
  const fullSelectList = buildSelectList(baseSelect, includeCols)
  if (!isNonEmptyString(fullSelectList)) {
    throw new Error('SELECT requires at least one selected field or include')
  }
  parts.push(fullSelectList)
  parts.push(SQL_TEMPLATES.FROM, from.table, from.alias)
  pushJoinGroups(parts, whereJoins, countJoins)
  const conditions = buildConditions(whereClause, cursorClause)
  pushWhere(parts, conditions)
  if (isNonEmptyString(orderBy)) parts.push(SQL_TEMPLATES.ORDER_BY, orderBy)
  let sql = parts.join(' ').trim()
  sql = appendPagination(sql, spec)
  return finalizeSql(sql, params, dialect)
}
