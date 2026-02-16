import type { PrismaQueryArgs } from '../../types'
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
import { joinNonEmpty } from '../shared/string-builder'
import { getRelationFieldSet } from '../shared/model-field-cache'
import { parseOrderByValue } from '../pagination'
import {
  normalizeOrderByInput,
  OrderBySortObject,
} from '../shared/order-by-utils'
import { buildFlatJoinSql, canUseFlatJoinForAll } from './flat-join'
import { buildArrayAggSql, canUseArrayAggForAll } from './array-agg'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import {
  countIncludeDepth,
  shouldPreferFlatJoinStrategy,
} from './strategy-estimator'

type OrderByEntry = {
  field: string
  direction: 'asc' | 'desc'
  nulls?: 'first' | 'last'
}

const SELECT_FIELD_REGEX =
  /^\s*("(?:[^"]|"")+"|[a-z_][a-z0-9_]*)\s*\.\s*("(?:[^"]|"")+"|[a-z_][a-z0-9_]*)(?:\s+AS\s+("(?:[^"]|"")+"|[a-z_][a-z0-9_]*))?\s*$/i

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

function buildSelectList(baseSelect: string, extraCols: string): string {
  const base = baseSelect.trim()
  const extra = extraCols.trim()
  if (!base) return extra
  if (!extra) return base
  return base + SQL_SEPARATORS.FIELD_LIST + extra
}

function finalizeSql(
  sql: string,
  params: SelectQuerySpec['params'],
  dialect: SelectQuerySpec['dialect'],
): SqlResult {
  const snapshot = params.snapshot()
  validateSelectQuery(sql)
  validateParamConsistencyByDialect(sql, snapshot.params, dialect)
  return {
    sql,
    params: [...snapshot.params],
    paramMappings: [...snapshot.mappings],
  }
}

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
    ? [...scalarNames, ...includeNames, '_count']
    : [...scalarNames, ...includeNames]

  const formatted = outputCols
    .map((n) => quote(n))
    .join(SQL_SEPARATORS.FIELD_LIST)
  if (!isNonEmptyString(formatted)) {
    throw new Error('distinct emulation requires at least one output column')
  }
  return formatted
}

function getOrderByEntries(spec: SelectQuerySpec): OrderByEntry[] {
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

function renderOrderBySql(
  entries: OrderByEntry[],
  alias: string,
  dialect: string,
  model?: any,
): string {
  if (entries.length === 0) return ''
  const out: string[] = []
  for (const e of entries) {
    const dir = e.direction.toUpperCase()
    const c = col(alias, e.field, model)
    if (dialect === 'postgres') {
      const nulls = isNotNullish(e.nulls)
        ? ` NULLS ${e.nulls.toUpperCase()}`
        : ''
      out.push(c + ' ' + dir + nulls)
    } else if (isNotNullish(e.nulls)) {
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

function ensureIdTiebreakerEntries(
  entries: OrderByEntry[],
  model: any,
): OrderByEntry[] {
  const idField = model?.fields?.find?.(
    (f: any) => f.name === 'id' && !f.isRelation,
  )
  if (!idField) return entries
  if (entries.some((e) => e.field === 'id')) return entries
  return [...entries, { field: 'id', direction: 'asc' }]
}

function ensurePostgresDistinctOrderEntries(args: {
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

function buildSqliteDistinctQuery(
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
  const hasCount = Boolean(spec.args?.select?._count)

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
  const outerOrder = renderOrderBySimple(outerEntries, '"__tp_distinct"')

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
    '"__tp_rn"',
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
    '"__tp_distinct"',
    SQL_TEMPLATES.WHERE,
    '"__tp_rn" = 1',
  ]
  if (isNonEmptyString(outerOrder)) {
    outerParts.push(SQL_TEMPLATES.ORDER_BY, outerOrder)
  }
  return outerParts.join(' ')
}

function resolveCountSelect(
  countSelectRaw: unknown,
  model: SelectQuerySpec['model'],
): Record<string, boolean> | null {
  if (countSelectRaw === true) {
    const relationSet = getRelationFieldSet(model)
    if (relationSet.size === 0) return null
    const allRelations: Record<string, boolean> = {}
    for (const name of relationSet) {
      allRelations[name] = true
    }
    return allRelations
  }

  if (isPlainObject(countSelectRaw) && 'select' in countSelectRaw) {
    return (countSelectRaw as { select: Record<string, boolean> }).select
  }

  return null
}

function buildIncludeColumns(spec: SelectQuerySpec): {
  includeCols: string
  selectWithIncludes: string
  countJoins: string[]
  includeJoins: string[]
} {
  const { select, includes, dialect, model, schemas, from, params } = spec
  const baseSelect = (select ?? '').trim()

  let countCols = ''
  let countJoins: string[] = []

  const countSelectRaw = spec.args?.select?._count
  if (countSelectRaw) {
    const resolvedCountSelect = resolveCountSelect(countSelectRaw, model)

    if (resolvedCountSelect && Object.keys(resolvedCountSelect).length > 0) {
      const countBuild = buildRelationCountSql(
        resolvedCountSelect,
        model,
        schemas,
        from.alias,
        params,
        dialect,
      )
      if (countBuild.jsonPairs) {
        countCols =
          jsonBuildObject(countBuild.jsonPairs, dialect) +
          ' ' +
          SQL_TEMPLATES.AS +
          ' ' +
          quote('_count')
      }
      countJoins = countBuild.joins
    }
  }

  const hasIncludes = isNonEmptyArray(includes)
  const hasCountCols = isNonEmptyString(countCols)

  if (!hasIncludes && !hasCountCols) {
    return {
      includeCols: '',
      selectWithIncludes: baseSelect,
      countJoins: [],
      includeJoins: [],
    }
  }

  const emptyJson = dialect === 'postgres' ? `'[]'::json` : `json('[]')`

  const correlatedParts: string[] = []
  const joinIncludeJoins: string[] = []
  const joinIncludeSelects: string[] = []

  if (hasIncludes) {
    for (const inc of includes) {
      if (inc.joinSql && inc.selectExpr) {
        joinIncludeJoins.push(inc.joinSql)
        joinIncludeSelects.push(inc.selectExpr)
      } else {
        const expr = inc.isOneToOne
          ? '(' + inc.sql + ')'
          : 'COALESCE((' + inc.sql + '), ' + emptyJson + ')'
        correlatedParts.push(
          expr + ' ' + SQL_TEMPLATES.AS + ' ' + quote(inc.name),
        )
      }
    }
  }

  const correlatedCols = correlatedParts.join(SQL_SEPARATORS.FIELD_LIST)
  const joinSelectCols = joinIncludeSelects.join(SQL_SEPARATORS.FIELD_LIST)

  const allIncludeCols = joinNonEmpty(
    [correlatedCols, joinSelectCols, countCols],
    SQL_SEPARATORS.FIELD_LIST,
  )
  const selectWithIncludes = buildSelectList(baseSelect, allIncludeCols)

  return {
    includeCols: allIncludeCols,
    selectWithIncludes,
    countJoins,
    includeJoins: joinIncludeJoins,
  }
}

export function appendPagination(sql: string, spec: SelectQuerySpec): string {
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
  if (spec.dialect !== 'sqlite') return false
  const d = spec.distinct
  return isNotNullish(d) && isNonEmptyArray(d)
}

function hasAnyDistinct(spec: SelectQuerySpec): boolean {
  return isNotNullish(spec.distinct) && isNonEmptyArray(spec.distinct)
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
  return SQL_TEMPLATES.DISTINCT_ON + ' (' + distinctCols + ')'
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

function extractIncludeSpec(args: PrismaQueryArgs): Record<string, any> {
  const includeSpec: Record<string, any> = {}

  if (args.include && isPlainObject(args.include)) {
    for (const [key, value] of Object.entries(args.include)) {
      if (value !== false) {
        includeSpec[key] = value
      }
    }
  }

  if (args.select && isPlainObject(args.select)) {
    for (const [key, value] of Object.entries(args.select)) {
      if (value !== false && value !== true && isPlainObject(value)) {
        const selectVal = value as Record<string, any>
        if (selectVal.include || selectVal.select) {
          includeSpec[key] = value
        }
      }
    }
  }

  return includeSpec
}

function hasNestedIncludes(includeSpec: Record<string, any>): boolean {
  return Object.keys(includeSpec).length > 0
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
    schemas,
    pagination,
    args,
  } = spec

  const useWindowDistinct = hasWindowDistinct(spec)
  assertDistinctAllowed(method, useWindowDistinct)

  const hasDistinct = hasAnyDistinct(spec)
  assertDistinctAllowed(method, hasDistinct)

  const includeSpec = extractIncludeSpec(args)
  const hasIncludes = hasNestedIncludes(includeSpec)

  const hasPagination = isNotNullish(pagination.take)

  const takeValue = typeof pagination.take === 'number' ? pagination.take : null

  const includeDepth = hasIncludes
    ? countIncludeDepth(includeSpec, model, schemas)
    : 0

  const flatJoinEligible =
    dialect === 'postgres' &&
    hasIncludes &&
    canUseFlatJoinForAll(includeSpec, model, schemas)

  const shouldUseFlatJoin = shouldPreferFlatJoinStrategy({
    includeSpec,
    model,
    schemas,
    hasPagination,
    takeValue,
    canUseFlatJoin: flatJoinEligible,
  })

  if (shouldUseFlatJoin) {
    const flatResult = buildFlatJoinSql(spec)

    if (flatResult.sql) {
      validateSelectQuery(flatResult.sql)
      validateParamConsistencyByDialect(
        flatResult.sql,
        flatResult.params,
        dialect,
      )
      return {
        sql: flatResult.sql,
        params: flatResult.params,
        paramMappings: flatResult.params.map((v, i) => ({
          index: i + 1,
          value: v,
        })),
        requiresReduction: true,
        includeSpec: flatResult.includeSpec,
      }
    }
  }

  const shouldUseArrayAgg =
    dialect === 'postgres' &&
    hasIncludes &&
    method === 'findMany' &&
    canUseArrayAggForAll(includeSpec, model, schemas)

  if (shouldUseArrayAgg) {
    const aaResult = buildArrayAggSql(spec)

    if (aaResult.sql) {
      const baseSqlResult = finalizeSql(aaResult.sql, params, dialect)
      return {
        sql: baseSqlResult.sql,
        params: baseSqlResult.params,
        paramMappings: baseSqlResult.paramMappings,
        requiresReduction: true,
        includeSpec: aaResult.includeSpec,
        isArrayAgg: true,
      }
    }
  }

  const { includeCols, selectWithIncludes, countJoins, includeJoins } =
    buildIncludeColumns(spec)

  if (useWindowDistinct) {
    const allExtraJoins = [...countJoins, ...includeJoins]
    const spec2 = withCountJoins(spec, allExtraJoins, whereJoins)
    let sql = buildSqliteDistinctQuery(spec2, selectWithIncludes).trim()
    sql = appendPagination(sql, spec)
    return finalizeSql(sql, params, dialect)
  }

  const parts: string[] = []
  if (cursorCte) parts.push('WITH ' + cursorCte)

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

  if (cursorCte) {
    const cteName = cursorCte.split(' AS ')[0].trim()
    parts.push('CROSS JOIN', cteName)
  }

  pushJoinGroups(parts, whereJoins, countJoins)

  if (isNonEmptyArray(includeJoins)) {
    parts.push(includeJoins.join(' '))
  }

  const conditions = buildConditions(whereClause, cursorClause)
  pushWhere(parts, conditions)

  let finalOrderBy = orderBy
  if (dialect === 'postgres' && isNonEmptyArray(distinct)) {
    const currentEntries = getOrderByEntries(spec)
    const mergedEntries = ensurePostgresDistinctOrderEntries({
      entries: currentEntries.length > 0 ? currentEntries : [],
      distinct: [...distinct],
      model,
    })
    finalOrderBy = renderOrderBySql(mergedEntries, from.alias, dialect, model)
  }

  if (isNonEmptyString(finalOrderBy))
    parts.push(SQL_TEMPLATES.ORDER_BY, finalOrderBy)

  let sql = parts.join(' ').trim()
  sql = appendPagination(sql, spec)
  return finalizeSql(sql, params, dialect)
}
