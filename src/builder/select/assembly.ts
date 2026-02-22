import type { PrismaQueryArgs } from '../../types'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { quote } from '../shared/sql-utils'
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
import { buildRelationCountSql } from './include-count'
import { emptyJsonArray } from './include-join'

import { getRelationFieldSet } from '../shared/model-field-cache'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import {
  hasChildPaginationAnywhere,
  pickIncludeStrategy,
} from './strategy-estimator'
import { buildFlatJoinSql, canUseFlatJoinForAll } from './flat-join'
import { buildLateralJoinSql, canUseLateralJoin } from './lateral-join'
import {
  getOrderByEntries,
  renderOrderBySql,
  ensurePostgresDistinctOrderEntries,
  buildSqliteDistinctQuery,
  buildPostgresDistinctOnClause,
  COUNT_SELECT_KEY,
} from './distinct'
import { joinNonEmpty } from '../shared/array-utils'
import { getOrCreateModelMap } from '../shared/include-tree-walker'

const ALWAYS_TRUE_CONDITION = '1=1'

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

  const countSelectRaw =
    spec.args?.select?.[COUNT_SELECT_KEY] ??
    spec.args?.include?.[COUNT_SELECT_KEY]
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
          quote(COUNT_SELECT_KEY)
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

  const emptyJson = emptyJsonArray(dialect)

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
  if (whereClause && whereClause !== ALWAYS_TRUE_CONDITION)
    conditions.push(whereClause)
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
      if (value !== false && key !== COUNT_SELECT_KEY) {
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

  if (dialect === 'postgres' && hasIncludes) {
    const modelMap = getOrCreateModelMap(schemas)

    const canFlatJoin = canUseFlatJoinForAll(
      includeSpec,
      model,
      schemas,
      false,
      modelMap,
    )
    const canLateral = canUseLateralJoin(includeSpec, model, schemas, modelMap)
    const hasChildPag = hasChildPaginationAnywhere(
      includeSpec,
      model,
      schemas,
      0,
      modelMap,
    )

    const strategy = pickIncludeStrategy({
      includeSpec,
      model,
      schemas,
      method,
      args,
      takeValue,
      hasPagination,
      canFlatJoin,
      canLateral,
      hasChildPagination: hasChildPag,
      modelMap,
    })

    if (strategy === 'flat-join') {
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
          paramMappings: flatResult.params.map((v: any, i: number) => ({
            index: i + 1,
            value: v,
          })),
          requiresReduction: true,
          includeSpec: flatResult.includeSpec,
        }
      }
    }

    if (strategy === 'lateral') {
      const lateralResult = buildLateralJoinSql(spec)
      if (lateralResult.sql) {
        return {
          sql: lateralResult.sql,
          params: lateralResult.params,
          requiresReduction: true,
          includeSpec: lateralResult.includeSpec,
          isLateral: true,
          lateralMeta: lateralResult.lateralMeta,
        }
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
