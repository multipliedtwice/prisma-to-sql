import { Model, Field } from '../../types'
import { SqlDialect, jsonBuildObject } from '../../sql-builder-dialect'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { buildTableReference, quote, quoteColumn } from '../shared/sql-utils'
import { ParamStore } from '../shared/param-store'
import { IncludeSpec } from '../shared/types'
import { isNotNullish, isPlainObject } from '../shared/validators/type-guards'
import { addAutoScoped } from '../shared/dynamic-params'
import { getRelationFieldSet } from '../shared/model-field-cache'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import { resolveRelationKeys } from '../shared/relation-key-utils'
import {
  buildFkSelectList,
  buildFkGroupBy,
  buildFkPartitionBy,
  buildFkJoinCondition,
} from '../shared/fk-join-utils'

const AGG_COLUMN = '__agg'
const ROW_COLUMN = '__row'
const ROW_NUMBER_COLUMN = '__rn'
const INC_ALIAS_PREFIX = '__inc_'
const RANKED_ALIAS_PREFIX = '__ranked_'
const INCLUDE_SCOPE_ROOT = 'include'
const INCLUDE_SCOPE_SEGMENT = '.include'
const PG_EMPTY_JSON_ARRAY = "'[]'::json"
const SQLITE_EMPTY_JSON_ARRAY = "json('[]')"
export const DEFAULT_PRIMARY_KEY = 'id'
const JOIN_INCLUDE_MAX_DEPTH = 0

type OptionalIntOrDynamic = number | string | undefined

export function emptyJsonArray(dialect: SqlDialect): string {
  return dialect === 'postgres' ? PG_EMPTY_JSON_ARRAY : SQLITE_EMPTY_JSON_ARRAY
}

export function buildIncludeScope(includePath: readonly string[]): string {
  if (includePath.length === 0) return INCLUDE_SCOPE_ROOT
  let scope = INCLUDE_SCOPE_ROOT
  for (let i = 0; i < includePath.length; i++) {
    scope += `.${includePath[i]}`
    if (i < includePath.length - 1) {
      scope += INCLUDE_SCOPE_SEGMENT
    }
  }
  return scope
}

export function getRelationTableReference(
  relModel: Model,
  dialect: SqlDialect,
): string {
  return buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    relModel.tableName,
    dialect,
  )
}

export function hasNestedRelationInArgs(
  relArgs: unknown,
  relModel: Model,
): boolean {
  if (!isPlainObject(relArgs)) return false

  const relationSet = getRelationFieldSet(relModel)
  const checkSource = (src: unknown): boolean => {
    if (!isPlainObject(src)) return false
    for (const k of Object.keys(src)) {
      if (relationSet.has(k) && (src as Record<string, unknown>)[k] !== false)
        return true
    }
    return false
  }

  if (checkSource((relArgs as Record<string, unknown>).include)) return true
  if (checkSource((relArgs as Record<string, unknown>).select)) return true

  return false
}

export function canUseJoinInclude(
  dialect: string,
  isList: boolean,
  takeVal: OptionalIntOrDynamic,
  skipVal: OptionalIntOrDynamic,
  depth: number,
  outerHasLimit: boolean,
  hasNestedIncludes: boolean,
): boolean {
  if (dialect !== 'postgres') return false
  if (!isList) return false
  if (depth > JOIN_INCLUDE_MAX_DEPTH) return false
  if (outerHasLimit) return false
  if (hasNestedIncludes) return false
  if (isDynamicParameter(takeVal) || isDynamicParameter(skipVal)) return false
  return true
}

export function buildJoinBasedNonPaginated(args: {
  relName: string
  relTable: string
  relAlias: string
  relModel: Model
  field: Field
  whereJoins: string
  rawWhereClause: string
  orderBySql: string
  relSelect: string
  ctx: {
    parentAlias: string
    model: Model
    dialect: SqlDialect
    aliasGen: { next: (base: string) => string }
  }
  nestedJoins: string[]
}): IncludeSpec {
  const { childKeys: relKeyFields, parentKeys: parentKeyFields } =
    resolveRelationKeys(args.field, 'include')

  const joinAlias = args.ctx.aliasGen.next(`${INC_ALIAS_PREFIX}${args.relName}`)

  const fkSelect = buildFkSelectList(args.relAlias, args.relModel, relKeyFields)
  const fkGroupBy = buildFkPartitionBy(
    args.relAlias,
    args.relModel,
    relKeyFields,
  )
  const rowExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)

  const aggExpr = args.orderBySql
    ? `json_agg(${rowExpr} ORDER BY ${args.orderBySql})`
    : `json_agg(${rowExpr})`

  const allJoins = [args.whereJoins, ...args.nestedJoins]
    .filter((j) => j)
    .join(' ')
  const joinsPart = allJoins ? ` ${allJoins}` : ''
  const wherePart = args.rawWhereClause
    ? ` ${SQL_TEMPLATES.WHERE} ${args.rawWhereClause}`
    : ''

  const subquery =
    `SELECT ${fkSelect}${SQL_SEPARATORS.FIELD_LIST}${aggExpr} AS ${AGG_COLUMN}` +
    ` FROM ${args.relTable} ${args.relAlias}${joinsPart}${wherePart}` +
    ` GROUP BY ${fkGroupBy}`

  const onCondition = buildFkJoinCondition(
    joinAlias,
    args.ctx.parentAlias,
    args.ctx.model,
    parentKeyFields,
  )

  const joinSql = `LEFT JOIN (${subquery}) ${joinAlias} ON ${onCondition}`
  const selectExpr = `COALESCE(${joinAlias}.${AGG_COLUMN}, ${PG_EMPTY_JSON_ARRAY}) AS ${quote(args.relName)}`

  return Object.freeze({
    name: args.relName,
    sql: '',
    isOneToOne: false,
    joinSql,
    selectExpr,
  })
}

export function buildJoinBasedPaginated(args: {
  relName: string
  relTable: string
  relAlias: string
  relModel: Model
  field: Field
  whereJoins: string
  rawWhereClause: string
  orderBySql: string
  relSelect: string
  takeVal: number | undefined
  skipVal: number | undefined
  ctx: {
    parentAlias: string
    model: Model
    dialect: SqlDialect
    aliasGen: { next: (base: string) => string }
    params: ParamStore
    includePath: string[]
  }
  nestedJoins: string[]
}): IncludeSpec {
  const { childKeys: relKeyFields, parentKeys: parentKeyFields } =
    resolveRelationKeys(args.field, 'include')

  const joinAlias = args.ctx.aliasGen.next(`${INC_ALIAS_PREFIX}${args.relName}`)
  const rankedAlias = args.ctx.aliasGen.next(
    `${RANKED_ALIAS_PREFIX}${args.relName}`,
  )

  const fkSelect = buildFkSelectList(args.relAlias, args.relModel, relKeyFields)
  const partitionBy = buildFkPartitionBy(
    args.relAlias,
    args.relModel,
    relKeyFields,
  )
  const rowExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)

  const orderExpr =
    args.orderBySql ||
    `${args.relAlias}.${quoteColumn(args.relModel, DEFAULT_PRIMARY_KEY)} ASC`

  const allJoins = [args.whereJoins, ...args.nestedJoins]
    .filter((j) => j)
    .join(' ')
  const joinsPart = allJoins ? ` ${allJoins}` : ''
  const wherePart = args.rawWhereClause
    ? ` ${SQL_TEMPLATES.WHERE} ${args.rawWhereClause}`
    : ''

  const innerSql =
    `SELECT ${fkSelect}${SQL_SEPARATORS.FIELD_LIST}` +
    `${rowExpr} AS ${ROW_COLUMN}${SQL_SEPARATORS.FIELD_LIST}` +
    `ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderExpr}) AS ${ROW_NUMBER_COLUMN}` +
    ` FROM ${args.relTable} ${args.relAlias}${joinsPart}${wherePart}`

  const scopeBase = buildIncludeScope(args.ctx.includePath)
  const rnFilterParts: string[] = []

  if (isNotNullish(args.skipVal) && args.skipVal > 0) {
    const skipPh = addAutoScoped(
      args.ctx.params,
      args.skipVal,
      `${scopeBase}.skip`,
    )
    rnFilterParts.push(`${ROW_NUMBER_COLUMN} > ${skipPh}`)

    if (isNotNullish(args.takeVal)) {
      const takePh = addAutoScoped(
        args.ctx.params,
        args.takeVal,
        `${scopeBase}.take`,
      )
      rnFilterParts.push(`${ROW_NUMBER_COLUMN} <= (${skipPh} + ${takePh})`)
    }
  } else if (isNotNullish(args.takeVal)) {
    const takePh = addAutoScoped(
      args.ctx.params,
      args.takeVal,
      `${scopeBase}.take`,
    )
    rnFilterParts.push(`${ROW_NUMBER_COLUMN} <= ${takePh}`)
  }

  const rnFilter =
    rnFilterParts.length > 0
      ? ` ${SQL_TEMPLATES.WHERE} ${rnFilterParts.join(SQL_SEPARATORS.CONDITION_AND)}`
      : ''

  const fkGroupByOuter = buildFkGroupBy(relKeyFields)

  const outerSql =
    `SELECT ${fkGroupByOuter}${SQL_SEPARATORS.FIELD_LIST}` +
    `json_agg(${ROW_COLUMN} ORDER BY ${ROW_NUMBER_COLUMN}) AS ${AGG_COLUMN}` +
    ` FROM (${innerSql}) ${rankedAlias}${rnFilter}` +
    ` GROUP BY ${fkGroupByOuter}`

  const onCondition = buildFkJoinCondition(
    joinAlias,
    args.ctx.parentAlias,
    args.ctx.model,
    parentKeyFields,
  )

  const joinSql = `LEFT JOIN (${outerSql}) ${joinAlias} ON ${onCondition}`
  const selectExpr = `COALESCE(${joinAlias}.${AGG_COLUMN}, ${PG_EMPTY_JSON_ARRAY}) AS ${quote(args.relName)}`

  return Object.freeze({
    name: args.relName,
    sql: '',
    isOneToOne: false,
    joinSql,
    selectExpr,
  })
}
