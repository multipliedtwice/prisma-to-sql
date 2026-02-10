import { joinCondition, isValidRelationField } from '../joins'
import { buildOrderBy, readSkipTake, parseOrderByValue } from '../pagination'
import { buildWhereClause } from '../where'
import { jsonAgg, jsonBuildObject, SqlDialect } from '../../sql-builder-dialect'
import { buildRelationSelect } from './fields'
import { Model, PrismaQueryArgs, Field } from '../../types'
import { createAliasGenerator } from '../shared/alias-generator'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import {
  buildTableReference,
  quote,
  sqlStringLiteral,
  normalizeKeyList,
  quoteColumn,
} from '../shared/sql-utils'
import { ParamStore } from '../shared/param-store'
import { IncludeSpec, AliasGenerator } from '../shared/types'
import { isValidWhereClause } from '../shared/validators/sql-validators'
import {
  hasProperty,
  isNonEmptyArray,
  isNotNullish,
  isPlainObject,
} from '../shared/validators/type-guards'
import {
  reverseOrderByInput,
  normalizeOrderByInput,
} from '../shared/order-by-utils'
import { addAutoScoped } from '../shared/dynamic-params'
import {
  getRelationFieldSet,
  getScalarFieldSet,
} from '../shared/model-field-cache'
import { ensureDeterministicOrderByInput } from '../shared/order-by-determinism'
import { isDynamicParameter } from '@dee-wan/schema-parser'

const MAX_INCLUDE_DEPTH = 10

interface IncludeComplexityStats {
  totalIncludes: number
  totalSubqueries: number
  maxDepth: number
}

const MAX_TOTAL_SUBQUERIES = 100
const MAX_TOTAL_INCLUDES = 50

type DynamicInt = string
type IntOrDynamic = number | DynamicInt
type OptionalIntOrDynamic = IntOrDynamic | undefined
type OrderByInput = unknown
type IncludeSelectArgs = Pick<PrismaQueryArgs, 'include' | 'select'>

interface IncludeBuildContext {
  model: Model
  schemas: Model[]
  schemaByName: Map<string, Model>
  parentAlias: string
  aliasGen: AliasGenerator
  dialect: SqlDialect
  params: ParamStore
  includePath: string[]
  visitPath?: string[]
  depth?: number
  stats?: IncludeComplexityStats
  outerHasLimit?: boolean
}

interface FlatJoinEligibility {
  canUse: boolean
  reason?: string
}

export function canUseFlatJoinForInclude(
  relationName: string,
  relArgs: unknown,
  relModel: Model,
  depth: number,
  outerHasLimit: boolean,
): FlatJoinEligibility {
  if (depth > 0) {
    return { canUse: false, reason: 'nested_depth' }
  }

  const { hasSkip, hasTake } = readSkipTake(relArgs)
  if (hasSkip || hasTake) {
    return { canUse: false, reason: 'child_pagination' }
  }

  if (hasNestedRelationInArgs(relArgs, relModel)) {
    return { canUse: false, reason: 'nested_includes' }
  }

  if (!outerHasLimit) {
    return { canUse: false, reason: 'no_outer_limit' }
  }

  return { canUse: true }
}

function buildIncludeScope(includePath: readonly string[]): string {
  if (includePath.length === 0) return 'include'
  let scope = 'include'
  for (let i = 0; i < includePath.length; i++) {
    scope += `.${includePath[i]}`
    if (i < includePath.length - 1) {
      scope += '.include'
    }
  }
  return scope
}

function getRelationTableReference(
  relModel: Model,
  dialect: SqlDialect,
): string {
  return buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    relModel.tableName,
    dialect,
  )
}

function resolveRelationOrThrow(
  model: Model,
  schemaByName: Map<string, Model>,
  relName: string,
): { field: Field; relModel: Model } {
  const field = model.fields.find((f) => f.name === relName)

  if (!isNotNullish(field)) {
    throw new Error(
      `Unknown relation '${relName}' on model ${model.name}. ` +
        `Available relation fields: ${model.fields
          .filter((f) => f.isRelation)
          .map((f) => f.name)
          .join(', ')}`,
    )
  }

  if (!isValidRelationField(field)) {
    throw new Error(
      `Invalid relation metadata for '${relName}' on model ${model.name}. ` +
        `This usually indicates a schema parsing error (missing foreignKey/references).`,
    )
  }

  const relatedModelName = field.relatedModel
  if (
    !isNotNullish(relatedModelName) ||
    String(relatedModelName).trim().length === 0
  ) {
    throw new Error(
      `Relation '${relName}' on model ${model.name} is missing relatedModel metadata.`,
    )
  }

  const relModel = schemaByName.get(relatedModelName)
  if (!isNotNullish(relModel)) {
    throw new Error(
      `Relation '${relName}' on model ${model.name} references missing model '${relatedModelName}'.`,
    )
  }

  return { field, relModel }
}

function relationEntriesFromArgs(
  args: IncludeSelectArgs,
  model: Model,
): Array<[string, unknown]> {
  const relationSet = getRelationFieldSet(model)
  const out: Array<[string, unknown]> = []
  const seen = new Set<string>()

  const pushFrom = (src: unknown): void => {
    if (!isPlainObject(src)) return
    for (const [k, v] of Object.entries(src)) {
      if (v === false) continue
      if (!relationSet.has(k)) continue
      if (seen.has(k)) continue
      seen.add(k)
      out.push([k, v])
    }
  }

  pushFrom(args.include)
  pushFrom(args.select)

  return out
}

function validateOrderByForModel(model: Model, orderBy: unknown): void {
  if (!isNotNullish(orderBy)) return

  const scalarSet = getScalarFieldSet(model)
  const normalized = normalizeOrderByInput(orderBy, parseOrderByValue)

  for (const item of normalized) {
    const entries = Object.entries(item)
    if (entries.length !== 1) {
      throw new Error('orderBy array entries must have exactly one field')
    }

    const fieldName = String(entries[0][0]).trim()
    if (fieldName.length === 0) {
      throw new Error('orderBy field name cannot be empty')
    }
    if (!scalarSet.has(fieldName)) {
      throw new Error(
        `orderBy references unknown or non-scalar field '${fieldName}' on model ${model.name}`,
      )
    }
  }
}

function appendLimitOffset(
  sql: string,
  dialect: SqlDialect,
  params: ParamStore,
  takeVal: IntOrDynamic | undefined,
  skipVal: IntOrDynamic | undefined,
  scope: string,
): string {
  const hasTake = isNotNullish(takeVal)
  const hasSkip = isNotNullish(skipVal)

  if (dialect === 'sqlite' && !hasTake && hasSkip) {
    const skipPh = addAutoScoped(params, skipVal, `${scope}.skip`)
    return `${sql} ${SQL_TEMPLATES.LIMIT} -1 ${SQL_TEMPLATES.OFFSET} ${skipPh}`
  }

  if (hasTake) {
    const takePh = addAutoScoped(params, takeVal, `${scope}.take`)
    sql = `${sql} ${SQL_TEMPLATES.LIMIT} ${takePh}`
  }

  if (hasSkip) {
    const skipPh = addAutoScoped(params, skipVal, `${scope}.skip`)
    sql = `${sql} ${SQL_TEMPLATES.OFFSET} ${skipPh}`
  }

  return sql
}

function readWhereInput(relArgs: unknown): Record<string, unknown> {
  if (!isPlainObject(relArgs)) return {}
  if (!hasProperty(relArgs, 'where')) return {}
  const w = (relArgs as Record<string, unknown>).where
  return isPlainObject(w) ? w : {}
}

function readOrderByInput(relArgs: unknown): {
  hasOrderBy: boolean
  orderBy: OrderByInput
} {
  if (!isPlainObject(relArgs)) return { hasOrderBy: false, orderBy: undefined }
  if (!('orderBy' in relArgs)) return { hasOrderBy: false, orderBy: undefined }
  return {
    hasOrderBy: true,
    orderBy: (relArgs as any).orderBy,
  }
}

function extractRelationPaginationConfig(relArgs: unknown): {
  hasOrderBy: boolean
  orderBy: unknown
  hasSkip: boolean
  hasTake: boolean
  skipVal: OptionalIntOrDynamic
  takeVal: OptionalIntOrDynamic
} {
  const { hasOrderBy, orderBy: rawOrderByInput } = readOrderByInput(relArgs)
  const {
    hasSkip,
    hasTake,
    skipVal,
    takeVal: rawTakeVal,
  } = readSkipTake(relArgs)

  return {
    hasOrderBy,
    orderBy: rawOrderByInput,
    hasSkip,
    hasTake,
    skipVal,
    takeVal: rawTakeVal,
  }
}

function maybeReverseNegativeTake(
  takeVal: OptionalIntOrDynamic,
  hasOrderBy: boolean,
  orderByInput: unknown,
): { takeVal: OptionalIntOrDynamic; orderByInput: unknown } {
  if (typeof takeVal !== 'number') return { takeVal, orderByInput }
  if (takeVal >= 0) return { takeVal, orderByInput }
  if (!hasOrderBy) {
    throw new Error('Negative take requires orderBy for deterministic results')
  }
  return {
    takeVal: Math.abs(takeVal),
    orderByInput: reverseOrderByInput(orderByInput),
  }
}

function finalizeOrderByForInclude(args: {
  relModel: Model
  hasOrderBy: boolean
  orderByInput: unknown
  hasPagination: boolean
}): unknown {
  if (args.hasOrderBy && isNotNullish(args.orderByInput)) {
    validateOrderByForModel(args.relModel, args.orderByInput)
  }

  if (!args.hasPagination) return args.orderByInput

  return ensureDeterministicOrderByInput({
    orderBy: args.hasOrderBy ? args.orderByInput : undefined,
    model: args.relModel,
    parseValue: parseOrderByValue,
  })
}

function buildSelectWithNestedIncludes(
  relArgs: unknown,
  relModel: Model,
  relAlias: string,
  ctx: IncludeBuildContext,
): string {
  let relSelect = buildRelationSelect(relArgs, relModel, relAlias)

  const nestedIncludes = isPlainObject(relArgs)
    ? buildIncludeSqlInternal(relArgs as PrismaQueryArgs, {
        ...ctx,
        model: relModel,
        parentAlias: relAlias,
        depth: (ctx.depth || 0) + 1,
      })
    : []

  if (isNonEmptyArray(nestedIncludes)) {
    const emptyJson = ctx.dialect === 'postgres' ? `'[]'::json` : `json('[]')`
    const nestedSelects = nestedIncludes
      .map((inc) =>
        inc.isOneToOne
          ? `${sqlStringLiteral(inc.name)}, (${inc.sql})`
          : `${sqlStringLiteral(inc.name)}, COALESCE((${inc.sql}), ${emptyJson})`,
      )
      .join(SQL_SEPARATORS.FIELD_LIST)

    relSelect =
      isNotNullish(relSelect) && relSelect.trim().length > 0
        ? `${relSelect}${SQL_SEPARATORS.FIELD_LIST}${nestedSelects}`
        : nestedSelects
  }

  if (!isNotNullish(relSelect) || relSelect.trim().length === 0) {
    throw new Error(
      `Select must include at least one field or nested relation for model ${relModel.name}`,
    )
  }

  return relSelect
}

function buildWhereParts(
  whereInput: Record<string, unknown>,
  relModel: Model,
  relAlias: string,
  ctx: IncludeBuildContext,
): { joins: string; whereClause: string; rawClause: string } {
  const whereResult = buildWhereClause(whereInput, {
    alias: relAlias,
    schemaModels: ctx.schemas,
    model: relModel,
    params: ctx.params,
    isSubquery: true,
    aliasGen: ctx.aliasGen,
    dialect: ctx.dialect,
  })

  const joins = whereResult.joins.join(' ')
  const hasClause = isValidWhereClause(whereResult.clause)

  return {
    joins,
    whereClause: hasClause ? ` ${SQL_TEMPLATES.AND} ${whereResult.clause}` : '',
    rawClause: hasClause ? whereResult.clause : '',
  }
}

function limitOneSql(
  sql: string,
  params: ParamStore,
  skipVal: OptionalIntOrDynamic,
  scope: string,
): string {
  if (isNotNullish(skipVal)) {
    const skipPh = addAutoScoped(params, skipVal, `${scope}.skip`)
    return `${sql} ${SQL_TEMPLATES.LIMIT} 1 ${SQL_TEMPLATES.OFFSET} ${skipPh}`
  }
  return `${sql} ${SQL_TEMPLATES.LIMIT} 1`
}

function buildOrderBySql(
  finalOrderByInput: unknown,
  relAlias: string,
  dialect: SqlDialect,
  relModel: Model,
): string {
  return isNotNullish(finalOrderByInput)
    ? buildOrderBy(finalOrderByInput, relAlias, dialect, relModel)
    : ''
}

function buildBaseSql(args: {
  selectExpr: string
  relTable: string
  relAlias: string
  joins: string
  joinPredicate: string
  whereClause: string
}): string {
  const joins = args.joins ? ` ${args.joins}` : ''
  const where = `${SQL_TEMPLATES.WHERE} ${args.joinPredicate}${args.whereClause}`
  return (
    `${SQL_TEMPLATES.SELECT} ${args.selectExpr} ` +
    `${SQL_TEMPLATES.FROM} ${args.relTable} ${args.relAlias}${joins} ` +
    where
  )
}

function buildOneToOneIncludeSql(args: {
  relName: string
  relTable: string
  relAlias: string
  joins: string
  joinPredicate: string
  whereClause: string
  orderBySql: string
  relSelect: string
  takeVal: OptionalIntOrDynamic
  skipVal: OptionalIntOrDynamic
  ctx: IncludeBuildContext
}): string {
  const objExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)

  let sql = buildBaseSql({
    selectExpr: objExpr,
    relTable: args.relTable,
    relAlias: args.relAlias,
    joins: args.joins,
    joinPredicate: args.joinPredicate,
    whereClause: args.whereClause,
  })

  if (args.orderBySql) sql += ` ${SQL_TEMPLATES.ORDER_BY} ${args.orderBySql}`

  const scopeBase = buildIncludeScope(args.ctx.includePath)

  if (isNotNullish(args.takeVal)) {
    return appendLimitOffset(
      sql,
      args.ctx.dialect,
      args.ctx.params,
      args.takeVal,
      args.skipVal,
      scopeBase,
    )
  }

  return limitOneSql(sql, args.ctx.params, args.skipVal, scopeBase)
}

function buildListIncludeSpec(args: {
  relName: string
  relTable: string
  relAlias: string
  joins: string
  joinPredicate: string
  whereClause: string
  orderBySql: string
  relSelect: string
  takeVal: OptionalIntOrDynamic
  skipVal: OptionalIntOrDynamic
  ctx: IncludeBuildContext
}): IncludeSpec {
  const rowExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)
  const noTake = !isNotNullish(args.takeVal)
  const noSkip = !isNotNullish(args.skipVal)

  const emptyJson =
    args.ctx.dialect === 'postgres' ? `'[]'::json` : `json('[]')`

  if (args.ctx.dialect === 'postgres' && noTake && noSkip) {
    const rawAgg = args.orderBySql
      ? `json_agg(${rowExpr} ORDER BY ${args.orderBySql})`
      : `json_agg(${rowExpr})`

    const selectExpr = `COALESCE(${rawAgg}, ${emptyJson})`

    const sql = buildBaseSql({
      selectExpr,
      relTable: args.relTable,
      relAlias: args.relAlias,
      joins: args.joins,
      joinPredicate: args.joinPredicate,
      whereClause: args.whereClause,
    })

    return Object.freeze({ name: args.relName, sql, isOneToOne: false })
  }

  const rowAlias = args.ctx.aliasGen.next(`${args.relName}_row`)

  let base = buildBaseSql({
    selectExpr: `${rowExpr} ${SQL_TEMPLATES.AS} row`,
    relTable: args.relTable,
    relAlias: args.relAlias,
    joins: args.joins,
    joinPredicate: args.joinPredicate,
    whereClause: args.whereClause,
  })

  if (args.orderBySql) base += ` ${SQL_TEMPLATES.ORDER_BY} ${args.orderBySql}`

  const scopeBase = buildIncludeScope(args.ctx.includePath)

  base = appendLimitOffset(
    base,
    args.ctx.dialect,
    args.ctx.params,
    args.takeVal,
    args.skipVal,
    scopeBase,
  )

  const agg = jsonAgg('row', args.ctx.dialect)
  const selectExpr = `COALESCE(${agg}, ${emptyJson})`

  const sql =
    `${SQL_TEMPLATES.SELECT} ${selectExpr} ` +
    `${SQL_TEMPLATES.FROM} (${base}) ${SQL_TEMPLATES.AS} ${rowAlias}`

  return Object.freeze({ name: args.relName, sql, isOneToOne: false })
}

function resolveIncludeKeyPairs(field: Field): {
  relKeyFields: string[]
  parentKeyFields: string[]
} {
  const fkFields = normalizeKeyList(field.foreignKey)
  if (fkFields.length === 0) {
    throw new Error(
      `Relation '${field.name}' is missing foreignKey for join-based include`,
    )
  }

  const refs = normalizeKeyList(field.references)
  const refFields = refs.length > 0 ? refs : fkFields.length === 1 ? ['id'] : []

  if (refFields.length !== fkFields.length) {
    throw new Error(
      `Relation '${field.name}' references count doesn't match foreignKey count`,
    )
  }

  return {
    relKeyFields: field.isForeignKeyLocal ? refFields : fkFields,
    parentKeyFields: field.isForeignKeyLocal ? fkFields : refFields,
  }
}

function buildFkSelectList(
  relAlias: string,
  relModel: Model,
  relKeyFields: string[],
): string {
  return relKeyFields
    .map((f, i) => `${relAlias}.${quoteColumn(relModel, f)} AS "__fk${i}"`)
    .join(SQL_SEPARATORS.FIELD_LIST)
}

function buildFkGroupByUnqualified(relKeyFields: string[]): string {
  return relKeyFields
    .map((_, i) => `"__fk${i}"`)
    .join(SQL_SEPARATORS.FIELD_LIST)
}

function buildJoinOnCondition(
  joinAlias: string,
  parentAlias: string,
  parentModel: Model,
  parentKeyFields: string[],
): string {
  const parts = parentKeyFields.map(
    (f, i) =>
      `${joinAlias}."__fk${i}" = ${parentAlias}.${quoteColumn(parentModel, f)}`,
  )
  return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
}

function buildPartitionBy(
  relAlias: string,
  relModel: Model,
  relKeyFields: string[],
): string {
  return relKeyFields
    .map((f) => `${relAlias}.${quoteColumn(relModel, f)}`)
    .join(SQL_SEPARATORS.FIELD_LIST)
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

function canUseJoinInclude(
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
  if (depth > 0) return false
  if (outerHasLimit) return false
  if (hasNestedIncludes) return false
  if (isDynamicParameter(takeVal) || isDynamicParameter(skipVal)) return false
  return true
}

function buildJoinBasedNonPaginated(args: {
  relName: string
  relTable: string
  relAlias: string
  relModel: Model
  field: Field
  whereJoins: string
  rawWhereClause: string
  orderBySql: string
  relSelect: string
  ctx: IncludeBuildContext
}): IncludeSpec {
  const { relKeyFields, parentKeyFields } = resolveIncludeKeyPairs(args.field)
  const joinAlias = args.ctx.aliasGen.next(`__inc_${args.relName}`)

  const fkSelect = buildFkSelectList(args.relAlias, args.relModel, relKeyFields)
  const fkGroupBy = buildPartitionBy(args.relAlias, args.relModel, relKeyFields)
  const rowExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)

  const aggExpr = args.orderBySql
    ? `json_agg(${rowExpr} ORDER BY ${args.orderBySql})`
    : `json_agg(${rowExpr})`

  const joinsPart = args.whereJoins ? ` ${args.whereJoins}` : ''
  const wherePart = args.rawWhereClause
    ? ` ${SQL_TEMPLATES.WHERE} ${args.rawWhereClause}`
    : ''

  const subquery =
    `SELECT ${fkSelect}${SQL_SEPARATORS.FIELD_LIST}${aggExpr} AS __agg` +
    ` FROM ${args.relTable} ${args.relAlias}${joinsPart}${wherePart}` +
    ` GROUP BY ${fkGroupBy}`

  const onCondition = buildJoinOnCondition(
    joinAlias,
    args.ctx.parentAlias,
    args.ctx.model,
    parentKeyFields,
  )

  const joinSql = `LEFT JOIN (${subquery}) ${joinAlias} ON ${onCondition}`
  const selectExpr = `COALESCE(${joinAlias}.__agg, '[]'::json) AS ${quote(args.relName)}`

  return Object.freeze({
    name: args.relName,
    sql: '',
    isOneToOne: false,
    joinSql,
    selectExpr,
  })
}

function buildJoinBasedPaginated(args: {
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
  ctx: IncludeBuildContext
}): IncludeSpec {
  const { relKeyFields, parentKeyFields } = resolveIncludeKeyPairs(args.field)
  const joinAlias = args.ctx.aliasGen.next(`__inc_${args.relName}`)
  const rankedAlias = args.ctx.aliasGen.next(`__ranked_${args.relName}`)

  const fkSelect = buildFkSelectList(args.relAlias, args.relModel, relKeyFields)
  const partitionBy = buildPartitionBy(
    args.relAlias,
    args.relModel,
    relKeyFields,
  )
  const rowExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)

  const orderExpr =
    args.orderBySql ||
    `${args.relAlias}.${quoteColumn(args.relModel, 'id')} ASC`

  const joinsPart = args.whereJoins ? ` ${args.whereJoins}` : ''
  const wherePart = args.rawWhereClause
    ? ` ${SQL_TEMPLATES.WHERE} ${args.rawWhereClause}`
    : ''

  const innerSql =
    `SELECT ${fkSelect}${SQL_SEPARATORS.FIELD_LIST}` +
    `${rowExpr} AS __row${SQL_SEPARATORS.FIELD_LIST}` +
    `ROW_NUMBER() OVER (PARTITION BY ${partitionBy} ORDER BY ${orderExpr}) AS __rn` +
    ` FROM ${args.relTable} ${args.relAlias}${joinsPart}${wherePart}`

  const scopeBase = buildIncludeScope(args.ctx.includePath)
  const rnFilterParts: string[] = []

  if (isNotNullish(args.skipVal) && args.skipVal > 0) {
    const skipPh = addAutoScoped(
      args.ctx.params,
      args.skipVal,
      `${scopeBase}.skip`,
    )
    rnFilterParts.push(`__rn > ${skipPh}`)

    if (isNotNullish(args.takeVal)) {
      const takePh = addAutoScoped(
        args.ctx.params,
        args.takeVal,
        `${scopeBase}.take`,
      )
      rnFilterParts.push(`__rn <= (${skipPh} + ${takePh})`)
    }
  } else if (isNotNullish(args.takeVal)) {
    const takePh = addAutoScoped(
      args.ctx.params,
      args.takeVal,
      `${scopeBase}.take`,
    )
    rnFilterParts.push(`__rn <= ${takePh}`)
  }

  const rnFilter =
    rnFilterParts.length > 0
      ? ` ${SQL_TEMPLATES.WHERE} ${rnFilterParts.join(SQL_SEPARATORS.CONDITION_AND)}`
      : ''

  const fkGroupByOuter = buildFkGroupByUnqualified(relKeyFields)

  const outerSql =
    `SELECT ${fkGroupByOuter}${SQL_SEPARATORS.FIELD_LIST}` +
    `json_agg(__row ORDER BY __rn) AS __agg` +
    ` FROM (${innerSql}) ${rankedAlias}${rnFilter}` +
    ` GROUP BY ${fkGroupByOuter}`

  const onCondition = buildJoinOnCondition(
    joinAlias,
    args.ctx.parentAlias,
    args.ctx.model,
    parentKeyFields,
  )

  const joinSql = `LEFT JOIN (${outerSql}) ${joinAlias} ON ${onCondition}`
  const selectExpr = `COALESCE(${joinAlias}.__agg, '[]'::json) AS ${quote(args.relName)}`

  return Object.freeze({
    name: args.relName,
    sql: '',
    isOneToOne: false,
    joinSql,
    selectExpr,
  })
}

function buildSingleInclude(
  relName: string,
  relArgs: unknown,
  field: Field,
  relModel: Model,
  ctx: IncludeBuildContext,
): IncludeSpec {
  const relTable = getRelationTableReference(relModel, ctx.dialect)
  const relAlias = ctx.aliasGen.next(relName)

  const isList = typeof field.type === 'string' && field.type.endsWith('[]')
  const joinPredicate = joinCondition(
    field,
    ctx.model,
    relModel,
    ctx.parentAlias,
    relAlias,
  )

  const whereInput = readWhereInput(relArgs)
  const relSelect = buildSelectWithNestedIncludes(
    relArgs,
    relModel,
    relAlias,
    ctx,
  )
  const whereParts = buildWhereParts(whereInput, relModel, relAlias, ctx)

  const paginationConfig = extractRelationPaginationConfig(relArgs)

  if (
    !isList &&
    typeof paginationConfig.takeVal === 'number' &&
    paginationConfig.takeVal < 0
  ) {
    throw new Error('Negative take is only supported for list relations')
  }

  const adjusted = maybeReverseNegativeTake(
    paginationConfig.takeVal,
    paginationConfig.hasOrderBy,
    paginationConfig.orderBy,
  )

  const hasPagination = paginationConfig.hasSkip || paginationConfig.hasTake

  const finalOrderByInput = finalizeOrderByForInclude({
    relModel,
    hasOrderBy: paginationConfig.hasOrderBy,
    orderByInput: adjusted.orderByInput,
    hasPagination,
  })

  const orderBySql = buildOrderBySql(
    finalOrderByInput,
    relAlias,
    ctx.dialect,
    relModel,
  )

  if (!isList) {
    const sql = buildOneToOneIncludeSql({
      relName,
      relTable,
      relAlias,
      joins: whereParts.joins,
      joinPredicate,
      whereClause: whereParts.whereClause,
      orderBySql,
      relSelect,
      takeVal: adjusted.takeVal,
      skipVal: paginationConfig.skipVal,
      ctx,
    })
    return Object.freeze({ name: relName, sql, isOneToOne: true })
  }

  const depth = ctx.depth || 0
  const outerHasLimit = ctx.outerHasLimit === true
  const nestedIncludes = hasNestedRelationInArgs(relArgs, relModel)

  if (
    canUseJoinInclude(
      ctx.dialect,
      isList,
      adjusted.takeVal,
      paginationConfig.skipVal,
      depth,
      outerHasLimit,
      nestedIncludes,
    )
  ) {
    const hasTakeOrSkip =
      isNotNullish(adjusted.takeVal) || isNotNullish(paginationConfig.skipVal)

    if (!hasTakeOrSkip) {
      return buildJoinBasedNonPaginated({
        relName,
        relTable,
        relAlias,
        relModel,
        field,
        whereJoins: whereParts.joins,
        rawWhereClause: whereParts.rawClause,
        orderBySql,
        relSelect,
        ctx,
      })
    }

    return buildJoinBasedPaginated({
      relName,
      relTable,
      relAlias,
      relModel,
      field,
      whereJoins: whereParts.joins,
      rawWhereClause: whereParts.rawClause,
      orderBySql,
      relSelect,
      takeVal: adjusted.takeVal as number | undefined,
      skipVal: paginationConfig.skipVal as number | undefined,
      ctx,
    })
  }

  return buildListIncludeSpec({
    relName,
    relTable,
    relAlias,
    joins: whereParts.joins,
    joinPredicate,
    whereClause: whereParts.whereClause,
    orderBySql,
    relSelect,
    takeVal: adjusted.takeVal,
    skipVal: paginationConfig.skipVal,
    ctx,
  })
}

function buildIncludeSqlInternal(
  args: IncludeSelectArgs,
  ctx: IncludeBuildContext,
): IncludeSpec[] {
  const stats = ctx.stats || {
    totalIncludes: 0,
    totalSubqueries: 0,
    maxDepth: 0,
  }
  const depth = ctx.depth || 0
  const visitPath = ctx.visitPath || []

  if (depth > MAX_INCLUDE_DEPTH) {
    throw new Error(
      `Maximum include depth of ${MAX_INCLUDE_DEPTH} exceeded. ` +
        `Path: ${visitPath.join(' -> ')}. ` +
        `Deep includes cause exponential SQL complexity and performance issues.`,
    )
  }

  stats.maxDepth = Math.max(stats.maxDepth, depth)

  const includes: IncludeSpec[] = []
  const entries = relationEntriesFromArgs(args, ctx.model)

  for (const [relName, relArgs] of entries) {
    if (relArgs === false) continue

    stats.totalIncludes++
    if (stats.totalIncludes > MAX_TOTAL_INCLUDES) {
      throw new Error(
        `Maximum total includes (${MAX_TOTAL_INCLUDES}) exceeded. ` +
          `Current: ${stats.totalIncludes} includes. ` +
          `Query has ${stats.maxDepth} levels deep. ` +
          `Simplify your query structure or use multiple queries.`,
      )
    }

    stats.totalSubqueries++
    if (stats.totalSubqueries > MAX_TOTAL_SUBQUERIES) {
      throw new Error(
        `Query complexity limit exceeded: ${stats.totalSubqueries} subqueries generated. ` +
          `Maximum allowed: ${MAX_TOTAL_SUBQUERIES}. ` +
          `This indicates exponential include nesting. ` +
          `Stats: depth=${stats.maxDepth}, includes=${stats.totalIncludes}. ` +
          `Path: ${visitPath.join(' -> ')}. ` +
          `Simplify your include structure or split into multiple queries.`,
      )
    }

    const resolved = resolveRelationOrThrow(
      ctx.model,
      ctx.schemaByName,
      relName,
    )

    const relationPath = `${ctx.model.name}.${relName}`
    const currentPath = [...visitPath, relationPath]

    if (visitPath.includes(relationPath)) {
      throw new Error(
        `Circular include detected: ${currentPath.join(' -> ')}. Relation '${relationPath}' creates an infinite loop.`,
      )
    }

    const modelOccurrences = currentPath.filter((p) =>
      p.startsWith(`${resolved.relModel.name}.`),
    ).length
    if (modelOccurrences > 2) {
      throw new Error(
        `Include too deeply nested: model '${resolved.relModel.name}' ` +
          `appears ${modelOccurrences} times in path: ${currentPath.join(' -> ')}`,
      )
    }

    const nextIncludePath = [...ctx.includePath, relName]

    includes.push(
      buildSingleInclude(relName, relArgs, resolved.field, resolved.relModel, {
        ...ctx,
        includePath: nextIncludePath,
        visitPath: currentPath,
        depth: depth,
        stats,
      }),
    )
  }

  return includes
}

export function buildIncludeSql(
  args: PrismaQueryArgs,
  model: Model,
  schemas: Model[],
  parentAlias: string,
  params: ParamStore,
  dialect: SqlDialect,
  outerHasLimit: boolean = true,
): IncludeSpec[] {
  const aliasGen = createAliasGenerator()
  const stats: IncludeComplexityStats = {
    totalIncludes: 0,
    totalSubqueries: 0,
    maxDepth: 0,
  }

  const schemaByName = new Map<string, Model>()
  for (const m of schemas) schemaByName.set(m.name, m)

  return buildIncludeSqlInternal(args, {
    model,
    schemas,
    schemaByName,
    parentAlias,
    aliasGen,
    params,
    dialect,
    includePath: [],
    visitPath: [],
    depth: 0,
    stats,
    outerHasLimit,
  })
}

interface RelationCountBuild {
  joins: string[]
  jsonPairs: string
}

function resolveCountRelationOrThrow(
  relName: string,
  model: Model,
  schemaByName: Map<string, Model>,
): { field: Field; relModel: Model } {
  const relationSet = getRelationFieldSet(model)
  if (!relationSet.has(relName)) {
    throw new Error(
      `_count.${relName} references unknown relation on model ${model.name}`,
    )
  }

  const field = model.fields.find((f) => f.name === relName)
  if (!field) {
    throw new Error(
      `_count.${relName} references unknown relation on model ${model.name}`,
    )
  }

  if (!isValidRelationField(field)) {
    throw new Error(
      `_count.${relName} has invalid relation metadata on model ${model.name}`,
    )
  }

  const relatedModelName = field.relatedModel
  if (
    !isNotNullish(relatedModelName) ||
    String(relatedModelName).trim().length === 0
  ) {
    throw new Error(
      `_count.${relName} is missing relatedModel metadata on model ${model.name}`,
    )
  }

  const relModel = schemaByName.get(relatedModelName)
  if (!relModel) {
    throw new Error(
      `Related model '${relatedModelName}' not found for _count.${relName}`,
    )
  }

  return { field, relModel }
}

function defaultReferencesForCount(fkCount: number): string[] {
  if (fkCount === 1) return ['id']
  throw new Error(
    'Relation count for composite keys requires explicit references matching...',
  )
}

function resolveCountKeyPairs(field: Field): {
  relKeyFields: string[]
  parentKeyFields: string[]
} {
  const fkFields = normalizeKeyList(field.foreignKey)
  if (fkFields.length === 0) {
    throw new Error('Relation count requires foreignKey')
  }

  const refs = normalizeKeyList(field.references)
  const refFields =
    refs.length > 0 ? refs : defaultReferencesForCount(fkFields.length)

  if (refFields.length !== fkFields.length) {
    throw new Error(
      'Relation count requires references count to match foreignKey count',
    )
  }

  const relKeyFields = field.isForeignKeyLocal ? refFields : fkFields
  const parentKeyFields = field.isForeignKeyLocal ? fkFields : refFields

  return { relKeyFields, parentKeyFields }
}

function aliasQualifiedColumn(
  alias: string,
  model: Model,
  field: string,
): string {
  return `${alias}.${quoteColumn(model, field)}`
}

function subqueryForCount(args: {
  dialect: SqlDialect
  relTable: string
  countAlias: string
  relModel: Model
  relKeyFields: string[]
}): string {
  const selectKeys = args.relKeyFields
    .map(
      (f, i) =>
        `${aliasQualifiedColumn(args.countAlias, args.relModel, f)} AS "__fk${i}"`,
    )
    .join(SQL_SEPARATORS.FIELD_LIST)

  const groupByKeys = args.relKeyFields
    .map((f) => aliasQualifiedColumn(args.countAlias, args.relModel, f))
    .join(SQL_SEPARATORS.FIELD_LIST)

  const cntExpr =
    args.dialect === 'postgres' ? 'COUNT(*)::int AS __cnt' : 'COUNT(*) AS __cnt'

  return `(SELECT ${selectKeys}${SQL_SEPARATORS.FIELD_LIST}${cntExpr} FROM ${args.relTable} ${args.countAlias} GROUP BY ${groupByKeys})`
}

function leftJoinOnForCount(args: {
  joinAlias: string
  parentAlias: string
  parentModel: Model
  parentKeyFields: string[]
}): string {
  const parts = args.parentKeyFields.map(
    (f, i) =>
      `${args.joinAlias}."__fk${i}" = ${aliasQualifiedColumn(args.parentAlias, args.parentModel, f)}`,
  )
  return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
}

function nextAliasAvoiding(
  aliasGen: ReturnType<typeof createAliasGenerator>,
  base: string,
  forbidden: Set<string>,
): string {
  let a = aliasGen.next(base)
  while (forbidden.has(a)) a = aliasGen.next(base)
  return a
}

function buildCountJoinAndPair(args: {
  relName: string
  field: Field
  relModel: Model
  parentModel: Model
  parentAlias: string
  dialect: SqlDialect
  aliasGen: ReturnType<typeof createAliasGenerator>
}): { joinSql: string; pairSql: string } {
  const relTable = getRelationTableReference(args.relModel, args.dialect)
  const { relKeyFields, parentKeyFields } = resolveCountKeyPairs(args.field)

  const forbidden = new Set<string>([args.parentAlias])

  const countAlias = nextAliasAvoiding(
    args.aliasGen,
    `__tp_cnt_${args.relName}`,
    forbidden,
  )
  forbidden.add(countAlias)

  const subquery = subqueryForCount({
    dialect: args.dialect,
    relTable,
    countAlias,
    relModel: args.relModel,
    relKeyFields,
  })

  const joinAlias = nextAliasAvoiding(
    args.aliasGen,
    `__tp_cnt_j_${args.relName}`,
    forbidden,
  )

  const leftJoinOn = leftJoinOnForCount({
    joinAlias,
    parentAlias: args.parentAlias,
    parentModel: args.parentModel,
    parentKeyFields,
  })

  return {
    joinSql: `LEFT JOIN ${subquery} ${joinAlias} ON ${leftJoinOn}`,
    pairSql: `${sqlStringLiteral(args.relName)}, COALESCE(${joinAlias}.__cnt, 0)`,
  }
}

export function buildRelationCountSql(
  countSelect: Record<string, boolean>,
  model: Model,
  schemas: readonly Model[],
  parentAlias: string,
  _params: ParamStore,
  dialect: SqlDialect,
): RelationCountBuild {
  const joins: string[] = []
  const pairs: string[] = []
  const aliasGen = createAliasGenerator()

  const schemaByName = new Map<string, Model>()
  for (const m of schemas) schemaByName.set(m.name, m)

  for (const [relName, shouldCount] of Object.entries(countSelect)) {
    if (!shouldCount) continue

    const resolved = resolveCountRelationOrThrow(relName, model, schemaByName)
    const built = buildCountJoinAndPair({
      relName,
      field: resolved.field,
      relModel: resolved.relModel,
      parentModel: model,
      parentAlias,
      dialect,
      aliasGen,
    })

    joins.push(built.joinSql)
    pairs.push(built.pairSql)
  }

  return { joins, jsonPairs: pairs.join(SQL_SEPARATORS.FIELD_LIST) }
}
