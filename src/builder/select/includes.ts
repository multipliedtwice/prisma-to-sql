import { joinCondition, isValidRelationField } from '../joins'
import { buildOrderBy, readSkipTake } from '../pagination'
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
import { reverseOrderByInput } from '../shared/order-by-utils'
import { addAutoScoped } from '../shared/dynamic-params'
import { getRelationFieldSet } from '../shared/model-field-cache'

type DynamicInt = string
type IntOrDynamic = number | DynamicInt
type OptionalIntOrDynamic = IntOrDynamic | undefined
type OrderByInput = unknown
type IncludeSelectArgs = Pick<PrismaQueryArgs, 'include' | 'select'>

interface IncludeBuildContext {
  model: Model
  schemas: Model[]
  parentAlias: string
  aliasGen: AliasGenerator
  dialect: SqlDialect
  params: ParamStore
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
  schemas: readonly Model[],
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

  const relModel = schemas.find((m) => m.name === field.relatedModel)
  if (!isNotNullish(relModel)) {
    throw new Error(
      `Relation '${relName}' on model ${model.name} references missing model '${field.relatedModel}'.`,
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

function assertScalarField(model: Model, fieldName: string): void {
  const f = model.fields.find((x) => x.name === fieldName)
  if (!f) {
    throw new Error(
      `orderBy references unknown field '${fieldName}' on model ${model.name}`,
    )
  }
  if (f.isRelation) {
    throw new Error(
      `orderBy does not support relation field '${fieldName}' on model ${model.name}`,
    )
  }
}

function validateOrderByDirection(fieldName: string, v: unknown): void {
  const s = String(v).toLowerCase()
  if (s !== 'asc' && s !== 'desc') {
    throw new Error(
      `Invalid orderBy direction for '${fieldName}': ${String(v)}`,
    )
  }
}

function validateOrderByObject(
  fieldName: string,
  v: Record<string, unknown>,
): void {
  if (!('sort' in v)) {
    throw new Error(
      `orderBy for '${fieldName}' must be 'asc' | 'desc' or { sort, nulls? }`,
    )
  }

  validateOrderByDirection(fieldName, (v as any).sort)

  if ('nulls' in v && isNotNullish((v as any).nulls)) {
    const n = String((v as any).nulls).toLowerCase()
    if (n !== 'first' && n !== 'last') {
      throw new Error(
        `Invalid orderBy.nulls for '${fieldName}': ${String((v as any).nulls)}`,
      )
    }
  }

  const allowed = new Set(['sort', 'nulls'])
  for (const k of Object.keys(v)) {
    if (!allowed.has(k)) {
      throw new Error(`Unsupported orderBy key '${k}' for field '${fieldName}'`)
    }
  }
}

function normalizeOrderByFieldName(name: unknown): string {
  const fieldName = String(name).trim()
  if (fieldName.length === 0) {
    throw new Error('orderBy field name cannot be empty')
  }
  return fieldName
}

function requirePlainObjectForOrderByEntry(
  v: unknown,
): Record<string, unknown> {
  if (typeof v !== 'object' || v === null || Array.isArray(v)) {
    throw new Error('orderBy array entries must be objects')
  }
  return v as Record<string, unknown>
}

function parseSingleFieldOrderByObject(
  obj: Record<string, unknown>,
): [string, unknown] {
  const entries = Object.entries(obj)
  if (entries.length !== 1) {
    throw new Error('orderBy array entries must have exactly one field')
  }
  const fieldName = normalizeOrderByFieldName(entries[0][0])
  return [fieldName, entries[0][1]]
}

function parseOrderByArray(orderBy: unknown[]): Array<[string, unknown]> {
  return orderBy.map((item) =>
    parseSingleFieldOrderByObject(requirePlainObjectForOrderByEntry(item)),
  )
}

function parseOrderByObject(
  orderBy: Record<string, unknown>,
): Array<[string, unknown]> {
  const out: Array<[string, unknown]> = []
  for (const [k, v] of Object.entries(orderBy)) {
    out.push([normalizeOrderByFieldName(k), v])
  }
  return out
}

function getOrderByEntries(orderBy: unknown): Array<[string, unknown]> {
  if (!isNotNullish(orderBy)) return []

  if (Array.isArray(orderBy)) {
    return parseOrderByArray(orderBy)
  }

  if (typeof orderBy === 'object' && orderBy !== null) {
    return parseOrderByObject(orderBy as Record<string, unknown>)
  }

  throw new Error('orderBy must be an object or array of objects')
}

function validateOrderByForModel(model: Model, orderBy: unknown): void {
  const entries = getOrderByEntries(orderBy)
  for (const [fieldName, v] of entries) {
    assertScalarField(model, fieldName)
    if (typeof v === 'string') {
      validateOrderByDirection(fieldName, v)
      continue
    }
    if (isPlainObject(v)) {
      validateOrderByObject(fieldName, v)
      continue
    }
    throw new Error(
      `orderBy for '${fieldName}' must be 'asc' | 'desc' or { sort, nulls? }`,
    )
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
  const w = (relArgs as any).where
  return isPlainObject(w) ? w : {}
}

function readOrderByInput(relArgs: unknown): {
  hasOrderBy: boolean
  orderBy: OrderByInput
} {
  if (!isPlainObject(relArgs)) return { hasOrderBy: false, orderBy: undefined }
  if (!('orderBy' in relArgs)) return { hasOrderBy: false, orderBy: undefined }
  return { hasOrderBy: true, orderBy: (relArgs as any).orderBy }
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

function hasIdTiebreaker(orderByInput: unknown): boolean {
  const entries = Array.isArray(orderByInput) ? orderByInput : [orderByInput]
  return entries.some((entry) =>
    isPlainObject(entry)
      ? Object.prototype.hasOwnProperty.call(entry, 'id')
      : false,
  )
}

function modelHasScalarId(relModel: Model): boolean {
  const idField = relModel.fields.find((f) => f.name === 'id')
  return Boolean(idField && !idField.isRelation)
}

function addIdTiebreaker(orderByInput: unknown): unknown {
  if (Array.isArray(orderByInput)) return [...orderByInput, { id: 'asc' }]
  return [orderByInput, { id: 'asc' }]
}

function ensureDeterministicOrderBy(
  relModel: Model,
  hasOrderBy: boolean,
  orderByInput: unknown,
  hasPagination: boolean,
): unknown {
  if (!hasPagination) {
    if (hasOrderBy && isNotNullish(orderByInput)) {
      validateOrderByForModel(relModel, orderByInput)
    }
    return orderByInput
  }

  if (!hasOrderBy) {
    return modelHasScalarId(relModel) ? { id: 'asc' } : orderByInput
  }

  if (isNotNullish(orderByInput)) {
    validateOrderByForModel(relModel, orderByInput)
  }

  if (!modelHasScalarId(relModel)) return orderByInput
  if (hasIdTiebreaker(orderByInput)) return orderByInput
  return addIdTiebreaker(orderByInput)
}

function buildSelectWithNestedIncludes(
  relArgs: unknown,
  relModel: Model,
  relAlias: string,
  ctx: IncludeBuildContext,
): string {
  let relSelect = buildRelationSelect(relArgs, relModel, relAlias)

  const nestedIncludes = isPlainObject(relArgs)
    ? buildIncludeSqlInternal(
        relArgs as PrismaQueryArgs,
        relModel,
        ctx.schemas,
        relAlias,
        ctx.aliasGen,
        ctx.params,
        ctx.dialect,
      )
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
): { joins: string; whereClause: string } {
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
  const whereClause = isValidWhereClause(whereResult.clause)
    ? ` ${SQL_TEMPLATES.AND} ${whereResult.clause}`
    : ''

  return { joins, whereClause }
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
  return (
    `${SQL_TEMPLATES.SELECT} ${args.selectExpr} ` +
    `${SQL_TEMPLATES.FROM} ${args.relTable} ${args.relAlias} ${args.joins} ` +
    `${SQL_TEMPLATES.WHERE} ${args.joinPredicate}${args.whereClause}`
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

  if (args.orderBySql) {
    sql += ` ${SQL_TEMPLATES.ORDER_BY} ${args.orderBySql}`
  }

  if (isNotNullish(args.takeVal)) {
    return appendLimitOffset(
      sql,
      args.ctx.dialect,
      args.ctx.params,
      args.takeVal,
      args.skipVal,
      `include.${args.relName}`,
    )
  }

  return limitOneSql(
    sql,
    args.ctx.params,
    args.skipVal,
    `include.${args.relName}`,
  )
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

  if (args.ctx.dialect === 'postgres' && noTake && noSkip) {
    const selectExpr = args.orderBySql
      ? `json_agg(${rowExpr} ORDER BY ${args.orderBySql})`
      : `json_agg(${rowExpr})`

    const sql = buildBaseSql({
      selectExpr,
      relTable: args.relTable,
      relAlias: args.relAlias,
      joins: args.joins,
      joinPredicate: args.joinPredicate,
      whereClause: args.whereClause,
    })

    return Object.freeze({
      name: args.relName,
      sql,
      isOneToOne: false,
    })
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

  if (args.orderBySql) {
    base += ` ${SQL_TEMPLATES.ORDER_BY} ${args.orderBySql}`
  }

  base = appendLimitOffset(
    base,
    args.ctx.dialect,
    args.ctx.params,
    args.takeVal,
    args.skipVal,
    `include.${args.relName}`,
  )

  const selectExpr = jsonAgg('row', args.ctx.dialect)
  const sql = `${SQL_TEMPLATES.SELECT} ${selectExpr} ${SQL_TEMPLATES.FROM} (${base}) ${rowAlias}`

  return Object.freeze({
    name: args.relName,
    sql,
    isOneToOne: false,
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
  const finalOrderByInput = ensureDeterministicOrderBy(
    relModel,
    paginationConfig.hasOrderBy,
    adjusted.orderByInput,
    hasPagination,
  )

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

    return Object.freeze({
      name: relName,
      sql,
      isOneToOne: true,
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
  model: Model,
  schemas: Model[],
  parentAlias: string,
  aliasGen: AliasGenerator,
  params: ParamStore,
  dialect: SqlDialect,
): IncludeSpec[] {
  const includes: IncludeSpec[] = []
  const entries = relationEntriesFromArgs(args, model)

  for (const [relName, relArgs] of entries) {
    if (relArgs === false) continue

    const resolved = resolveRelationOrThrow(model, schemas, relName)

    const include = buildSingleInclude(
      relName,
      relArgs,
      resolved.field,
      resolved.relModel,
      {
        model,
        schemas,
        parentAlias,
        aliasGen,
        dialect,
        params,
      },
    )

    includes.push(include)
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
): IncludeSpec[] {
  const aliasGen = createAliasGenerator()
  return buildIncludeSqlInternal(
    args,
    model,
    schemas,
    parentAlias,
    aliasGen,
    params,
    dialect,
  )
}

interface RelationCountBuild {
  joins: string[]
  jsonPairs: string
}

function resolveCountRelationOrThrow(
  relName: string,
  model: Model,
  schemas: readonly Model[],
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

  const relModel = schemas.find((m) => m.name === field.relatedModel)
  if (!relModel) {
    throw new Error(
      `Related model '${field.relatedModel}' not found for _count.${relName}`,
    )
  }

  return { field, relModel }
}

function groupByColForCount(field: Field, countAlias: string): string {
  const fkFields = normalizeKeyList((field as any).foreignKey)
  const refFields = normalizeKeyList((field as any).references)

  return field.isForeignKeyLocal
    ? `${countAlias}.${quote(refFields[0] || 'id')}`
    : `${countAlias}.${quote(fkFields[0])}`
}

function leftJoinOnForCount(
  field: Field,
  parentAlias: string,
  joinAlias: string,
): string {
  const fkFields = normalizeKeyList((field as any).foreignKey)
  const refFields = normalizeKeyList((field as any).references)

  return field.isForeignKeyLocal
    ? `${joinAlias}.__fk = ${parentAlias}.${quote(fkFields[0])}`
    : `${joinAlias}.__fk = ${parentAlias}.${quote(refFields[0] || 'id')}`
}

function subqueryForCount(
  dialect: SqlDialect,
  relTable: string,
  countAlias: string,
  groupByCol: string,
): string {
  return dialect === 'postgres'
    ? `(SELECT ${groupByCol} AS __fk, COUNT(*)::int AS __cnt FROM ${relTable} ${countAlias} GROUP BY ${groupByCol})`
    : `(SELECT ${groupByCol} AS __fk, COUNT(*) AS __cnt FROM ${relTable} ${countAlias} GROUP BY ${groupByCol})`
}

function buildCountJoinAndPair(args: {
  relName: string
  field: Field
  relModel: Model
  parentAlias: string
  dialect: SqlDialect
}): { joinSql: string; pairSql: string } {
  const relTable = getRelationTableReference(args.relModel, args.dialect)

  const countAlias = `__tp_cnt_${args.relName}`
  const groupByCol = groupByColForCount(args.field, countAlias)
  const subquery = subqueryForCount(
    args.dialect,
    relTable,
    countAlias,
    groupByCol,
  )

  const joinAlias = `__tp_cnt_j_${args.relName}`
  const leftJoinOn = leftJoinOnForCount(args.field, args.parentAlias, joinAlias)

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

  for (const [relName, shouldCount] of Object.entries(countSelect)) {
    if (!shouldCount) continue

    const resolved = resolveCountRelationOrThrow(relName, model, schemas)
    const built = buildCountJoinAndPair({
      relName,
      field: resolved.field,
      relModel: resolved.relModel,
      parentAlias,
      dialect,
    })

    joins.push(built.joinSql)
    pairs.push(built.pairSql)
  }

  return {
    joins,
    jsonPairs: pairs.join(SQL_SEPARATORS.FIELD_LIST),
  }
}
