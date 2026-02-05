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
  visitPath?: string[]
  depth?: number
  stats?: IncludeComplexityStats
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
    if (fieldName.length === 0)
      throw new Error('orderBy field name cannot be empty')
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
    orderBy: (relArgs as Record<string, unknown>).orderBy,
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
  if (!hasOrderBy)
    throw new Error('Negative take requires orderBy for deterministic results')
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

  if (!args.hasPagination) {
    return args.orderByInput
  }

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
    ? buildIncludeSqlInternal(
        relArgs as PrismaQueryArgs,
        relModel,
        ctx.schemas,
        ctx.schemaByName,
        relAlias,
        ctx.aliasGen,
        ctx.params,
        ctx.dialect,
        ctx.visitPath || [],
        (ctx.depth || 0) + 1,
        ctx.stats,
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

  return Object.freeze({ name: args.relName, sql, isOneToOne: false })
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
  schemaByName: Map<string, Model>,
  parentAlias: string,
  aliasGen: AliasGenerator,
  params: ParamStore,
  dialect: SqlDialect,
  visitPath: string[] = [],
  depth: number = 0,
  stats?: IncludeComplexityStats,
): IncludeSpec[] {
  if (!stats) stats = { totalIncludes: 0, totalSubqueries: 0, maxDepth: 0 }

  if (depth > MAX_INCLUDE_DEPTH) {
    throw new Error(
      `Maximum include depth of ${MAX_INCLUDE_DEPTH} exceeded. ` +
        `Path: ${visitPath.join(' -> ')}. ` +
        `Deep includes cause exponential SQL complexity and performance issues.`,
    )
  }

  stats.maxDepth = Math.max(stats.maxDepth, depth)

  const includes: IncludeSpec[] = []
  const entries = relationEntriesFromArgs(args, model)

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
      model,
      schemas,
      schemaByName,
      relName,
    )

    const relationPath = `${model.name}.${relName}`
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

    includes.push(
      buildSingleInclude(relName, relArgs, resolved.field, resolved.relModel, {
        model,
        schemas,
        schemaByName,
        parentAlias,
        aliasGen,
        dialect,
        params,
        visitPath: currentPath,
        depth: depth + 1,
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
): IncludeSpec[] {
  const aliasGen = createAliasGenerator()
  const stats: IncludeComplexityStats = {
    totalIncludes: 0,
    totalSubqueries: 0,
    maxDepth: 0,
  }

  const schemaByName = new Map<string, Model>()
  for (const m of schemas) schemaByName.set(m.name, m)

  return buildIncludeSqlInternal(
    args,
    model,
    schemas,
    schemaByName,
    parentAlias,
    aliasGen,
    params,
    dialect,
    [],
    0,
    stats,
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
  schemaByName: Map<string, Model>,
): { field: Field; relModel: Model } {
  const relationSet = getRelationFieldSet(model)
  if (!relationSet.has(relName)) {
    throw new Error(
      `_count.${relName} references unknown relation on model ${model.name}`,
    )
  }

  const field = model.fields.find((f) => f.name === relName)
  if (!field)
    throw new Error(
      `_count.${relName} references unknown relation on model ${model.name}`,
    )

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
    'Relation count for composite keys requires explicit references matching foreignKey length',
  )
}

function resolveCountKeyPairs(field: Field): {
  relKeyFields: string[]
  parentKeyFields: string[]
} {
  const fkFields = normalizeKeyList((field as any).foreignKey)
  if (fkFields.length === 0)
    throw new Error('Relation count requires foreignKey')

  const refsRaw = (field as any).references
  const refs = normalizeKeyList(refsRaw)
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

    const resolved = resolveCountRelationOrThrow(
      relName,
      model,
      schemas,
      schemaByName,
    )
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
