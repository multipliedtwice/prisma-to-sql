import { joinCondition, isValidRelationField } from '../joins'
import { buildOrderBy, readSkipTake, parseOrderByValue } from '../pagination'
import { buildWhereClause } from '../where'
import { jsonAgg, jsonBuildObject, SqlDialect } from '../../sql-builder-dialect'
import { buildRelationSelect } from './fields'
import { Model, PrismaQueryArgs, Field } from '../../types'
import { createAliasGenerator } from '../shared/alias-generator'
import { SQL_TEMPLATES, SQL_SEPARATORS, LIMITS } from '../shared/constants'
import { quote, sqlStringLiteral } from '../shared/sql-utils'
import { ParamStore } from '../shared/param-store'
import { IncludeSpec, AliasGenerator } from '../shared/types'
import { isValidWhereClause } from '../shared/validators/sql-validators'
import {
  isNonEmptyArray,
  isNotNullish,
  isPlainObject,
} from '../shared/validators/type-guards'
import { normalizeOrderByInput as normalizeOrderByShared } from '../shared/order-by-utils'
import { addAutoScoped } from '../shared/dynamic-params'
import {
  getFieldIndices,
  getRelationFieldSet,
  getScalarFieldSet,
} from '../shared/model-field-cache'
import { ensureDeterministicOrderByInput } from '../shared/order-by-determinism'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import { extractRelationEntries } from '../shared/relation-extraction-utils'
import {
  buildIncludeScope,
  getRelationTableReference,
  emptyJsonArray,
  canUseJoinInclude,
  hasNestedRelationInArgs,
  buildJoinBasedNonPaginated,
  buildJoinBasedPaginated,
} from './include-join'
import { extractWhereInput } from '../shared/relation-query-context'
import { isListRelation } from '../shared/field-type-utils'
import { maybeReverseNegativeTake } from '../shared/negative-take-utils'

const ROW_SUBQUERY_ALIAS_SUFFIX = '_row'

interface IncludeComplexityStats {
  totalIncludes: number
  totalSubqueries: number
  maxDepth: number
}

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
  visitSet: Set<string>
  depth: number
  stats: IncludeComplexityStats
  outerHasLimit?: boolean
}

interface NestedToOneRelation {
  name: string
  field: Field
  model: Model
  args: unknown
}

function resolveRelationOrThrow(
  model: Model,
  schemaByName: Map<string, Model>,
  relName: string,
): { field: Field; relModel: Model } {
  const indices = getFieldIndices(model)
  const field = indices.allFieldsByName.get(relName)

  if (!isNotNullish(field)) {
    throw new Error(
      `Unknown relation '${relName}' on model ${model.name}. ` +
        `Available relation fields: ${indices.relationNames.join(', ')}`,
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

function validateOrderByForModel(model: Model, orderBy: unknown): void {
  if (!isNotNullish(orderBy)) return

  const scalarSet = getScalarFieldSet(model)
  const normalized = normalizeOrderByShared(orderBy, parseOrderByValue)

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

function extractNestedToOneRelations(
  relArgs: unknown,
  relModel: Model,
  schemaByName: Map<string, Model>,
): NestedToOneRelation[] {
  if (!isPlainObject(relArgs)) return []

  const entries = extractRelationEntries(relArgs as any, relModel)
  const toOneRelations: NestedToOneRelation[] = []

  for (const entry of entries) {
    const indices = getFieldIndices(relModel)
    const field = indices.allFieldsByName.get(entry.name)
    if (!field || !isValidRelationField(field as Field)) continue

    const isList = isListRelation(field as Field)
    if (isList) continue

    const nestedModel = schemaByName.get(field.relatedModel!)
    if (!nestedModel) continue

    toOneRelations.push({
      name: entry.name,
      field: field as Field,
      model: nestedModel,
      args: entry.value,
    })
  }

  return toOneRelations
}

function buildNestedToOneJoins(
  relations: NestedToOneRelation[],
  baseAlias: string,
  baseModel: Model,
  aliasGen: AliasGenerator,
  dialect: SqlDialect,
): { joins: string[]; aliasMap: Map<string, string> } {
  const joins: string[] = []
  const aliasMap = new Map<string, string>()

  for (const rel of relations) {
    const relTable = getRelationTableReference(rel.model, dialect)
    const relAlias = aliasGen.next(`${rel.name}_joined`)
    const joinCond = joinCondition(
      rel.field,
      baseModel,
      rel.model,
      baseAlias,
      relAlias,
    )

    joins.push(`LEFT JOIN ${relTable} ${relAlias} ON ${joinCond}`)
    aliasMap.set(rel.name, relAlias)
  }

  return { joins, aliasMap }
}

function buildNestedToOneSelects(
  relations: NestedToOneRelation[],
  aliasMap: Map<string, string>,
): string[] {
  const selects: string[] = []

  for (const rel of relations) {
    const relAlias = aliasMap.get(rel.name)
    if (!relAlias) continue

    const relSelect = buildRelationSelect(rel.args, rel.model, relAlias)
    if (!relSelect || relSelect.trim().length === 0) continue

    selects.push(`${sqlStringLiteral(rel.name)}, ${relSelect}`)
  }

  return selects
}

function buildSelectWithNestedIncludes(
  relArgs: unknown,
  relModel: Model,
  relAlias: string,
  ctx: IncludeBuildContext,
): { select: string; nestedJoins: string[] } {
  const nestedToOnes = extractNestedToOneRelations(
    relArgs,
    relModel,
    ctx.schemaByName,
  )

  if (nestedToOnes.length === 0) {
    let relSelect = buildRelationSelect(relArgs, relModel, relAlias)

    let nestedIncludes: IncludeSpec[] = []
    if (isPlainObject(relArgs)) {
      const prevModel = ctx.model
      const prevParentAlias = ctx.parentAlias
      const prevDepth = ctx.depth

      ctx.model = relModel
      ctx.parentAlias = relAlias
      ctx.depth = prevDepth + 1

      try {
        nestedIncludes = buildIncludeSqlInternal(
          relArgs as PrismaQueryArgs,
          ctx,
        )
      } finally {
        ctx.model = prevModel
        ctx.parentAlias = prevParentAlias
        ctx.depth = prevDepth
      }
    }

    if (isNonEmptyArray(nestedIncludes)) {
      const emptyJson = emptyJsonArray(ctx.dialect)
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

    return { select: relSelect, nestedJoins: [] }
  }

  const { joins, aliasMap } = buildNestedToOneJoins(
    nestedToOnes,
    relAlias,
    relModel,
    ctx.aliasGen,
    ctx.dialect,
  )

  const baseSelect = buildRelationSelect(relArgs, relModel, relAlias)
  const nestedSelects = buildNestedToOneSelects(nestedToOnes, aliasMap)

  const allParts: string[] = []
  if (baseSelect && baseSelect.trim().length > 0) {
    allParts.push(baseSelect)
  }
  for (const ns of nestedSelects) {
    allParts.push(ns)
  }

  return {
    select: allParts.join(SQL_SEPARATORS.FIELD_LIST),
    nestedJoins: joins,
  }
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
  nestedJoins: string[]
}): string {
  const allJoins = [args.joins, ...args.nestedJoins].filter((j) => j).join(' ')
  const joinsStr = allJoins ? ` ${allJoins}` : ''
  const where = `${SQL_TEMPLATES.WHERE} ${args.joinPredicate}${args.whereClause}`
  return (
    `${SQL_TEMPLATES.SELECT} ${args.selectExpr} ` +
    `${SQL_TEMPLATES.FROM} ${args.relTable} ${args.relAlias}${joinsStr} ` +
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
  nestedJoins: string[]
}): string {
  const objExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)

  let sql = buildBaseSql({
    selectExpr: objExpr,
    relTable: args.relTable,
    relAlias: args.relAlias,
    joins: args.joins,
    joinPredicate: args.joinPredicate,
    whereClause: args.whereClause,
    nestedJoins: args.nestedJoins,
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
  nestedJoins: string[]
}): IncludeSpec {
  const rowExpr = jsonBuildObject(args.relSelect, args.ctx.dialect)
  const noTake = !isNotNullish(args.takeVal)
  const noSkip = !isNotNullish(args.skipVal)

  const emptyJson = emptyJsonArray(args.ctx.dialect)

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
      nestedJoins: args.nestedJoins,
    })

    return Object.freeze({ name: args.relName, sql, isOneToOne: false })
  }

  const rowAlias = args.ctx.aliasGen.next(
    `${args.relName}${ROW_SUBQUERY_ALIAS_SUFFIX}`,
  )

  let base = buildBaseSql({
    selectExpr: `${rowExpr} ${SQL_TEMPLATES.AS} row`,
    relTable: args.relTable,
    relAlias: args.relAlias,
    joins: args.joins,
    joinPredicate: args.joinPredicate,
    whereClause: args.whereClause,
    nestedJoins: args.nestedJoins,
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

function buildSingleInclude(
  relName: string,
  relArgs: unknown,
  field: Field,
  relModel: Model,
  ctx: IncludeBuildContext,
): IncludeSpec {
  const relTable = getRelationTableReference(relModel, ctx.dialect)
  const relAlias = ctx.aliasGen.next(relName)

  const isList = isListRelation(field)
  const joinPredicate = joinCondition(
    field,
    ctx.model,
    relModel,
    ctx.parentAlias,
    relAlias,
  )

  const whereInput = extractWhereInput(relArgs)
  const { select: relSelect, nestedJoins } = buildSelectWithNestedIncludes(
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
      nestedJoins,
    })
    return Object.freeze({ name: relName, sql, isOneToOne: true })
  }

  const depth = ctx.depth
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
        nestedJoins,
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
      nestedJoins,
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
    nestedJoins,
  })
}

function buildIncludeSqlInternal(
  args: IncludeSelectArgs,
  ctx: IncludeBuildContext,
): IncludeSpec[] {
  const depth = ctx.depth

  if (depth > LIMITS.MAX_INCLUDE_DEPTH) {
    throw new Error(
      `Maximum include depth of ${LIMITS.MAX_INCLUDE_DEPTH} exceeded. ` +
        `Path: ${Array.from(ctx.visitSet).join(' -> ')}. ` +
        `Deep includes cause exponential SQL complexity and performance issues.`,
    )
  }

  const modelPath = ctx.includePath
    .map((p) => {
      const parts = p.split('.')
      return parts.length > 0 ? parts[0] : ''
    })
    .filter((p) => p.length > 0)

  const currentModelCount = modelPath.filter((m) => m === ctx.model.name).length
  if (currentModelCount > LIMITS.MAX_SELF_REFERENTIAL_DEPTH) {
    throw new Error(
      `Circular relation detected: Model '${ctx.model.name}' appears ${currentModelCount} times ` +
        `in include path: ${ctx.includePath.join(' -> ')}. ` +
        `Self-referential relations must be limited to ${LIMITS.MAX_SELF_REFERENTIAL_DEPTH} levels deep.`,
    )
  }

  ctx.stats.maxDepth = Math.max(ctx.stats.maxDepth, depth)

  const includes: IncludeSpec[] = []
  const entries = extractRelationEntries(args, ctx.model)

  if (entries.length > LIMITS.MAX_INCLUDES_PER_LEVEL) {
    throw new Error(
      `Too many includes at depth ${depth} (${entries.length} > ${LIMITS.MAX_INCLUDES_PER_LEVEL}). ` +
        `Path: ${Array.from(ctx.visitSet).join(' -> ')}`,
    )
  }

  for (const entry of entries) {
    const relName = entry.name
    const relArgs = entry.value

    if (relArgs === false) continue

    ctx.stats.totalIncludes++
    if (ctx.stats.totalIncludes > LIMITS.MAX_TOTAL_SUBQUERIES) {
      throw new Error(
        `Query complexity limit exceeded: ${ctx.stats.totalIncludes} includes generated. ` +
          `Maximum allowed: ${LIMITS.MAX_TOTAL_SUBQUERIES}. ` +
          `This indicates exponential include nesting. ` +
          `Stats: depth=${ctx.stats.maxDepth}, includes=${ctx.stats.totalIncludes}. ` +
          `Path: ${Array.from(ctx.visitSet).join(' -> ')}. ` +
          `Simplify your include structure or split into multiple queries.`,
      )
    }

    ctx.stats.totalSubqueries++

    const resolved = resolveRelationOrThrow(
      ctx.model,
      ctx.schemaByName,
      relName,
    )

    const relationPath = `${ctx.model.name}.${relName}`

    if (ctx.visitSet.has(relationPath)) {
      throw new Error(
        `Circular include detected: ${Array.from(ctx.visitSet).join(' -> ')} -> ${relationPath}. ` +
          `Relation '${relationPath}' creates an infinite loop.`,
      )
    }

    ctx.includePath.push(relName)
    ctx.visitSet.add(relationPath)

    try {
      includes.push(
        buildSingleInclude(
          relName,
          relArgs,
          resolved.field,
          resolved.relModel,
          ctx,
        ),
      )
    } finally {
      ctx.includePath.pop()
      ctx.visitSet.delete(relationPath)
    }
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
    visitSet: new Set<string>(),
    depth: 0,
    stats,
    outerHasLimit,
  })
}
