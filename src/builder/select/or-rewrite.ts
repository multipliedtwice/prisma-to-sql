import { Model, PrismaQueryArgs } from '../../types'
import { SqlDialect } from '../../sql-builder-dialect'
import { SQL_TEMPLATES } from '../shared/constants'
import {
  assertSafeAlias,
  assertSafeTableRef,
  col,
  quote,
  quoteColumn,
} from '../shared/sql-utils'
import { createParamStore, ParamStore } from '../shared/param-store'
import { createAliasGenerator } from '../shared/alias-generator'
import { buildWhereClause } from '../where'
import { buildSelectFields } from './fields'
import { buildOrderByWithRelations } from '../shared/order-by-relation'
import { getPaginationParams } from '../pagination'
import { getPrimaryKeyFields } from '../shared/primary-key-utils'
import {
  getScalarFieldSet,
  getRelationFieldSet,
} from '../shared/model-field-cache'
import {
  isValidWhereClause,
  validateSelectQuery,
  validateParamConsistencyByDialect,
} from '../shared/validators/sql-validators'
import {
  isNotNullish,
  isNonEmptyArray,
  isPlainObject,
} from '../shared/validators/type-guards'
import { addAutoScoped } from '../shared/dynamic-params'
import { expandOrderByInput } from '../shared/order-by-utils'
import { SqlResult } from '../shared/types'

interface UnionOfIdsInput {
  method: string
  normalizedArgs: PrismaQueryArgs
  model: Model
  schemas: Model[]
  tableName: string
  alias: string
  dialect: SqlDialect
}

const PK_OUTPUT_ALIAS = '__tp_or_pk'
const PK_OUTPUT_ALIAS_QUOTED = quote(PK_OUTPUT_ALIAS)
const UNION_INNER_ALIAS = '__tp_or_union'
const IDS_OUTER_ALIAS = '__tp_or_ids'

function isUnsupportedOrRewriteTarget(value: unknown): boolean {
  if (!isPlainObject(value)) return false
  return Object.prototype.hasOwnProperty.call(value, 'NOT')
}

function orderByReferencesOnlyRootScalars(
  orderBy: unknown,
  model: Model,
): boolean {
  if (!isNotNullish(orderBy)) return true
  const expanded = expandOrderByInput(orderBy)
  if (expanded.length === 0) return true

  const scalarSet = getScalarFieldSet(model)
  for (const [field, value] of expanded) {
    if (!scalarSet.has(field)) return false
    if (typeof value === 'string') continue
    if (!isPlainObject(value)) return false
    if (!Object.prototype.hasOwnProperty.call(value, 'sort')) return false
  }
  return true
}

function selectIsRootScalarOnly(select: unknown, model: Model): boolean {
  if (!isNotNullish(select)) return true
  if (!isPlainObject(select)) return false

  const scalarSet = getScalarFieldSet(model)
  const relationSet = getRelationFieldSet(model)

  for (const [key, value] of Object.entries(select)) {
    if (key === '_count') return false
    if (relationSet.has(key)) return false
    if (!scalarSet.has(key)) return false
    if (value !== true && value !== false && value !== undefined) {
      return false
    }
  }
  return true
}

function extractOrBranches(where: unknown): {
  branches: Record<string, unknown>[]
  siblings: Record<string, unknown>
} | null {
  if (!isPlainObject(where)) return null
  const orValue = (where as Record<string, unknown>).OR
  if (!Array.isArray(orValue) || orValue.length < 2) return null

  const branches: Record<string, unknown>[] = []
  for (const item of orValue) {
    if (!isPlainObject(item)) return null
    branches.push(item as Record<string, unknown>)
  }

  const siblings: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(where)) {
    if (key === 'OR') continue
    if (key === 'AND' || key === 'NOT') return null
    siblings[key] = value
  }

  return { branches, siblings }
}

export function canUseUnionOfIdsRewrite(input: {
  method: string
  args: PrismaQueryArgs
  model: Model
  schemas: Model[]
  dialect: SqlDialect
}): boolean {
  const { method, args, model, dialect } = input

  if (method !== 'findMany') return false
  if (dialect !== 'postgres') return false

  const pkFields = getPrimaryKeyFields(model)
  if (pkFields.length !== 1) return false

  if (isNotNullish(args.include)) return false
  if (isNotNullish(args.cursor)) return false
  if (isNotNullish(args.distinct) && isNonEmptyArray(args.distinct))
    return false

  if (!selectIsRootScalarOnly(args.select, model)) return false
  if (!orderByReferencesOnlyRootScalars(args.orderBy, model)) return false

  const extracted = extractOrBranches(args.where)
  if (!extracted) return false

  for (const branch of extracted.branches) {
    if (isUnsupportedOrRewriteTarget(branch)) return false
  }

  return true
}

function buildBranchSql(args: {
  branchWhere: Record<string, unknown>
  siblings: Record<string, unknown>
  model: Model
  schemas: Model[]
  tableName: string
  alias: string
  dialect: SqlDialect
  params: ParamStore
  pkField: string
}): string {
  const combinedWhere: Record<string, unknown> =
    Object.keys(args.siblings).length > 0
      ? { AND: [args.siblings, args.branchWhere] }
      : args.branchWhere

  const aliasGen = createAliasGenerator()
  const whereResult = buildWhereClause(combinedWhere, {
    alias: args.alias,
    model: args.model,
    schemaModels: args.schemas,
    params: args.params,
    isSubquery: false,
    aliasGen,
    dialect: args.dialect,
  })

  const pkExpr = col(args.alias, args.pkField, args.model)
  const parts: string[] = [
    SQL_TEMPLATES.SELECT,
    `${pkExpr} ${SQL_TEMPLATES.AS} ${PK_OUTPUT_ALIAS_QUOTED}`,
    SQL_TEMPLATES.FROM,
    args.tableName,
    args.alias,
  ]

  if (isNonEmptyArray(whereResult.joins)) {
    parts.push(whereResult.joins.join(' '))
  }

  if (isValidWhereClause(whereResult.clause)) {
    parts.push(SQL_TEMPLATES.WHERE, whereResult.clause)
  }

  return parts.join(' ')
}

function buildOuterSelect(args: {
  model: Model
  normalizedArgs: PrismaQueryArgs
  tableName: string
  alias: string
  pkField: string
  innerSql: string
  dialect: SqlDialect
  params: ParamStore
  schemas: Model[]
}): string {
  const selectFields = buildSelectFields(
    { select: args.normalizedArgs.select },
    args.model,
    args.alias,
  )

  const orderByResult = buildOrderByWithRelations(
    args.normalizedArgs.orderBy,
    args.alias,
    args.dialect,
    args.model,
    args.schemas,
  )

  if (isNonEmptyArray(orderByResult.joins)) {
    throw new Error(
      'union-of-ids rewrite produced relation joins for orderBy; eligibility check is broken',
    )
  }

  const pkColName = quoteColumn(args.model, args.pkField)

  const dedupedSubquery =
    `(SELECT ${PK_OUTPUT_ALIAS_QUOTED} ` +
    `FROM (${args.innerSql}) ${UNION_INNER_ALIAS} ` +
    `GROUP BY ${PK_OUTPUT_ALIAS_QUOTED})`

  const joinCondition = `${args.alias}.${pkColName} = ${IDS_OUTER_ALIAS}.${PK_OUTPUT_ALIAS_QUOTED}`

  const parts: string[] = [
    SQL_TEMPLATES.SELECT,
    selectFields,
    SQL_TEMPLATES.FROM,
    dedupedSubquery,
    IDS_OUTER_ALIAS,
    'JOIN',
    args.tableName,
    args.alias,
    'ON',
    joinCondition,
  ]

  if (orderByResult.sql) {
    parts.push(SQL_TEMPLATES.ORDER_BY, orderByResult.sql)
  }

  const { take, skip } = getPaginationParams('findMany', args.normalizedArgs)

  if (isNotNullish(take)) {
    const placeholder = addAutoScoped(args.params, take, 'or-rewrite.take')
    parts.push(SQL_TEMPLATES.LIMIT, placeholder)
  }

  if (isNotNullish(skip)) {
    const placeholder = addAutoScoped(args.params, skip, 'or-rewrite.skip')
    parts.push(SQL_TEMPLATES.OFFSET, placeholder)
  }

  return parts.join(' ')
}

export function tryBuildUnionOfIdsSelectSql(
  input: UnionOfIdsInput,
): SqlResult | null {
  const { method, normalizedArgs, model, schemas, tableName, alias, dialect } =
    input

  if (
    !canUseUnionOfIdsRewrite({
      method,
      args: normalizedArgs,
      model,
      schemas,
      dialect,
    })
  ) {
    return null
  }

  assertSafeAlias(alias)
  assertSafeTableRef(tableName)

  const extracted = extractOrBranches(normalizedArgs.where)
  if (!extracted) return null

  const pkFields = getPrimaryKeyFields(model)
  const pkField = pkFields[0]

  const params = createParamStore(1, dialect)

  const branchSqls: string[] = []
  for (const branch of extracted.branches) {
    const sql = buildBranchSql({
      branchWhere: branch,
      siblings: extracted.siblings,
      model,
      schemas,
      tableName,
      alias,
      dialect,
      params,
      pkField,
    })
    branchSqls.push(sql)
  }

  const innerSql = branchSqls.join(' UNION ALL ')

  const finalSql = buildOuterSelect({
    model,
    normalizedArgs,
    tableName,
    alias,
    pkField,
    innerSql,
    dialect,
    params,
    schemas,
  })

  const snapshot = params.snapshot()

  validateSelectQuery(finalSql)
  validateParamConsistencyByDialect(finalSql, snapshot.params, dialect)

  return {
    sql: finalSql,
    params: [...snapshot.params],
    paramMappings: [...snapshot.mappings],
  }
}
