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
import { jsonBuildObject } from '../../sql-builder-dialect'
import { buildRelationCountSql } from './include-count'
import { COUNT_SELECT_KEY } from './distinct'
import { getModelStats } from './strategy-estimator'

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

const SMALL_TABLE_THRESHOLD = 1000
const HEURISTIC_DISTINCT_COLUMN_MIN = 3
const BRANCH_WALK_MAX_DEPTH = 10

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

function selectIsRootScalarOrCount(select: unknown, model: Model): boolean {
  if (!isNotNullish(select)) return true
  if (!isPlainObject(select)) return false

  const scalarSet = getScalarFieldSet(model)
  const relationSet = getRelationFieldSet(model)

  for (const [key, value] of Object.entries(select)) {
    if (key === COUNT_SELECT_KEY) {
      if (value === false || value === true) continue
      if (!isPlainObject(value)) return false
      continue
    }
    if (relationSet.has(key)) return false
    if (!scalarSet.has(key)) return false
    if (value !== true && value !== false && value !== undefined) {
      return false
    }
  }
  return true
}

function includeIsCountOnly(include: unknown): boolean {
  if (!isNotNullish(include)) return true
  if (!isPlainObject(include)) return false
  for (const [k, v] of Object.entries(include)) {
    if (k !== COUNT_SELECT_KEY) return false
    if (v === false) continue
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

function isLogicalKey(k: string): boolean {
  return k === 'AND' || k === 'OR' || k === 'NOT'
}

function collectBranchColumns(
  node: Record<string, unknown>,
  model: Model,
  out: Set<string>,
  hasRelationRef: { value: boolean },
  depth: number,
): void {
  if (depth > BRANCH_WALK_MAX_DEPTH) return
  const scalarSet = getScalarFieldSet(model)
  const relationSet = getRelationFieldSet(model)
  for (const [key, value] of Object.entries(node)) {
    if (value === undefined) continue
    if (isLogicalKey(key)) {
      const items = Array.isArray(value) ? value : [value]
      for (const item of items) {
        if (isPlainObject(item)) {
          collectBranchColumns(item, model, out, hasRelationRef, depth + 1)
        }
      }
      continue
    }
    if (relationSet.has(key)) {
      hasRelationRef.value = true
      continue
    }
    if (scalarSet.has(key)) {
      out.add(key)
    }
  }
}

function analyzeBranches(
  branches: Record<string, unknown>[],
  model: Model,
): { hasRelationBranch: boolean; distinctColumnCount: number } {
  const columns = new Set<string>()
  const hasRelation = { value: false }
  for (const branch of branches) {
    collectBranchColumns(branch, model, columns, hasRelation, 0)
  }
  return {
    hasRelationBranch: hasRelation.value,
    distinctColumnCount: columns.size,
  }
}

function isBelowSmallTableThreshold(modelName: string): boolean {
  const stats = getModelStats()
  if (!stats) return false
  const m = stats[modelName]
  if (!m) return false
  return m.rowCount < SMALL_TABLE_THRESHOLD
}

function heuristicAccepts(
  branches: Record<string, unknown>[],
  model: Model,
): boolean {
  if (isBelowSmallTableThreshold(model.name)) return false
  const { hasRelationBranch, distinctColumnCount } = analyzeBranches(
    branches,
    model,
  )
  return (
    hasRelationBranch || distinctColumnCount >= HEURISTIC_DISTINCT_COLUMN_MIN
  )
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

  if (!includeIsCountOnly(args.include)) return false
  if (isNotNullish(args.cursor)) return false
  if (isNotNullish(args.distinct) && isNonEmptyArray(args.distinct)) {
    return false
  }

  if (!selectIsRootScalarOrCount(args.select, model)) return false
  if (!orderByReferencesOnlyRootScalars(args.orderBy, model)) return false

  const extracted = extractOrBranches(args.where)
  if (!extracted) return false

  for (const branch of extracted.branches) {
    if (isUnsupportedOrRewriteTarget(branch)) return false
  }

  return heuristicAccepts(extracted.branches, model)
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

function resolveCountSelectFromArgs(args: PrismaQueryArgs): unknown {
  if (isPlainObject(args.select)) {
    const v = (args.select as Record<string, unknown>)[COUNT_SELECT_KEY]
    if (v !== undefined) return v
  }
  if (isPlainObject(args.include)) {
    return (args.include as Record<string, unknown>)[COUNT_SELECT_KEY]
  }
  return undefined
}

function resolveCountSelectShape(
  raw: unknown,
  model: Model,
): Record<string, boolean> | null {
  if (raw === true) {
    const relationSet = getRelationFieldSet(model)
    if (relationSet.size === 0) return null
    const all: Record<string, boolean> = {}
    for (const name of relationSet) all[name] = true
    return all
  }
  if (isPlainObject(raw) && 'select' in raw) {
    return (raw as { select: Record<string, boolean> }).select
  }
  return null
}

interface CountColumnBuild {
  selectExpr: string
  joins: string[]
}

function buildCountColumn(args: {
  normalizedArgs: PrismaQueryArgs
  model: Model
  schemas: Model[]
  alias: string
  params: ParamStore
  dialect: SqlDialect
}): CountColumnBuild {
  const raw = resolveCountSelectFromArgs(args.normalizedArgs)
  if (!raw) return { selectExpr: '', joins: [] }

  const resolved = resolveCountSelectShape(raw, args.model)
  if (!resolved || Object.keys(resolved).length === 0) {
    return { selectExpr: '', joins: [] }
  }

  const build = buildRelationCountSql(
    resolved,
    args.model,
    args.schemas,
    args.alias,
    args.params,
    args.dialect,
  )

  if (!build.jsonPairs) return { selectExpr: '', joins: [] }

  const selectExpr =
    jsonBuildObject(build.jsonPairs, args.dialect) +
    ' ' +
    SQL_TEMPLATES.AS +
    ' ' +
    quote(COUNT_SELECT_KEY)

  return { selectExpr, joins: build.joins }
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
  const baseSelect = buildSelectFields(
    { select: args.normalizedArgs.select },
    args.model,
    args.alias,
  )

  const countCol = buildCountColumn({
    normalizedArgs: args.normalizedArgs,
    model: args.model,
    schemas: args.schemas,
    alias: args.alias,
    params: args.params,
    dialect: args.dialect,
  })

  const selectParts: string[] = []
  if (baseSelect) selectParts.push(baseSelect)
  if (countCol.selectExpr) selectParts.push(countCol.selectExpr)
  if (selectParts.length === 0) {
    throw new Error(
      'union-of-ids rewrite produced empty SELECT list; check eligibility',
    )
  }
  const selectFields = selectParts.join(', ')

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

  if (isNonEmptyArray(countCol.joins)) {
    parts.push(countCol.joins.join(' '))
  }

  if (orderByResult.sql) {
    parts.push(SQL_TEMPLATES.ORDER_BY, orderByResult.sql)
  }

  const { take, skip } = getPaginationParams('findMany', args.normalizedArgs)

  if (isNotNullish(take)) {
    const placeholder = addAutoScoped(args.params, take, 'root.pagination.take')
    parts.push(SQL_TEMPLATES.LIMIT, placeholder)
  }

  if (isNotNullish(skip)) {
    const placeholder = addAutoScoped(args.params, skip, 'root.pagination.skip')
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
    branchSqls.push(
      buildBranchSql({
        branchWhere: branch,
        siblings: extracted.siblings,
        model,
        schemas,
        tableName,
        alias,
        dialect,
        params,
        pkField,
      }),
    )
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
    orRewriteApplied: 'union-of-ids',
  }
}
