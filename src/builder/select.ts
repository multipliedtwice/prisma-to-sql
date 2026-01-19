import { SqlDialect, getGlobalDialect } from '../sql-builder-dialect'
import { PrismaQueryArgs, Model } from '../types'
import {
  buildOrderByClause,
  getPaginationParams,
  buildCursorCondition,
  parseOrderByValue,
} from './pagination'
import { constructFinalSql } from './select/assembly'
import { buildSelectFields } from './select/fields'
import { buildIncludeSql } from './select/includes'
import {
  reverseOrderByInput,
  normalizeOrderByInput as normalizeOrderByShared,
} from './shared/order-by-utils'
import { createParamStoreFrom } from './shared/param-store'
import { assertSafeAlias, assertSafeTableRef } from './shared/sql-utils'
import { WhereClauseResult, SqlResult, SelectQuerySpec } from './shared/types'
import {
  isNotNullish,
  isNonEmptyArray,
  isPlainObject,
} from './shared/validators/type-guards'

type OrderByValue =
  | 'asc'
  | 'desc'
  | { sort: 'asc' | 'desc'; nulls?: 'first' | 'last' }

type OrderByItem = Record<string, OrderByValue>

function normalizeOrderByInput(
  orderBy: PrismaQueryArgs['orderBy'],
): OrderByItem[] {
  return normalizeOrderByShared(orderBy, parseOrderByValue)
}

function normalizeDistinctFields(
  distinct: PrismaQueryArgs['distinct'],
): string[] {
  if (!isNonEmptyArray(distinct)) return []
  return distinct
    .filter((f) => typeof f === 'string')
    .map((f) => f.trim())
    .filter((f) => f.length > 0)
}

function mapFirstOrderByByField(
  existing: OrderByItem[],
): Map<string, OrderByItem> {
  const m = new Map<string, OrderByItem>()
  for (const obj of existing) {
    const field = Object.keys(obj)[0]
    if (field && !m.has(field)) m.set(field, obj)
  }
  return m
}

function buildPostgresDistinctOrderBy(
  distinctFields: string[],
  existing: OrderByItem[],
): OrderByItem[] {
  const firstByField = mapFirstOrderByByField(existing)

  const next: OrderByItem[] = []
  for (const f of distinctFields) {
    next.push(firstByField.get(f) ?? { [f]: 'asc' })
  }

  const distinctSet = new Set(distinctFields)
  for (const obj of existing) {
    const field = Object.keys(obj)[0]
    if (!distinctSet.has(field)) next.push(obj)
  }

  return next
}

function applyPostgresDistinctOrderBy(
  args: PrismaQueryArgs,
  _model: Model,
): PrismaQueryArgs {
  const distinctFields = normalizeDistinctFields(args.distinct)
  if (distinctFields.length === 0) return args
  if (!isNotNullish(args.orderBy)) return args

  const existing = normalizeOrderByInput(args.orderBy)
  if (existing.length === 0) return args

  return {
    ...args,
    orderBy: buildPostgresDistinctOrderBy(distinctFields, existing),
  }
}

function assertScalarFieldOnModel(
  model: Model,
  fieldName: string,
  ctx: string,
): void {
  const f = model.fields.find((x) => x.name === fieldName)
  if (!f) {
    throw new Error(
      `${ctx} references unknown field '${fieldName}' on model ${model.name}`,
    )
  }
  if (f.isRelation) {
    throw new Error(
      `${ctx} does not support relation field '${fieldName}' on model ${model.name}`,
    )
  }
}

function validateDistinct(
  model: Model,
  distinct: PrismaQueryArgs['distinct'],
): void {
  if (!isNotNullish(distinct) || !isNonEmptyArray(distinct)) return
  const seen = new Set<string>()
  for (const raw of distinct) {
    const f = String(raw).trim()
    if (f.length === 0) continue
    if (seen.has(f)) {
      throw new Error(`distinct must not contain duplicates (field: '${f}')`)
    }
    seen.add(f)
    assertScalarFieldOnModel(model, f, 'distinct')
  }
}

function validateOrderByValue(fieldName: string, v: unknown): void {
  parseOrderByValue(v, fieldName)
}

function validateOrderBy(
  model: Model,
  orderBy: PrismaQueryArgs['orderBy'],
): void {
  if (!isNotNullish(orderBy)) return

  const items = normalizeOrderByInput(orderBy)
  if (items.length === 0) return

  for (const it of items) {
    const entries = Object.entries(it)
    const fieldName = String(entries[0][0]).trim()
    if (fieldName.length === 0) {
      throw new Error('orderBy field name cannot be empty')
    }
    assertScalarFieldOnModel(model, fieldName, 'orderBy')
    validateOrderByValue(fieldName, entries[0][1])
  }
}

function validateCursor(model: Model, cursor: unknown): void {
  if (!isNotNullish(cursor)) return
  if (!isPlainObject(cursor)) {
    throw new Error('cursor must be an object')
  }
  const entries = Object.entries(cursor)
  if (entries.length === 0) {
    throw new Error('cursor must have at least one field')
  }
  for (const [fieldName] of entries) {
    const f = String(fieldName).trim()
    if (f.length === 0) {
      throw new Error('cursor field name cannot be empty')
    }
    assertScalarFieldOnModel(model, f, 'cursor')
  }
}

function resolveDialect(dialect?: SqlDialect): SqlDialect {
  return dialect ?? getGlobalDialect()
}

function normalizeArgsForNegativeTake(
  method: string,
  args: PrismaQueryArgs,
): PrismaQueryArgs {
  if (method !== 'findMany') return args
  if (typeof args.take !== 'number') return args
  if (!Number.isInteger(args.take)) return args
  if (args.take >= 0) return args

  if (!isNotNullish(args.orderBy)) {
    throw new Error('Negative take requires orderBy for deterministic results')
  }

  return {
    ...args,
    take: Math.abs(args.take),
    orderBy: reverseOrderByInput(args.orderBy),
  }
}

function normalizeArgsForDialect(
  dialect: SqlDialect,
  args: PrismaQueryArgs,
  model: Model,
): PrismaQueryArgs {
  if (dialect !== 'postgres') return args
  return applyPostgresDistinctOrderBy(args, model)
}

function buildCursorClauseIfAny(input: {
  cursor: unknown
  orderBy: PrismaQueryArgs['orderBy']
  tableName: string
  alias: string
  params: ReturnType<typeof createParamStoreFrom>
  dialect: SqlDialect
}): string | undefined {
  const { cursor, orderBy, tableName, alias, params, dialect } = input
  if (!isNotNullish(cursor)) return undefined
  return buildCursorCondition(
    cursor,
    orderBy,
    tableName,
    alias,
    params,
    dialect,
  )
}

function buildSelectSpec(input: {
  method: string
  normalizedArgs: PrismaQueryArgs
  model: Model
  schemas: Model[]
  tableName: string
  alias: string
  whereResult: WhereClauseResult
  dialect: SqlDialect
}): SelectQuerySpec {
  const {
    method,
    normalizedArgs,
    model,
    schemas,
    tableName,
    alias,
    whereResult,
    dialect,
  } = input

  const selectFields = buildSelectFields(
    { select: normalizedArgs.select },
    model,
    alias,
  )

  const orderByClause = buildOrderByClause(normalizedArgs, alias, dialect)
  const { take, skip, cursor } = getPaginationParams(method, normalizedArgs)

  const params = createParamStoreFrom(
    whereResult.params,
    whereResult.paramMappings,
    whereResult.nextParamIndex,
  )

  const includes = buildIncludeSql(
    normalizedArgs,
    model,
    schemas,
    alias,
    params,
    dialect,
  )

  const cursorClause = buildCursorClauseIfAny({
    cursor,
    orderBy: normalizedArgs.orderBy,
    tableName,
    alias,
    params,
    dialect,
  })

  return {
    select: selectFields,
    includes,
    from: { table: tableName, alias },
    whereClause: whereResult.clause,
    whereJoins: whereResult.joins,
    orderBy: orderByClause,
    pagination: { take, skip },
    distinct: normalizedArgs.distinct,
    method,
    cursorClause,
    params,
    dialect,
    model,
    schemas,
    args: normalizedArgs,
  }
}

type BuildSelectSqlInput = {
  method: string
  args: PrismaQueryArgs
  model: Model
  schemas: Model[]
  from: { tableName: string; alias: string }
  whereResult: WhereClauseResult
  dialect?: SqlDialect
}

export function buildSelectSql(input: BuildSelectSqlInput): SqlResult {
  const { method, args, model, schemas, from, whereResult, dialect } = input

  assertSafeAlias(from.alias)
  assertSafeTableRef(from.tableName)

  const dialectToUse = resolveDialect(dialect)

  const argsForSql = normalizeArgsForNegativeTake(method, args)
  const normalizedArgs = normalizeArgsForDialect(
    dialectToUse,
    argsForSql,
    model,
  )

  validateDistinct(model, normalizedArgs.distinct)
  validateOrderBy(model, normalizedArgs.orderBy)
  validateCursor(model, normalizedArgs.cursor)

  const spec = buildSelectSpec({
    method,
    normalizedArgs,
    model,
    schemas,
    tableName: from.tableName,
    alias: from.alias,
    whereResult,
    dialect: dialectToUse,
  })

  return constructFinalSql(spec)
}
