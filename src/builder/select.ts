import { SqlDialect, getGlobalDialect } from '../sql-builder-dialect'
import { PrismaQueryArgs, Model } from '../types'
import {
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
  expandOrderByInput,
} from './shared/order-by-utils'
import { buildOrderByWithRelations } from './shared/order-by-relation'
import { createParamStoreFrom } from './shared/param-store'
import { assertSafeAlias, assertSafeTableRef, quote } from './shared/sql-utils'
import { WhereClauseResult, SqlResult, SelectQuerySpec } from './shared/types'
import {
  isNotNullish,
  isNonEmptyArray,
  isPlainObject,
} from './shared/validators/type-guards'
import { assertScalarField } from './shared/validators/field-assertions'
import {
  getScalarFieldSet,
  getRelationFieldSet,
} from './shared/model-field-cache'
import { tryBuildUnionOfIdsSelectSql } from './select/or-rewrite'

export type OrRewriteMode = 'default' | 'union-of-ids'

export interface SqlBuildOptions {
  orRewrite?: OrRewriteMode
}

type OrderByValue =
  | 'asc'
  | 'desc'
  | { direction: 'asc' | 'desc'; nulls?: 'first' | 'last' }

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

function applyPostgresDistinctOrderBy(args: PrismaQueryArgs): PrismaQueryArgs {
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

function validateDistinct(
  model: Model,
  distinct: PrismaQueryArgs['distinct'],
): void {
  if (!isNotNullish(distinct) || !isNonEmptyArray(distinct)) return

  const seen = new Set<string>()
  const scalarSet = getScalarFieldSet(model)

  for (const raw of distinct) {
    if (typeof raw !== 'string') {
      throw new Error(
        `distinct values must be strings. Got ${typeof raw}: ${JSON.stringify(raw)}`,
      )
    }

    const f = String(raw).trim()

    if (f.length === 0) {
      throw new Error('distinct field name cannot be empty')
    }

    if (f.length > 255) {
      throw new Error(
        `distinct field name too long (${f.length} chars, max 255): ${f.slice(0, 50)}...`,
      )
    }

    if (seen.has(f)) {
      throw new Error(`distinct must not contain duplicates (field: '${f}')`)
    }

    seen.add(f)

    if (!scalarSet.has(f)) {
      const relationSet = getRelationFieldSet(model)
      if (relationSet.has(f)) {
        throw new Error(
          `distinct field '${f}' is a relation field. Only scalar fields are allowed.\n` +
            `Available scalar fields: ${[...scalarSet].join(', ')}`,
        )
      }
      throw new Error(
        `distinct field '${f}' does not exist on model ${model.name}.\n` +
          `Available fields: ${[...scalarSet].join(', ')}`,
      )
    }

    assertScalarField(model, f, 'distinct')
  }
}

function validateOrderBy(
  model: Model,
  orderBy: PrismaQueryArgs['orderBy'],
  schemas: Model[],
): void {
  if (!isNotNullish(orderBy)) return

  const expanded = expandOrderByInput(orderBy)
  if (expanded.length === 0) return

  const scalarSet = getScalarFieldSet(model)
  const relationSet = getRelationFieldSet(model)

  for (const [fieldName, value] of expanded) {
    const f = String(fieldName).trim()

    if (f.length === 0) {
      throw new Error('orderBy field name cannot be empty')
    }

    if (f.length > 255) {
      throw new Error(
        `orderBy field name too long (${f.length} chars, max 255): ${f.slice(0, 50)}...`,
      )
    }

    if (scalarSet.has(f)) {
      assertScalarField(model, f, 'orderBy')
      continue
    }

    if (relationSet.has(f)) {
      if (!isPlainObject(value)) {
        throw new Error(`Relation orderBy for '${f}' must be an object`)
      }
      continue
    }

    throw new Error(
      `orderBy field '${f}' does not exist on model ${model.name}.\n` +
        `Available fields: ${[...scalarSet].join(', ')}`,
    )
  }
}

function validateCursor(
  model: Model,
  cursor: unknown,
  distinct?: unknown,
): void {
  if (!isNotNullish(cursor)) return
  if (!isPlainObject(cursor)) {
    throw new Error('cursor must be an object')
  }
  const entries = Object.entries(cursor)

  const definedEntries = entries.filter(([_, value]) => value !== undefined)
  if (definedEntries.length === 0) {
    throw new Error('cursor must have at least one field with defined value')
  }

  for (const [fieldName] of definedEntries) {
    const f = String(fieldName).trim()
    if (f.length === 0) {
      throw new Error('cursor field name cannot be empty')
    }
    assertScalarField(model, f, 'cursor')
  }

  if (isNotNullish(distinct) && isNonEmptyArray(distinct)) {
    const cursorFields = new Set(definedEntries.map(([k]) => k))
    const distinctSet = new Set(distinct.map((d) => String(d)))

    for (const cursorField of cursorFields) {
      if (!distinctSet.has(cursorField)) {
        throw new Error(
          `Cursor field '${cursorField}' must be included in distinct fields.\n` +
            `Current distinct: [${[...distinctSet].join(', ')}]\n` +
            `Cursor fields: [${[...cursorFields].join(', ')}]`,
        )
      }
    }
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

const NEG_TAKE_REORDER_ALIAS = '__tp_negtake'

/**
 * Set of output field identifiers for a query, or `null` when no explicit
 * `select` is given (in which case every scalar field is returned and any
 * scalar orderBy field is therefore present in the output).
 */
function selectedOutputFields(args: PrismaQueryArgs): Set<string> | null {
  const sel = args.select
  if (!isPlainObject(sel)) return null
  const out = new Set<string>()
  for (const k of Object.keys(sel)) {
    if ((sel as Record<string, unknown>)[k]) out.add(k)
  }
  return out
}

function renderReorderOrderBy(entries: OrderByItem[], alias: string): string {
  const parts: string[] = []
  for (const item of entries) {
    const field = Object.keys(item)[0]
    const val = (item as Record<string, OrderByValue>)[field]
    const dir = typeof val === 'string' ? val : val.direction
    const nulls = typeof val === 'string' ? undefined : val.nulls
    let clause = `${alias}.${quote(field)} ${dir.toUpperCase()}`
    if (nulls) clause += ` NULLS ${nulls.toUpperCase()}`
    parts.push(clause)
  }
  return parts.join(', ')
}

/**
 * Prisma's `take: -N` returns the *last* N rows in the requested order.
 * `normalizeArgsForNegativeTake` reverses the orderBy and takes `abs(N)` so a
 * single LIMIT captures the correct *set*, but that leaves the rows in reversed
 * order. This plans an outer wrapper that restores the original order.
 *
 * Returns `null` when no re-reversal is needed. Throws for combinations that
 * cannot be re-reversed by output column (relation orderBy, or an orderBy field
 * excluded by an explicit `select`) — a clear error beats silently wrong order.
 */
function planNegativeTakeReorder(
  method: string,
  args: PrismaQueryArgs,
): { orderBySql: string } | null {
  if (method !== 'findMany') return null
  const take = args.take
  if (typeof take !== 'number' || !Number.isInteger(take) || take >= 0) {
    return null
  }
  if (!isNotNullish(args.orderBy)) return null

  const entries = normalizeOrderByInput(args.orderBy)
  if (entries.length === 0) return null

  const output = selectedOutputFields(args)

  for (const item of entries) {
    const field = Object.keys(item)[0]
    const val = (item as Record<string, unknown>)[field]
    const isScalarDirection =
      typeof val === 'string' || (isPlainObject(val) && 'direction' in val)
    if (!isScalarDirection) {
      throw new Error(
        `Negative take with orderBy on relation field '${field}' is not supported. ` +
          `Order by a scalar field, or use a positive take with a reversed orderBy.`,
      )
    }
    if (output && !output.has(field)) {
      throw new Error(
        `Negative take requires every orderBy field to be selected. Field '${field}' ` +
          `is ordered by but missing from 'select'. Add '${field}: true' to select, or drop it from orderBy.`,
      )
    }
  }

  return {
    orderBySql: renderReorderOrderBy(entries, quote(NEG_TAKE_REORDER_ALIAS)),
  }
}

function wrapNegativeTakeReorder(
  result: SqlResult,
  plan: { orderBySql: string } | null,
): SqlResult {
  if (!plan) return result
  // Flat-join reduction ships one flat row per parent×child; re-ordering it here
  // would interleave a parent's child rows. That strategy is not chosen for the
  // multi-parent findMany that negative take requires, so skip rather than risk it.
  if (result.requiresReduction) return result

  return {
    ...result,
    sql: `SELECT * FROM (${result.sql}) AS ${quote(
      NEG_TAKE_REORDER_ALIAS,
    )} ORDER BY ${plan.orderBySql}`,
  }
}

function normalizeArgsForDialect(
  dialect: SqlDialect,
  args: PrismaQueryArgs,
): PrismaQueryArgs {
  if (dialect !== 'postgres') return args
  return applyPostgresDistinctOrderBy(args)
}

function normalizeCompoundCursor(
  cursor: Record<string, unknown>,
  model: Model,
): Record<string, unknown> {
  const keys = Object.keys(cursor)
  if (keys.length !== 1) return cursor

  const key = keys[0]
  const value = cursor[key]

  const scalarSet = getScalarFieldSet(model)
  if (scalarSet.has(key)) return cursor

  if (!isPlainObject(value)) return cursor

  const nested = value as Record<string, unknown>
  const nestedKeys = Object.keys(nested)
  if (nestedKeys.length === 0) return cursor

  for (const nk of nestedKeys) {
    if (!scalarSet.has(nk)) return cursor
  }

  return nested
}

function normalizeArgsCompoundCursor(
  args: PrismaQueryArgs,
  model: Model,
): PrismaQueryArgs {
  if (!isNotNullish(args.cursor) || !isPlainObject(args.cursor)) return args
  const flat = normalizeCompoundCursor(
    args.cursor as Record<string, unknown>,
    model,
  )
  if (flat === args.cursor) return args
  return { ...args, cursor: flat }
}

function buildCursorClauseIfAny(input: {
  cursor: unknown
  orderBy: PrismaQueryArgs['orderBy']
  tableName: string
  alias: string
  params: ReturnType<typeof createParamStoreFrom>
  skip: unknown
  dialect: SqlDialect
  model: Model
}): { cte?: string; condition?: string; consumesSkip?: boolean } {
  const { cursor, orderBy, tableName, alias, params, skip, dialect, model } =
    input
  if (!isNotNullish(cursor)) return {}
  return buildCursorCondition(
    cursor,
    orderBy,
    tableName,
    alias,
    params,
    skip,
    dialect,
    model,
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

  const orderByResult = buildOrderByWithRelations(
    normalizedArgs.orderBy,
    alias,
    dialect,
    model,
    schemas,
  )

  const { take, skip, cursor } = getPaginationParams(method, normalizedArgs)

  const params = createParamStoreFrom(
    whereResult.params,
    whereResult.paramMappings,
    whereResult.nextParamIndex,
    dialect,
  )

  const outerHasLimit = isNotNullish(take)

  const includes = buildIncludeSql(
    normalizedArgs,
    model,
    schemas,
    alias,
    params,
    dialect,
    outerHasLimit,
  )

  const cursorResult = buildCursorClauseIfAny({
    cursor,
    orderBy: normalizedArgs.orderBy,
    tableName,
    alias,
    params,
    skip,
    dialect,
    model,
  })

  if (
    dialect === 'sqlite' &&
    isNonEmptyArray(normalizedArgs.distinct) &&
    cursorResult.condition
  ) {
    throw new Error(
      'Cursor pagination with distinct is not supported in SQLite due to window function limitations. ' +
        'Use findMany with skip/take instead, or remove distinct.',
    )
  }

  const finalSkip = cursorResult.consumesSkip ? undefined : skip

  const orderByJoins = orderByResult.joins
  const combinedWhereJoins: readonly string[] = whereResult.joins
    ? [...whereResult.joins, ...orderByJoins]
    : orderByJoins.length > 0
      ? orderByJoins
      : []

  return {
    select: selectFields,
    includes,
    from: { table: tableName, alias },
    whereClause: whereResult.clause,
    whereJoins: combinedWhereJoins,
    orderBy: orderByResult.sql,
    pagination: { take, skip: finalSkip },
    distinct: normalizedArgs.distinct,
    method,
    cursorCte: cursorResult.cte,
    cursorClause: cursorResult.condition,
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
  options?: SqlBuildOptions
}

export function buildSelectSql(input: BuildSelectSqlInput): SqlResult {
  const { method, args, model, schemas, from, whereResult, dialect } = input

  assertSafeAlias(from.alias)
  assertSafeTableRef(from.tableName)

  const dialectToUse = resolveDialect(dialect)

  // Computed from the original args, before the orderBy is reversed below.
  const negTakeReorder = planNegativeTakeReorder(method, args)

  const argsForSql = normalizeArgsForNegativeTake(method, args)
  const argsWithDialect = normalizeArgsForDialect(dialectToUse, argsForSql)
  const normalizedArgs = normalizeArgsCompoundCursor(argsWithDialect, model)

  validateDistinct(model, normalizedArgs.distinct)
  validateOrderBy(model, normalizedArgs.orderBy, schemas)
  validateCursor(model, normalizedArgs.cursor, normalizedArgs.distinct)

  if (method === 'findMany') {
    const rewritten = tryBuildUnionOfIdsSelectSql({
      method,
      normalizedArgs,
      model,
      schemas,
      tableName: from.tableName,
      alias: from.alias,
      dialect: dialectToUse,
    })
    if (rewritten) return wrapNegativeTakeReorder(rewritten, negTakeReorder)
  }

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

  return wrapNegativeTakeReorder(constructFinalSql(spec), negTakeReorder)
}
