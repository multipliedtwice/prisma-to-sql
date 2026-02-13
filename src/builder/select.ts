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
  isNonEmptyString,
} from './shared/validators/type-guards'
import { assertScalarField } from './shared/validators/field-assertions'
import {
  getScalarFieldSet,
  getRelationFieldSet,
} from './shared/model-field-cache'

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
          `distinct field '${f}' is a relation. Only scalar fields are allowed.\n` +
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
): void {
  if (!isNotNullish(orderBy)) return

  const items = normalizeOrderByInput(orderBy)
  if (items.length === 0) return

  const scalarSet = getScalarFieldSet(model)
  const relationSet = getRelationFieldSet(model)

  for (const it of items) {
    const entries = Object.entries(it)
    if (entries.length !== 1) {
      throw new Error(
        'orderBy array entries must have exactly one field. ' +
          `Got ${entries.length} fields: ${Object.keys(it).join(', ')}`,
      )
    }

    const [fieldName, value] = entries[0]
    const f = String(fieldName).trim()

    if (f.length === 0) {
      throw new Error('orderBy field name cannot be empty')
    }

    if (f.length > 255) {
      throw new Error(
        `orderBy field name too long (${f.length} chars, max 255): ${f.slice(0, 50)}...`,
      )
    }

    if (!scalarSet.has(f)) {
      if (relationSet.has(f)) {
        throw new Error(
          `orderBy field '${f}' is a relation. Only scalar fields are allowed.\n` +
            `Available scalar fields: ${[...scalarSet].join(', ')}`,
        )
      }
      throw new Error(
        `orderBy field '${f}' does not exist on model ${model.name}.\n` +
          `Available fields: ${[...scalarSet].join(', ')}`,
      )
    }

    assertScalarField(model, f, 'orderBy')
    parseOrderByValue(value, f)
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

function normalizeArgsForDialect(
  dialect: SqlDialect,
  args: PrismaQueryArgs,
): PrismaQueryArgs {
  if (dialect !== 'postgres') return args
  return applyPostgresDistinctOrderBy(args)
}

function buildCursorClauseIfAny(input: {
  cursor: unknown
  orderBy: PrismaQueryArgs['orderBy']
  tableName: string
  alias: string
  params: ReturnType<typeof createParamStoreFrom>
  dialect: SqlDialect
  model: Model
}): { cte?: string; condition?: string } {
  const { cursor, orderBy, tableName, alias, params, dialect, model } = input
  if (!isNotNullish(cursor)) return {}
  return buildCursorCondition(
    cursor,
    orderBy,
    tableName,
    alias,
    params,
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

  const orderByClause = buildOrderByClause(
    normalizedArgs,
    alias,
    dialect,
    model,
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
}

export function buildSelectSql(input: BuildSelectSqlInput): SqlResult {
  const { method, args, model, schemas, from, whereResult, dialect } = input

  assertSafeAlias(from.alias)
  assertSafeTableRef(from.tableName)

  const dialectToUse = resolveDialect(dialect)

  const argsForSql = normalizeArgsForNegativeTake(method, args)
  const normalizedArgs = normalizeArgsForDialect(dialectToUse, argsForSql)

  validateDistinct(model, normalizedArgs.distinct)
  validateOrderBy(model, normalizedArgs.orderBy)
  validateCursor(model, normalizedArgs.cursor, normalizedArgs.distinct)

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
