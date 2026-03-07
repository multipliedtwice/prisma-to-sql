import { PrismaQueryArgs, Model } from '../types'
import { SQL_SEPARATORS } from './shared/constants'
import {
  col,
  quoteColumn,
  assertSafeAlias,
  assertSafeTableRef,
} from './shared/sql-utils'
import { ParamStore } from './shared/param-store'
import { SqlDialect, getGlobalDialect } from '../sql-builder-dialect'
import {
  isNotNullish,
  isNonEmptyString,
  isPlainObject,
} from './shared/validators/type-guards'
import { normalizeIntLike } from './shared/int-like'
import { addAutoScoped } from './shared/dynamic-params'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import { normalizeAndValidateOrderBy } from './shared/order-by-utils'
import { ensureDeterministicOrderByInput } from './shared/order-by-determinism'
import { assertScalarField } from './shared/validators/field-assertions'

type OrderByDirection = 'asc' | 'desc'
type NullsPosition = 'first' | 'last'

type OrderByValueObject = { direction: OrderByDirection; nulls?: NullsPosition }

type OrderByEntry = {
  field: string
  direction: OrderByDirection
  nulls?: NullsPosition
}

type IntOrDynamic = number | string
type MaybeIntOrDynamic = IntOrDynamic | undefined

const MAX_LIMIT_OFFSET = 2147483647
const ORDER_BY_ALLOWED_KEYS = new Set(['sort', 'nulls'])

const DEBUG_CURSOR =
  typeof process !== 'undefined' && process.env?.DEBUG_CURSOR === '1'

function debugCursor(label: string, payload?: Record<string, unknown>): void {
  if (!DEBUG_CURSOR) return
  if (payload) {
    console.error(`[cursor] ${label}`, payload)
    return
  }
  console.error(`[cursor] ${label}`)
}

function parseDirectionRaw(raw: unknown, errorLabel: string): OrderByDirection {
  const s = String(raw).toLowerCase()
  if (s === 'asc' || s === 'desc') return s
  throw new Error(`Invalid ${errorLabel}: ${raw}`)
}

function parseNullsRaw(
  raw: unknown,
  errorLabel: string,
): NullsPosition | undefined {
  if (!isNotNullish(raw)) return undefined
  const s = String(raw).toLowerCase()
  if (s === 'first' || s === 'last') return s
  throw new Error(`Invalid ${errorLabel}: ${raw}`)
}

function requireOrderByObject(
  v: unknown,
  errorPrefix: string,
): Record<string, unknown> {
  if (!isPlainObject(v) || !('sort' in v)) {
    throw new Error(`${errorPrefix} must be 'asc' | 'desc' or { sort, nulls? }`)
  }
  return v
}

function assertAllowedOrderByKeys(
  obj: Record<string, unknown>,
  fieldName?: string,
): void {
  for (const k of Object.keys(obj)) {
    if (!ORDER_BY_ALLOWED_KEYS.has(k)) {
      throw new Error(
        fieldName
          ? `Unsupported orderBy key '${k}' for field '${fieldName}'`
          : `Unsupported orderBy key '${k}'`,
      )
    }
  }
}

export function parseOrderByValue(
  v: unknown,
  fieldName?: string,
): OrderByValueObject {
  const errorPrefix = fieldName ? `orderBy for '${fieldName}'` : 'orderBy value'

  if (typeof v === 'string') {
    return { direction: parseDirectionRaw(v, `${errorPrefix} direction`) }
  }

  const obj = requireOrderByObject(v, errorPrefix)

  const direction = parseDirectionRaw(obj.sort, `${errorPrefix}.sort`)
  const nulls = parseNullsRaw(obj.nulls, `${errorPrefix}.nulls`)

  assertAllowedOrderByKeys(obj, fieldName)

  return { direction, nulls }
}

function normalizeNonNegativeInt(name: 'skip', v: unknown): IntOrDynamic {
  if (isDynamicParameter(v)) return v as string
  const result = normalizeIntLike(name, v, {
    min: 0,
    max: MAX_LIMIT_OFFSET,
    allowZero: true,
  })
  if (result === undefined) {
    throw new Error(`${name} normalization returned undefined`)
  }
  return result
}

const MIN_NEGATIVE_TAKE = -10000

function normalizeIntAllowNegative(name: 'take', v: unknown): IntOrDynamic {
  if (isDynamicParameter(v)) return v as string
  const result = normalizeIntLike(name, v, {
    min: MIN_NEGATIVE_TAKE,
    max: MAX_LIMIT_OFFSET,
    allowZero: true,
  })
  if (result === undefined) {
    throw new Error(`${name} normalization returned undefined`)
  }
  return result
}

function hasNonNullishProp(
  v: unknown,
  key: 'skip' | 'take',
): v is Record<string, unknown> {
  return isPlainObject(v) && key in v && isNotNullish((v as any)[key])
}

export function readSkipTake(relArgs: unknown): {
  hasSkip: boolean
  hasTake: boolean
  skipVal: MaybeIntOrDynamic
  takeVal: MaybeIntOrDynamic
} {
  const hasSkip = hasNonNullishProp(relArgs, 'skip')
  const hasTake = hasNonNullishProp(relArgs, 'take')

  if (!hasSkip && !hasTake) {
    return {
      hasSkip: false,
      hasTake: false,
      skipVal: undefined,
      takeVal: undefined,
    }
  }

  const obj = relArgs

  const skipVal = hasSkip
    ? normalizeNonNegativeInt('skip', obj.skip)
    : undefined
  const takeVal = hasTake
    ? normalizeIntAllowNegative('take', obj.take)
    : undefined

  return { hasSkip, hasTake, skipVal, takeVal }
}

export function buildOrderByFragment(
  entries: OrderByEntry[],
  alias: string,
  dialect: SqlDialect,
  model?: Model,
): string {
  if (entries.length === 0) return ''

  const out: string[] = []
  for (const e of entries) {
    const dir = e.direction.toUpperCase()
    const c = col(alias, e.field, model)

    if (dialect === 'postgres') {
      const nulls = isNotNullish(e.nulls)
        ? ` NULLS ${e.nulls.toUpperCase()}`
        : ''
      out.push(c + ' ' + dir + nulls)
      continue
    }

    if (isNotNullish(e.nulls)) {
      const isNullExpr = `(${c} IS NULL)`
      const nullRankDir = e.nulls === 'first' ? 'DESC' : 'ASC'
      out.push(isNullExpr + ' ' + nullRankDir)
      out.push(c + ' ' + dir)
      continue
    }

    out.push(c + ' ' + dir)
  }

  return out.join(SQL_SEPARATORS.ORDER_BY)
}

function defaultNullsFor(
  dialect: SqlDialect,
  direction: OrderByDirection,
): NullsPosition {
  if (dialect === 'postgres') return direction === 'asc' ? 'last' : 'first'
  return direction === 'asc' ? 'first' : 'last'
}

function ensureCursorFieldsInOrder(
  orderEntries: OrderByEntry[],
  cursorEntries: Array<[string, unknown]>,
): OrderByEntry[] {
  if (cursorEntries.length === 0) return orderEntries

  const existing = new Set<string>()
  for (const entry of orderEntries) existing.add(entry.field)

  let out: OrderByEntry[] | null = null

  for (const [field] of cursorEntries) {
    if (!existing.has(field)) {
      if (!out) out = orderEntries.slice()
      out.push({ field, direction: 'asc' })
      existing.add(field)
    }
  }

  return out ?? orderEntries
}

function validateCompositeCursorOrder(
  cursorEntries: Array<[string, unknown]>,
  orderEntries: OrderByEntry[],
): void {
  if (cursorEntries.length <= 1) return
  if (orderEntries.length < cursorEntries.length) {
    throw new Error(
      `Composite cursor requires orderBy to start with cursor fields in the same order. Cursor fields: ${cursorEntries
        .map(([f]) => f)
        .join(', ')}`,
    )
  }

  for (let i = 0; i < cursorEntries.length; i++) {
    const cursorField = cursorEntries[i][0]
    const orderField = orderEntries[i]?.field
    if (orderField !== cursorField) {
      throw new Error(
        `Composite cursor/orderBy mismatch at position ${i + 1}. Expected orderBy field '${cursorField}', got '${orderField ?? 'undefined'}'.`,
      )
    }
  }
}

function buildCursorFilterParts(
  cursor: Record<string, unknown>,
  cursorAlias: string,
  params: ParamStore,
  model?: Model,
): { whereSql: string } {
  const parts: string[] = []

  for (const field in cursor) {
    if (!Object.prototype.hasOwnProperty.call(cursor, field)) continue

    const value = cursor[field]
    if (value === undefined) continue

    const c = cursorAlias + '.' + quoteColumn(model, field)

    if (value === null) {
      throw new Error(
        `Cursor field '${field}' cannot be null. Keyset pagination requires non-null cursor values.`,
      )
    }

    const ph = addAutoScoped(params, value, `cursor.filter.${field}`)
    parts.push(c + ' = ' + ph)
  }

  if (parts.length === 0) {
    throw new Error('cursor must have at least one field with defined value')
  }

  return {
    whereSql: parts.length === 1 ? parts[0] : '(' + parts.join(' AND ') + ')',
  }
}

function buildCursorEqualityExpr(
  columnExpr: string,
  cursorField: string,
): string {
  return `${columnExpr} = ${cursorField}`
}

function buildCursorInequalityExpr(
  columnExpr: string,
  direction: OrderByDirection,
  nulls: NullsPosition,
  cursorField: string,
): string {
  const op = direction === 'asc' ? '>' : '<'

  if (nulls === 'first') {
    return `(CASE WHEN ${cursorField} IS NULL THEN (${columnExpr} IS NOT NULL) ELSE (${columnExpr} ${op} ${cursorField}) END)`
  }

  return `(CASE WHEN ${cursorField} IS NULL THEN 0=1 ELSE ((${columnExpr} ${op} ${cursorField}) OR (${columnExpr} IS NULL)) END)`
}

function buildCursorCteSelectList(
  cursorEntries: Array<[string, unknown]>,
  orderEntries: OrderByEntry[],
  model?: Model,
): string {
  const seen = new Set<string>()
  const ordered: string[] = []

  for (const [f] of cursorEntries) {
    if (!seen.has(f)) {
      seen.add(f)
      ordered.push(f)
    }
  }

  for (const e of orderEntries) {
    if (!seen.has(e.field)) {
      seen.add(e.field)
      ordered.push(e.field)
    }
  }

  if (ordered.length === 0) {
    throw new Error('cursor cte select list is empty')
  }

  return ordered
    .map((f) => quoteColumn(model, f))
    .join(SQL_SEPARATORS.FIELD_LIST)
}

function truncateIdent(name: string, maxLen: number): string {
  const s = String(name)
  return s.length <= maxLen ? s : s.slice(0, maxLen)
}

function buildCursorNames(outerAlias: string): {
  cteName: string
  srcAlias: string
} {
  const maxLen = 63
  const base = outerAlias.toLowerCase()
  const cteName = truncateIdent(`__tp_cursor_${base}`, maxLen)
  const srcAlias = truncateIdent(`__tp_cursor_src_${base}`, maxLen)

  if (cteName === outerAlias || srcAlias === outerAlias) {
    return {
      cteName: truncateIdent(`__tp_cursor_${base}_x`, maxLen),
      srcAlias: truncateIdent(`__tp_cursor_src_${base}_x`, maxLen),
    }
  }

  return { cteName, srcAlias }
}

function assertCursorAndOrderFieldsScalar(
  model: Model | undefined,
  cursor: Record<string, unknown>,
  orderEntries: OrderByEntry[],
): void {
  if (!model) return
  for (const k in cursor) {
    if (!Object.prototype.hasOwnProperty.call(cursor, k)) continue
    assertScalarField(model, k, 'cursor')
  }
  for (const e of orderEntries) {
    assertScalarField(model, e.field, 'orderBy')
  }
}

function isPositiveSkip(skip: unknown): boolean {
  return typeof skip === 'number' && skip > 0
}

function buildExclusiveOperator(direction: OrderByDirection): '>' | '<' {
  return direction === 'asc' ? '>' : '<'
}

function buildInclusiveOperator(direction: OrderByDirection): '>=' | '<=' {
  return direction === 'asc' ? '>=' : '<='
}

function normalizeOrderEntriesWithoutModel(orderBy: unknown): OrderByEntry[] {
  if (!isNotNullish(orderBy)) return []

  const list = Array.isArray(orderBy) ? orderBy : [orderBy]
  const entries: OrderByEntry[] = []

  for (const item of list) {
    if (!isPlainObject(item)) {
      throw new Error('orderBy entries must be objects')
    }

    for (const [field, rawValue] of Object.entries(item)) {
      if (
        isPlainObject(rawValue) &&
        !('sort' in rawValue) &&
        !('nulls' in rawValue)
      ) {
        throw new Error(
          `Relation orderBy requires model metadata and cannot be resolved for field '${field}'`,
        )
      }

      const parsed = parseOrderByValue(rawValue, field)
      entries.push({
        field,
        direction: parsed.direction,
        nulls: parsed.nulls,
      })
    }
  }

  return entries
}

function normalizeCursorOrderEntries(
  orderBy: unknown,
  model?: Model,
): OrderByEntry[] {
  if (model) {
    return normalizeAndValidateOrderBy(orderBy, model, parseOrderByValue)
  }
  return normalizeOrderEntriesWithoutModel(orderBy)
}

function reorderCursorEntriesLikeOrderBy(
  cursorEntries: Array<[string, unknown]>,
  orderEntries: OrderByEntry[],
): Array<[string, unknown]> {
  if (cursorEntries.length <= 1) return cursorEntries
  if (orderEntries.length < cursorEntries.length) return cursorEntries

  const byField = new Map<string, unknown>()
  for (const [field, value] of cursorEntries) {
    byField.set(field, value)
  }

  if (byField.size !== cursorEntries.length) {
    return cursorEntries
  }

  for (let i = 0; i < cursorEntries.length; i++) {
    if (!byField.has(orderEntries[i].field)) {
      return cursorEntries
    }
  }

  return orderEntries
    .slice(0, cursorEntries.length)
    .map(
      (entry) => [entry.field, byField.get(entry.field)] as [string, unknown],
    )
}

function isRequiredNonRelationField(
  model: Model | undefined,
  fieldName: string,
): boolean {
  if (!model) return false
  const field = model.fields.find((f) => f.name === fieldName)
  if (!field || field.isRelation) return false
  return Boolean(field.isRequired || (field as any).isId)
}

function canUseTupleComparison(
  cursorEntries: Array<[string, unknown]>,
  orderEntries: OrderByEntry[],
  dialect: SqlDialect,
  model?: Model,
): boolean {
  if (dialect !== 'postgres') return false
  if (cursorEntries.length < 2) return false
  if (orderEntries.length < cursorEntries.length) return false

  const firstDirection = orderEntries[0]?.direction
  if (!firstDirection) return false

  for (let i = 0; i < cursorEntries.length; i++) {
    const orderEntry = orderEntries[i]
    const cursorField = cursorEntries[i]?.[0]

    if (orderEntry.field !== cursorField) return false
    if (orderEntry.direction !== firstDirection) return false
    if (orderEntry.nulls !== undefined) return false

    if (model && !isRequiredNonRelationField(model, orderEntry.field)) {
      return false
    }
  }

  return true
}

function buildTupleComparisonCondition(
  cursorEntries: Array<[string, unknown]>,
  orderEntries: OrderByEntry[],
  alias: string,
  params: ParamStore,
  skip: unknown,
  model?: Model,
): string {
  const direction = orderEntries[0].direction
  const operator = isPositiveSkip(skip)
    ? buildExclusiveOperator(direction)
    : buildInclusiveOperator(direction)

  const tupleLength = cursorEntries.length

  const leftTuple =
    '(' +
    orderEntries
      .slice(0, tupleLength)
      .map((entry) => col(alias, entry.field, model))
      .join(SQL_SEPARATORS.FIELD_LIST) +
    ')'

  const rightTuple =
    '(' +
    cursorEntries
      .map(([field, value]) => addAutoScoped(params, value, `cursor.${field}`))
      .join(SQL_SEPARATORS.FIELD_LIST) +
    ')'

  return `${leftTuple} ${operator} ${rightTuple}`
}

export function buildCursorCondition(
  cursor: Record<string, unknown>,
  orderBy: unknown,
  tableName: string,
  alias: string,
  params: ParamStore,
  skip: unknown,
  dialect?: SqlDialect,
  model?: Model,
): { cte: string; condition: string; consumesSkip: boolean } {
  assertSafeTableRef(tableName)
  assertSafeAlias(alias)

  const d = dialect ?? getGlobalDialect()
  const consumesSkip = isPositiveSkip(skip)

  let cursorEntries: Array<[string, unknown]> = []
  for (const k in cursor) {
    if (Object.prototype.hasOwnProperty.call(cursor, k)) {
      const value = cursor[k]
      if (value !== undefined) {
        if (value === null) {
          throw new Error(
            `Cursor field '${k}' cannot be null. Keyset pagination requires non-null cursor values.`,
          )
        }
        cursorEntries.push([k, value])
      }
    }
  }

  if (cursorEntries.length === 0) {
    throw new Error('cursor must have at least one field with defined value')
  }

  const rawOrderEntries = normalizeCursorOrderEntries(orderBy, model)
  cursorEntries = reorderCursorEntriesLikeOrderBy(
    cursorEntries,
    rawOrderEntries,
  )

  debugCursor('buildCursorCondition:start', {
    dialect: d,
    tableName,
    alias,
    skip,
    model: model?.name,
    cursorEntries,
    rawOrderEntries,
  })

  if (cursorEntries.length === 1 && rawOrderEntries.length === 0) {
    const [field, value] = cursorEntries[0]
    const ph = addAutoScoped(params, value, `cursor.${field}`)
    const c = col(alias, field, model)
    const op = consumesSkip ? '>' : '>='
    const condition = `${c} ${op} ${ph}`

    debugCursor('fast-path:no-orderBy', {
      field,
      skip,
      op,
      condition,
    })

    return {
      cte: '',
      condition,
      consumesSkip,
    }
  }

  if (cursorEntries.length === 1 && rawOrderEntries.length === 1) {
    const [cursorField, cursorValue] = cursorEntries[0]
    const orderEntry = rawOrderEntries[0]

    debugCursor('fast-path:single-orderBy:check', {
      cursorField,
      orderField: orderEntry.field,
      direction: orderEntry.direction,
      skip,
      dialect: d,
      matches: orderEntry.field === cursorField,
    })

    if (orderEntry.field === cursorField) {
      const ph = addAutoScoped(params, cursorValue, `cursor.${cursorField}`)
      const c = col(alias, cursorField, model)
      const op = consumesSkip
        ? buildExclusiveOperator(orderEntry.direction)
        : buildInclusiveOperator(orderEntry.direction)
      const condition = `${c} ${op} ${ph}`

      debugCursor('fast-path:single-orderBy:hit', {
        cursorField,
        direction: orderEntry.direction,
        skip,
        dialect: d,
        op,
        condition,
      })

      return {
        cte: '',
        condition,
        consumesSkip,
      }
    }
  }

  const { cteName, srcAlias } = buildCursorNames(alias)
  assertSafeAlias(cteName)
  assertSafeAlias(srcAlias)

  let finalOrderEntries: OrderByEntry[]

  if (model) {
    const deterministicOrderBy = ensureDeterministicOrderByInput({
      orderBy,
      model,
      parseValue: parseOrderByValue,
    })

    finalOrderEntries = normalizeAndValidateOrderBy(
      deterministicOrderBy,
      model,
      parseOrderByValue,
    )
  } else {
    finalOrderEntries = rawOrderEntries
  }

  if (finalOrderEntries.length === 0) {
    finalOrderEntries = cursorEntries.map(([field]) => ({
      field,
      direction: 'asc' as const,
    }))
  } else {
    validateCompositeCursorOrder(cursorEntries, finalOrderEntries)
    finalOrderEntries = ensureCursorFieldsInOrder(
      finalOrderEntries,
      cursorEntries,
    )
  }

  assertCursorAndOrderFieldsScalar(model, cursor, finalOrderEntries)

  if (canUseTupleComparison(cursorEntries, finalOrderEntries, d, model)) {
    const condition = buildTupleComparisonCondition(
      cursorEntries,
      finalOrderEntries,
      alias,
      params,
      skip,
      model,
    )

    debugCursor('fast-path:tuple-comparison', {
      dialect: d,
      skip,
      finalOrderEntries,
      condition,
    })

    return {
      cte: '',
      condition,
      consumesSkip,
    }
  }

  const { whereSql: cursorWhereSql } = buildCursorFilterParts(
    cursor,
    srcAlias,
    params,
    model,
  )

  const cursorOrderBy = finalOrderEntries
    .map(
      (e) =>
        srcAlias +
        '.' +
        quoteColumn(model, e.field) +
        ' ' +
        e.direction.toUpperCase(),
    )
    .join(', ')

  const selectList = buildCursorCteSelectList(
    cursorEntries,
    finalOrderEntries,
    model,
  )

  const cte =
    cteName +
    ' AS (\n    SELECT ' +
    selectList +
    ' FROM ' +
    tableName +
    ' ' +
    srcAlias +
    '\n    WHERE ' +
    cursorWhereSql +
    '\n    ORDER BY ' +
    cursorOrderBy +
    '\n    LIMIT 1\n  )'

  const existsExpr = 'EXISTS (SELECT 1 FROM ' + cteName + ')'

  const orClauses: string[] = []

  for (let level = 0; level < finalOrderEntries.length; level++) {
    const andParts: string[] = []

    for (let i = 0; i < level; i++) {
      const e = finalOrderEntries[i]
      const c = col(alias, e.field, model)
      const cursorField = cteName + '.' + quoteColumn(model, e.field)
      andParts.push(buildCursorEqualityExpr(c, cursorField))
    }

    const e = finalOrderEntries[level]
    const c = col(alias, e.field, model)
    const cursorField = cteName + '.' + quoteColumn(model, e.field)
    const nulls = e.nulls ?? defaultNullsFor(d, e.direction)
    andParts.push(buildCursorInequalityExpr(c, e.direction, nulls, cursorField))

    orClauses.push('(' + andParts.join(SQL_SEPARATORS.CONDITION_AND) + ')')
  }

  const exclusive = orClauses.join(SQL_SEPARATORS.CONDITION_OR)

  const equalityAll = finalOrderEntries
    .map(
      (e) =>
        `${col(alias, e.field, model)} = ${cteName}.${quoteColumn(model, e.field)}`,
    )
    .join(SQL_SEPARATORS.CONDITION_AND)

  const inclusive = equalityAll
    ? `(${equalityAll}${SQL_SEPARATORS.CONDITION_OR}${exclusive})`
    : `(${exclusive})`

  const comparator = consumesSkip ? `(${exclusive})` : inclusive

  const condition =
    '(' + existsExpr + SQL_SEPARATORS.CONDITION_AND + comparator + ')'

  debugCursor('fallback:composite-path', {
    dialect: d,
    skip,
    finalOrderEntries,
    cteName,
    condition,
  })

  return { cte, condition, consumesSkip }
}

export function buildOrderBy(
  orderBy: unknown,
  alias: string,
  dialect?: SqlDialect,
  model?: Model,
): string {
  assertSafeAlias(alias)

  const entries = normalizeAndValidateOrderBy(
    orderBy,
    model!,
    parseOrderByValue,
  )
  if (entries.length === 0) return ''

  const d = dialect ?? getGlobalDialect()
  return buildOrderByFragment(entries, alias, d, model)
}

export function buildOrderByClause(
  args: PrismaQueryArgs,
  alias: string,
  dialect?: SqlDialect,
  model?: Model,
): string {
  if (!isNotNullish(args.orderBy)) return ''

  const result = buildOrderBy(args.orderBy, alias, dialect, model)
  if (!isNonEmptyString(result)) {
    throw new Error(
      'buildOrderByClause: orderBy specified but produced empty result',
    )
  }
  return result
}

function normalizeTakeLike(v: unknown): MaybeIntOrDynamic {
  const n = normalizeIntLike('take', v, {
    min: Number.MIN_SAFE_INTEGER,
    max: MAX_LIMIT_OFFSET,
    allowZero: true,
  })
  if (typeof n === 'number' && n === 0) return 0
  return n as MaybeIntOrDynamic
}

function normalizeSkipLike(v: unknown): MaybeIntOrDynamic {
  return normalizeIntLike('skip', v, {
    min: 0,
    max: MAX_LIMIT_OFFSET,
    allowZero: true,
  }) as MaybeIntOrDynamic
}

export function getPaginationParams(
  method: string,
  args: PrismaQueryArgs,
): {
  take?: IntOrDynamic
  skip?: IntOrDynamic
  cursor?: Record<string, unknown>
} {
  if (method === 'findMany') {
    return {
      take: normalizeTakeLike(args.take),
      skip: normalizeSkipLike(args.skip),
      cursor: args.cursor,
    }
  }

  if (method === 'findFirst') {
    const skip = normalizeSkipLike(args.skip)
    return { take: 1, skip: skip ?? 0 }
  }

  if (method === 'findUnique') {
    return { take: 1, skip: 0 }
  }

  return {}
}
