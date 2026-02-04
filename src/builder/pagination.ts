import { PrismaQueryArgs, Model } from '../types'
import { SQL_SEPARATORS, SQL_TEMPLATES } from './shared/constants'
import { col, quoteColumn } from './shared/sql-utils'
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
import { normalizeOrderByInput } from './shared/order-by-utils'

type OrderByDirection = 'asc' | 'desc'
type NullsPosition = 'first' | 'last'

type OrderByEntry = {
  field: string
  direction: OrderByDirection
  nulls?: NullsPosition
}

type IntOrDynamic = number | string
type MaybeIntOrDynamic = IntOrDynamic | undefined

type SkipTakeReadResult = {
  hasSkip: boolean
  hasTake: boolean
  skipVal: MaybeIntOrDynamic
  takeVal: MaybeIntOrDynamic
}

const MAX_LIMIT_OFFSET = 2147483647

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
  const allowed = new Set(['sort', 'nulls'])
  for (const k of Object.keys(obj)) {
    if (!allowed.has(k)) {
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
): { direction: OrderByDirection; nulls?: NullsPosition } {
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

function normalizeFiniteInteger(name: string, v: unknown): number {
  if (typeof v !== 'number' || !Number.isFinite(v) || !Number.isInteger(v)) {
    throw new Error(`${name} must be an integer`)
  }
  return v
}

function normalizeNonNegativeInt(name: string, v: unknown): IntOrDynamic {
  if (isDynamicParameter(v)) return v as string

  const n = normalizeFiniteInteger(name, v)

  if (n < 0) {
    throw new Error(`${name} must be >= 0`)
  }
  if (n > MAX_LIMIT_OFFSET) {
    throw new Error(`${name} must be <= ${MAX_LIMIT_OFFSET}`)
  }
  return n
}

function hasNonNullishProp(
  v: unknown,
  key: 'skip' | 'take',
): v is Record<string, unknown> {
  return isPlainObject(v) && key in v && isNotNullish((v as any)[key])
}

function normalizeIntegerOrDynamic(name: 'take', v: unknown): IntOrDynamic {
  if (isDynamicParameter(v)) return v as string
  return normalizeFiniteInteger(name, v)
}

export function readSkipTake(relArgs: unknown): SkipTakeReadResult {
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
    ? normalizeIntegerOrDynamic('take', obj.take)
    : undefined

  return { hasSkip, hasTake, skipVal, takeVal }
}

function buildOrderByFragment(
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
      out.push(`${c} ${dir}${nulls}`)
      continue
    }

    if (isNotNullish(e.nulls)) {
      const isNullExpr = `(${c} IS NULL)`
      const nullRankDir = e.nulls === 'first' ? 'DESC' : 'ASC'
      out.push(`${isNullExpr} ${nullRankDir}`)
      out.push(`${c} ${dir}`)
      continue
    }

    out.push(`${c} ${dir}`)
  }

  return out.join(SQL_SEPARATORS.ORDER_BY)
}

function defaultNullsFor(
  dialect: SqlDialect,
  direction: OrderByDirection,
): NullsPosition {
  if (dialect === 'postgres') {
    return direction === 'asc' ? 'last' : 'first'
  }
  return direction === 'asc' ? 'first' : 'last'
}

function ensureCursorFieldsInOrder(
  orderEntries: OrderByEntry[],
  cursorEntries: Array<[string, unknown]>,
): OrderByEntry[] {
  const existing = new Map<string, OrderByEntry>()
  for (const e of orderEntries) existing.set(e.field, e)

  const out: OrderByEntry[] = [...orderEntries]

  for (const [field] of cursorEntries) {
    if (!existing.has(field)) {
      out.push({ field, direction: 'asc' })
      existing.set(field, out[out.length - 1])
    }
  }

  return out
}

function buildCursorFilterParts(
  cursor: Record<string, unknown>,
  cursorAlias: string,
  params: ParamStore,
  model?: Model,
): {
  whereSql: string
  placeholdersByField: Map<string, string>
} {
  const entries = Object.entries(cursor)
  if (entries.length === 0) {
    throw new Error('cursor must have at least one field')
  }

  const placeholdersByField = new Map<string, string>()
  const parts: string[] = []

  for (const [field, value] of entries) {
    const c = `${cursorAlias}.${quoteColumn(model, field)}`
    if (value === null) {
      parts.push(`${c} IS NULL`)
      continue
    }
    const ph = addAutoScoped(params, value, `cursor.filter.${field}`)
    placeholdersByField.set(field, ph)
    parts.push(`${c} = ${ph}`)
  }

  return {
    whereSql: parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`,
    placeholdersByField,
  }
}

function cursorValueExpr(
  tableName: string,
  cursorAlias: string,
  cursorWhereSql: string,
  field: string,
  model?: Model,
): string {
  const colName = quoteColumn(model, field)
  return `(SELECT ${cursorAlias}.${colName} ${SQL_TEMPLATES.FROM} ${tableName} ${cursorAlias} ${SQL_TEMPLATES.WHERE} ${cursorWhereSql} ${SQL_TEMPLATES.LIMIT} 1)`
}

function buildCursorRowExistsExpr(
  tableName: string,
  cursorAlias: string,
  cursorWhereSql: string,
): string {
  return `EXISTS (${SQL_TEMPLATES.SELECT} 1 ${SQL_TEMPLATES.FROM} ${tableName} ${cursorAlias} ${SQL_TEMPLATES.WHERE} ${cursorWhereSql} ${SQL_TEMPLATES.LIMIT} 1)`
}

function buildCursorEqualityExpr(
  columnExpr: string,
  valueExpr: string,
): string {
  return `((${valueExpr} IS NULL AND ${columnExpr} IS NULL) OR (${valueExpr} IS NOT NULL AND ${columnExpr} = ${valueExpr}))`
}

function buildCursorInequalityExpr(
  columnExpr: string,
  direction: OrderByDirection,
  nulls: NullsPosition,
  valueExpr: string,
): string {
  const op = direction === 'asc' ? '>' : '<'

  if (nulls === 'first') {
    return `(CASE WHEN ${valueExpr} IS NULL THEN (${columnExpr} IS NOT NULL) ELSE (${columnExpr} ${op} ${valueExpr}) END)`
  }

  return `(CASE WHEN ${valueExpr} IS NULL THEN 0=1 ELSE ((${columnExpr} ${op} ${valueExpr}) OR (${columnExpr} IS NULL)) END)`
}

function buildOuterCursorMatch(
  cursor: Record<string, unknown>,
  outerAlias: string,
  placeholdersByField: Map<string, string>,
  params: ParamStore,
  model?: Model,
): string {
  const parts: string[] = []

  for (const [field, value] of Object.entries(cursor)) {
    const c = col(outerAlias, field, model)
    if (value === null) {
      parts.push(`${c} IS NULL`)
      continue
    }

    const existing = placeholdersByField.get(field)
    if (typeof existing === 'string' && existing.length > 0) {
      parts.push(`${c} = ${existing}`)
      continue
    }

    const ph = addAutoScoped(params, value, `cursor.outerMatch.${field}`)
    parts.push(`${c} = ${ph}`)
  }

  return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
}

function buildOrderEntries(orderBy: unknown): OrderByEntry[] {
  const normalized = normalizeOrderByInput(orderBy, parseOrderByValue)
  const entries: OrderByEntry[] = []

  for (const item of normalized) {
    for (const [field, value] of Object.entries(item)) {
      if (typeof value === 'string') {
        entries.push({ field, direction: value as OrderByDirection })
      } else {
        entries.push({
          field,
          direction: value.sort,
          nulls: value.nulls,
        })
      }
    }
  }

  return entries
}

export function buildCursorCondition(
  cursor: Record<string, unknown>,
  orderBy: unknown,
  tableName: string,
  alias: string,
  params: ParamStore,
  dialect?: SqlDialect,
  model?: Model,
): string {
  const d = dialect ?? getGlobalDialect()

  const cursorEntries = Object.entries(cursor)
  if (cursorEntries.length === 0) {
    throw new Error('cursor must have at least one field')
  }

  const cursorAlias = '__tp_cursor_src'
  const { whereSql: cursorWhereSql, placeholdersByField } =
    buildCursorFilterParts(cursor, cursorAlias, params, model)

  let orderEntries = buildOrderEntries(orderBy)

  if (orderEntries.length === 0) {
    orderEntries = cursorEntries.map(([field]) => ({
      field,
      direction: 'asc' as OrderByDirection,
    }))
  } else {
    orderEntries = ensureCursorFieldsInOrder(orderEntries, cursorEntries)
  }

  const existsExpr = buildCursorRowExistsExpr(
    tableName,
    cursorAlias,
    cursorWhereSql,
  )

  const outerCursorMatch = buildOuterCursorMatch(
    cursor,
    alias,
    placeholdersByField,
    params,
    model,
  )

  const valueExprByField = new Map<string, string>()
  const getValueExpr = (field: string): string => {
    const existing = valueExprByField.get(field)
    if (existing) return existing
    const v = cursorValueExpr(
      tableName,
      cursorAlias,
      cursorWhereSql,
      field,
      model,
    )
    valueExprByField.set(field, v)
    return v
  }

  const orClauses: string[] = []

  for (let level = 0; level < orderEntries.length; level++) {
    const andParts: string[] = []

    for (let i = 0; i < level; i++) {
      const e = orderEntries[i]
      const c = col(alias, e.field, model)
      const v = getValueExpr(e.field)
      andParts.push(buildCursorEqualityExpr(c, v))
    }

    const e = orderEntries[level]
    const c = col(alias, e.field, model)
    const v = getValueExpr(e.field)
    const nulls = e.nulls ?? defaultNullsFor(d, e.direction)
    andParts.push(buildCursorInequalityExpr(c, e.direction, nulls, v))

    orClauses.push(`(${andParts.join(SQL_SEPARATORS.CONDITION_AND)})`)
  }

  const exclusive = orClauses.join(SQL_SEPARATORS.CONDITION_OR)
  return `(${existsExpr} ${SQL_SEPARATORS.CONDITION_AND} ((${exclusive})${SQL_SEPARATORS.CONDITION_OR}(${outerCursorMatch})))`
}

export function buildOrderBy(
  orderBy: unknown,
  alias: string,
  dialect?: SqlDialect,
  model?: Model,
): string {
  const entries = buildOrderEntries(orderBy)
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
  if (typeof n === 'number') {
    if (n === 0) return 0
  }
  return n as MaybeIntOrDynamic
}

export function normalizeSkipLike(v: unknown): MaybeIntOrDynamic {
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
