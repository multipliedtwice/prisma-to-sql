import { isNotNullish, isPlainObject } from './validators/type-guards'

type OrderByObject = Record<string, unknown>
type OrderByArray = Array<Record<string, unknown>>

export type OrderByType =
  | OrderByObject
  | OrderByArray
  | string
  | null
  | undefined

type OrderByDirection = 'asc' | 'desc'
type OrderByNulls = 'first' | 'last'
type OrderBySortObject = { sort: OrderByDirection; nulls?: OrderByNulls }
type NormalizedOrderByValue = OrderByDirection | OrderBySortObject
type NormalizedOrderBy = Array<Record<string, NormalizedOrderByValue>>
type ParseOrderByValue = (
  v: unknown,
  field?: string,
) => { direction: OrderByDirection; nulls?: OrderByNulls }

const flipNulls = (v: unknown): unknown => {
  const s = String(v).toLowerCase()
  if (s === 'first') return 'last'
  if (s === 'last') return 'first'
  return v
}

const flipSortString = (v: unknown): unknown => {
  if (typeof v !== 'string') return v
  const s = v.toLowerCase()
  if (s === 'asc') return 'desc'
  if (s === 'desc') return 'asc'
  return v
}

const getNextSort = (sortRaw: unknown): unknown => {
  if (typeof sortRaw !== 'string') return sortRaw

  const s = sortRaw.toLowerCase()
  if (s === 'asc') return 'desc'
  if (s === 'desc') return 'asc'
  return sortRaw
}

const flipObjectSort = (
  obj: Record<string, unknown>,
): Record<string, unknown> => {
  const sortRaw = obj.sort
  const out: Record<string, unknown> = { ...obj, sort: getNextSort(sortRaw) }

  const nullsRaw = obj.nulls
  if (typeof nullsRaw === 'string') {
    out.nulls = flipNulls(nullsRaw)
  }

  return out
}

const flipValue = (v: unknown): unknown => {
  if (typeof v === 'string') return flipSortString(v)
  if (isPlainObject(v)) return flipObjectSort(v)
  return v
}

const assertSingleFieldObject = (item: unknown): [string, unknown] => {
  if (!isPlainObject(item)) {
    throw new Error('orderBy array entries must be objects')
  }

  const entries = Object.entries(item)
  if (entries.length !== 1) {
    throw new Error('orderBy array entries must have exactly one field')
  }

  return entries[0]
}

const flipOrderByArray = (orderBy: unknown[]): { [x: string]: unknown }[] => {
  return orderBy.map((item) => {
    const [k, v] = assertSingleFieldObject(item)
    return { [k]: flipValue(v) }
  })
}

const flipOrderByObject = (
  orderBy: Record<string, unknown>,
): Record<string, unknown> => {
  const out: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(orderBy)) {
    out[k] = flipValue(v)
  }
  return out
}

export function reverseOrderByInput(orderBy: unknown): OrderByType {
  if (!isNotNullish(orderBy)) return orderBy

  if (Array.isArray(orderBy)) {
    return flipOrderByArray(orderBy)
  }

  if (isPlainObject(orderBy)) {
    return flipOrderByObject(orderBy)
  }

  throw new Error('orderBy must be an object or array of objects')
}

const normalizePairs = (
  pairs: Array<[string, unknown]>,
  parseValue: ParseOrderByValue,
): NormalizedOrderBy => {
  return pairs.map(([field, rawValue]) => {
    const parsed = parseValue(rawValue, field)
    return {
      [field]:
        parsed.nulls !== undefined
          ? { sort: parsed.direction, nulls: parsed.nulls }
          : parsed.direction,
    }
  })
}

export function normalizeOrderByInput(
  orderBy: unknown,
  parseValue: ParseOrderByValue,
): NormalizedOrderBy {
  if (!isNotNullish(orderBy)) return []

  if (Array.isArray(orderBy)) {
    const pairs = orderBy.map(assertSingleFieldObject)
    return normalizePairs(pairs, parseValue)
  }

  if (isPlainObject(orderBy)) {
    return normalizePairs(Object.entries(orderBy), parseValue)
  }

  throw new Error('orderBy must be an object or array of objects')
}
