import { isNotNullish, isPlainObject } from './validators/type-guards'
import { Model } from '../../types'
import { getScalarFieldSet, getRelationFieldSet } from './model-field-cache'

type OrderByObject = Record<string, unknown>
type OrderByArray = Array<Record<string, unknown>>

type OrderByType = OrderByObject | OrderByArray | string | null | undefined

type OrderByDirection = 'asc' | 'desc'
type OrderByNulls = 'first' | 'last'

export type OrderBySortObject = {
  direction: OrderByDirection
  nulls?: OrderByNulls
}

type NormalizedOrderByValue = OrderByDirection | OrderBySortObject
type NormalizedOrderBy = Array<Record<string, NormalizedOrderByValue>>

type ParseOrderByValue = (
  v: unknown,
  field?: string,
) => { direction: OrderByDirection; nulls?: OrderByNulls }

interface OrderByEntry {
  field: string
  direction: OrderByDirection
  nulls?: OrderByNulls
}

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

const flipScalarSortObject = (
  obj: Record<string, unknown>,
): Record<string, unknown> => {
  const out: Record<string, unknown> = { ...obj }

  const hasSort = Object.prototype.hasOwnProperty.call(obj, 'sort')
  const hasDirection = Object.prototype.hasOwnProperty.call(obj, 'direction')

  if (hasSort) {
    out.sort = getNextSort(obj.sort)
  } else if (hasDirection) {
    out.direction = getNextSort(obj.direction)
  } else {
    out.sort = getNextSort(obj.sort)
  }

  if (typeof obj.nulls === 'string') {
    out.nulls = flipNulls(obj.nulls)
  }

  return out
}

function isScalarSortConfig(obj: Record<string, unknown>): boolean {
  return (
    Object.prototype.hasOwnProperty.call(obj, 'sort') ||
    Object.prototype.hasOwnProperty.call(obj, 'direction')
  )
}

function flipRelationOrderByValue(
  obj: Record<string, unknown>,
): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(obj)) {
    out[k] = flipValue(v)
  }
  return out
}

const flipValue = (v: unknown): unknown => {
  if (typeof v === 'string') return flipSortString(v)
  if (isPlainObject(v)) {
    if (isScalarSortConfig(v)) return flipScalarSortObject(v)
    return flipRelationOrderByValue(v)
  }
  return v
}

const expandToSingleFieldEntries = (
  item: unknown,
): Array<[string, unknown]> => {
  if (!isPlainObject(item)) {
    throw new Error('orderBy array entries must be objects')
  }

  const entries = Object.entries(item).filter(([, v]) => v !== undefined)
  if (entries.length === 0) {
    throw new Error('orderBy array entries must have at least one field')
  }

  return entries
}

export function expandOrderByInput(orderBy: unknown): Array<[string, unknown]> {
  if (!isNotNullish(orderBy)) return []

  if (Array.isArray(orderBy)) {
    const result: Array<[string, unknown]> = []
    for (const item of orderBy) {
      result.push(...expandToSingleFieldEntries(item))
    }
    return result
  }

  if (isPlainObject(orderBy)) {
    return Object.entries(orderBy)
  }

  throw new Error('orderBy must be an object or array of objects')
}

function isScalarOrderByValue(v: unknown): boolean {
  if (typeof v === 'string') {
    const lower = v.toLowerCase()
    return lower === 'asc' || lower === 'desc'
  }
  if (isPlainObject(v) && isScalarSortConfig(v)) return true
  return false
}

const flipOrderByArray = (orderBy: unknown[]): { [x: string]: unknown }[] => {
  const result: { [x: string]: unknown }[] = []
  for (const item of orderBy) {
    for (const [k, v] of expandToSingleFieldEntries(item)) {
      result.push({ [k]: flipValue(v) })
    }
  }
  return result
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
  const result: NormalizedOrderBy = []
  for (const [field, rawValue] of pairs) {
    if (typeof rawValue === 'string') {
      const lower = rawValue.toLowerCase()
      if (lower !== 'asc' && lower !== 'desc') {
        throw new Error(
          `Invalid orderBy direction '${rawValue}' for field '${field}'. Must be 'asc' or 'desc'`,
        )
      }
    }
    if (!isScalarOrderByValue(rawValue)) continue
    const parsed = parseValue(rawValue, field)
    result.push({
      [field]:
        parsed.nulls !== undefined
          ? { direction: parsed.direction, nulls: parsed.nulls }
          : parsed.direction,
    })
  }
  return result
}

export function normalizeOrderByInput(
  orderBy: unknown,
  parseValue: ParseOrderByValue,
): NormalizedOrderBy {
  if (!isNotNullish(orderBy)) return []

  if (Array.isArray(orderBy)) {
    const allPairs: Array<[string, unknown]> = []
    for (const item of orderBy) {
      allPairs.push(...expandToSingleFieldEntries(item))
    }
    return normalizePairs(allPairs, parseValue)
  }

  if (isPlainObject(orderBy)) {
    return normalizePairs(Object.entries(orderBy), parseValue)
  }

  throw new Error('orderBy must be an object or array of objects')
}

export function normalizeAndValidateOrderBy(
  orderBy: unknown,
  model: Model,
  parseValue: ParseOrderByValue,
): OrderByEntry[] {
  if (!isNotNullish(orderBy)) return []

  const normalized = normalizeOrderByInput(orderBy, parseValue)
  const entries: OrderByEntry[] = []
  const scalarSet = getScalarFieldSet(model)
  const relationSet = getRelationFieldSet(model)

  for (const item of normalized) {
    const [[field, value]] = Object.entries(item)

    if (!scalarSet.has(field)) {
      if (relationSet.has(field)) continue
      throw new Error(
        `orderBy field '${field}' not found on model ${model.name}`,
      )
    }

    if (typeof value === 'string') {
      entries.push({ field, direction: value as OrderByDirection })
    } else {
      entries.push({
        field,
        direction: (value as OrderBySortObject).direction,
        nulls: (value as OrderBySortObject).nulls,
      })
    }
  }

  return entries
}
