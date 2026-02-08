import { Model } from '../../types'
import { getScalarFieldSet } from './model-field-cache'
import { isNotNullish } from './validators/type-guards'
import { normalizeOrderByInput } from './order-by-utils'

type OrderByDirection = 'asc' | 'desc'
type NullsPosition = 'first' | 'last'

type ParsedOrderByValue = {
  direction: OrderByDirection
  nulls?: NullsPosition
}

type ParseOrderByValue = (v: unknown, field?: string) => ParsedOrderByValue

function findTiebreakerField(model?: Model): string | null {
  if (!model) return null

  const scalarSet = getScalarFieldSet(model)

  for (const f of model.fields) {
    if (f.isId && !f.isRelation && scalarSet.has(f.name)) return f.name
  }

  if (scalarSet.has('id')) return 'id'

  return null
}

function hasTiebreaker(
  orderBy: unknown,
  parse: ParseOrderByValue,
  field: string,
): boolean {
  if (!isNotNullish(orderBy)) return false
  const normalized = normalizeOrderByInput(orderBy, parse)
  return normalized.some((obj) =>
    Object.prototype.hasOwnProperty.call(obj, field),
  )
}

function addTiebreaker(orderBy: unknown, field: string): unknown {
  if (Array.isArray(orderBy)) return [...orderBy, { [field]: 'asc' }]
  return [orderBy, { [field]: 'asc' }]
}

export function ensureDeterministicOrderByInput(args: {
  orderBy: unknown
  model?: Model
  parseValue: ParseOrderByValue
}): unknown {
  const { orderBy, model, parseValue } = args

  const tiebreaker = findTiebreakerField(model)
  if (!tiebreaker) return orderBy

  if (!isNotNullish(orderBy)) {
    return { [tiebreaker]: 'asc' }
  }

  if (hasTiebreaker(orderBy, parseValue, tiebreaker)) return orderBy
  return addTiebreaker(orderBy, tiebreaker)
}
