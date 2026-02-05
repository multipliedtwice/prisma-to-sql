import { Model } from '../../types'
import { getScalarFieldSet } from './model-field-cache'
import { isNotNullish } from './validators/type-guards'
import { normalizeOrderByInput } from './order-by-utils'

type OrderByDirection = 'asc' | 'desc'
type NullsPosition = 'first' | 'last'

export type ParsedOrderByValue = {
  direction: OrderByDirection
  nulls?: NullsPosition
}

type ParseOrderByValue = (v: unknown, field?: string) => ParsedOrderByValue

function modelHasScalarId(model?: Model): boolean {
  if (!model) return false
  return getScalarFieldSet(model).has('id')
}

function hasIdTiebreaker(orderBy: unknown, parse: ParseOrderByValue): boolean {
  if (!isNotNullish(orderBy)) return false
  const normalized = normalizeOrderByInput(orderBy, parse)
  return normalized.some((obj) =>
    Object.prototype.hasOwnProperty.call(obj, 'id'),
  )
}

function addIdTiebreaker(orderBy: unknown): unknown {
  if (Array.isArray(orderBy)) return [...orderBy, { id: 'asc' }]
  return [orderBy, { id: 'asc' }]
}

export function ensureDeterministicOrderByInput(args: {
  orderBy: unknown
  model?: Model
  parseValue: ParseOrderByValue
}): unknown {
  const { orderBy, model, parseValue } = args

  if (!modelHasScalarId(model)) return orderBy

  if (!isNotNullish(orderBy)) {
    return { id: 'asc' }
  }

  if (hasIdTiebreaker(orderBy, parseValue)) return orderBy
  return addIdTiebreaker(orderBy)
}
