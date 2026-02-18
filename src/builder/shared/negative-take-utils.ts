import { reverseOrderByInput } from './order-by-utils'

type IntOrDynamic = number | string
type OptionalIntOrDynamic = IntOrDynamic | undefined

export function maybeReverseNegativeTake(
  takeVal: OptionalIntOrDynamic,
  hasOrderBy: boolean,
  orderByInput: unknown,
): { takeVal: OptionalIntOrDynamic; orderByInput: unknown } {
  if (typeof takeVal !== 'number') return { takeVal, orderByInput }
  if (takeVal >= 0) return { takeVal, orderByInput }
  if (!hasOrderBy) {
    throw new Error('Negative take requires orderBy for deterministic results')
  }
  return {
    takeVal: Math.abs(takeVal),
    orderByInput: reverseOrderByInput(orderByInput),
  }
}
