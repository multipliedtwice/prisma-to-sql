import { Ops, SQL_TEMPLATES } from './constants'
import { createError } from './errors'

export function buildNullComparison(
  expr: string,
  op: string,
  allowNull: boolean = false,
): string | null {
  if (op === Ops.EQUALS) return `${expr} ${SQL_TEMPLATES.IS_NULL}`
  if (op === Ops.NOT) return `${expr} ${SQL_TEMPLATES.IS_NOT_NULL}`
  return null
}

export function tryBuildNullComparison(
  expr: string,
  op: string,
  val: unknown,
  context: string,
): string | null {
  if (val !== null) return null

  const clause = buildNullComparison(expr, op)
  if (clause === null) {
    throw createError(`Operator '${op}' doesn't support null in ${context}`, {
      operator: op,
    })
  }
  return clause
}
