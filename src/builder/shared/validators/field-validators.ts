import { Model, Field } from '../../../types'
import { Ops } from '../constants'
import { createError } from '../errors'
import { isNotNullish, isArrayType, isJsonType } from './type-guards'

export function assertFieldExists(
  name: string,
  model: Model,
  path: readonly string[],
): Field {
  const field = model.fields.find((f) => f.name === name)
  if (!isNotNullish(field)) {
    throw createError(`Field '${name}' does not exist on '${model.name}'`, {
      field: name,
      path,
      modelName: model.name,
      availableFields: model.fields.map((f) => f.name),
    })
  }
  return field
}

export function assertValidOperator(
  fieldName: string,
  op: string,
  fieldType: string | undefined,
  path: readonly string[],
  modelName: string,
): void {
  if (!isNotNullish(fieldType)) return

  const ARRAY_OPS = new Set([
    Ops.HAS,
    Ops.HAS_SOME,
    Ops.HAS_EVERY,
    Ops.IS_EMPTY,
  ])
  const JSON_OPS = new Set([
    Ops.PATH,
    Ops.STRING_CONTAINS,
    Ops.STRING_STARTS_WITH,
    Ops.STRING_ENDS_WITH,
  ])

  const isArrayOp = ARRAY_OPS.has(op as any)
  const isFieldArray = isArrayType(fieldType)
  const arrayOpMismatch = isArrayOp && !isFieldArray

  if (arrayOpMismatch) {
    throw createError(`'${op}' requires array field, got '${fieldType}'`, {
      operator: op,
      field: fieldName,
      path,
      modelName,
    })
  }

  const isJsonOp = JSON_OPS.has(op as any)
  const isFieldJson = isJsonType(fieldType)
  const jsonOpMismatch = isJsonOp && !isFieldJson

  if (jsonOpMismatch) {
    throw createError(`'${op}' requires JSON field, got '${fieldType}'`, {
      operator: op,
      field: fieldName,
      path,
      modelName,
    })
  }
}
