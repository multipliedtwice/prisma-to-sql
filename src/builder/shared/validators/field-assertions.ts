// src/builder/shared/validators/field-assertions.ts
import { Model, Field } from '../../../types'
import { createError } from '../errors'
import { getFieldByName } from '../model-field-cache'

export function assertFieldExists(
  fieldName: string,
  model: Model,
  path: readonly string[],
): Field {
  const field = getFieldByName(model, fieldName)
  if (!field) {
    throw createError(
      `Field '${fieldName}' does not exist on model ${model.name}`,
      {
        field: fieldName,
        modelName: model.name,
        path,
      },
    )
  }
  return field
}

export function assertScalarField(
  model: Model,
  fieldName: string,
  context: string,
): void {
  const field = getFieldByName(model, fieldName)

  if (!field) {
    throw new Error(
      `${context}: field '${fieldName}' does not exist on model '${model.name}'`,
    )
  }

  if (field.isRelation) {
    throw new Error(
      `${context}: field '${fieldName}' is a relation field, expected scalar field`,
    )
  }
}

export function assertNumericField(
  model: Model,
  fieldName: string,
  context: string,
): void {
  assertScalarField(model, fieldName, context)

  const field = getFieldByName(model, fieldName)
  if (!field) return

  const numericTypes = new Set([
    'Int',
    'BigInt',
    'Float',
    'Decimal',
    'Int?',
    'BigInt?',
    'Float?',
    'Decimal?',
  ])

  if (!numericTypes.has(field.type)) {
    throw new Error(
      `${context}: field '${fieldName}' must be numeric (Int, BigInt, Float, Decimal), got '${field.type}'`,
    )
  }
}

export function assertValidOperator(
  fieldName: string,
  operator: string,
  fieldType: string,
  path: readonly string[],
  modelName: string,
): void {
  const stringOps = new Set([
    'equals',
    'not',
    'in',
    'notIn',
    'lt',
    'lte',
    'gt',
    'gte',
    'contains',
    'startsWith',
    'endsWith',
    'mode',
    'search',
  ])

  const numericOps = new Set([
    'equals',
    'not',
    'in',
    'notIn',
    'lt',
    'lte',
    'gt',
    'gte',
  ])

  const jsonOps = new Set([
    'equals',
    'not',
    'path',
    'string_contains',
    'string_starts_with',
    'string_ends_with',
    'array_contains',
    'array_starts_with',
    'array_ends_with',
  ])

  const isString = fieldType === 'String' || fieldType === 'String?'
  const isNumeric = ['Int', 'BigInt', 'Float', 'Decimal'].some(
    (t) => fieldType === t || fieldType === `${t}?`,
  )
  const isJson = fieldType === 'Json' || fieldType === 'Json?'

  if (isString && !stringOps.has(operator)) {
    throw createError(
      `Operator '${operator}' is not valid for String field '${fieldName}'`,
      { field: fieldName, modelName, path, operator },
    )
  }

  if (isNumeric && !numericOps.has(operator)) {
    throw createError(
      `Operator '${operator}' is not valid for numeric field '${fieldName}'`,
      { field: fieldName, modelName, path, operator },
    )
  }

  if (isJson && !jsonOps.has(operator)) {
    throw createError(
      `Operator '${operator}' is not valid for Json field '${fieldName}'`,
      { field: fieldName, modelName, path, operator },
    )
  }
}
