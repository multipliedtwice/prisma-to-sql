import { Model, Field } from '../../../types'
import { createError } from '../errors'
import { getFieldByName } from '../primary-key-utils'

export function assertFieldExists(
  fieldName: string,
  model: Model,
  context: string,
  path: readonly string[] = [],
): Field {
  const field = getFieldByName(model, fieldName)
  if (!field) {
    throw createError(
      `${context}: field '${fieldName}' does not exist on model '${model.name}'`,
      {
        field: fieldName,
        modelName: model.name,
        path,
        availableFields: model.fields.map((f) => f.name),
      },
    )
  }
  return field
}

export function assertScalarField(
  model: Model,
  fieldName: string,
  context: string,
): Field {
  const field = assertFieldExists(fieldName, model, context)

  if (field.isRelation) {
    throw createError(
      `${context}: field '${fieldName}' is a relation field, expected scalar field`,
      { field: fieldName, modelName: model.name },
    )
  }

  return field
}

export function assertNumericField(
  model: Model,
  fieldName: string,
  context: string,
): Field {
  const field = assertScalarField(model, fieldName, context)

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
    throw createError(
      `${context}: field '${fieldName}' must be numeric (Int, BigInt, Float, Decimal), got '${field.type}'`,
      { field: fieldName, modelName: model.name },
    )
  }

  return field
}
