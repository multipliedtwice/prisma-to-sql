import { Model } from '../../../types'
import { createError } from '../errors'
import { getFieldInfo } from '../model-field-cache'

const NUMERIC_TYPES = new Set(['Int', 'Float', 'Decimal', 'BigInt'])

export interface FieldInfo {
  name: string
  type: string
  dbName: string
  isRelation: boolean
  isRequired: boolean
}

export function assertScalarField(
  model: Model,
  fieldName: string,
  context: string,
): FieldInfo {
  const field = getFieldInfo(model, fieldName)

  if (!field) {
    throw createError(
      `${context} references unknown field '${fieldName}' on model ${model.name}`,
      {
        field: fieldName,
        modelName: model.name,
        availableFields: model.fields.map((f) => f.name),
      },
    )
  }

  if (field.isRelation) {
    throw createError(
      `${context} does not support relation field '${fieldName}'`,
      { field: fieldName, modelName: model.name },
    )
  }

  return field
}

export function assertNumericField(
  model: Model,
  fieldName: string,
  context: string,
): FieldInfo {
  const field = assertScalarField(model, fieldName, context)

  const baseType = field.type.replace(/\[\]|\?/g, '')
  if (!NUMERIC_TYPES.has(baseType)) {
    throw createError(
      `${context} requires numeric field, got '${field.type}'`,
      { field: fieldName, modelName: model.name },
    )
  }

  return field
}

export function assertFieldType(
  field: FieldInfo,
  allowedTypes: Set<string>,
  context: string,
  modelName: string,
): void {
  const baseType = field.type.replace(/\[\]|\?/g, '')

  if (!allowedTypes.has(baseType)) {
    throw createError(`${context} does not support type '${field.type}'`, {
      field: field.name,
      modelName,
    })
  }
}

export function isNumericType(fieldType: string): boolean {
  const baseType = fieldType.replace(/\[\]|\?/g, '')
  return NUMERIC_TYPES.has(baseType)
}
