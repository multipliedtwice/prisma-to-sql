import { Field, Model } from '../../types'
import { Field as ParsedField } from '@dee-wan/schema-parser'
import { getFieldIndices } from './model-field-cache'

const FIELD_BY_NAME_CACHE = new WeakMap<Model, Map<string, Field>>()

function normalizeField(field: ParsedField): Field {
  return field as unknown as Field
}

export function getPrimaryKeyFields(model: Model): string[] {
  const cached = getFieldIndices(model).pkFields
  if (cached.length > 0) return [...cached]

  const idField = model.fields.find(
    (f: any) => f.name === 'id' && !f.isRelation,
  )
  if (idField) return ['id']

  throw new Error(
    `Model ${model.name} has no primary key field. Models must have either fields with isId=true or a field named 'id'.`,
  )
}

export function getPrimaryKeyField(model: Model): string {
  const fields = getPrimaryKeyFields(model)
  if (fields.length !== 1) {
    throw new Error(
      `getPrimaryKeyField requires single-field PK, but ${model.name} has ${fields.length} fields`,
    )
  }
  return fields[0]
}

export function getFieldByName(
  model: Model,
  fieldName: string,
): Field | undefined {
  let cache = FIELD_BY_NAME_CACHE.get(model)
  if (!cache) {
    cache = new Map()
    for (const rawField of model.fields) {
      const field = normalizeField(rawField)
      cache.set(field.name, field)
    }
    FIELD_BY_NAME_CACHE.set(model, cache)
  }

  return cache.get(fieldName)
}
