import { Model, Field } from '../types'
import { SPECIAL_FIELDS } from './shared/constants'
import { createError } from './shared/errors'
import { getRelationFieldSet } from './shared/model-field-cache'
import { quoteColumn, normalizeKeyList } from './shared/sql-utils'
import { isNotNullish } from './shared/validators/type-guards'

export function isRelationField(fieldName: string, model: Model): boolean {
  return getRelationFieldSet(model).has(fieldName)
}

export function isValidRelationField(field: Field | undefined): field is Field {
  if (!isNotNullish(field)) return false
  if (!field.isRelation) return false
  if (
    !isNotNullish(field.relatedModel) ||
    field.relatedModel.trim().length === 0
  )
    return false

  const fk = normalizeKeyList((field as any).foreignKey)
  if (fk.length === 0) return false

  const refsRaw = (field as any).references
  const refs = normalizeKeyList(refsRaw)

  if (refs.length === 0) return false
  if (refs.length !== fk.length) return false

  return true
}

function getReferenceFieldNames(
  field: Field,
  foreignKeyCount: number,
): string[] {
  const refsRaw = (field as any).references
  const refs = normalizeKeyList(refsRaw)

  if (refs.length === 0) {
    if (foreignKeyCount === 1) return [SPECIAL_FIELDS.ID]
    return []
  }

  if (refs.length !== foreignKeyCount) return []
  return refs
}

export function joinCondition(
  field: Field,
  parentModel: Model,
  childModel: Model,
  parentAlias: string,
  childAlias: string,
): string {
  const fkFields = normalizeKeyList((field as any).foreignKey)

  if (fkFields.length === 0) {
    throw createError(
      `Relation '${field.name}' is missing foreignKey. This indicates a schema parsing error. Relations must specify fields/references.`,
      { field: field.name },
    )
  }

  const refFields = getReferenceFieldNames(field, fkFields.length)

  if (refFields.length !== fkFields.length) {
    throw createError(
      `Relation '${field.name}' is missing references (or references count does not match foreignKey count). This is required to support non-id and composite keys.`,
      { field: field.name },
    )
  }

  const parts: string[] = []

  for (let i = 0; i < fkFields.length; i++) {
    const fk = fkFields[i]
    const ref = refFields[i]

    const left = field.isForeignKeyLocal
      ? `${childAlias}.${quoteColumn(childModel, ref)}`
      : `${childAlias}.${quoteColumn(childModel, fk)}`
    const right = field.isForeignKeyLocal
      ? `${parentAlias}.${quoteColumn(parentModel, fk)}`
      : `${parentAlias}.${quoteColumn(parentModel, ref)}`

    parts.push(`${left} = ${right}`)
  }

  return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
}

export function getModelByName(
  schemas: Model[],
  name: string,
): Model | undefined {
  return schemas.find((m) => m.name === name)
}
