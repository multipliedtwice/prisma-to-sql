import { Model, Field } from '../types'
import { SPECIAL_FIELDS } from './shared/constants'
import { createError } from './shared/errors'
import { getRelationFieldSet } from './shared/model-field-cache'
import {
  assertSafeAlias,
  normalizeKeyList,
  quoteColumn,
} from './shared/sql-utils'
import {
  resolveRelationKeys,
  tryResolveRelationKeys,
} from './shared/relation-key-utils'
import { isNotNullish } from './shared/validators/type-guards'

export function isRelationField(fieldName: string, model: Model): boolean {
  return getRelationFieldSet(model).has(fieldName)
}

export function isValidRelationField(field: any): field is Field {
  if (!isNotNullish(field)) return false
  if (!field.isRelation) return false
  if (
    !isNotNullish(field.relatedModel) ||
    field.relatedModel.trim().length === 0
  )
    return false

  const fk = normalizeKeyList(field.foreignKey)
  if (fk.length === 0) return false

  const refs = normalizeKeyList(field.references)

  if (refs.length === 0) {
    return fk.length === 1
  }

  if (refs.length !== fk.length) return false

  return true
}

export function joinCondition(
  field: Field,
  parentModel: Model,
  childModel: Model,
  parentAlias: string,
  childAlias: string,
): string {
  assertSafeAlias(parentAlias)
  assertSafeAlias(childAlias)

  const { childKeys, parentKeys } = resolveRelationKeys(field, 'include')

  const parts: string[] = []

  for (let i = 0; i < parentKeys.length; i++) {
    const left = field.isForeignKeyLocal
      ? `${childAlias}.${quoteColumn(childModel, childKeys[i])}`
      : `${childAlias}.${quoteColumn(childModel, childKeys[i])}`
    const right = field.isForeignKeyLocal
      ? `${parentAlias}.${quoteColumn(parentModel, parentKeys[i])}`
      : `${parentAlias}.${quoteColumn(parentModel, parentKeys[i])}`

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
