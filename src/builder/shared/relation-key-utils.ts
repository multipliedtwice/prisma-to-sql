import { Field } from '../../types'
import { normalizeKeyList } from './sql-utils'

export interface RelationKeys {
  childKeys: string[]
  parentKeys: string[]
}

export function resolveRelationKeys(
  field: Field,
  context: 'include' | 'count' | 'whereIn' = 'include',
): RelationKeys {
  const fkFields = normalizeKeyList(field.foreignKey)

  if (fkFields.length === 0) {
    throw new Error(
      `Relation '${field.name}' is missing foreignKey for ${context}`,
    )
  }

  const refs = normalizeKeyList(field.references)
  const refFields = refs.length > 0 ? refs : fkFields.length === 1 ? ['id'] : []

  if (refFields.length !== fkFields.length) {
    throw new Error(
      `Relation '${field.name}' references count (${refFields.length}) ` +
        `doesn't match foreignKey count (${fkFields.length}) (context: ${context})`,
    )
  }

  const childKeys = field.isForeignKeyLocal ? refFields : fkFields
  const parentKeys = field.isForeignKeyLocal ? fkFields : refFields

  return { childKeys, parentKeys }
}
