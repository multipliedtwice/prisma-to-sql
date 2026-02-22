import { Field } from '../../types'
import { normalizeKeyList } from './sql-utils'

interface RelationKeys {
  childKeys: string[]
  parentKeys: string[]
}

const RELATION_KEYS_CACHE = new WeakMap<Field, RelationKeys>()
const RELATION_KEYS_INVALID = new WeakSet<Field>()

function computeRelationKeys(field: Field, context: string): RelationKeys {
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

export function resolveRelationKeys(
  field: Field,
  context: 'include' | 'count' | 'whereIn' = 'include',
): RelationKeys {
  let cached = RELATION_KEYS_CACHE.get(field)
  if (cached) return cached

  cached = computeRelationKeys(field, context)
  RELATION_KEYS_CACHE.set(field, cached)
  return cached
}

export function tryResolveRelationKeys(field: Field): RelationKeys | null {
  const cached = RELATION_KEYS_CACHE.get(field)
  if (cached) return cached

  if (RELATION_KEYS_INVALID.has(field)) return null

  try {
    const keys = computeRelationKeys(field, 'include')
    RELATION_KEYS_CACHE.set(field, keys)
    return keys
  } catch {
    RELATION_KEYS_INVALID.add(field)
    return null
  }
}
