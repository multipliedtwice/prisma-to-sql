import type { Model } from '../../types'
import { extractNestedIncludeSpec } from './relation-utils'
import { isPlainObject } from './validators/type-guards'
import { getFieldIndices } from './model-field-cache'

type ModelField = Model['fields'][number]

export interface ResolvedRelation {
  relName: string
  value: unknown
  field: ModelField
  relModel: Model
  isList: boolean
  nestedSpec: Record<string, any>
}

const MODEL_MAP_CACHE = new WeakMap<readonly Model[], Map<string, Model>>()

export function getOrCreateModelMap(
  schemas: readonly Model[],
): Map<string, Model> {
  let map = MODEL_MAP_CACHE.get(schemas)
  if (map) return map
  map = new Map<string, Model>()
  for (const m of schemas) map.set(m.name, m)
  MODEL_MAP_CACHE.set(schemas, map)
  return map
}

export function resolveIncludeRelations(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
  modelMap?: Map<string, Model>,
): ResolvedRelation[] {
  const map = modelMap ?? getOrCreateModelMap(schemas)
  const results: ResolvedRelation[] = []

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = getFieldIndices(model).allFieldsByName.get(relName)
    if (!field?.isRelation || !field.relatedModel) continue

    const relModel = map.get(field.relatedModel)
    if (!relModel) continue

    const isList = typeof field.type === 'string' && field.type.endsWith('[]')

    const nestedSpec = isPlainObject(value)
      ? extractNestedIncludeSpec(value, relModel)
      : {}

    results.push({
      relName,
      value,
      field: field as unknown as ModelField,
      relModel,
      isList,
      nestedSpec,
    })
  }

  return results
}
