import type { Model } from '../../types'
import { extractNestedIncludeSpec } from './relation-utils'
import { isPlainObject } from './validators/type-guards'

type ModelField = Model['fields'][number]

export interface ResolvedRelation {
  relName: string
  value: unknown
  field: ModelField
  relModel: Model
  isList: boolean
  nestedSpec: Record<string, any>
}

export function resolveIncludeRelations(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): ResolvedRelation[] {
  const modelMap = new Map(schemas.map((m) => [m.name, m]))
  const results: ResolvedRelation[] = []

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = model.fields.find((f) => f.name === relName)
    if (!field?.isRelation || !field.relatedModel) continue

    const relModel = modelMap.get(field.relatedModel)
    if (!relModel) continue

    const isList = typeof field.type === 'string' && field.type.endsWith('[]')

    const nestedSpec = isPlainObject(value)
      ? extractNestedIncludeSpec(value, relModel)
      : {}

    results.push({
      relName,
      value,
      field,
      relModel,
      isList,
      nestedSpec,
    })
  }

  return results
}
