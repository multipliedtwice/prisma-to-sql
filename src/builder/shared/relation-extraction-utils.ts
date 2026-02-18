import { Model } from '../../types'
import { getFieldIndices } from './model-field-cache'
import { isPlainObject } from './validators/type-guards'

interface RelationEntry {
  name: string
  value: unknown
}

export function extractRelationEntries(
  args: { include?: unknown; select?: unknown },
  model: Model,
): RelationEntry[] {
  const indices = getFieldIndices(model)
  const entries: RelationEntry[] = []
  const seen = new Set<string>()

  const sources = [
    args.include && isPlainObject(args.include) ? args.include : null,
    args.select && isPlainObject(args.select) ? args.select : null,
  ]

  for (const source of sources) {
    if (!source) continue

    for (const key in source) {
      if (!Object.prototype.hasOwnProperty.call(source, key)) continue
      if (!indices.relationFields.has(key)) continue
      if (seen.has(key)) continue

      const value = (source as any)[key]
      if (value === false) continue

      seen.add(key)
      entries.push({ name: key, value })
    }
  }

  return entries
}
