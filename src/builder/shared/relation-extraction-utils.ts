import { Model } from '../../types'
import { getFieldIndices } from './model-field-cache'
import { isPlainObject } from './validators/type-guards'

export interface RelationEntry {
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

export function extractRelationEntriesFromSelect(
  select: unknown,
  model: Model,
): RelationEntry[] {
  if (!isPlainObject(select)) return []

  const indices = getFieldIndices(model)
  const entries: RelationEntry[] = []

  for (const key in select) {
    if (!Object.prototype.hasOwnProperty.call(select, key)) continue
    if (!indices.relationFields.has(key)) continue

    const value = (select as any)[key]
    if (value === false) continue
    if (value === true) continue
    if (!isPlainObject(value)) continue

    const v = value as Record<string, unknown>
    if (v.include || v.select) {
      entries.push({ name: key, value })
    }
  }

  return entries
}
