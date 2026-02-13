import { Model } from '../../types'
import { getRelationFieldSet } from './model-field-cache'
import { isPlainObject } from './validators/type-guards'

export function hasChildPagination(relArgs: unknown): boolean {
  if (!isPlainObject(relArgs)) return false
  const args = relArgs as Record<string, unknown>
  if (args.take !== undefined && args.take !== null) return true
  if (args.skip !== undefined && args.skip !== null) return true
  return false
}

export function extractScalarSelection(
  relArgs: unknown,
  relModel: Model,
): { includeAllScalars: boolean; selectedScalarFields: string[] } {
  const scalarFields = relModel.fields
    .filter((f) => !f.isRelation)
    .map((f) => f.name)
  const scalarSet = new Set(scalarFields)

  if (relArgs === true || !isPlainObject(relArgs)) {
    return { includeAllScalars: true, selectedScalarFields: scalarFields }
  }

  const obj = relArgs as Record<string, unknown>
  if (!isPlainObject(obj.select)) {
    return { includeAllScalars: true, selectedScalarFields: scalarFields }
  }

  const sel = obj.select as Record<string, unknown>
  const selected: string[] = []
  for (const [k, v] of Object.entries(sel)) {
    if (!scalarSet.has(k)) continue
    if (v === true) selected.push(k)
  }

  return { includeAllScalars: false, selectedScalarFields: selected }
}

export function extractNestedIncludeSpec(
  relArgs: unknown,
  relModel: Model,
): Record<string, any> {
  const relationSet = getRelationFieldSet(relModel)
  const out: Record<string, any> = {}

  if (!isPlainObject(relArgs)) return out
  const obj = relArgs as Record<string, unknown>

  if (isPlainObject(obj.include)) {
    for (const [k, v] of Object.entries(
      obj.include as Record<string, unknown>,
    )) {
      if (!relationSet.has(k)) continue
      if (v === false) continue
      out[k] = v
    }
  }

  if (isPlainObject(obj.select)) {
    for (const [k, v] of Object.entries(
      obj.select as Record<string, unknown>,
    )) {
      if (!relationSet.has(k)) continue
      if (v === false) continue
      if (v === true) {
        out[k] = true
        continue
      }
      if (isPlainObject(v)) {
        const vv = v as Record<string, unknown>
        if (isPlainObject(vv.include) || isPlainObject(vv.select)) {
          out[k] = v
        }
      }
    }
  }

  return out
}

export interface RelationEntry {
  name: string
  value: unknown
}

export function extractRelationEntries(
  args: { include?: unknown; select?: unknown },
  model: Model,
): RelationEntry[] {
  const relationSet = getRelationFieldSet(model)
  const entries: RelationEntry[] = []
  const seen = new Set<string>()

  const scanSource = (source: unknown): void => {
    if (!isPlainObject(source)) return

    for (const [key, value] of Object.entries(source)) {
      if (!relationSet.has(key)) continue
      if (value === false) continue
      if (seen.has(key)) continue

      seen.add(key)
      entries.push({ name: key, value })
    }
  }

  scanSource(args.include)
  scanSource(args.select)

  return entries
}
