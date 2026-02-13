import { RelStats } from '../../cardinality-planner'
import { isPlainObject } from '../shared/validators/type-guards'
import { extractRelationEntries } from '../shared/relation-extraction-utils'
import { resolveRelationKeys } from '../shared/relation-key-utils'
import { Field, Model } from '../../types'
import { getRelationFieldSet } from '../shared/model-field-cache'
import { hasChildPagination } from '../shared/relation-utils'

export interface WhereInSegment {
  relationName: string
  relArgs: unknown
  childModelName: string
  fkFieldName: string
  parentKeyFieldName: string
  isList: boolean
}

export interface QueryPlan {
  filteredArgs: any
  whereInSegments: WhereInSegment[]
}

type RelationStatsLike = Record<string, Record<string, RelStats>>

const HARD_FANOUT_CAP = 5000
const MAX_ESTIMATED_ROWS = Number.MAX_SAFE_INTEGER / 1000

function isList(field: Field): boolean {
  return typeof field.type === 'string' && field.type.endsWith('[]')
}

function resolveRelation(
  model: Model,
  relName: string,
  allModels: readonly Model[],
): { field: Field; relModel: Model } | null {
  const field = model.fields.find((f) => f.name === relName)
  if (!field || !field.isRelation || !field.relatedModel) return null

  const relModel = allModels.find((m) => m.name === field.relatedModel)
  if (!relModel) return null

  return { field: field as Field, relModel }
}

function effectiveFanout(stats: RelStats): number {
  return 1 + stats.coverage * (stats.avg - 1)
}

function estimateFlatRows(
  parentCount: number,
  relations: Array<{ modelName: string; relName: string; field: Field }>,
  stats?: RelationStatsLike,
): number {
  let rows = parentCount
  for (const rel of relations) {
    const relStats = stats?.[rel.modelName]?.[rel.relName]
    const fanout = relStats ? effectiveFanout(relStats) : 10
    const next = rows * fanout
    if (next > MAX_ESTIMATED_ROWS) {
      return MAX_ESTIMATED_ROWS
    }
    rows = next
  }
  return Math.ceil(rows)
}

function collectOneToManyRelations(
  entries: Array<{ name: string; value: unknown }>,
  model: Model,
  allModels: readonly Model[],
): Array<{
  name: string
  relArgs: unknown
  field: Field
  relModel: Model
  hasPagination: boolean
}> {
  const out: Array<{
    name: string
    relArgs: unknown
    field: Field
    relModel: Model
    hasPagination: boolean
  }> = []

  for (const entry of entries) {
    const resolved = resolveRelation(model, entry.name, allModels)
    if (!resolved) continue
    if (!isList(resolved.field)) continue

    out.push({
      name: entry.name,
      relArgs: entry.value,
      field: resolved.field,
      relModel: resolved.relModel,
      hasPagination: hasChildPagination(entry.value),
    })
  }

  return out
}

function getParentCount(method: string, args: any): number | null {
  if (method === 'findFirst' || method === 'findUnique') return 1

  if (args?.take !== undefined && args?.take !== null) {
    const take = typeof args.take === 'number' ? Math.abs(args.take) : null
    if (take !== null) return take
  }

  return null
}

function buildWhereInSegment(
  name: string,
  relArgs: unknown,
  field: Field,
  relModel: Model,
): WhereInSegment | null {
  const keys = resolveRelationKeys(field, 'whereIn')
  if (keys.childKeys.length !== 1) return null

  return {
    relationName: name,
    relArgs,
    childModelName: relModel.name,
    fkFieldName: keys.childKeys[0],
    parentKeyFieldName: keys.parentKeys[0],
    isList: true,
  }
}

function deepClone<T>(obj: T): T {
  if (obj === null || typeof obj !== 'object') return obj
  if (obj instanceof Date) return new Date(obj.getTime()) as any
  if (obj instanceof RegExp) return new RegExp(obj.source, obj.flags) as any
  if (Array.isArray(obj)) return obj.map((item) => deepClone(item)) as any

  const cloned: any = {}
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      cloned[key] = deepClone((obj as any)[key])
    }
  }
  return cloned
}

function removeRelationsFromArgs(args: any, names: Set<string>): any {
  if (!args) return args

  const filtered = deepClone(args)

  if (filtered.include && isPlainObject(filtered.include)) {
    for (const name of names) {
      delete filtered.include[name]
    }
    if (Object.keys(filtered.include).length === 0) {
      delete filtered.include
    }
  }

  if (filtered.select && isPlainObject(filtered.select)) {
    for (const name of names) {
      delete filtered.select[name]
    }
  }

  return filtered
}

export function planQueryStrategy(params: {
  model: Model
  method: string
  args: any
  allModels: readonly Model[]
  relationStats?: RelationStatsLike
  dialect: 'postgres' | 'sqlite'
}): QueryPlan {
  const { model, method, args, allModels, relationStats } = params

  const entries = extractRelationEntries(args, model)
  if (entries.length === 0) {
    return { filteredArgs: args, whereInSegments: [] }
  }

  const oneToManyRels = collectOneToManyRelations(entries, model, allModels)

  const unpaginatedOneToMany = oneToManyRels.filter((r) => !r.hasPagination)

  if (unpaginatedOneToMany.length === 0) {
    return { filteredArgs: args, whereInSegments: [] }
  }

  const parentCount = getParentCount(method, args)
  const whereInSegments: WhereInSegment[] = []
  const toRemove = new Set<string>()

  if (unpaginatedOneToMany.length > 1) {
    for (const rel of unpaginatedOneToMany) {
      const segment = buildWhereInSegment(
        rel.name,
        rel.relArgs,
        rel.field,
        rel.relModel,
      )
      if (segment) {
        whereInSegments.push(segment)
        toRemove.add(rel.name)
      }
    }
  } else if (unpaginatedOneToMany.length === 1) {
    const rel = unpaginatedOneToMany[0]

    if (parentCount === null) {
      const segment = buildWhereInSegment(
        rel.name,
        rel.relArgs,
        rel.field,
        rel.relModel,
      )
      if (segment) {
        whereInSegments.push(segment)
        toRemove.add(rel.name)
      }
    } else {
      const estimated = estimateFlatRows(
        parentCount,
        [{ modelName: model.name, relName: rel.name, field: rel.field }],
        relationStats,
      )

      if (estimated > HARD_FANOUT_CAP) {
        const segment = buildWhereInSegment(
          rel.name,
          rel.relArgs,
          rel.field,
          rel.relModel,
        )
        if (segment) {
          whereInSegments.push(segment)
          toRemove.add(rel.name)
        }
      }
    }
  }

  if (toRemove.size === 0) {
    return { filteredArgs: args, whereInSegments: [] }
  }

  const filteredArgs = removeRelationsFromArgs(args, toRemove)

  return { filteredArgs, whereInSegments }
}
