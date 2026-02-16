import type { Model } from '../../types'
import { extractNestedIncludeSpec } from '../shared/relation-utils'
import { isPlainObject } from '../shared/validators/type-guards'

type RelStats = {
  avg: number
  p95: number
  p99: number
  max: number
  coverage: number
}

type RelationStatsMap = Record<string, Record<string, RelStats>>

let globalRelationStats: RelationStatsMap | undefined

const DEFAULT_LIST_FANOUT = 10
const DEFAULT_ONE_FANOUT = 1
const MAX_PAGINATED_TOTAL_ROWS = 10000
const MAX_NON_PAGINATED_EXPANSION = 1000
const DEFAULT_TAKE_ESTIMATE = 100

export function setRelationStats(stats: RelationStatsMap): void {
  globalRelationStats = stats
}

export function getRelationStats(): RelationStatsMap | undefined {
  return globalRelationStats
}

function getEffectiveFanout(
  modelName: string,
  relName: string,
  isList: boolean,
  relArgs: unknown,
): number {
  if (!isList) return DEFAULT_ONE_FANOUT

  const stats = globalRelationStats?.[modelName]?.[relName]
  const statsFanout = stats
    ? Math.max(stats.p95, stats.avg)
    : DEFAULT_LIST_FANOUT

  if (isPlainObject(relArgs)) {
    const obj = relArgs as Record<string, unknown>
    if ('take' in obj && typeof obj.take === 'number' && obj.take > 0) {
      return Math.min(statsFanout, obj.take)
    }
  }

  return Math.max(1, statsFanout)
}

export function countIncludeDepth(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): number {
  const modelMap = new Map(schemas.map((m) => [m.name, m]))
  let maxDepth = 0

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = model.fields.find((f) => f.name === relName)
    if (!field?.isRelation || !field.relatedModel) continue

    const relModel = modelMap.get(field.relatedModel)
    if (!relModel) continue

    let childDepth = 1

    const nestedSpec = isPlainObject(value)
      ? extractNestedIncludeSpec(value, relModel)
      : {}

    if (Object.keys(nestedSpec).length > 0) {
      childDepth += countIncludeDepth(nestedSpec, relModel, schemas)
    }

    if (childDepth > maxDepth) maxDepth = childDepth
  }

  return maxDepth
}

function countSiblingListRelations(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): number {
  let listCount = 0

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = model.fields.find((f) => f.name === relName)
    if (!field?.isRelation) continue

    const isList = typeof field.type === 'string' && field.type.endsWith('[]')
    if (isList) listCount++
  }

  return listCount
}

function hasMultipleSiblingListsAnywhere(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): boolean {
  const modelMap = new Map(schemas.map((m) => [m.name, m]))

  if (countSiblingListRelations(includeSpec, model, schemas) > 1) return true

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = model.fields.find((f) => f.name === relName)
    if (!field?.isRelation || !field.relatedModel) continue

    const relModel = modelMap.get(field.relatedModel)
    if (!relModel) continue

    const nestedSpec = isPlainObject(value)
      ? extractNestedIncludeSpec(value, relModel)
      : {}

    if (Object.keys(nestedSpec).length > 0) {
      if (hasMultipleSiblingListsAnywhere(nestedSpec, relModel, schemas)) {
        return true
      }
    }
  }

  return false
}

export function estimateJoinExpansion(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): number {
  const modelMap = new Map(schemas.map((m) => [m.name, m]))
  let total = 1

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = model.fields.find((f) => f.name === relName)
    if (!field?.isRelation || !field.relatedModel) continue

    const isList = typeof field.type === 'string' && field.type.endsWith('[]')
    total *= getEffectiveFanout(model.name, relName, isList, value)

    const relModel = modelMap.get(field.relatedModel)
    if (!relModel) continue

    const nestedSpec = isPlainObject(value)
      ? extractNestedIncludeSpec(value, relModel)
      : {}

    if (Object.keys(nestedSpec).length > 0) {
      total *= estimateJoinExpansion(nestedSpec, relModel, schemas)
    }
  }

  return total
}

export function shouldPreferFlatJoinStrategy(params: {
  includeSpec: Record<string, any>
  model: Model
  schemas: readonly Model[]
  hasPagination: boolean
  takeValue: number | null
  canUseFlatJoin: boolean
  debug?: boolean
  source?: string
}): boolean {
  const {
    includeSpec,
    model,
    schemas,
    hasPagination,
    takeValue,
    canUseFlatJoin,
    debug,
    source,
  } = params

  if (Object.keys(includeSpec).length === 0) {
    return false
  }

  if (hasMultipleSiblingListsAnywhere(includeSpec, model, schemas)) {
    return false
  }

  const expansion = estimateJoinExpansion(includeSpec, model, schemas)
  const depth = countIncludeDepth(includeSpec, model, schemas)

  if (hasPagination) {
    const effectiveTake = takeValue ?? DEFAULT_TAKE_ESTIMATE
    const totalRows = effectiveTake * expansion
    const result = totalRows <= MAX_PAGINATED_TOTAL_ROWS

    return result
  }

  const result = depth >= 2 && expansion <= MAX_NON_PAGINATED_EXPANSION

  return result
}
