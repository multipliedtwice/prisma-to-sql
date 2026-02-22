import type { Model } from '../../types'
import type { WhereInSegment } from '../select/segment-planner'
import { getPrimaryKeyField } from './primary-key-utils'

const MAX_RECURSIVE_DEPTH = 10

function buildParentKeyIndex(
  parentRows: any[],
  parentKeyFieldName: string,
): Map<unknown, any[]> {
  const index = new Map<unknown, any[]>()
  for (const parent of parentRows) {
    const keyVal = parent[parentKeyFieldName]
    if (keyVal == null) continue
    let arr = index.get(keyVal)
    if (!arr) {
      arr = []
      index.set(keyVal, arr)
    }
    arr.push(parent)
  }
  return index
}

function needsPerParentPagination(segment: WhereInSegment): boolean {
  return (
    segment.isList &&
    ((segment.perParentSkip != null && segment.perParentSkip > 0) ||
      segment.perParentTake != null)
  )
}

function stitchChildrenToParents(
  children: any[],
  segment: WhereInSegment,
  parentKeyIndex: Map<unknown, any[]>,
): void {
  const grouped = new Map<unknown, any[]>()
  for (const child of children) {
    const childKey = child[segment.fkFieldName]
    if (childKey == null) continue
    let arr = grouped.get(childKey)
    if (!arr) {
      arr = []
      grouped.set(childKey, arr)
    }
    arr.push(child)
  }

  const perParentPaginated = needsPerParentPagination(segment)

  for (const [fkVal, groupedChildren] of grouped) {
    const matchingParents = parentKeyIndex.get(fkVal)
    if (!matchingParents) continue

    let sliced = groupedChildren
    if (perParentPaginated) {
      const start = segment.perParentSkip || 0
      const end =
        segment.perParentTake != null
          ? start + segment.perParentTake
          : undefined
      sliced = groupedChildren.slice(start, end)
    }

    for (const parent of matchingParents) {
      if (segment.isList) {
        if (!Array.isArray(parent[segment.relationName])) {
          parent[segment.relationName] = []
        }
        for (const child of sliced) {
          parent[segment.relationName].push(child)
        }
      } else {
        parent[segment.relationName] = sliced[0] ?? null
      }
    }
  }
}

function buildChildArgs(
  relArgs: unknown,
  fkFieldName: string,
  uniqueIds: unknown[],
  stripPagination: boolean,
): any {
  const isObject =
    relArgs !== true && typeof relArgs === 'object' && relArgs !== null

  const source = isObject ? (relArgs as any) : null

  const base: any = {}

  if (source) {
    if (source.select !== undefined) base.select = source.select
    if (source.include !== undefined) base.include = source.include
    if (source.orderBy !== undefined) base.orderBy = source.orderBy
    if (!stripPagination) {
      if (source.take !== undefined) base.take = source.take
      if (source.skip !== undefined) base.skip = source.skip
    }
    if (source.cursor !== undefined) base.cursor = source.cursor
    if (source.distinct !== undefined) base.distinct = source.distinct
  }

  const inCondition = { [fkFieldName]: { in: uniqueIds } }

  base.where =
    source && source.where ? { AND: [source.where, inCondition] } : inCondition

  return base
}

function ensureFkInSelect(childArgs: any, fkFieldName: string): boolean {
  if (!childArgs.select) return false
  if (childArgs.select[fkFieldName]) return false
  childArgs.select = { ...childArgs.select, [fkFieldName]: true }
  return true
}

function ensureOrderByPk(childArgs: any, childModel: Model): void {
  if (childArgs.orderBy) return
  const pkField = getPrimaryKeyField(childModel)
  childArgs.orderBy = { [pkField]: 'asc' }
}

function initRelationPlaceholders(row: any, segments: WhereInSegment[]): void {
  for (const seg of segments) {
    row[seg.relationName] = seg.isList ? [] : null
  }
}

export {
  MAX_RECURSIVE_DEPTH,
  buildParentKeyIndex,
  needsPerParentPagination,
  stitchChildrenToParents,
  buildChildArgs,
  ensureFkInSelect,
  ensureOrderByPk,
  initRelationPlaceholders,
}
