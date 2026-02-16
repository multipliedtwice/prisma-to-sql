import type { Model } from '../../types'
import { getPrimaryKeyField } from '../shared/primary-key-utils'
import type { WhereInSegment } from '../select/segment-planner'
import {
  buildArrayAggReducerConfig,
  buildReducerConfig,
  buildSQL,
  reduceArrayAggRows,
  reduceFlatRows,
} from '../..'

const MAX_RECURSIVE_DEPTH = 10

interface StreamingWhereInParams {
  segments: WhereInSegment[]
  parentSql: string
  parentParams: unknown[]
  parentModel: Model
  allModels: readonly Model[]
  modelMap: Map<string, Model>
  dialect: 'postgres' | 'sqlite'
  execute: (sql: string, params: unknown[]) => Promise<any[]>
}

interface PreFetchedWhereInParams {
  segments: WhereInSegment[]
  parentRows: any[]
  parentModel: Model
  allModels: readonly Model[]
  modelMap: Map<string, Model>
  dialect: 'postgres' | 'sqlite'
  execute: (sql: string, params: unknown[]) => Promise<any[]>
}

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
  const base: any =
    relArgs === true || typeof relArgs !== 'object' || relArgs === null
      ? {}
      : { ...(relArgs as any) }

  if (stripPagination) {
    delete base.take
    delete base.skip
  }

  const existingWhere = base.where
  const inCondition = { [fkFieldName]: { in: uniqueIds } }

  base.where = existingWhere
    ? { AND: [existingWhere, inCondition] }
    : inCondition

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

export async function executeWhereInSegmentsStreaming(
  params: StreamingWhereInParams,
): Promise<any[]> {
  const {
    segments,
    parentSql,
    parentParams,
    parentModel,
    allModels,
    modelMap,
    dialect,
    execute,
  } = params

  if (segments.length === 0) {
    throw new Error('executeWhereInSegmentsStreaming requires segments')
  }

  if (dialect !== 'postgres') {
    throw new Error('Streaming WHERE IN requires postgres dialect')
  }

  const parentRows = await execute(parentSql, parentParams)

  if (parentRows.length === 0) return []

  for (const row of parentRows) {
    for (const seg of segments) {
      row[seg.relationName] = seg.isList ? [] : null
    }
  }

  await resolveSegments(
    segments,
    parentRows,
    allModels,
    modelMap,
    dialect,
    execute,
    0,
  )

  return parentRows
}

export async function executeWithPreFetchedParents(
  params: PreFetchedWhereInParams,
): Promise<any[]> {
  const {
    segments,
    parentRows,
    parentModel,
    allModels,
    modelMap,
    dialect,
    execute,
  } = params

  if (segments.length === 0) return parentRows
  if (parentRows.length === 0) return []

  for (const row of parentRows) {
    for (const seg of segments) {
      row[seg.relationName] = seg.isList ? [] : null
    }
  }

  await resolveSegments(
    segments,
    parentRows,
    allModels,
    modelMap,
    dialect,
    execute,
    0,
  )

  return parentRows
}

async function resolveSegments(
  segments: WhereInSegment[],
  parentRows: any[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: (sql: string, params: unknown[]) => Promise<any[]>,
  depth: number,
): Promise<void> {
  if (depth > MAX_RECURSIVE_DEPTH) return
  if (segments.length === 0) return

  if (segments.length === 1) {
    await resolveSingleSegment(
      segments[0],
      parentRows,
      allModels,
      modelMap,
      dialect,
      execute,
      depth,
    )
    return
  }

  await Promise.all(
    segments.map((seg) =>
      resolveSingleSegment(
        seg,
        parentRows,
        allModels,
        modelMap,
        dialect,
        execute,
        depth,
      ),
    ),
  )
}

async function resolveSingleSegment(
  segment: WhereInSegment,
  parentRows: any[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: (sql: string, params: unknown[]) => Promise<any[]>,
  depth: number,
): Promise<void> {
  const childModel = modelMap.get(segment.childModelName)
  if (!childModel) {
    return
  }

  const parentIds = parentRows
    .map((r) => r[segment.parentKeyFieldName])
    .filter((v) => v != null)

  if (parentIds.length === 0) {
    return
  }

  const uniqueIds = [...new Set(parentIds)]
  const stripPagination = needsPerParentPagination(segment)

  const childArgs = buildChildArgs(
    segment.relArgs,
    segment.fkFieldName,
    uniqueIds,
    stripPagination,
  )

  ensureOrderByPk(childArgs, childModel)

  const needsStripFk = ensureFkInSelect(childArgs, segment.fkFieldName)

  const result = buildSQL(
    childModel,
    allModels as Model[],
    'findMany',
    childArgs,
    dialect,
  )

  let children = await execute(result.sql, result.params as unknown[])

  if (result.isArrayAgg && result.includeSpec) {
    const config = buildArrayAggReducerConfig(
      childModel,
      result.includeSpec,
      allModels,
    )
    children = reduceArrayAggRows(children, config)
  } else if (result.requiresReduction && result.includeSpec) {
    const config = buildReducerConfig(childModel, result.includeSpec, allModels)
    children = reduceFlatRows(children, config)
  }

  const parentKeyIndex = buildParentKeyIndex(
    parentRows,
    segment.parentKeyFieldName,
  )
  stitchChildrenToParents(children, segment, parentKeyIndex)

  if (needsStripFk) {
    for (const child of children) {
      delete child[segment.fkFieldName]
    }
  }
}
