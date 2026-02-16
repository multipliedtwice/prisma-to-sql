import type { Model } from '../../types'
import { getPrimaryKeyField } from '../shared/primary-key-utils'
import type { WhereInSegment } from '../select/segment-planner'
import { planQueryStrategy } from '../select/segment-planner'
import { buildSQL } from '../..'
import { buildReducerConfig, reduceFlatRows } from './reducer'
import {
  buildArrayAggReducerConfig,
  reduceArrayAggRows,
} from './array-agg-reducer'

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

function stitchChildrenToParents(
  children: any[],
  segment: WhereInSegment,
  parentKeyIndex: Map<unknown, any[]>,
): void {
  for (const child of children) {
    const childKey = child[segment.fkFieldName]
    const matchingParents = parentKeyIndex.get(childKey)
    if (!matchingParents) continue

    for (const parent of matchingParents) {
      if (segment.isList) {
        if (!Array.isArray(parent[segment.relationName])) {
          parent[segment.relationName] = []
        }
        parent[segment.relationName].push(child)
      } else {
        parent[segment.relationName] = child
      }
    }
  }
}

function ensureFkInSelect(childArgs: any, fkFieldName: string): boolean {
  if (!childArgs.select) return false
  if (childArgs.select[fkFieldName]) return false
  childArgs.select = { ...childArgs.select, [fkFieldName]: true }
  return true
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

  const childArgs = buildChildArgs(
    segment.relArgs,
    segment.fkFieldName,
    uniqueIds,
  )

  const needsStripFk = ensureFkInSelect(childArgs, segment.fkFieldName)

  const childPlan = planQueryStrategy({
    model: childModel,
    method: 'findMany',
    args: childArgs,
    allModels,
    dialect,
  })

  const result = buildSQL(
    childModel,
    allModels as Model[],
    'findMany',
    childPlan.filteredArgs,
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

  if (childPlan.whereInSegments.length > 0 && children.length > 0) {
    for (const child of children) {
      for (const nestedSeg of childPlan.whereInSegments) {
        child[nestedSeg.relationName] = nestedSeg.isList ? [] : null
      }
    }

    await resolveSegments(
      childPlan.whereInSegments,
      children,
      allModels,
      modelMap,
      dialect,
      execute,
      depth + 1,
    )

    if (childPlan.injectedParentKeys.length > 0) {
      for (const child of children) {
        for (const key of childPlan.injectedParentKeys) {
          delete child[key]
        }
      }
    }
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

function buildChildArgs(
  relArgs: unknown,
  fkFieldName: string,
  parentIds: unknown[],
): any {
  const base: any =
    relArgs === true || typeof relArgs !== 'object' || relArgs === null
      ? {}
      : { ...(relArgs as any) }

  const existingWhere = base.where
  const inCondition = { [fkFieldName]: { in: parentIds } }

  base.where = existingWhere
    ? { AND: [existingWhere, inCondition] }
    : inCondition

  return base
}
