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

const ADAPTIVE_PARENT_THRESHOLD = 15
const ADAPTIVE_DEPTH_THRESHOLD = 2

interface StreamingWhereInParams {
  segments: WhereInSegment[]
  parentSql: string
  parentParams: unknown[]
  parentModel: Model
  allModels: readonly Model[]
  modelMap: Map<string, Model>
  dialect: 'postgres' | 'sqlite'
  execute: (sql: string, params: unknown[]) => Promise<any[]>
  batchSize?: number
  maxConcurrency?: number
  originalArgs?: any
  method?: string
}

function measureRelArgsDepth(relArgs: unknown): number {
  if (!relArgs || relArgs === true || typeof relArgs !== 'object') return 0

  const args = relArgs as Record<string, any>
  const nested = args.include || args.select
  if (!nested || typeof nested !== 'object') return 0

  let maxChildDepth = 0
  for (const val of Object.values(nested)) {
    if (val === false) continue
    if (val === true) {
      maxChildDepth = Math.max(maxChildDepth, 1)
      continue
    }
    if (val && typeof val === 'object') {
      maxChildDepth = Math.max(maxChildDepth, 1 + measureRelArgsDepth(val))
    }
  }
  return maxChildDepth
}

function measureSegmentNestingDepth(segments: WhereInSegment[]): number {
  let maxDepth = 0
  for (const seg of segments) {
    const depth = 1 + measureRelArgsDepth(seg.relArgs)
    maxDepth = Math.max(maxDepth, depth)
  }
  return maxDepth
}

function shouldAdaptivelySwitch(
  actualParentCount: number,
  segments: WhereInSegment[],
): boolean {
  if (actualParentCount > ADAPTIVE_PARENT_THRESHOLD) return false
  if (segments.length === 0) return false

  const depth = measureSegmentNestingDepth(segments)
  return depth >= ADAPTIVE_DEPTH_THRESHOLD
}

async function executeCorrelatedFallback(
  parentRows: any[],
  parentModel: Model,
  allModels: readonly Model[],
  dialect: 'postgres' | 'sqlite',
  execute: (sql: string, params: unknown[]) => Promise<any[]>,
  originalArgs: any,
  method: string,
): Promise<any[]> {
  const pkField = getPrimaryKeyField(parentModel)
  const pks = parentRows.map((r) => r[pkField]).filter(Boolean)

  if (pks.length === 0) return []

  const fallbackArgs = { ...originalArgs }
  delete fallbackArgs.take
  delete fallbackArgs.skip
  delete fallbackArgs.cursor

  fallbackArgs.where = { [pkField]: { in: pks } }

  if (originalArgs.orderBy) {
    fallbackArgs.orderBy = originalArgs.orderBy
  }

  const result = buildSQL(
    parentModel,
    allModels as Model[],
    method as any,
    fallbackArgs,
    dialect,
  )

  let rows = await execute(result.sql, result.params as unknown[])

  if (result.isArrayAgg && result.includeSpec) {
    const config = buildArrayAggReducerConfig(
      parentModel,
      result.includeSpec,
      allModels,
    )
    rows = reduceArrayAggRows(rows, config)
  } else if (result.requiresReduction && result.includeSpec) {
    const config = buildReducerConfig(
      parentModel,
      result.includeSpec,
      allModels,
    )
    rows = reduceFlatRows(rows, config)
  }

  return rows
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
    originalArgs,
    method,
  } = params

  if (segments.length === 0) {
    throw new Error('executeWhereInSegmentsStreaming requires segments')
  }

  if (dialect !== 'postgres') {
    throw new Error('Streaming WHERE IN requires postgres dialect')
  }

  const pkField = getPrimaryKeyField(parentModel)
  const parentRows = await execute(parentSql, parentParams)

  if (
    originalArgs &&
    method &&
    shouldAdaptivelySwitch(parentRows.length, segments)
  ) {
    return executeCorrelatedFallback(
      parentRows,
      parentModel,
      allModels,
      dialect,
      execute,
      originalArgs,
      method,
    )
  }

  const parentMap = new Map<string, any>()
  for (const row of parentRows) {
    const pk = row[pkField]
    const enriched = { ...row }
    for (const seg of segments) {
      enriched[seg.relationName] = seg.isList ? [] : null
    }
    parentMap.set(pk, enriched)
  }

  await resolveSegments(
    segments,
    parentMap,
    allModels,
    modelMap,
    dialect,
    execute,
    0,
  )

  return Array.from(parentMap.values())
}

async function resolveSegments(
  segments: WhereInSegment[],
  parentMap: Map<string, any>,
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: (sql: string, params: unknown[]) => Promise<any[]>,
  depth: number,
): Promise<void> {
  if (depth > MAX_RECURSIVE_DEPTH) return
  if (segments.length === 0) return

  const parentRows = Array.from(parentMap.values())

  if (segments.length === 1) {
    await resolveSingleSegment(
      segments[0],
      parentRows,
      parentMap,
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
        parentMap,
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
  parentMap: Map<string, any>,
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: (sql: string, params: unknown[]) => Promise<any[]>,
  depth: number,
): Promise<void> {
  const childModel = modelMap.get(segment.childModelName)
  if (!childModel) {
    for (const parent of parentRows) {
      parent[segment.relationName] = segment.isList ? [] : null
    }
    return
  }

  const parentIds = parentRows
    .map((r) => r[segment.parentKeyFieldName])
    .filter((v) => v != null)

  if (parentIds.length === 0) {
    for (const parent of parentRows) {
      parent[segment.relationName] = segment.isList ? [] : null
    }
    return
  }

  const uniqueIds = [...new Set(parentIds)]

  const childArgs = buildChildArgs(
    segment.relArgs,
    segment.fkFieldName,
    uniqueIds,
  )

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
    const childPkField = getPrimaryKeyField(childModel)
    const childMap = new Map<string, any>()

    for (const child of children) {
      const pk = child[childPkField]
      if (pk != null) {
        childMap.set(pk, child)
      }
      for (const nestedSeg of childPlan.whereInSegments) {
        child[nestedSeg.relationName] = nestedSeg.isList ? [] : null
      }
    }

    await resolveSegments(
      childPlan.whereInSegments,
      childMap,
      allModels,
      modelMap,
      dialect,
      execute,
      depth + 1,
    )
  }

  for (const child of children) {
    const fkValue = child[segment.fkFieldName]
    const parent = parentMap.get(fkValue)
    if (!parent) continue

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
