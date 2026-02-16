import {
  executeSegmentBase,
  type ExecuteWhereInParams,
} from './shared/where-in-executor-base'
import { getPrimaryKeyField } from './shared/primary-key-utils'

import {
  buildArrayAggReducerConfig,
  reduceArrayAggRows,
} from './select/array-agg-reducer'
import { WhereInSegment } from './select/segment-planner'
import { buildReducerConfig, buildSQL, Model, reduceFlatRows } from '..'

const ADAPTIVE_PARENT_THRESHOLD = 15
const ADAPTIVE_DEPTH_THRESHOLD = 2

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
  segments: WhereInSegment[],
): Promise<void> {
  const pkField = getPrimaryKeyField(parentModel)
  const pks = parentRows.map((r) => r[pkField]).filter(Boolean)

  if (pks.length === 0) {
    for (const parent of parentRows) {
      for (const seg of segments) {
        parent[seg.relationName] = seg.isList ? [] : null
      }
    }
    return
  }

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

  const rowsByPk = new Map<unknown, any>()
  for (const row of rows) {
    rowsByPk.set(row[pkField], row)
  }

  for (const parent of parentRows) {
    const pk = parent[pkField]
    const fullRow = rowsByPk.get(pk)
    if (fullRow) {
      for (const seg of segments) {
        parent[seg.relationName] = fullRow[seg.relationName]
      }
    } else {
      for (const seg of segments) {
        parent[seg.relationName] = seg.isList ? [] : null
      }
    }
  }
}

export async function executeWhereInSegments(
  params: ExecuteWhereInParams,
): Promise<void> {
  const {
    segments,
    parentRows,
    parentModel,
    allModels,
    modelMap,
    dialect,
    execute,
    originalArgs,
    method,
  } = params

  if (
    originalArgs &&
    method &&
    parentModel &&
    shouldAdaptivelySwitch(parentRows.length, segments)
  ) {
    await executeCorrelatedFallback(
      parentRows,
      parentModel,
      allModels,
      dialect,
      execute,
      originalArgs,
      method,
      segments,
    )
    return
  }

  if (segments.length === 1) {
    await executeSegmentBase(
      segments[0],
      parentRows,
      allModels,
      modelMap,
      dialect,
      execute,
      0,
    )
    return
  }

  await Promise.all(
    segments.map((segment) =>
      executeSegmentBase(
        segment,
        parentRows,
        allModels,
        modelMap,
        dialect,
        execute,
        0,
      ),
    ),
  )
}
