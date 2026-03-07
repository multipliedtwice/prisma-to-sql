import type { Model } from '../../types'
import type { WhereInSegment } from '../select/segment-planner'
import { planQueryStrategy } from '../select/segment-planner'
import { buildSQL } from '../..'
import { buildReducerConfig, reduceFlatRows } from '../select/reducer'
import {
  MAX_RECURSIVE_DEPTH,
  buildParentKeyIndex,
  needsPerParentPagination,
  stitchChildrenToParents,
  buildChildArgs,
  ensureFkInSelect,
  ensureOrderByPk,
  initRelationPlaceholders,
} from './where-in-utils'
import { withValidationSuppressed } from './validators/sql-validators'
import { resolvePrerenderedParams } from './prerendered-where-in'

export type ExecuteFn = (sql: string, params: unknown[]) => Promise<any[]>

async function resolveWithPrerendered(
  segment: WhereInSegment,
  parentRows: any[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: ExecuteFn,
  depth: number,
): Promise<boolean> {
  const pre = segment.prerendered
  if (!pre) return false

  const parentIds = parentRows
    .map((r) => r[segment.parentKeyFieldName])
    .filter((v) => v != null)

  if (parentIds.length === 0) return true

  const uniqueIds = [...new Set(parentIds)]

  const resolvedParams = resolvePrerenderedParams(
    pre.paramMappings,
    pre.dynamicInName,
    uniqueIds,
    dialect,
  )

  if (!resolvedParams) return false

  let children = await execute(pre.sql, resolvedParams)

  if (pre.requiresReduction && pre.reducerConfig) {
    children = reduceFlatRows(children, pre.reducerConfig)
  }

  if (pre.nestedSegments.length > 0 && children.length > 0) {
    for (const child of children) {
      initRelationPlaceholders(child, pre.nestedSegments)
    }

    await resolveSegments(
      pre.nestedSegments,
      children,
      allModels,
      modelMap,
      dialect,
      execute,
      depth + 1,
    )

    if (pre.injectedParentKeys.length > 0) {
      for (const child of children) {
        for (const key of pre.injectedParentKeys) {
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

  if (pre.needsStripFk) {
    for (const child of children) {
      delete child[segment.fkFieldName]
    }
  }

  return true
}

export async function resolveSingleSegment(
  segment: WhereInSegment,
  parentRows: any[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: ExecuteFn,
  depth: number,
): Promise<void> {
  if (depth > MAX_RECURSIVE_DEPTH) return

  if (segment.prerendered) {
    const handled = await resolveWithPrerendered(
      segment,
      parentRows,
      allModels,
      modelMap,
      dialect,
      execute,
      depth,
    )
    if (handled) return
  }

  const childModel = modelMap.get(segment.childModelName)
  if (!childModel) return

  const parentIds = parentRows
    .map((r) => r[segment.parentKeyFieldName])
    .filter((v) => v != null)

  if (parentIds.length === 0) return

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

  const childPlan = planQueryStrategy({
    model: childModel,
    method: 'findMany',
    args: childArgs,
    allModels,
    dialect,
  })

  const result = withValidationSuppressed(() =>
    buildSQL(
      childModel,
      allModels as Model[],
      'findMany',
      childPlan.filteredArgs,
      dialect,
    ),
  )

  let children = await execute(result.sql, result.params as unknown[])

  if (result.requiresReduction && result.includeSpec) {
    const config = buildReducerConfig(childModel, result.includeSpec, allModels)
    children = reduceFlatRows(children, config)
  }

  if (childPlan.whereInSegments.length > 0 && children.length > 0) {
    for (const child of children) {
      initRelationPlaceholders(child, childPlan.whereInSegments)
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

export async function resolveSegments(
  segments: WhereInSegment[],
  parentRows: any[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: ExecuteFn,
  depth: number,
): Promise<void> {
  if (depth > MAX_RECURSIVE_DEPTH) return
  if (segments.length === 0 || parentRows.length === 0) return

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

export async function initAndResolve(
  segments: WhereInSegment[],
  rows: any[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: ExecuteFn,
): Promise<void> {
  if (segments.length === 0 || rows.length === 0) return

  for (const row of rows) {
    initRelationPlaceholders(row, segments)
  }

  await resolveSegments(
    segments,
    rows,
    allModels,
    modelMap,
    dialect,
    execute,
    0,
  )
}
