import type { Model } from '../../types'
import type { WhereInSegment } from '../select/segment-planner'
import { planQueryStrategy } from '../select/segment-planner'
import { buildSQL } from '../..'
import { buildReducerConfig, reduceFlatRows } from '../select/reducer'
import {
  buildParentKeyIndex,
  needsPerParentPagination,
  stitchChildrenToParents,
  buildChildArgs,
  ensureFkInSelect,
  ensureOrderByPk,
  initRelationPlaceholders,
  extractTuples,
  dedupeTuples,
  maxTuplesPerBatch,
  extractMainTableAlias,
  buildTupleInClause,
  injectAndWhere,
} from './where-in-utils'
import { LIMITS } from './constants'
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
  if (segment.parentKeyFieldNames.length !== 1) return false

  const parentKeyField = segment.parentKeyFieldNames[0]
  const parentIds = parentRows
    .map((r) => r[parentKeyField])
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
    segment.parentKeyFieldNames,
  )
  stitchChildrenToParents(children, segment, parentKeyIndex)

  if (pre.needsStripFk) {
    for (const child of children) {
      for (const fk of segment.fkFieldNames) {
        delete child[fk]
      }
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
  if (depth > LIMITS.MAX_WHERE_IN_RECURSIVE_DEPTH) return

  if (segment.prerendered && segment.fkFieldNames.length === 1) {
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

  const parentTuples = extractTuples(parentRows, segment.parentKeyFieldNames)
  if (parentTuples.length === 0) return

  const uniqueTuples = dedupeTuples(parentTuples)
  if (uniqueTuples.length === 0) return

  const stripPagination = needsPerParentPagination(segment)
  const isComposite = segment.fkFieldNames.length > 1
  const keyColumnCount = segment.fkFieldNames.length

  const probeBatch = [uniqueTuples[0]]
  const probeChildArgs = buildChildArgs(
    segment.relArgs,
    segment.fkFieldNames,
    probeBatch,
    stripPagination,
  )
  ensureOrderByPk(probeChildArgs, childModel)
  ensureFkInSelect(probeChildArgs, segment.fkFieldNames)
  const probePlan = planQueryStrategy({
    model: childModel,
    method: 'findMany',
    args: probeChildArgs,
    allModels,
    dialect,
  })
  const probeResult = withValidationSuppressed(() =>
    buildSQL(
      childModel,
      allModels as Model[],
      'findMany',
      probePlan.filteredArgs,
      dialect,
    ),
  )
  const probeParamCount = (probeResult.params as unknown[]).length
  const baseChildParams = isComposite
    ? probeParamCount
    : Math.max(0, probeParamCount - 1)
  const batchSize = maxTuplesPerBatch(dialect, keyColumnCount, baseChildParams)

  const allChildren: any[] = []
  let needsStripFk = false
  let injectedKeysFromLastBatch: string[] = []

  for (let i = 0; i < uniqueTuples.length; i += batchSize) {
    const batch = uniqueTuples.slice(i, i + batchSize)

    const childArgs = buildChildArgs(
      segment.relArgs,
      segment.fkFieldNames,
      batch,
      stripPagination,
    )

    ensureOrderByPk(childArgs, childModel)
    const stripped = ensureFkInSelect(childArgs, segment.fkFieldNames)
    if (stripped.length > 0) needsStripFk = true

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

    let sql: string = result.sql
    let params: unknown[] = result.params as unknown[]

    if (isComposite) {
      const tableAlias = extractMainTableAlias(sql)
      if (tableAlias === null && /\bJOIN\b/i.test(sql)) {
        throw new Error(
          `Composite-FK where-in for '${segment.relationName}' on ${childModel.name}: ` +
            `could not extract the child table alias from generated SQL, and the SQL ` +
            `contains joins. Emitting unqualified column references would cause ` +
            `ambiguous-column errors. This indicates either an unexpected SQL shape ` +
            `from the builder or a fragility in extractMainTableAlias.`,
        )
      }
      const tupleClause = buildTupleInClause({
        childModel,
        fkFieldNames: segment.fkFieldNames,
        tuples: batch,
        tableAlias,
        paramStartIdx: params.length + 1,
        dialect,
      })
      sql = injectAndWhere(sql, tupleClause.sql)
      params = [...params, ...tupleClause.params]
    }

    console.log('[where-in segment]', segment.relationName)
    console.log('[where-in child model]', childModel.name)
    console.log('[where-in fk fields]', segment.fkFieldNames)
    console.log('[where-in parent key fields]', segment.parentKeyFieldNames)
    console.log('[where-in parent tuples]', batch)
    console.log('[where-in sql]')
    console.log(sql)
    console.log('[where-in params]')
    console.dir(params, { depth: null })

    let batchChildren = await execute(sql, params)

    console.log(
      '[where-in raw rows]',
      segment.relationName,
      batchChildren.length,
    )
    if (batchChildren.length > 0) {
      console.log('[where-in raw first row]', segment.relationName)
      console.dir(batchChildren[0], { depth: null })
    }

    if (result.requiresReduction && result.includeSpec) {
      const config = buildReducerConfig(
        childModel,
        result.includeSpec,
        allModels,
      )
      batchChildren = reduceFlatRows(batchChildren, config)

      if (process.env.DEBUG_WHERE_IN === '1') {
        console.log(
          '[where-in reduced rows]',
          segment.relationName,
          batchChildren.length,
        )
        if (batchChildren.length > 0) {
          console.log('[where-in reduced first row]', segment.relationName)
          console.dir(batchChildren[0], { depth: null })
        }
      }
    }

    if (result.requiresReduction && result.includeSpec) {
      const config = buildReducerConfig(
        childModel,
        result.includeSpec,
        allModels,
      )
      batchChildren = reduceFlatRows(batchChildren, config)
    }

    if (childPlan.whereInSegments.length > 0 && batchChildren.length > 0) {
      for (const child of batchChildren) {
        initRelationPlaceholders(child, childPlan.whereInSegments)
      }
      await resolveSegments(
        childPlan.whereInSegments,
        batchChildren,
        allModels,
        modelMap,
        dialect,
        execute,
        depth + 1,
      )
      injectedKeysFromLastBatch = childPlan.injectedParentKeys
    }

    allChildren.push(...batchChildren)
  }

  if (injectedKeysFromLastBatch.length > 0) {
    for (const child of allChildren) {
      for (const key of injectedKeysFromLastBatch) {
        delete child[key]
      }
    }
  }

  const parentKeyIndex = buildParentKeyIndex(
    parentRows,
    segment.parentKeyFieldNames,
  )
  stitchChildrenToParents(allChildren, segment, parentKeyIndex)

  if (needsStripFk) {
    for (const child of allChildren) {
      for (const fk of segment.fkFieldNames) {
        delete child[fk]
      }
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
  if (depth > LIMITS.MAX_WHERE_IN_RECURSIVE_DEPTH) return
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
