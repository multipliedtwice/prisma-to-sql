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
  batchSize?: number
  maxConcurrency?: number
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
    batchSize = 100,
    maxConcurrency = 10,
  } = params

  if (segments.length === 0) {
    throw new Error('executeWhereInSegmentsStreaming requires segments')
  }

  if (dialect !== 'postgres') {
    throw new Error('Streaming WHERE IN requires postgres dialect')
  }

  const parentMap = new Map<string, any>()
  const batches = new Map<string, unknown[]>()
  const inFlightMap = new Map<Promise<void>, string>()

  for (const seg of segments) {
    batches.set(seg.relationName, [])
  }

  const pkField = getPrimaryKeyField(parentModel)

  const parentRows = await execute(parentSql, parentParams)

  for (const row of parentRows) {
    const pk = row[pkField]
    parentMap.set(pk, { ...row })

    for (const seg of segments) {
      row[seg.relationName] = seg.isList ? [] : null
    }

    for (const seg of segments) {
      const batch = batches.get(seg.relationName)!
      const parentKey = row[seg.parentKeyFieldName]
      batch.push(parentKey)

      if (batch.length >= batchSize) {
        const idsToFetch = [...batch]
        batch.length = 0

        const promise = fetchAndAttachChildren(
          seg,
          idsToFetch,
          parentMap,
          allModels,
          modelMap,
          dialect,
          execute,
          0,
        )

        inFlightMap.set(promise, seg.relationName)

        promise.finally(() => {
          inFlightMap.delete(promise)
        })

        if (inFlightMap.size >= maxConcurrency) {
          await Promise.race(inFlightMap.keys())
        }
      }
    }
  }

  for (const seg of segments) {
    const batch = batches.get(seg.relationName)!
    if (batch.length > 0) {
      const promise = fetchAndAttachChildren(
        seg,
        batch,
        parentMap,
        allModels,
        modelMap,
        dialect,
        execute,
        0,
      )

      inFlightMap.set(promise, seg.relationName)

      promise.finally(() => {
        inFlightMap.delete(promise)
      })
    }
  }

  await Promise.all(inFlightMap.keys())

  return Array.from(parentMap.values())
}

async function fetchAndAttachChildren(
  segment: any,
  parentIds: unknown[],
  parentMap: Map<string, any>,
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: (sql: string, params: unknown[]) => Promise<any[]>,
  depth: number,
): Promise<void> {
  if (depth > MAX_RECURSIVE_DEPTH) return

  const childModel = modelMap.get(segment.childModelName)
  if (!childModel) return

  const childArgs = buildChildArgs(
    segment.relArgs,
    segment.fkFieldName,
    parentIds,
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

    for (const nestedSeg of childPlan.whereInSegments) {
      const nestedIds = children
        .map((c) => c[nestedSeg.parentKeyFieldName])
        .filter((v) => v != null)

      const uniqueNestedIds = [...new Set(nestedIds)]

      if (uniqueNestedIds.length > 0) {
        await fetchAndAttachChildren(
          nestedSeg,
          uniqueNestedIds,
          childMap,
          allModels,
          modelMap,
          dialect,
          execute,
          depth + 1,
        )
      }
    }
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
