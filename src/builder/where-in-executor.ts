import type { Model } from '../types'
import type { WhereInSegment } from './select/segment-planner'
import { initAndResolve, type ExecuteFn } from './shared/where-in-resolver'

interface ExecuteWhereInParams {
  segments: WhereInSegment[]
  parentRows: any[]
  parentModel: Model
  allModels: readonly Model[]
  modelMap: Map<string, Model>
  dialect: 'postgres' | 'sqlite'
  execute: ExecuteFn
}

export async function executeWhereInSegments(
  params: ExecuteWhereInParams,
): Promise<void> {
  const { segments, parentRows, allModels, modelMap, dialect, execute } = params

  if (segments.length === 0) return
  if (parentRows.length === 0) return

  await initAndResolve(
    segments,
    parentRows,
    allModels,
    modelMap,
    dialect,
    execute,
  )
}
