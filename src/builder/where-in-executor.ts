import {
  ExecuteWhereInParams,
  executeSegmentBase,
} from './shared/where-in-executor-base'

export async function executeWhereInSegments(
  params: ExecuteWhereInParams,
): Promise<void> {
  const { segments, parentRows, allModels, modelMap, dialect, execute } = params

  if (segments.length === 0) return
  if (parentRows.length === 0) return

  for (const segment of segments) {
    await executeSegmentBase(
      segment,
      parentRows,
      allModels,
      modelMap,
      dialect,
      execute,
    )
  }
}
