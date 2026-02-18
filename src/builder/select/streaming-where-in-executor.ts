import type { Model } from '../../types'
import type { WhereInSegment } from '../select/segment-planner'
import {
  resolveSegments,
  initAndResolve,
  type ExecuteFn,
} from '../shared/where-in-resolver'
import { initRelationPlaceholders } from '../shared/where-in-utils'

const MIN_CHUNK_PIPELINE_SIZE = 10
const CHUNK_SIZE_DEFAULT = 50
const CHUNK_SIZE_TINY = 2
const CHUNK_SIZE_SMALL = 5
const CHUNK_SIZE_MEDIUM = 20
const CHUNK_THRESHOLD_TINY = 5
const CHUNK_THRESHOLD_SMALL = 20
const CHUNK_THRESHOLD_MEDIUM = 100

interface StreamingWhereInParams {
  segments: WhereInSegment[]
  parentSql: string
  parentParams: unknown[]
  parentModel: Model
  allModels: readonly Model[]
  modelMap: Map<string, Model>
  dialect: 'postgres' | 'sqlite'
  execute: ExecuteFn
  stream?: (
    sql: string,
    params: unknown[],
    onRow: (row: any) => void,
  ) => Promise<void>
  parentTake?: number
}

interface PreFetchedWhereInParams {
  segments: WhereInSegment[]
  parentRows: any[]
  parentModel: Model
  allModels: readonly Model[]
  modelMap: Map<string, Model>
  dialect: 'postgres' | 'sqlite'
  execute: ExecuteFn
}

function computeChunkSize(parentTake: number | undefined): number {
  if (parentTake == null) return CHUNK_SIZE_DEFAULT
  if (parentTake <= CHUNK_THRESHOLD_TINY) return CHUNK_SIZE_TINY
  if (parentTake <= CHUNK_THRESHOLD_SMALL) return CHUNK_SIZE_SMALL
  if (parentTake <= CHUNK_THRESHOLD_MEDIUM) return CHUNK_SIZE_MEDIUM
  return CHUNK_SIZE_DEFAULT
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
    stream,
    parentTake,
  } = params

  if (segments.length === 0) {
    throw new Error('executeWhereInSegmentsStreaming requires segments')
  }

  if (dialect !== 'postgres') {
    throw new Error('Streaming WHERE IN requires postgres dialect')
  }

  if (stream && (parentTake == null || parentTake >= MIN_CHUNK_PIPELINE_SIZE)) {
    return executePipelined(
      segments,
      parentSql,
      parentParams,
      allModels,
      modelMap,
      dialect,
      execute,
      stream,
      parentTake,
    )
  }

  return executeSequential(
    segments,
    parentSql,
    parentParams,
    allModels,
    modelMap,
    dialect,
    execute,
  )
}

async function executeSequential(
  segments: WhereInSegment[],
  parentSql: string,
  parentParams: unknown[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: ExecuteFn,
): Promise<any[]> {
  const parentRows = await execute(parentSql, parentParams)
  if (parentRows.length === 0) return []

  await initAndResolve(
    segments,
    parentRows,
    allModels,
    modelMap,
    dialect,
    execute,
  )

  return parentRows
}

async function executePipelined(
  segments: WhereInSegment[],
  parentSql: string,
  parentParams: unknown[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: ExecuteFn,
  stream: (
    sql: string,
    params: unknown[],
    onRow: (row: any) => void,
  ) => Promise<void>,
  parentTake: number | undefined,
): Promise<any[]> {
  const chunkSize = computeChunkSize(parentTake)
  const allParentRows: any[] = []
  let currentChunk: any[] = []
  const inflightChunks: Promise<void>[] = []

  await stream(parentSql, parentParams, (row: any) => {
    initRelationPlaceholders(row, segments)
    allParentRows.push(row)
    currentChunk.push(row)

    if (currentChunk.length >= chunkSize) {
      const chunk = currentChunk
      currentChunk = []
      inflightChunks.push(
        resolveSegments(
          segments,
          chunk,
          allModels,
          modelMap,
          dialect,
          execute,
          0,
        ),
      )
    }
  })

  if (currentChunk.length > 0) {
    inflightChunks.push(
      resolveSegments(
        segments,
        currentChunk,
        allModels,
        modelMap,
        dialect,
        execute,
        0,
      ),
    )
  }

  await Promise.all(inflightChunks)

  return allParentRows
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

  await initAndResolve(
    segments,
    parentRows,
    allModels,
    modelMap,
    dialect,
    execute,
  )

  return parentRows
}
