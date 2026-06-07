import {
  buildReducerConfig,
  reduceFlatRows,
  normalizeValue,
  getRowTransformer,
  createStreamingReducer,
  createProgressiveReducer,
} from './index'
import {
  buildLateralReducerConfig,
  reduceLateralRows,
} from './builder/select/lateral-reducer'
import type { LateralRelationMeta } from './builder/select/lateral-join'
import type { Model } from './types'
import { LIMITS } from './builder/shared/constants'

export const SQLITE_STMT_CACHE = new WeakMap<any, Map<string, any>>()

export function getOrPrepareStatement(client: any, sql: string): any {
  let cache = SQLITE_STMT_CACHE.get(client)
  if (!cache) {
    cache = new Map()
    SQLITE_STMT_CACHE.set(client, cache)
  }

  let stmt = cache.get(sql)
  if (stmt) {
    cache.delete(sql)
    cache.set(sql, stmt)
    return stmt
  }

  stmt = client.prepare(sql)
  cache.set(sql, stmt)

  if (cache.size > LIMITS.STMT_CACHE_SIZE) {
    const firstKey = cache.keys().next().value
    cache.delete(firstKey!)
  }

  return stmt
}

export function shouldSqliteUseGet(method: string): boolean {
  return (
    method === 'count' ||
    method === 'findFirst' ||
    method === 'findUnique' ||
    method === 'aggregate'
  )
}

export function normalizeParams(params: unknown[]): unknown[] {
  return params.map((p) => normalizeValue(p))
}

export interface PostgresQueryOptions {
  client: any
  sql: string
  params: unknown[]
  method: string
  requiresReduction: boolean
  includeSpec?: Record<string, any>
  model: Model
  allModels: readonly Model[]
  isLateral?: boolean
  lateralMeta?: LateralRelationMeta[]
}

export async function executePostgresQuery(
  opts: PostgresQueryOptions,
): Promise<unknown[]> {
  const {
    client,
    sql,
    params,
    method,
    requiresReduction,
    includeSpec,
    model,
    allModels,
    isLateral,
    lateralMeta,
  } = opts

  const normalizedParams = normalizeParams(params)

  if (isLateral && lateralMeta) {
    const config = buildLateralReducerConfig(model, lateralMeta)
    const results: any[] = []

    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      results.push(row)
    })

    return reduceLateralRows(results, config)
  }

  if (requiresReduction && includeSpec) {
    const config = buildReducerConfig(model, includeSpec, allModels)
    const reducer = createStreamingReducer(config)

    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      reducer.processRow(row)
    })

    return reducer.getResults()
  }

  const rowTransformer = getRowTransformer(method, model)
  const results: any[] = []

  if (rowTransformer) {
    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      results.push(rowTransformer(row))
    })
    return results
  }

  await client.unsafe(sql, normalizedParams).forEach((row: any) => {
    results.push(row)
  })

  return results
}

export interface StreamReduceConfig<TAcc, TOut> {
  init: () => TAcc
  onRow: (row: any, acc: TAcc) => void
  finalize: (acc: TAcc) => TOut
}

export interface StreamReduceOptions<TAcc, TOut> {
  client: any
  sql: string
  params: unknown[]
  model: Model
  allModels: readonly Model[]
  requiresReduction: boolean
  includeSpec?: Record<string, any>
  isLateral?: boolean
  lateralMeta?: LateralRelationMeta[]
  reduce: StreamReduceConfig<TAcc, TOut>
}

/**
 * Single-pass score-and-accumulate over the postgres socket cursor.
 *
 * Fully hydrated entities are handed to `reduce.onRow` as they complete: at the
 * parent-key transition for progressive reduction, after lateral reduction, or
 * directly per row when no reduction is required. The caller owns the
 * accumulator (e.g. a bounded top-K heap) and produces the ordered result in
 * `finalize` when the stream ends — no buffered full-pool array, no second sort
 * pass for the non-lateral paths.
 *
 * The lateral path must materialize all rows before reduction, so its memory is
 * not bounded by the accumulator; the progressive and plain paths hold only the
 * accumulator plus one in-flight parent group.
 */
export async function streamReduce<TAcc, TOut>(
  opts: StreamReduceOptions<TAcc, TOut>,
): Promise<TOut> {
  const {
    client,
    sql,
    params,
    model,
    allModels,
    requiresReduction,
    includeSpec,
    isLateral,
    lateralMeta,
    reduce,
  } = opts

  const acc = reduce.init()
  const normalizedParams = normalizeParams(params)

  if (isLateral && lateralMeta) {
    const config = buildLateralReducerConfig(model, lateralMeta)
    const rows: any[] = []
    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      rows.push(row)
    })
    for (const entity of reduceLateralRows(rows, config)) {
      reduce.onRow(entity, acc)
    }
    return reduce.finalize(acc)
  }

  if (requiresReduction && includeSpec) {
    const config = buildReducerConfig(model, includeSpec, allModels)
    const reducer = createProgressiveReducer(config)
    let lastParentKey: string | null = null

    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      reducer.processRow(row)
      const currentKey = reducer.getCurrentParentKey(row)

      if (currentKey !== lastParentKey && lastParentKey !== null) {
        const parent = reducer.getCompletedParent(lastParentKey)
        if (parent) {
          reduce.onRow(parent, acc)
        }
      }

      lastParentKey = currentKey
    })

    for (const parent of reducer.getRemainingParents()) {
      reduce.onRow(parent, acc)
    }

    return reduce.finalize(acc)
  }

  await client.unsafe(sql, normalizedParams).forEach((row: any) => {
    reduce.onRow(row, acc)
  })

  return reduce.finalize(acc)
}

export function executeSqliteQuery(
  client: any,
  sql: string,
  params: unknown[],
  method: string,
  model?: Model,
): unknown[] {
  const normalizedParams = normalizeParams(params)
  const stmt = getOrPrepareStatement(client, sql)
  const useGet = shouldSqliteUseGet(method)
  const rawResults = useGet
    ? stmt.get(...normalizedParams)
    : stmt.all(...normalizedParams)
  const results = Array.isArray(rawResults) ? rawResults : [rawResults]

  const rowTransformer = getRowTransformer(method, model)
  return rowTransformer ? results.map(rowTransformer) : results
}

export async function executeRaw(
  client: any,
  sql: string,
  params: unknown[] | undefined,
  dialect: string,
): Promise<unknown[]> {
  if (dialect === 'postgres') {
    return await client.unsafe(sql, (params || []) as any[])
  }
  throw new Error('Raw execution for sqlite not supported in transactions')
}
