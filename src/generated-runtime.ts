import {
  transformAggregateRow,
  extractCountValue,
  buildReducerConfig,
  reduceFlatRows,
  normalizeValue,
  getRowTransformer,
  createStreamingReducer,
} from './index'
import {
  buildArrayAggReducerConfig,
  reduceArrayAggRows,
} from './builder/select/array-agg-reducer'

export const SQLITE_STMT_CACHE = new WeakMap<any, Map<string, any>>()

const STMT_CACHE_LIMIT = 1000

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

  if (cache.size > STMT_CACHE_LIMIT) {
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

export async function executePostgresQuery(
  client: any,
  sql: string,
  params: unknown[],
  method: string,
  requiresReduction: boolean,
  includeSpec: Record<string, any> | undefined,
  model: any,
  allModels: readonly any[],
  isArrayAgg?: boolean,
): Promise<unknown[]> {
  const normalizedParams = normalizeParams(params)

  if (isArrayAgg && includeSpec) {
    const config = buildArrayAggReducerConfig(model, includeSpec, allModels)
    const results: any[] = []

    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      results.push(row)
    })

    return reduceArrayAggRows(results, config)
  }

  if (requiresReduction && includeSpec) {
    const config = buildReducerConfig(model, includeSpec, allModels)
    const reducer = createStreamingReducer(config)

    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      reducer.processRow(row)
    })

    return reducer.getResults()
  }

  const needsTransform =
    method === 'groupBy' || method === 'aggregate' || method === 'count'

  if (!needsTransform) {
    const results: any[] = []
    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      results.push(row)
    })
    return results
  }

  const rowTransformer = getRowTransformer(method)
  const results: any[] = []

  if (rowTransformer) {
    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      results.push(rowTransformer(row))
    })
  } else {
    await client.unsafe(sql, normalizedParams).forEach((row: any) => {
      results.push(row)
    })
  }

  return results
}

export function executeSqliteQuery(
  client: any,
  sql: string,
  params: unknown[],
  method: string,
  requiresReduction: boolean,
  includeSpec: Record<string, any> | undefined,
  model: any,
  allModels: readonly any[],
): unknown[] {
  const normalizedParams = normalizeParams(params)
  const shouldTransform =
    method === 'groupBy' || method === 'aggregate' || method === 'count'

  if (requiresReduction && includeSpec) {
    const config = buildReducerConfig(model, includeSpec, allModels)
    const stmt = getOrPrepareStatement(client, sql)

    const useGet = shouldSqliteUseGet(method)
    const rawResults = useGet
      ? stmt.get(...normalizedParams)
      : stmt.all(...normalizedParams)
    const results = Array.isArray(rawResults) ? rawResults : [rawResults]

    const transformed = shouldTransform
      ? results.map(transformAggregateRow)
      : results
    return reduceFlatRows(transformed, config)
  }

  const stmt = getOrPrepareStatement(client, sql)
  const useGet = shouldSqliteUseGet(method)
  const rawResults = useGet
    ? stmt.get(...normalizedParams)
    : stmt.all(...normalizedParams)
  const results = Array.isArray(rawResults) ? rawResults : [rawResults]

  return shouldTransform ? results.map(transformAggregateRow) : results
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
