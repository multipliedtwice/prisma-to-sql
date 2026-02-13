import {
  transformAggregateRow,
  extractCountValue,
  buildReducerConfig,
  reduceFlatRows,
  normalizeValue,
} from './index'

export const SQLITE_STMT_CACHE = new WeakMap<any, Map<string, any>>()

export function getOrPrepareStatement(client: any, sql: string): any {
  let cache = SQLITE_STMT_CACHE.get(client)
  if (!cache) {
    cache = new Map()
    SQLITE_STMT_CACHE.set(client, cache)
  }

  let stmt = cache.get(sql)
  if (!stmt) {
    stmt = client.prepare(sql)
    cache.set(sql, stmt)

    if (cache.size > 1000) {
      const firstKey = cache.keys().next().value
      cache.delete(firstKey!)
    }
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
  model: any | undefined,
  allModels: readonly any[],
): Promise<unknown[]> {
  const normalizedParams = normalizeParams(params)
  const query = client.unsafe(sql, normalizedParams)

  if (requiresReduction && includeSpec && model) {
    const config = buildReducerConfig(model, includeSpec, allModels)
    const { createStreamingReducer } = await import('./index')
    const reducer = createStreamingReducer(config)

    await query.forEach((row: any) => {
      reducer.processRow(row)
    })

    return reducer.getResults()
  }

  if (method === 'count') {
    const results: any[] = []
    await query.forEach((row: any) => {
      results.push(extractCountValue(row))
    })
    return results
  }

  if (method === 'groupBy' || method === 'aggregate') {
    const results: any[] = []
    await query.forEach((row: any) => {
      results.push(transformAggregateRow(row))
    })
    return results
  }

  const results: any[] = []
  await query.forEach((row: any) => {
    results.push(row)
  })
  return results
}

export function executeSqliteQuery(
  client: any,
  sql: string,
  params: unknown[],
  method: string,
  requiresReduction: boolean,
  includeSpec: Record<string, any> | undefined,
  model: any | undefined,
  allModels: readonly any[],
): unknown[] {
  const normalizedParams = normalizeParams(params)
  const stmt = getOrPrepareStatement(client, sql)

  if (shouldSqliteUseGet(method)) {
    const row = stmt.get(...normalizedParams)
    if (row === undefined) {
      return method === 'count' ? [0] : []
    }

    if (method === 'count') {
      return [extractCountValue(row)]
    }

    if (method === 'aggregate') {
      return [transformAggregateRow(row)]
    }

    return [row]
  }

  const rows = stmt.all(...normalizedParams)

  if (method === 'count') {
    if (rows.length === 0) return [0]
    return [extractCountValue(rows[0])]
  }

  if (method === 'groupBy' || method === 'aggregate') {
    return rows.map((row: any) => transformAggregateRow(row))
  }

  if (requiresReduction && includeSpec && model) {
    const config = buildReducerConfig(model, includeSpec, allModels)
    return reduceFlatRows(rows as any[], config)
  }

  return rows
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
