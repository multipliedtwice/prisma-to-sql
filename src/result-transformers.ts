import { PrismaMethod } from './types'
import { getRowTransformer } from './builder/select/row-transformers'

export function transformQueryResults(
  method: string,
  results: unknown,
): unknown {
  const rowTransformer = getRowTransformer(method)

  let transformed = results
  if (rowTransformer) {
    if (Array.isArray(results)) {
      transformed = results.map((row) => rowTransformer(row))
    } else if (results && typeof results === 'object') {
      transformed = rowTransformer(results)
    }
  }

  if (method === 'findFirst' || method === 'findUnique') {
    if (Array.isArray(transformed)) {
      return transformed[0] ?? null
    }
  }

  if (method === 'aggregate') {
    if (Array.isArray(transformed)) {
      return transformed[0] ?? null
    }
  }

  if (method === 'count') {
    if (Array.isArray(transformed) && transformed.length > 0) {
      const row = transformed[0]

      if (typeof row === 'number' || typeof row === 'bigint') {
        return row
      }

      if (row && typeof row === 'object' && '_count._all' in row) {
        return (row as any)['_count._all']
      }

      return row
    }
    return 0
  }

  return transformed
}
