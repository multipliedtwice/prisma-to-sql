import { PrismaMethod } from './types'
import { getRowTransformer } from './builder/select/row-transformers'

export function transformQueryResults(
  method: string,
  results: unknown,
): unknown {
  if (method === 'findFirst' || method === 'findUnique') {
    if (Array.isArray(results)) {
      return results[0] ?? null
    }
  }

  if (method === 'aggregate') {
    if (Array.isArray(results)) {
      return results[0] ?? null
    }
  }

  if (method === 'count') {
    if (Array.isArray(results) && results.length > 0) {
      return results[0]
    }
    return 0
  }

  return results
}
