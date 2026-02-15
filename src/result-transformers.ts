import { PrismaMethod } from './types'
import {
  extractCountValue,
  getRowTransformer,
} from './builder/select/row-transformers'

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
      const first = results[0]
      if (typeof first === 'number' || typeof first === 'bigint') {
        return first
      }
      return extractCountValue(first)
    }
    return 0
  }

  return results
}
