import { PrismaMethod } from './types'

export function transformQueryResults(
  method: string,
  results: unknown,
): unknown {
  if (method === 'findFirst' || method === 'findUnique') {
    if (Array.isArray(results)) {
      return results[0] ?? null
    }
  }

  if (method === 'count') {
    if (Array.isArray(results) && results.length > 0) {
      const row = results[0]

      // If the row is already a number or bigint, return it
      if (typeof row === 'number' || typeof row === 'bigint') {
        return row
      }

      // If it's an object with _count._all, extract the simple count
      if (row && typeof row === 'object' && '_count._all' in row) {
        return (row as any)['_count._all']
      }

      // Otherwise return the row as-is (for count with select)
      return row
    }
    return 0
  }

  return results
}
