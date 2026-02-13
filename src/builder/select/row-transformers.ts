import { AGGREGATE_PREFIXES } from '../shared/constants'

export function transformAggregateRow(row: any): any {
  if (!row || typeof row !== 'object') return row

  const result: any = {}

  for (const key in row) {
    if (!Object.prototype.hasOwnProperty.call(row, key)) continue

    const value = row[key]
    const dotIndex = key.indexOf('.')

    if (dotIndex === -1) {
      result[key] = value
      continue
    }

    const prefix = key.slice(0, dotIndex)
    const suffix = key.slice(dotIndex + 1)

    if (AGGREGATE_PREFIXES.has(prefix)) {
      if (!result[prefix]) {
        result[prefix] = {}
      }
      result[prefix][suffix] = value
    } else {
      result[key] = value
    }
  }

  return result
}

export function extractCountValue(row: any): number | bigint {
  if (!row || typeof row !== 'object') return 0

  if ('_count._all' in row) {
    return row['_count._all'] as number | bigint
  }

  if ('_count' in row && row['_count'] && typeof row['_count'] === 'object') {
    const countObj = row['_count'] as Record<string, unknown>
    if ('_all' in countObj) {
      return countObj['_all'] as number | bigint
    }
  }

  const keys = Object.keys(row)
  for (const key of keys) {
    if (key.includes('count') || key.includes('COUNT')) {
      const value = row[key]
      if (typeof value === 'number' || typeof value === 'bigint') {
        return value
      }
    }
  }

  return 0
}

export function getRowTransformer(method: string): ((row: any) => any) | null {
  if (method === 'count') {
    return extractCountValue
  }

  if (method === 'groupBy' || method === 'aggregate') {
    return transformAggregateRow
  }

  return null
}
