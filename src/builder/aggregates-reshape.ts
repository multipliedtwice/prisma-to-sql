import { AGGREGATE_PREFIXES } from './shared/constants'

function hasNestedAggregates(row: any): boolean {
  for (const prefix of AGGREGATE_PREFIXES) {
    if (prefix in row && row[prefix] && typeof row[prefix] === 'object') {
      return true
    }
  }
  return false
}

function flattenAggregateRow(row: any): any {
  const result: any = {}

  for (const key in row) {
    if (!Object.prototype.hasOwnProperty.call(row, key)) continue

    const value = row[key]

    if (
      AGGREGATE_PREFIXES.has(key) &&
      value &&
      typeof value === 'object' &&
      !Array.isArray(value)
    ) {
      for (const subKey in value) {
        if (Object.prototype.hasOwnProperty.call(value, subKey)) {
          result[`${key}.${subKey}`] = value[subKey]
        }
      }
    } else {
      result[key] = value
    }
  }

  return result
}

export function reshapeAggregateResults(rows: any[]): any[] {
  if (!Array.isArray(rows) || rows.length === 0) return rows

  const needsFlattening = rows.some(
    (row) => row && typeof row === 'object' && hasNestedAggregates(row),
  )

  if (needsFlattening) {
    return rows.map(flattenAggregateRow)
  }

  return rows
}
