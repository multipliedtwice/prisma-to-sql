import { PrismaMethod } from './types'

function parseAggregateValue(value: unknown): unknown {
  if (typeof value === 'string' && /^-?\d+(\.\d+)?$/.test(value)) {
    return parseFloat(value)
  }
  return value
}

function transformGroupByResults(results: unknown[]): unknown[] {
  return results.map((row) => {
    const raw = row as Record<string, unknown>
    const parsed: any = {}
    for (const [key, value] of Object.entries(raw)) {
      const parts = key.split('.')
      if (parts.length === 2) {
        const [group, field] = parts
        if (!parsed[group]) parsed[group] = {}
        parsed[group][field] = parseAggregateValue(value)
      } else {
        parsed[key] = value
      }
    }
    return parsed
  })
}

function transformCountResults(results: unknown[]): number {
  const result = results[0] as any
  const count = result?.['_count._all'] ?? result?.count ?? 0
  return typeof count === 'string' ? parseInt(count, 10) : count
}

function transformAggregateResults(results: unknown[]): Record<string, any> {
  const raw = (results[0] || {}) as Record<string, unknown>
  const parsed: any = {}
  for (const [key, value] of Object.entries(raw)) {
    const parts = key.split('.')
    if (parts.length === 2) {
      const [group, field] = parts
      if (!parsed[group]) parsed[group] = {}
      parsed[group][field] = parseAggregateValue(value)
    } else {
      parsed[key] = value
    }
  }
  return parsed
}

export const RESULT_TRANSFORMERS: Partial<
  Record<PrismaMethod, (results: unknown[]) => unknown>
> = {
  findFirst: (results) => results[0] || null,
  findUnique: (results) => results[0] || null,
  count: transformCountResults,
  aggregate: transformAggregateResults,
  groupBy: transformGroupByResults,
}

export function transformQueryResults(
  method: PrismaMethod,
  results: unknown[],
): unknown {
  const transformer = RESULT_TRANSFORMERS[method]
  return transformer ? transformer(results) : results
}
