import type { RelStats } from '../cardinality-planner'

export function toNumberOrZero(v: unknown): number {
  if (typeof v === 'number' && Number.isFinite(v)) return v
  if (typeof v === 'bigint') return Number(v)
  if (typeof v === 'string' && v.trim() !== '') {
    const n = Number(v)
    if (Number.isFinite(n)) return n
  }
  return 0
}

export function clampStatsMonotonic(
  avg: number,
  p95: number,
  p99: number,
  max: number,
  coverage: number,
): RelStats {
  const safeAvg = Math.max(1, avg)
  const safeP95 = Math.max(safeAvg, p95)
  const safeP99 = Math.max(safeP95, p99)
  const safeMax = Math.max(safeP99, max)
  const safeCoverage = Math.max(0, Math.min(1, coverage))
  return {
    avg: safeAvg,
    p95: safeP95,
    p99: safeP99,
    max: safeMax,
    coverage: safeCoverage,
  }
}

export function normalizeStats(row: Record<string, unknown>): RelStats {
  return clampStatsMonotonic(
    toNumberOrZero(row.avg),
    toNumberOrZero(row.p95),
    toNumberOrZero(row.p99),
    toNumberOrZero(row.max),
    toNumberOrZero(row.coverage),
  )
}

export function stableJson(value: unknown): string {
  return JSON.stringify(
    value,
    (_k, v) => {
      if (!v || typeof v !== 'object' || Array.isArray(v)) return v
      const obj = v as Record<string, unknown>
      const out: Record<string, unknown> = {}
      for (const k of Object.keys(obj).sort()) out[k] = obj[k]
      return out
    },
    2,
  )
}

export function cleanDatabaseUrl(url: string): string {
  try {
    const parsed = new URL(url)
    parsed.search = ''

    if (parsed.password) {
      parsed.password = '***'
    }

    if (parsed.username && parsed.username.length > 0) {
      if (parsed.username.length <= 3) {
        parsed.username = '***'
      } else {
        parsed.username = parsed.username.slice(0, 3) + '***'
      }
    }

    return parsed.toString()
  } catch (error) {
    return '[invalid-url]'
  }
}

export function createQueryKey(
  processedQuery: Record<string, unknown>,
): string {
  return JSON.stringify(processedQuery, (key, value) => {
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const sorted: Record<string, unknown> = {}
      for (const k of Object.keys(value).sort()) {
        sorted[k] = (value as any)[k]
      }
      return sorted
    }
    return value
  })
}

export function countTotalQueries(
  queries: Map<string, Map<string, Map<string, any>>>,
): number {
  return Array.from(queries.values()).reduce(
    (sum, methodMap) =>
      sum +
      Array.from(methodMap.values()).reduce(
        (s, queryMap) => s + queryMap.size,
        0,
      ),
    0,
  )
}
