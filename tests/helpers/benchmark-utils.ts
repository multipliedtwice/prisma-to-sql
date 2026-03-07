import type { TestDB } from './db'
import {
  deepEqual,
  normalizeValue,
  sortByField,
  stableStringify,
  deepSortNestedArrays,
} from './compare'
import type { CapturedQuery } from './query-capture'
import {
  withDrizzleCapture,
  withPrismaCapture,
  formatCapturedQueries,
  withExtensionCapture,
} from './query-capture'

export interface BenchmarkStats {
  mean: number
  median: number
  stdDev: number
  min: number
  max: number
  cv: number
  iterations: number
  p95: number
  p99: number
}

export interface BenchmarkResult {
  name: string
  prismaMs: number
  extendedMs: number
  drizzleMs: number
  speedupVsPrisma: number
  speedupVsDrizzle: number
  prismaStats: BenchmarkStats
  extendedStats: BenchmarkStats
  drizzleStats?: BenchmarkStats
  prismaMeasurements?: number[]
  extendedMeasurements?: number[]
  drizzleMeasurements?: number[]
  failed?: boolean
  regressionLog?: {
    extendedQueries: CapturedQuery[]
    prismaQueries: CapturedQuery[]
    drizzleQueries: CapturedQuery[]
  }
}

export interface BenchmarkConfig {
  version: number
  dialect: 'postgres' | 'sqlite'
  shouldOutputJson: boolean
}

interface DistributionBucket {
  min: number
  max: number
  count: number
}

function envBool(key: string, fallback = false): boolean {
  const v = process.env[key]
  if (v === undefined) return fallback
  return v === '1' || v === 'true' || v === 'yes' || v === 'on'
}

function envNum(key: string, fallback: number): number {
  const v = process.env[key]
  if (!v) return fallback
  const n = Number(v)
  return Number.isFinite(n) ? n : fallback
}

function truncate(s: string, max: number): string {
  if (s.length <= max) return s
  return s.slice(0, max) + `\n... (truncated, total ${s.length} chars)`
}

function safeJson(value: unknown): string {
  try {
    return stableStringify(value)
  } catch (e: any) {
    return `<<stringify failed: ${String(e?.message ?? e)}>>`
  }
}

function calculateStats(measurements: number[]): BenchmarkStats {
  if (measurements.length === 0) {
    return {
      mean: 0,
      median: 0,
      stdDev: 0,
      min: 0,
      max: 0,
      cv: 0,
      iterations: 0,
      p95: 0,
      p99: 0,
    }
  }

  const sorted = [...measurements].sort((a, b) => a - b)
  const mean =
    measurements.reduce((sum, val) => sum + val, 0) / measurements.length
  const medianIndex = Math.floor(sorted.length / 2)
  const median =
    sorted.length % 2 === 0
      ? (sorted[medianIndex - 1] + sorted[medianIndex]) / 2
      : sorted[medianIndex]
  const min = sorted[0]
  const max = sorted[sorted.length - 1]

  const p95Index = Math.min(
    Math.ceil(sorted.length * 0.95) - 1,
    sorted.length - 1,
  )
  const p99Index = Math.min(
    Math.ceil(sorted.length * 0.99) - 1,
    sorted.length - 1,
  )
  const p95 = sorted[p95Index]
  const p99 = sorted[p99Index]

  const variance =
    measurements.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) /
    measurements.length
  const stdDev = Math.sqrt(variance)
  const cv = mean > 0 ? (stdDev / mean) * 100 : 0

  return {
    mean,
    median,
    stdDev,
    min,
    max,
    cv,
    iterations: measurements.length,
    p95,
    p99,
  }
}

function createDistribution(
  measurements: number[],
  buckets: number = 10,
): DistributionBucket[] {
  if (measurements.length === 0) return []

  const sorted = [...measurements].sort((a, b) => a - b)
  const min = sorted[0]
  const max = sorted[sorted.length - 1]
  const range = max - min

  if (range === 0) {
    return [{ min, max, count: measurements.length }]
  }

  const bucketSize = range / buckets
  const distribution: DistributionBucket[] = []

  for (let i = 0; i < buckets; i++) {
    const bucketMin = min + i * bucketSize
    const bucketMax = min + (i + 1) * bucketSize
    const count = measurements.filter((m) =>
      i === buckets - 1
        ? m >= bucketMin && m <= bucketMax
        : m >= bucketMin && m < bucketMax,
    ).length

    distribution.push({ min: bucketMin, max: bucketMax, count })
  }

  return distribution
}

function renderDistributionGraph(
  measurements: number[],
  maxWidth: number = 50,
): string[] {
  const lines: string[] = []
  const distribution = createDistribution(measurements, 8)

  if (distribution.length === 0) return lines

  const maxCount = Math.max(...distribution.map((b) => b.count))
  if (maxCount === 0) return lines

  for (const bucket of distribution) {
    const barLength = Math.round((bucket.count / maxCount) * maxWidth)
    const bar = '█'.repeat(barLength)
    const rangeStr = `${bucket.min.toFixed(2)}-${bucket.max.toFixed(2)}ms`
    const countStr = `(n=${bucket.count})`
    lines.push(
      `    ${rangeStr.padEnd(22)} ${bar.padEnd(maxWidth + 2)} ${countStr}`,
    )
  }

  return lines
}

function detectOutliers(measurements: number[]): {
  outliers: number[]
  indices: number[]
} {
  if (measurements.length < 4) return { outliers: [], indices: [] }

  const sorted = [...measurements].sort((a, b) => a - b)
  const q1Index = Math.floor(measurements.length * 0.25)
  const q3Index = Math.floor(measurements.length * 0.75)

  const q1 = sorted[q1Index]
  const q3 = sorted[q3Index]
  const iqr = q3 - q1

  const lowerBound = q1 - 1.5 * iqr
  const upperBound = q3 + 1.5 * iqr

  const outliers: number[] = []
  const indices: number[] = []

  measurements.forEach((val, idx) => {
    if (val < lowerBound || val > upperBound) {
      outliers.push(val)
      indices.push(idx)
    }
  })

  return { outliers, indices }
}

function findDeepDifferences(
  expected: unknown,
  actual: unknown,
  path: string,
  maxDepth: number = 10,
): string[] {
  if (maxDepth <= 0) return []

  const diffs: string[] = []

  if (typeof expected !== typeof actual) {
    diffs.push(
      `${path || 'root'}: type mismatch (${typeof expected} vs ${typeof actual})`,
    )
    return diffs
  }

  if (
    expected === null ||
    expected === undefined ||
    actual === null ||
    actual === undefined
  ) {
    if (expected !== actual) {
      diffs.push(
        `${path || 'root'}: ${JSON.stringify(expected)} !== ${JSON.stringify(actual)}`,
      )
    }
    return diffs
  }

  if (Array.isArray(expected) && Array.isArray(actual)) {
    if (expected.length !== actual.length) {
      diffs.push(
        `${path || 'root'}[]: length ${expected.length} !== ${actual.length}`,
      )
    }

    const minLen = Math.min(expected.length, actual.length)
    for (let i = 0; i < Math.min(minLen, 5); i++) {
      const itemPath = `${path}[${i}]`
      diffs.push(
        ...findDeepDifferences(expected[i], actual[i], itemPath, maxDepth - 1),
      )
    }

    return diffs
  }

  if (typeof expected === 'object' && typeof actual === 'object') {
    const expObj = expected as Record<string, unknown>
    const actObj = actual as Record<string, unknown>

    const allKeys = new Set([...Object.keys(expObj), ...Object.keys(actObj)])

    for (const key of allKeys) {
      if (!(key in expObj)) {
        const val = JSON.stringify(actObj[key])
        diffs.push(
          `${path}.${key}: missing in expected (actual has ${truncate(val, 50)})`,
        )
        continue
      }

      if (!(key in actObj)) {
        const val = JSON.stringify(expObj[key])
        diffs.push(
          `${path}.${key}: missing in actual (expected has ${truncate(val, 50)})`,
        )
        continue
      }

      const expVal = expObj[key]
      const actVal = actObj[key]

      if (
        typeof expVal === 'object' &&
        expVal !== null &&
        typeof actVal === 'object' &&
        actVal !== null
      ) {
        diffs.push(
          ...findDeepDifferences(
            expVal,
            actVal,
            `${path}.${key}`,
            maxDepth - 1,
          ),
        )
      } else if (expVal !== actVal) {
        const expStr = JSON.stringify(expVal)
        const actStr = JSON.stringify(actVal)
        diffs.push(
          `${path}.${key}: ${truncate(expStr, 50)} !== ${truncate(actStr, 50)}`,
        )
      }
    }

    return diffs
  }

  if (expected !== actual) {
    diffs.push(
      `${path || 'root'}: ${JSON.stringify(expected)} !== ${JSON.stringify(actual)}`,
    )
  }

  return diffs
}

export async function runParityTest<T>(
  db: TestDB,
  benchmarkResults: BenchmarkResult[],
  name: string,
  model: string,
  args: Record<string, unknown>,
  prismaQuery: () => Promise<T>,
  options: {
    benchmark?: boolean
    iterations?: number
    sortField?: string
    drizzleQuery?: () => Promise<unknown[]>
    transactionOps?: Array<{ model: string; method: string; args: any }>
    verifyRaw?: (payload: {
      prismaRaw: T
      extendedRaw: T
    }) => Promise<void> | void
  } = {},
): Promise<void> {
  const {
    benchmark = true,
    sortField = 'id',
    transactionOps,
    verifyRaw,
  } = options

  const extendedQuery = async () => {
    if (transactionOps) {
      return await db.extended.$transaction(transactionOps)
    }

    const { method, ...queryArgs } = args
    return await (db.extended as any)[model][method as string](queryArgs)
  }

  let parityFailed = false

  try {
    const [extendedRaw, prismaRaw] = await Promise.all([
      extendedQuery(),
      prismaQuery(),
    ])

    if (verifyRaw) {
      await verifyRaw({
        prismaRaw: prismaRaw as T,
        extendedRaw: extendedRaw as T,
      })
    }

    const sortedPrisma =
      !transactionOps && sortField
        ? sortByField(prismaRaw as any[], sortField as any)
        : prismaRaw
    const sortedExtended =
      !transactionOps && sortField
        ? sortByField(extendedRaw as any[], sortField as any)
        : extendedRaw

    const normalizedPrisma = deepSortNestedArrays(normalizeValue(sortedPrisma))
    const normalizedExtended = deepSortNestedArrays(
      normalizeValue(sortedExtended),
    )

    if (!deepEqual(normalizedPrisma, normalizedExtended)) {
      parityFailed = true
      const maxChars = 12000
      const captureSql = envBool('PARITY_CAPTURE_SQL', false)

      const parts: string[] = []
      parts.push(`Parity check failed for ${name}`)

      if (transactionOps) {
        parts.push(`Transaction with ${transactionOps.length} operations`)
      } else {
        const { method } = args
        parts.push(`Model: ${model}, Method: ${String(method)}`)
      }

      const prismaLen = Array.isArray(normalizedPrisma)
        ? normalizedPrisma.length
        : 1
      const extLen = Array.isArray(normalizedExtended)
        ? normalizedExtended.length
        : 1
      parts.push(`Rows: Prisma = ${prismaLen}, Prisma-SQL = ${extLen}`)

      parts.push(``)
      parts.push(`Difference Analysis:`)
      const differences = findDeepDifferences(
        normalizedPrisma,
        normalizedExtended,
        '',
      )

      if (differences.length === 0) {
        parts.push(`  No specific differences found (normalization issue?)`)
      } else {
        for (const diff of differences.slice(0, 10)) {
          parts.push(`  ${diff}`)
        }
        if (differences.length > 10) {
          parts.push(`  ... and ${differences.length - 10} more differences`)
        }
      }

      if (captureSql) {
        parts.push(``)
        parts.push(`SQL (Prisma):`)
        const prismaCaptured = await withPrismaCapture(async () =>
          prismaQuery(),
        )
        parts.push(formatCapturedQueries(prismaCaptured.queries, 2))

        parts.push(``)
        parts.push(`SQL (Extended):`)
        const extendedCaptured = await withExtensionCapture(async () =>
          extendedQuery(),
        )
        parts.push(formatCapturedQueries(extendedCaptured.queries, 2))
      } else {
        parts.push(``)
        parts.push(`(Set PARITY_CAPTURE_SQL=1 for SQL queries)`)
      }

      const message = truncate(parts.join('\n'), maxChars)
      throw new Error(message)
    }
  } catch (error) {
    parityFailed = true
    throw error
  } finally {
    if (benchmark) {
      const warmupStart = performance.now()
      await prismaQuery()
      const estimatedPrismaMs = performance.now() - warmupStart

      const iterations =
        options.iterations ??
        (estimatedPrismaMs > 200 ? 5 : estimatedPrismaMs > 50 ? 10 : 50)

      for (let w = 0; w < 3; w++) {
        await extendedQuery()
        await prismaQuery()
        if (options.drizzleQuery) await options.drizzleQuery()
      }

      const extendedMeasurements: number[] = []
      for (let i = 0; i < iterations; i++) {
        const start = performance.now()
        await extendedQuery()
        extendedMeasurements.push(performance.now() - start)
      }

      const prismaMeasurements: number[] = []
      for (let i = 0; i < iterations; i++) {
        const start = performance.now()
        await prismaQuery()
        prismaMeasurements.push(performance.now() - start)
      }

      const drizzleMeasurements: number[] = []
      if (options.drizzleQuery) {
        for (let i = 0; i < iterations; i++) {
          const start = performance.now()
          await options.drizzleQuery()
          drizzleMeasurements.push(performance.now() - start)
        }
      }

      const prismaStats = calculateStats(prismaMeasurements)
      const extendedStats = calculateStats(extendedMeasurements)
      const drizzleStats =
        drizzleMeasurements.length > 0
          ? calculateStats(drizzleMeasurements)
          : undefined

      const prismaMs = prismaStats.mean
      const extendedMs = extendedStats.mean
      const drizzleMs = drizzleStats?.mean ?? 0

      const speedupVsPrisma = prismaMs / extendedMs
      const speedupVsDrizzle = drizzleMs > 0 ? drizzleMs / extendedMs : 0

      const NOISE_THRESHOLD_MS = 1.0

      const isRegression =
        (speedupVsPrisma < 1.0 &&
          extendedMs - prismaMs >= NOISE_THRESHOLD_MS) ||
        (drizzleMs > 0 &&
          speedupVsDrizzle < 1.0 &&
          extendedMs - drizzleMs >= NOISE_THRESHOLD_MS)

      let regressionLog: BenchmarkResult['regressionLog'] | undefined

      if (isRegression) {
        const prismaCaptured = await withPrismaCapture(async () =>
          prismaQuery(),
        )
        const extendedCaptured = await withExtensionCapture(async () =>
          extendedQuery(),
        )
        const drizzleCaptured = options.drizzleQuery
          ? await withDrizzleCapture(async () => options.drizzleQuery!())
          : { result: [] as unknown[], queries: [] as CapturedQuery[] }

        regressionLog = {
          extendedQueries: extendedCaptured.queries,
          prismaQueries: prismaCaptured.queries,
          drizzleQueries: drizzleCaptured.queries,
        }
      }

      const isBudgetProbe =
        name.includes('depth-') ||
        name.includes('findFirst depth-') ||
        name.includes('findUnique depth-')

      benchmarkResults.push({
        name,
        prismaMs,
        extendedMs,
        drizzleMs,
        speedupVsPrisma,
        speedupVsDrizzle,
        prismaStats,
        extendedStats,
        drizzleStats,
        prismaMeasurements: isBudgetProbe ? prismaMeasurements : undefined,
        extendedMeasurements: isBudgetProbe ? extendedMeasurements : undefined,
        drizzleMeasurements:
          isBudgetProbe && drizzleMeasurements.length > 0
            ? drizzleMeasurements
            : undefined,
        failed: parityFailed,
        regressionLog,
      })
    }
  }
}

export function formatBenchmarkResults(
  benchmarkResults: BenchmarkResult[],
  config: BenchmarkConfig,
) {
  return {
    version: config.version,
    dialect: config.dialect,
    tests: benchmarkResults.map((r) => ({
      name: r.name,
      prismaMs: Math.round(r.prismaMs * 1000) / 1000,
      extendedMs: Math.round(r.extendedMs * 1000) / 1000,
      drizzleMs: Math.round(r.drizzleMs * 1000) / 1000,
      speedupVsPrisma: Math.round(r.speedupVsPrisma * 100) / 100,
      speedupVsDrizzle: Math.round(r.speedupVsDrizzle * 100) / 100,
      prismaStats: {
        mean: Math.round(r.prismaStats.mean * 1000) / 1000,
        median: Math.round(r.prismaStats.median * 1000) / 1000,
        stdDev: Math.round(r.prismaStats.stdDev * 1000) / 1000,
        min: Math.round(r.prismaStats.min * 1000) / 1000,
        max: Math.round(r.prismaStats.max * 1000) / 1000,
        cv: Math.round(r.prismaStats.cv * 100) / 100,
        p95: Math.round(r.prismaStats.p95 * 1000) / 1000,
        p99: Math.round(r.prismaStats.p99 * 1000) / 1000,
        iterations: r.prismaStats.iterations,
      },
      extendedStats: {
        mean: Math.round(r.extendedStats.mean * 1000) / 1000,
        median: Math.round(r.extendedStats.median * 1000) / 1000,
        stdDev: Math.round(r.extendedStats.stdDev * 1000) / 1000,
        min: Math.round(r.extendedStats.min * 1000) / 1000,
        max: Math.round(r.extendedStats.max * 1000) / 1000,
        cv: Math.round(r.extendedStats.cv * 100) / 100,
        p95: Math.round(r.extendedStats.p95 * 1000) / 1000,
        p99: Math.round(r.extendedStats.p99 * 1000) / 1000,
        iterations: r.extendedStats.iterations,
      },
      drizzleStats: r.drizzleStats
        ? {
            mean: Math.round(r.drizzleStats.mean * 1000) / 1000,
            median: Math.round(r.drizzleStats.median * 1000) / 1000,
            stdDev: Math.round(r.drizzleStats.stdDev * 1000) / 1000,
            min: Math.round(r.drizzleStats.min * 1000) / 1000,
            max: Math.round(r.drizzleStats.max * 1000) / 1000,
            cv: Math.round(r.drizzleStats.cv * 100) / 100,
            p95: Math.round(r.drizzleStats.p95 * 1000) / 1000,
            p99: Math.round(r.drizzleStats.p99 * 1000) / 1000,
            iterations: r.drizzleStats.iterations,
          }
        : undefined,
      regressionLog: r.regressionLog,
    })),
    avgSpeedupVsPrisma:
      Math.round(
        (benchmarkResults.reduce((sum, r) => sum + r.speedupVsPrisma, 0) /
          benchmarkResults.length) *
          100,
      ) / 100,
    avgSpeedupVsDrizzle: (() => {
      const drizzleResults = benchmarkResults.filter((r) => r.drizzleMs > 0)
      return drizzleResults.length > 0
        ? Math.round(
            (drizzleResults.reduce((sum, r) => sum + r.speedupVsDrizzle, 0) /
              drizzleResults.length) *
              100,
          ) / 100
        : 0
    })(),
    timestamp: new Date().toISOString(),
  }
}

export async function outputBenchmarkResults(
  benchmarkResults: BenchmarkResult[],
  config: BenchmarkConfig,
) {
  const results = formatBenchmarkResults(benchmarkResults, config)

  const hasFailures = benchmarkResults.some((r) => r.failed)

  if (hasFailures) {
    console.log(
      '\n⚠ Benchmark results and statistics suppressed due to test failures (set FORCE_BENCHMARK_STATS=1 to override)',
    )
    return
  }

  if (config.shouldOutputJson) {
    const fs = await import('fs')
    const path = await import('path')
    const resultsDir = path.join(process.cwd(), 'benchmark-results')

    if (!fs.existsSync(resultsDir)) {
      fs.mkdirSync(resultsDir, { recursive: true })
    }

    const outputPath = path.join(
      resultsDir,
      `v${config.version}-${config.dialect}-latest.json`,
    )

    fs.writeFileSync(outputPath, JSON.stringify(results, null, 2))

    if (hasFailures) {
      console.log(
        `\n✓ Benchmark results saved to: ${outputPath} (with failures)`,
      )
      return
    } else {
      console.log(`\n✓ Benchmark results saved to: ${outputPath}`)
    }

    outputDistributionGraphs(benchmarkResults, config)
    return
  }

  console.log(`\n=== ${config.dialect.toUpperCase()} Benchmark Results ===`)
  console.log(
    '| Test | Prisma (ms) | Extended (ms) | Drizzle (ms) | vs Prisma | vs Drizzle | Prisma CV% | Extended CV% |',
  )
  console.log(
    '|------|-------------|---------------|--------------|-----------|------------|------------|--------------|',
  )

  for (const r of benchmarkResults) {
    const drizzleCol =
      r.drizzleMs > 0 ? r.drizzleMs.toFixed(3).padStart(12) : 'N/A'.padStart(12)
    const vsDrizzle =
      r.speedupVsDrizzle > 0
        ? `${r.speedupVsDrizzle.toFixed(2)}x`.padStart(10)
        : 'N/A'.padStart(10)

    const prismaCv = `${r.prismaStats.cv.toFixed(1)}%`.padStart(10)
    const extendedCv = `${r.extendedStats.cv.toFixed(1)}%`.padStart(12)

    console.log(
      `| ${r.name.padEnd(40)} | ${r.prismaMs.toFixed(3).padStart(11)} | ${r.extendedMs.toFixed(3).padStart(13)} | ${drizzleCol} | ${r.speedupVsPrisma.toFixed(2).padStart(9)}x | ${vsDrizzle} | ${prismaCv} | ${extendedCv} |`,
    )
  }

  const avgSpeedupPrisma =
    benchmarkResults.reduce((sum, r) => sum + r.speedupVsPrisma, 0) /
    benchmarkResults.length

  const drizzleResults = benchmarkResults.filter((r) => r.drizzleMs > 0)
  const avgSpeedupDrizzle =
    drizzleResults.length > 0
      ? drizzleResults.reduce((sum, r) => sum + r.speedupVsDrizzle, 0) /
        drizzleResults.length
      : 0

  console.log(
    `\nAverage speedup vs Prisma v${config.version}: ${avgSpeedupPrisma.toFixed(2)}x`,
  )
  if (avgSpeedupDrizzle > 0) {
    console.log(`Average speedup vs Drizzle: ${avgSpeedupDrizzle.toFixed(2)}x`)
  }

  const highVariance = benchmarkResults.filter(
    (r) => r.extendedStats.cv > 10 || r.prismaStats.cv > 10,
  )

  if (highVariance.length > 0) {
    console.log(
      `\n⚠ High variance detected (CV > 10%) in ${highVariance.length} tests:`,
    )
    for (const r of highVariance) {
      console.log(
        `  ${r.name}: Prisma ${r.prismaStats.cv.toFixed(1)}%, Extended ${r.extendedStats.cv.toFixed(1)}%`,
      )
    }
  }

  const regressions = benchmarkResults.filter(
    (r) =>
      r.speedupVsPrisma < 1.0 || (r.drizzleMs > 0 && r.speedupVsDrizzle < 1.0),
  )

  if (regressions.length > 0) {
    console.log(
      `\n⚠ Extended slower than baseline (${regressions.length} tests)`,
    )
  }

  outputDistributionGraphs(benchmarkResults, config)
}

export function outputDistributionGraphs(
  benchmarkResults: BenchmarkResult[],
  config: BenchmarkConfig,
) {
  const budgetProbes = benchmarkResults.filter(
    (r) =>
      r.prismaMeasurements &&
      r.extendedMeasurements &&
      (r.name.includes('depth-') ||
        r.name.includes('findFirst depth-') ||
        r.name.includes('findUnique depth-')),
  )

  if (budgetProbes.length === 0) return

  console.log(`\n${'='.repeat(100)}`)
  console.log(
    `BUDGET CALIBRATION PROBE DISTRIBUTIONS - ${config.dialect.toUpperCase()} v${config.version}`,
  )
  console.log('='.repeat(100))

  for (const probe of budgetProbes) {
    if (!probe.prismaMeasurements || !probe.extendedMeasurements) continue

    console.log(`\n${probe.name}`)
    console.log('-'.repeat(100))

    const prismaOutliers = detectOutliers(probe.prismaMeasurements)
    const extendedOutliers = detectOutliers(probe.extendedMeasurements)

    console.log(`\nPrisma v${config.version}:`)
    console.log(
      `  Mean: ${probe.prismaStats.mean.toFixed(3)}ms | Median: ${probe.prismaStats.median.toFixed(3)}ms | StdDev: ${probe.prismaStats.stdDev.toFixed(3)}ms`,
    )
    console.log(
      `  Range: ${probe.prismaStats.min.toFixed(3)}ms - ${probe.prismaStats.max.toFixed(3)}ms | CV: ${probe.prismaStats.cv.toFixed(1)}% | P95: ${probe.prismaStats.p95.toFixed(3)}ms | P99: ${probe.prismaStats.p99.toFixed(3)}ms`,
    )
    if (prismaOutliers.outliers.length > 0) {
      console.log(
        `  Outliers: ${prismaOutliers.outliers.length} (${((prismaOutliers.outliers.length / probe.prismaMeasurements.length) * 100).toFixed(1)}%)`,
      )
    }

    const prismaGraph = renderDistributionGraph(probe.prismaMeasurements)
    prismaGraph.forEach((line) => console.log(line))

    console.log(`\nprisma-sql:`)
    console.log(
      `  Mean: ${probe.extendedStats.mean.toFixed(3)}ms | Median: ${probe.extendedStats.median.toFixed(3)}ms | StdDev: ${probe.extendedStats.stdDev.toFixed(3)}ms`,
    )
    console.log(
      `  Range: ${probe.extendedStats.min.toFixed(3)}ms - ${probe.extendedStats.max.toFixed(3)}ms | CV: ${probe.extendedStats.cv.toFixed(1)}% | P95: ${probe.extendedStats.p95.toFixed(3)}ms | P99: ${probe.extendedStats.p99.toFixed(3)}ms`,
    )
    if (extendedOutliers.outliers.length > 0) {
      console.log(
        `  Outliers: ${extendedOutliers.outliers.length} (${((extendedOutliers.outliers.length / probe.extendedMeasurements.length) * 100).toFixed(1)}%)`,
      )
    }

    const extendedGraph = renderDistributionGraph(probe.extendedMeasurements)
    extendedGraph.forEach((line) => console.log(line))

    if (probe.drizzleMeasurements && probe.drizzleStats) {
      const drizzleOutliers = detectOutliers(probe.drizzleMeasurements)
      console.log(`\nDrizzle:`)
      console.log(
        `  Mean: ${probe.drizzleStats.mean.toFixed(3)}ms | Median: ${probe.drizzleStats.median.toFixed(3)}ms | StdDev: ${probe.drizzleStats.stdDev.toFixed(3)}ms`,
      )
      console.log(
        `  Range: ${probe.drizzleStats.min.toFixed(3)}ms - ${probe.drizzleStats.max.toFixed(3)}ms | CV: ${probe.drizzleStats.cv.toFixed(1)}% | P95: ${probe.drizzleStats.p95.toFixed(3)}ms | P99: ${probe.drizzleStats.p99.toFixed(3)}ms`,
      )
      if (drizzleOutliers.outliers.length > 0) {
        console.log(
          `  Outliers: ${drizzleOutliers.outliers.length} (${((drizzleOutliers.outliers.length / probe.drizzleMeasurements.length) * 100).toFixed(1)}%)`,
        )
      }

      const drizzleGraph = renderDistributionGraph(probe.drizzleMeasurements)
      drizzleGraph.forEach((line) => console.log(line))
    }

    console.log(`\nPerformance:`)
    console.log(
      `  Speedup: ${probe.speedupVsPrisma.toFixed(2)}x | Variance: Prisma ${probe.prismaStats.cv.toFixed(1)}% vs prisma-sql ${probe.extendedStats.cv.toFixed(1)}%`,
    )
    console.log(
      `  Consistency: ${probe.extendedStats.cv < probe.prismaStats.cv ? 'prisma-sql MORE stable' : probe.extendedStats.cv > probe.prismaStats.cv ? 'Prisma MORE stable' : 'EQUAL stability'}`,
    )
    if (probe.speedupVsDrizzle > 0) {
      console.log(`  vs Drizzle: ${probe.speedupVsDrizzle.toFixed(2)}x`)
    }
  }

  console.log(`\n${'='.repeat(100)}\n`)
}
