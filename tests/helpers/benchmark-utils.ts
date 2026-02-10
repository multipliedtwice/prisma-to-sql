import type { TestDB } from './db'
import {
  deepEqual,
  normalizeValue,
  sortByField,
  diffAny,
  stableStringify,
  typeSignature,
} from './compare'
import type { CapturedQuery } from './query-capture'
import {
  withDrizzleCapture,
  withPrismaCapture,
  formatCapturedQueries,
} from './query-capture'

export interface BenchmarkResult {
  name: string
  prismaMs: number
  extendedMs: number
  drizzleMs: number
  speedupVsPrisma: number
  speedupVsDrizzle: number
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
    transformPrisma?: (result: T) => unknown[]
    transformGenerated?: (result: unknown[]) => unknown[]
    transformDrizzle?: (result: unknown[]) => unknown[]
    drizzleQuery?: () => Promise<unknown[]>
  } = {},
): Promise<void> {
  const {
    benchmark = true,
    sortField = 'id',
    transformPrisma = (r) => (Array.isArray(r) ? r : r ? [r] : []),
    transformGenerated = (r) => r,
  } = options

  const { method, ...queryArgs } = args
  const extendedQuery = async () => {
    return await (db.extended as any)[model][method as string](queryArgs)
  }

  const [extendedRaw, prismaRaw] = await Promise.all([
    extendedQuery(),
    prismaQuery(),
  ])

  const prismaResult = transformPrisma(prismaRaw)
  const extendedResult = transformGenerated(extendedRaw as any)

  const sortedPrisma = sortField
    ? sortByField(prismaResult as any[], sortField as any)
    : prismaResult
  const sortedExtended = sortField
    ? sortByField(extendedResult as any[], sortField as any)
    : extendedResult

  const normalizedPrisma = normalizeValue(sortedPrisma)
  const normalizedExtended = normalizeValue(sortedExtended)

  if (!deepEqual(normalizedPrisma, normalizedExtended)) {
    const maxChars = envNum('PARITY_ERROR_MAX_CHARS', 12000)
    const captureSql = envBool('PARITY_CAPTURE_SQL', false)

    const parts: string[] = []
    parts.push(`Parity check failed for ${name}`)
    parts.push(`Model: ${model}`)
    parts.push(`Method: ${String(method)}`)
    parts.push(`Args: ${truncate(JSON.stringify(queryArgs, null, 2), 4000)}`)

    parts.push(``)
    parts.push(`Signature (normalized types)`)
    parts.push(
      `Prisma: ${truncate(JSON.stringify(typeSignature(sortedPrisma), null, 2), 4000)}`,
    )
    parts.push(
      `Extended: ${truncate(JSON.stringify(typeSignature(sortedExtended), null, 2), 4000)}`,
    )

    parts.push(``)
    parts.push(`Diff (first rows)`)
    const diffs = diffAny(normalizedPrisma, normalizedExtended)
    parts.push(diffs.length ? diffs.join('\n\n') : '(no diff produced)')

    const prismaJson = safeJson(normalizedPrisma)
    const extJson = safeJson(normalizedExtended)

    parts.push(``)
    parts.push(`Normalized outputs`)
    parts.push(`Prisma: ${truncate(prismaJson, 4000)}`)
    parts.push(`Extended: ${truncate(extJson, 4000)}`)

    if (captureSql) {
      parts.push(``)
      parts.push(`SQL capture (Prisma baseline)`)
      const prismaCaptured = await withPrismaCapture(async () => prismaQuery())
      parts.push(formatCapturedQueries(prismaCaptured.queries, 10))

      parts.push(``)
      parts.push(`SQL capture (Extended)`)
      const extendedCaptured = await withPrismaCapture(async () =>
        extendedQuery(),
      )
      parts.push(formatCapturedQueries(extendedCaptured.queries, 10))
    } else {
      parts.push(``)
      parts.push(`SQL capture disabled (set PARITY_CAPTURE_SQL=1 to include)`)
    }

    const message = truncate(parts.join('\n'), maxChars)
    throw new Error(message)
  }

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

    const extStart = performance.now()
    for (let i = 0; i < iterations; i++) {
      await extendedQuery()
    }
    const extendedMs = (performance.now() - extStart) / iterations

    const prismaStart = performance.now()
    for (let i = 0; i < iterations; i++) {
      await prismaQuery()
    }
    const prismaMs = (performance.now() - prismaStart) / iterations

    let drizzleMs = 0
    if (options.drizzleQuery) {
      const drizzleStart = performance.now()
      for (let i = 0; i < iterations; i++) {
        await options.drizzleQuery()
      }
      drizzleMs = (performance.now() - drizzleStart) / iterations
    }

    const speedupVsPrisma = prismaMs / extendedMs
    const speedupVsDrizzle = drizzleMs > 0 ? drizzleMs / extendedMs : 0

    let regressionLog: BenchmarkResult['regressionLog'] | undefined

    const isRegression =
      speedupVsPrisma < 1.0 || (drizzleMs > 0 && speedupVsDrizzle < 1.0)

    if (isRegression) {
      const prismaCaptured = await withPrismaCapture(async () => prismaQuery())
      const extendedCaptured = await withPrismaCapture(async () =>
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

    benchmarkResults.push({
      name,
      prismaMs,
      extendedMs,
      drizzleMs,
      speedupVsPrisma,
      speedupVsDrizzle,
      regressionLog,
    })
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
    console.log(`\n✓ Benchmark results saved to: ${outputPath}`)
    return
  }

  console.log(`\n=== ${config.dialect.toUpperCase()} Benchmark Results ===`)
  console.log(
    '| Test | Prisma (ms) | Extended (ms) | Drizzle (ms) | vs Prisma | vs Drizzle |',
  )
  console.log(
    '|------|-------------|---------------|--------------|-----------|------------|',
  )

  for (const r of benchmarkResults) {
    const drizzleCol =
      r.drizzleMs > 0 ? r.drizzleMs.toFixed(3).padStart(12) : 'N/A'.padStart(12)
    const vsDrizzle =
      r.speedupVsDrizzle > 0
        ? `${r.speedupVsDrizzle.toFixed(2)}x`.padStart(10)
        : 'N/A'.padStart(10)

    console.log(
      `| ${r.name.padEnd(40)} | ${r.prismaMs.toFixed(3).padStart(11)} | ${r.extendedMs.toFixed(3).padStart(13)} | ${drizzleCol} | ${r.speedupVsPrisma.toFixed(2).padStart(9)}x | ${vsDrizzle} |`,
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

  const regressions = benchmarkResults.filter(
    (r) =>
      r.speedupVsPrisma < 1.0 || (r.drizzleMs > 0 && r.speedupVsDrizzle < 1.0),
  )

  if (regressions.length > 0) {
    console.log(
      `\n⚠ Extended slower than baseline (${regressions.length} tests)`,
    )
  }
}
