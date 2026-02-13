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
    drizzleQuery?: () => Promise<unknown[]>
  } = {},
): Promise<void> {
  const { benchmark = true, sortField = 'id' } = options

  const { method, ...queryArgs } = args
  const extendedQuery = async () => {
    return await (db.extended as any)[model][method as string](queryArgs)
  }

  const [extendedRaw, prismaRaw] = await Promise.all([
    extendedQuery(),
    prismaQuery(),
  ])

  const sortedPrisma = sortField
    ? sortByField(prismaRaw as any[], sortField as any)
    : prismaRaw
  const sortedExtended = sortField
    ? sortByField(extendedRaw as any[], sortField as any)
    : extendedRaw

  const normalizedPrisma = deepSortNestedArrays(normalizeValue(sortedPrisma))
  const normalizedExtended = deepSortNestedArrays(
    normalizeValue(sortedExtended),
  )
  // console.log(
  //   'normalizedExtended :>> ',
  //   Array.isArray(normalizedExtended)
  //     ? `Array: [${JSON.stringify(normalizedExtended[0])}, ...]`
  //     : normalizedExtended,
  // )
  if (!deepEqual(normalizedPrisma, normalizedExtended)) {
    const maxChars = 12000
    const captureSql = envBool('PARITY_CAPTURE_SQL', false)

    const parts: string[] = []
    parts.push(`Parity check failed for ${name}`)
    parts.push(`Model: ${model}, Method: ${String(method)}`)

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
      const prismaCaptured = await withPrismaCapture(async () => prismaQuery())
      parts.push(formatCapturedQueries(prismaCaptured.queries, 2))

      parts.push(``)
      parts.push(`SQL (Extended):`)
      const extendedCaptured = await withPrismaCapture(async () =>
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
