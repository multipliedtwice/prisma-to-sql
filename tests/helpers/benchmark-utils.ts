import type { DMMF } from '@prisma/generator-helper'
import type { DirectiveProps } from '@dee-wan/schema-parser'
import { datamodel } from './datamodel'
import { generateSQL } from '../../src/sql-generator'
import { deepEqual, normalizeValue, sortByField } from './compare'

export interface BenchmarkResult {
  name: string
  prismaMs: number
  generatedMs: number
  sqlGenMs: number
  drizzleMs: number
  speedupVsPrisma: number
  speedupVsDrizzle: number
}

export interface BenchmarkConfig {
  version: number
  dialect: 'postgres' | 'sqlite'
  shouldOutputJson: boolean
}

export interface TestDB {
  execute: (sql: string, params: unknown[]) => Promise<unknown[]>
  prisma: any
}

export function createQuery(
  modelName: string,
  q: Record<string, unknown>,
): DirectiveProps {
  const model = datamodel.models.find((m) => m.name === modelName)!
  return {
    header: 'test',
    modelName,
    query: { original: q, processed: q, staticValues: [], dynamicKeys: [] },
    parameters: { all: [], required: [], optional: [], typeMap: {} },
    cache: { enabled: false },
    context: {
      model,
      datamodel,
      allModels: datamodel.models as DMMF.Model[],
      enums: [],
    },
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
    transformDrizzle = (r) => r,
    drizzleQuery,
  } = options

  const directive = createQuery(model, args)

  let estimatedPrismaMs = 0
  if (benchmark) {
    const warmupStart = performance.now()
    await prismaQuery()
    estimatedPrismaMs = performance.now() - warmupStart
  }

  const iterations =
    options.iterations ??
    (estimatedPrismaMs > 200 ? 5 : estimatedPrismaMs > 50 ? 10 : 50)

  let sqlGenMs = 0
  if (benchmark) {
    const sqlGenStart = performance.now()
    for (let i = 0; i < iterations; i++) {
      generateSQL(directive)
    }
    sqlGenMs = (performance.now() - sqlGenStart) / iterations
  }

  const generated = generateSQL(directive)

  const [generatedRaw, prismaRaw] = await Promise.all([
    db.execute(generated.sql, generated.staticParams),
    prismaQuery(),
  ])

  const prismaResult = transformPrisma(prismaRaw)
  const generatedResult = transformGenerated(generatedRaw)

  const sortedPrisma = sortField
    ? sortByField(prismaResult as any[], sortField as any)
    : prismaResult
  const sortedGenerated = sortField
    ? sortByField(generatedResult as any[], sortField as any)
    : generatedResult

  const normalizedPrisma = normalizeValue(sortedPrisma)
  const normalizedGenerated = normalizeValue(sortedGenerated)

  if (!deepEqual(normalizedPrisma, normalizedGenerated)) {
    console.log('\n=== PARITY FAILURE ===')
    console.log('Test:', name)
    console.log('SQL:', generated.sql)
    console.log('Params:', generated.staticParams)
    throw new Error(`Parity check failed for ${name}`)
  }

  if (benchmark) {
    const genStart = performance.now()
    for (let i = 0; i < iterations; i++) {
      await db.execute(generated.sql, generated.staticParams)
    }
    const generatedMs = (performance.now() - genStart) / iterations

    const prismaStart = performance.now()
    for (let i = 0; i < iterations; i++) {
      await prismaQuery()
    }
    const prismaMs = (performance.now() - prismaStart) / iterations

    let drizzleMs = 0
    if (drizzleQuery) {
      const drizzleStart = performance.now()
      for (let i = 0; i < iterations; i++) {
        const result = await drizzleQuery()
        if (transformDrizzle) {
          transformDrizzle(result)
        }
      }
      drizzleMs = (performance.now() - drizzleStart) / iterations
    }

    benchmarkResults.push({
      name,
      prismaMs,
      generatedMs,
      sqlGenMs,
      drizzleMs,
      speedupVsPrisma: prismaMs / generatedMs,
      speedupVsDrizzle: drizzleMs > 0 ? drizzleMs / generatedMs : 0,
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
      generatedMs: Math.round(r.generatedMs * 1000) / 1000,
      sqlGenMs: Math.round(r.sqlGenMs * 1000) / 1000,
      drizzleMs: Math.round(r.drizzleMs * 1000) / 1000,
      speedupVsPrisma: Math.round(r.speedupVsPrisma * 100) / 100,
      speedupVsDrizzle: Math.round(r.speedupVsDrizzle * 100) / 100,
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
  } else {
    console.log(`\n=== ${config.dialect.toUpperCase()} Benchmark Results ===`)
    console.log(
      '| Test | Prisma (ms) | Generated (ms) | Drizzle (ms) | SQL Gen (ms) | vs Prisma | vs Drizzle |',
    )
    console.log(
      '|------|-------------|----------------|--------------|--------------|-----------|------------|',
    )

    for (const r of benchmarkResults) {
      const drizzleCol =
        r.drizzleMs > 0
          ? r.drizzleMs.toFixed(3).padStart(12)
          : 'N/A'.padStart(12)
      const vsDrizzle =
        r.speedupVsDrizzle > 0
          ? `${r.speedupVsDrizzle.toFixed(2)}x`.padStart(10)
          : 'N/A'.padStart(10)
      console.log(
        `| ${r.name.padEnd(40)} | ${r.prismaMs.toFixed(3).padStart(11)} | ${r.generatedMs.toFixed(3).padStart(14)} | ${drizzleCol} | ${r.sqlGenMs.toFixed(3).padStart(12)} | ${r.speedupVsPrisma.toFixed(2).padStart(9)}x | ${vsDrizzle} |`,
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
      console.log(
        `Average speedup vs Drizzle: ${avgSpeedupDrizzle.toFixed(2)}x`,
      )
    }
  }
}
