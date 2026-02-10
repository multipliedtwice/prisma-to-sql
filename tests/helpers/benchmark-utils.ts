import type { DMMF } from '@prisma/generator-helper'
import type { DirectiveProps } from '@dee-wan/schema-parser'
import type { TestDB } from './db'
import { getDatamodel } from './datamodel'
import { generateSQL } from '../../src/sql-generator'
import { deepEqual, normalizeValue, sortByField } from './compare'

export interface BenchmarkResult {
  name: string
  prismaMs: number
  generatedMs: number
  sqlGenMs: number
  generatedServerMs: number
  generatedClientMs: number
  drizzleMs: number
  speedupVsPrisma: number
  speedupVsDrizzle: number
  generatedSql: string
}

export interface BenchmarkConfig {
  version: number
  dialect: 'postgres' | 'sqlite'
  shouldOutputJson: boolean
}

export function createQuery(
  modelName: string,
  q: Record<string, unknown>,
  datamodel: DMMF.Datamodel,
): DirectiveProps {
  const model = datamodel.models.find((m) => m.name === modelName)!
  const method = q.method as string

  return {
    method,
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

async function measureGeneratedServerMs(
  db: TestDB,
  sql: string,
  params: unknown[],
  iterations: number,
): Promise<number> {
  const isPostgres = db.dialect === 'postgres'
  const n = isPostgres ? Math.min(3, iterations) : iterations

  if (n <= 0) return 0

  await db.measureServerMs(sql, params)

  let total = 0
  for (let i = 0; i < n; i++) {
    total += await db.measureServerMs(sql, params)
  }

  return total / n
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
    drizzleQuery,
  } = options

  const dialect = db.dialect

  const datamodel = await getDatamodel(dialect)
  const directive = createQuery(model, args, datamodel)

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
    const SQL_GEN_ITERATIONS = 100
    const sqlGenStart = performance.now()
    for (let i = 0; i < SQL_GEN_ITERATIONS; i++) {
      generateSQL(directive)
    }
    const sqlGenMs = (performance.now() - sqlGenStart) / SQL_GEN_ITERATIONS

    const warmupStart = performance.now()
    await prismaQuery()
    const estimatedPrismaMs = performance.now() - warmupStart

    const iterations =
      options.iterations ??
      (estimatedPrismaMs > 200 ? 5 : estimatedPrismaMs > 50 ? 10 : 50)

    for (let w = 0; w < 3; w++) {
      await db.execute(generated.sql, generated.staticParams)
      await prismaQuery()
      if (drizzleQuery) await drizzleQuery()
    }

    const genStart = performance.now()
    for (let i = 0; i < iterations; i++) {
      await db.execute(generated.sql, generated.staticParams)
    }
    const generatedMs = (performance.now() - genStart) / iterations

    const generatedServerMs = await measureGeneratedServerMs(
      db,
      generated.sql,
      generated.staticParams,
      iterations,
    )

    const generatedClientMs = Math.max(0, generatedMs - generatedServerMs)

    const prismaStart = performance.now()
    for (let i = 0; i < iterations; i++) {
      await prismaQuery()
    }
    const prismaMs = (performance.now() - prismaStart) / iterations

    let drizzleMs = 0
    if (drizzleQuery) {
      const drizzleStart = performance.now()
      for (let i = 0; i < iterations; i++) {
        await drizzleQuery()
      }
      drizzleMs = (performance.now() - drizzleStart) / iterations
    }

    const totalGeneratedMs = generatedMs + sqlGenMs

    benchmarkResults.push({
      name,
      prismaMs,
      generatedMs,
      sqlGenMs,
      generatedServerMs,
      generatedClientMs,
      drizzleMs,
      speedupVsPrisma: prismaMs / totalGeneratedMs,
      speedupVsDrizzle: drizzleMs > 0 ? drizzleMs / totalGeneratedMs : 0,
      generatedSql: generated.sql,
    })
  }
}

function getTotalGeneratedMs(r: BenchmarkResult): number {
  return r.generatedMs + r.sqlGenMs
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
      generatedServerMs: Math.round(r.generatedServerMs * 1000) / 1000,
      generatedClientMs: Math.round(r.generatedClientMs * 1000) / 1000,
      sqlGenMs: Math.round(r.sqlGenMs * 1000) / 1000,
      totalGeneratedMs: Math.round(getTotalGeneratedMs(r) * 1000) / 1000,
      drizzleMs: Math.round(r.drizzleMs * 1000) / 1000,
      speedupVsPrisma: Math.round(r.speedupVsPrisma * 100) / 100,
      speedupVsDrizzle: Math.round(r.speedupVsDrizzle * 100) / 100,
      generatedSql: r.generatedSql,
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
      '| Test | Prisma (ms) | Gen Total | Gen Client | Gen Server | SQL Gen | Drizzle (ms) | vs Prisma | vs Drizzle |',
    )
    console.log(
      '|------|-------------|----------:|-----------:|-----------:|--------:|-------------|----------:|----------:|',
    )

    for (const r of benchmarkResults) {
      const totalGenerated = getTotalGeneratedMs(r)
      const drizzleCol =
        r.drizzleMs > 0
          ? r.drizzleMs.toFixed(3).padStart(12)
          : 'N/A'.padStart(12)
      const vsDrizzle =
        r.speedupVsDrizzle > 0
          ? `${r.speedupVsDrizzle.toFixed(2)}x`.padStart(10)
          : 'N/A'.padStart(10)

      console.log(
        `| ${r.name.padEnd(40)} | ${r.prismaMs.toFixed(3).padStart(11)} | ${totalGenerated.toFixed(3).padStart(9)} | ${r.generatedClientMs.toFixed(3).padStart(10)} | ${r.generatedServerMs.toFixed(3).padStart(10)} | ${r.sqlGenMs.toFixed(3).padStart(7)} | ${drizzleCol} | ${r.speedupVsPrisma.toFixed(2).padStart(9)}x | ${vsDrizzle} |`,
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

    const regressions: Array<{
      name: string
      totalMs: number
      sqlGenMs: number
      execMs: number
      clientMs: number
      serverMs: number
      opponent: string
      opponentMs: number
      speedup: number
      sql: string
    }> = []

    for (const r of benchmarkResults) {
      const totalGenerated = getTotalGeneratedMs(r)
      if (r.speedupVsPrisma < 1.0) {
        regressions.push({
          name: r.name,
          totalMs: totalGenerated,
          sqlGenMs: r.sqlGenMs,
          execMs: r.generatedMs,
          clientMs: r.generatedClientMs,
          serverMs: r.generatedServerMs,
          opponent: `Prisma v${config.version}`,
          opponentMs: r.prismaMs,
          speedup: r.speedupVsPrisma,
          sql: r.generatedSql,
        })
      }
      if (r.drizzleMs > 0 && r.speedupVsDrizzle < 1.0) {
        regressions.push({
          name: r.name,
          totalMs: totalGenerated,
          sqlGenMs: r.sqlGenMs,
          execMs: r.generatedMs,
          clientMs: r.generatedClientMs,
          serverMs: r.generatedServerMs,
          opponent: 'Drizzle',
          opponentMs: r.drizzleMs,
          speedup: r.speedupVsDrizzle,
          sql: r.generatedSql,
        })
      }
    }

    if (regressions.length > 0) {
      regressions.sort((a, b) => a.speedup - b.speedup)
      console.log(
        `\n⚠ Generated SQL slower than baseline (${regressions.length}):`,
      )
      for (const r of regressions) {
        console.log(
          `\n    ${r.name} vs ${r.opponent}: generated=${r.totalMs.toFixed(3)}ms (sqlGen=${r.sqlGenMs.toFixed(3)}ms + exec=${r.execMs.toFixed(3)}ms; execSplit client=${r.clientMs.toFixed(3)}ms + server=${r.serverMs.toFixed(3)}ms) vs ${r.opponentMs.toFixed(3)}ms → ${r.speedup.toFixed(2)}x`,
        )
        console.log(`    SQL: ${r.sql}`)
      }
    }
  }
}
