import { execSync } from 'child_process'
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'fs'
import path from 'path'

interface BenchmarkResult {
  version: number
  dialect: 'postgres' | 'sqlite'
  tests: Array<{
    name: string
    prismaMs: number
    generatedMs: number
    sqlGenMs: number
    drizzleMs: number
    speedupVsPrisma: number
    speedupVsDrizzle: number
  }>
  avgSpeedupVsPrisma: number
  avgSpeedupVsDrizzle: number
  timestamp: string
}

const RESULTS_DIR = path.join(process.cwd(), 'benchmark-results')

async function ensureResultsDir() {
  if (!existsSync(RESULTS_DIR)) {
    mkdirSync(RESULTS_DIR, { recursive: true })
  }
}

async function switchPrismaVersion(version: 6 | 7) {
  console.log(`\n${'='.repeat(60)}`)
  console.log(`Switching to Prisma v${version}...`)
  console.log('='.repeat(60))

  const packageJsonPath = path.join(process.cwd(), 'package.json')
  const pkg = JSON.parse(readFileSync(packageJsonPath, 'utf-8'))

  const prismaVersion = version === 6 ? '6.16.3' : '7.2.0'
  const adapterVersion = version === 6 ? '^6.16.3' : '^7.2.0'

  // Remove from all locations first to avoid conflicts
  delete pkg.dependencies?.['@prisma/client']
  delete pkg.dependencies?.prisma
  delete pkg.devDependencies?.['@prisma/client']
  delete pkg.devDependencies?.prisma
  delete pkg.dependencies?.['@prisma/client-v7']
  delete pkg.dependencies?.['prisma-v7']

  // Set in devDependencies (where they belong for a library)
  pkg.devDependencies = pkg.devDependencies || {}
  pkg.devDependencies['@prisma/client'] = prismaVersion
  pkg.devDependencies.prisma = prismaVersion
  pkg.devDependencies['@prisma/adapter-better-sqlite3'] = adapterVersion
  pkg.devDependencies['@prisma/adapter-pg'] = adapterVersion

  // Keep these at v7 since they're used in the generator itself
  pkg.dependencies = pkg.dependencies || {}
  pkg.dependencies['@prisma/generator-helper'] = '^7.2.0'
  pkg.dependencies['@prisma/internals'] = '^7.2.0'

  writeFileSync(packageJsonPath, JSON.stringify(pkg, null, 2) + '\n')

  console.log('Installing dependencies...')
  execSync('yarn install', { stdio: 'inherit' })
  console.log(`✓ Switched to Prisma v${version}\n`)
}

async function runBenchmark(
  version: 6 | 7,
  dialect: 'postgres' | 'sqlite',
): Promise<BenchmarkResult> {
  console.log(`\n${'='.repeat(60)}`)
  console.log(
    `Running Prisma v${version} ${dialect.toUpperCase()} benchmarks...`,
  )
  console.log('='.repeat(60))

  const testFile =
    dialect === 'postgres'
      ? 'tests/e2e/postgres.test.ts'
      : 'tests/e2e/sqlite.e2e.test.ts'

  const env = {
    ...process.env,
    PRISMA_VERSION: version.toString(),
    BENCHMARK_JSON_OUTPUT: '1',
  }

  try {
    // Use the e2e config explicitly
    execSync(
      `npx vitest run ${testFile} --config vitest.config.e2e.ts --reporter=dot`,
      {
        env,
        stdio: 'inherit',
      },
    )
  } catch (error) {
    console.error(`Benchmark failed for v${version} ${dialect}`)
    throw error
  }

  const resultFile = path.join(
    RESULTS_DIR,
    `v${version}-${dialect}-latest.json`,
  )

  if (!existsSync(resultFile)) {
    throw new Error(`Result file not found: ${resultFile}`)
  }

  return JSON.parse(readFileSync(resultFile, 'utf-8'))
}

function printComparison(results: BenchmarkResult[]) {
  console.log('\n' + '='.repeat(120))
  console.log('BENCHMARK RESULTS - Prisma v6 vs v7 vs Generated SQL')
  console.log('='.repeat(120))

  const byDialect = results.reduce(
    (acc, r) => {
      if (!acc[r.dialect]) acc[r.dialect] = []
      acc[r.dialect].push(r)
      return acc
    },
    {} as Record<string, BenchmarkResult[]>,
  )

  for (const [dialect, dialectResults] of Object.entries(byDialect)) {
    console.log(`\n${dialect.toUpperCase()} Results:`)
    console.log('-'.repeat(120))

    const v6 = dialectResults.find((r) => r.version === 6)
    const v7 = dialectResults.find((r) => r.version === 7)

    if (!v6 || !v7) continue

    console.log(
      '| Test                                     | Prisma v6 | Prisma v7 | Generated | Drizzle  | v6 Speedup | v7 Speedup |',
    )
    console.log(
      '|------------------------------------------|-----------|-----------|-----------|----------|------------|------------|',
    )

    const testNames = new Set([
      ...v6.tests.map((t) => t.name),
      ...v7.tests.map((t) => t.name),
    ])

    for (const testName of testNames) {
      const v6Test = v6.tests.find((t) => t.name === testName)
      const v7Test = v7.tests.find((t) => t.name === testName)

      if (!v6Test || !v7Test) continue

      const name = testName.padEnd(40)
      const v6Time = (v6Test.prismaMs.toFixed(2) + 'ms').padStart(9)
      const v7Time = (v7Test.prismaMs.toFixed(2) + 'ms').padStart(9)
      const genTime = (v6Test.generatedMs.toFixed(2) + 'ms').padStart(9)
      const drizzleTime =
        v6Test.drizzleMs > 0
          ? (v6Test.drizzleMs.toFixed(2) + 'ms').padStart(8)
          : 'N/A'.padStart(8)
      const v6Speedup = (v6Test.speedupVsPrisma.toFixed(2) + 'x').padStart(10)
      const v7Speedup = (v7Test.speedupVsPrisma.toFixed(2) + 'x').padStart(10)

      console.log(
        `| ${name} | ${v6Time} | ${v7Time} | ${genTime} | ${drizzleTime} | ${v6Speedup} | ${v7Speedup} |`,
      )
    }

    console.log('\n' + '-'.repeat(120))
    console.log('Summary:')
    console.log(
      `  Generated SQL vs Prisma v6: ${v6.avgSpeedupVsPrisma.toFixed(2)}x faster`,
    )
    console.log(
      `  Generated SQL vs Prisma v7: ${v7.avgSpeedupVsPrisma.toFixed(2)}x faster`,
    )
    if (v6.avgSpeedupVsDrizzle > 0) {
      console.log(
        `  Generated SQL vs Drizzle:   ${v6.avgSpeedupVsDrizzle.toFixed(2)}x faster`,
      )
    }
  }
}

async function main() {
  await ensureResultsDir()

  const dialects: Array<'postgres' | 'sqlite'> = ['postgres', 'sqlite']
  const versions: Array<6 | 7> = [6, 7]
  const allResults: BenchmarkResult[] = []

  for (const version of versions) {
    await switchPrismaVersion(version)

    for (const dialect of dialects) {
      const result = await runBenchmark(version, dialect)
      allResults.push(result)
    }
  }

  const summaryPath = path.join(
    RESULTS_DIR,
    `summary-${new Date().toISOString().split('T')[0]}.json`,
  )
  writeFileSync(summaryPath, JSON.stringify(allResults, null, 2))

  printComparison(allResults)

  console.log('\n' + '='.repeat(120))
  console.log(`✓ Results saved to: ${summaryPath}`)
  console.log('='.repeat(120) + '\n')
}

main().catch((error) => {
  console.error('Benchmark failed:', error)
  process.exit(1)
})
