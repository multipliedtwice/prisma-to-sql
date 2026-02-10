import { execSync } from 'child_process'
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'fs'
import path from 'path'

type CapturedQuery = {
  sql: string
  params: unknown[]
  durationMs?: number
}

interface BenchmarkTest {
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

interface BenchmarkResult {
  version: number
  dialect: 'postgres' | 'sqlite'
  tests: BenchmarkTest[]
  avgSpeedupVsPrisma: number
  avgSpeedupVsDrizzle: number
  timestamp: string
}

interface Regression {
  name: string
  extendedMs: number
  opponent: string
  opponentMs: number
  speedup: number
  sourceVersion: 6 | 7
  source: BenchmarkTest
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

  delete pkg.dependencies?.['@prisma/client']
  delete pkg.dependencies?.prisma
  delete pkg.devDependencies?.['@prisma/client']
  delete pkg.devDependencies?.prisma
  delete pkg.dependencies?.['@prisma/client-v7']
  delete pkg.dependencies?.['prisma-v7']

  pkg.devDependencies = pkg.devDependencies || {}
  pkg.devDependencies['@prisma/client'] = prismaVersion
  pkg.devDependencies.prisma = prismaVersion
  pkg.devDependencies['@prisma/adapter-better-sqlite3'] = adapterVersion
  pkg.devDependencies['@prisma/adapter-pg'] = adapterVersion

  pkg.dependencies = pkg.dependencies || {}
  pkg.dependencies['@prisma/generator-helper'] =
    version === 6 ? '^6.16.3' : '^7.2.0'
  pkg.dependencies['@prisma/internals'] = version === 6 ? '^6.16.3' : '^7.2.0'

  writeFileSync(packageJsonPath, JSON.stringify(pkg, null, 2) + '\n')

  console.log('Installing dependencies...')
  execSync('npm install', { stdio: 'inherit' })
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

function fmtMs(n: number) {
  return `${n?.toFixed(3)}ms`
}

function fmtX(n: number) {
  return `${n?.toFixed(2)}x`
}

function wrapText(input: string, width: number) {
  const text = (input ?? '').trim()
  if (!text) return []
  const out: string[] = []
  let i = 0
  while (i < text.length) {
    let end = Math.min(text.length, i + width)
    if (end < text.length) {
      const lastSpace = text.lastIndexOf(' ', end)
      if (lastSpace > i + Math.floor(width * 0.6)) end = lastSpace
    }
    out.push(text.slice(i, end).trim())
    i = end
    while (i < text.length && text[i] === ' ') i++
  }
  return out
}

function printKV(key: string, value: string, indent = '    ') {
  console.log(`${indent}${key.padEnd(18)}${value}`)
}

function printSqlBlock(title: string, sql: string, indent = '    ') {
  console.log(`${indent}${title}:`)
  const lines = wrapText(sql, 110)
  if (lines.length === 0) {
    console.log(`${indent}  (empty)`)
    return
  }
  for (const line of lines) {
    console.log(`${indent}  ${line}`)
  }
}

function printParamsBlock(title: string, params: unknown, indent = '    ') {
  console.log(`${indent}${title}: ${JSON.stringify(params)}`)
}

function printCapturedSection(title: string, qs: CapturedQuery[]) {
  const shown = qs.slice(0, 10)
  console.log(`    ${title}: ${qs.length}`)
  if (qs.length === 0) {
    console.log(`      (none captured)`)
    return
  }
  console.log(`      showing: ${shown.length}`)
  for (let i = 0; i < shown.length; i++) {
    const q = shown[i]
    console.log(`      ${i + 1})`)
    printSqlBlock('sql', q.sql, '        ')
    printParamsBlock('params', q.params, '        ')
    if (typeof q.durationMs === 'number') {
      printKV('duration', fmtMs(q.durationMs), '        ')
    }
  }
}

function printRegressionDetails(r: Regression) {
  const log = r.source.regressionLog
  if (!log) {
    console.log(`    Context: (no regressionLog)`)
    return
  }

  console.log(`\n    BASELINE`)
  if (r.opponent.startsWith('Prisma')) {
    printCapturedSection('Prisma queries', log.prismaQueries)
  } else if (r.opponent === 'Drizzle') {
    printCapturedSection('Drizzle queries', log.drizzleQueries)
  } else {
    printCapturedSection('Prisma queries', log.prismaQueries)
    console.log('')
    printCapturedSection('Drizzle queries', log.drizzleQueries)
  }

  console.log(`\n    EXTENDED`)
  printCapturedSection('Extended queries', log.extendedQueries)
}

function printComparison(results: BenchmarkResult[]) {
  console.log('\n' + '='.repeat(140))
  console.log('BENCHMARK RESULTS - Prisma v6 vs v7 vs Generated SQL')
  console.log('='.repeat(140))

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
    console.log('-'.repeat(140))

    const v6 = dialectResults.find((r) => r.version === 6)
    const v7 = dialectResults.find((r) => r.version === 7)

    if (!v6 || !v7) continue

    console.log(
      '| Test                                     | Prisma v6 | Prisma v7 | Extended  | Drizzle  | v6 Speedup | v7 Speedup | vs Drizzle |',
    )
    console.log(
      '|------------------------------------------|-----------|-----------|-----------|----------|------------|------------|------------|',
    )

    const testNames = new Set([
      ...v6.tests.map((t) => t.name),
      ...v7.tests.map((t) => t.name),
    ])

    const regressions: Regression[] = []

    for (const testName of testNames) {
      const v6Test = v6.tests.find((t) => t.name === testName)
      const v7Test = v7.tests.find((t) => t.name === testName)

      if (!v6Test || !v7Test) continue

      const name = testName.padEnd(40)
      const v6Time = (v6Test.prismaMs?.toFixed(2) + 'ms').padStart(9)
      const v7Time = (v7Test.prismaMs?.toFixed(2) + 'ms').padStart(9)
      const extTime = (v6Test.extendedMs?.toFixed(2) + 'ms').padStart(9)
      const drizzleTime =
        v6Test.drizzleMs > 0
          ? (v6Test.drizzleMs?.toFixed(2) + 'ms').padStart(8)
          : 'N/A'.padStart(8)
      const v6Speedup = (v6Test.speedupVsPrisma?.toFixed(2) + 'x').padStart(10)
      const v7Speedup = (v7Test.speedupVsPrisma?.toFixed(2) + 'x').padStart(10)
      const drizzleSpeedup =
        v6Test.speedupVsDrizzle > 0
          ? (v6Test.speedupVsDrizzle?.toFixed(2) + 'x').padStart(10)
          : 'N/A'.padStart(10)

      console.log(
        `| ${name} | ${v6Time} | ${v7Time} | ${extTime} | ${drizzleTime} | ${v6Speedup} | ${v7Speedup} | ${drizzleSpeedup} |`,
      )

      if (v6Test.speedupVsPrisma < 1.0) {
        regressions.push({
          name: testName,
          extendedMs: v6Test.extendedMs,
          opponent: 'Prisma v6',
          opponentMs: v6Test.prismaMs,
          speedup: v6Test.speedupVsPrisma,
          sourceVersion: 6,
          source: v6Test,
        })
      }

      if (v7Test.speedupVsPrisma < 1.0) {
        regressions.push({
          name: testName,
          extendedMs: v7Test.extendedMs,
          opponent: 'Prisma v7',
          opponentMs: v7Test.prismaMs,
          speedup: v7Test.speedupVsPrisma,
          sourceVersion: 7,
          source: v7Test,
        })
      }

      if (v6Test.drizzleMs > 0 && v6Test.speedupVsDrizzle < 1.0) {
        regressions.push({
          name: testName,
          extendedMs: v6Test.extendedMs,
          opponent: 'Drizzle',
          opponentMs: v6Test.drizzleMs,
          speedup: v6Test.speedupVsDrizzle,
          sourceVersion: 6,
          source: v6Test,
        })
      }
    }

    console.log('\n' + '-'.repeat(140))
    console.log('Summary:')
    console.log(
      `  Generated SQL vs Prisma v6: ${v6.avgSpeedupVsPrisma?.toFixed(2)}x faster`,
    )
    console.log(
      `  Generated SQL vs Prisma v7: ${v7.avgSpeedupVsPrisma?.toFixed(2)}x faster`,
    )
    if (v6.avgSpeedupVsDrizzle > 0) {
      console.log(
        `  Generated SQL vs Drizzle:   ${v6.avgSpeedupVsDrizzle?.toFixed(2)}x faster`,
      )
    }

    if (regressions.length > 0) {
      regressions.sort((a, b) => a.speedup - b.speedup)
      console.log(
        `\n⚠ ${dialect.toUpperCase()} — Generated SQL slower than baseline (${regressions.length}):`,
      )

      for (let i = 0; i < regressions.length; i++) {
        const r = regressions[i]
        console.log('\n' + '─'.repeat(140))
        console.log(
          `  [${String(i + 1).padStart(2, '0')}/${String(regressions.length).padStart(2, '0')}] ${r.name}`,
        )
        printKV('opponent', r.opponent, '    ')
        printKV(
          'speedup',
          `${fmtX(r.speedup)} (extended ${fmtMs(r.extendedMs)} vs ${fmtMs(r.opponentMs)})`,
          '    ',
        )

        console.log(`\n    PERF`)
        printKV('extended_ms', fmtMs(r.extendedMs), '      ')
        printKV('opponent_ms', fmtMs(r.opponentMs), '      ')

        printRegressionDetails(r)
      }

      console.log('\n' + '─'.repeat(140))
    }
  }
}

async function cleanupGeneratedSchemas() {
  const { unlink } = await import('fs/promises')
  const schemasToClean = [
    'schema-postgres.prisma',
    'schema-postgres-v7.prisma',
    'schema-sqlite.prisma',
    'schema-sqlite-v7.prisma',
  ]

  for (const schema of schemasToClean) {
    try {
      await unlink(path.join(process.cwd(), 'tests', 'prisma', schema))
    } catch {}
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

  console.log('\n' + '='.repeat(140))
  console.log(`✓ Results saved to: ${summaryPath}`)
  console.log('='.repeat(140) + '\n')

  console.log('Cleaning up generated schemas...')
  await cleanupGeneratedSchemas()
  console.log('✓ Cleanup complete\n')
}

main().catch((error) => {
  console.error('Benchmark failed:', error)
  process.exit(1)
})
