import { execSync } from 'child_process'
import { writeFileSync, readFileSync, existsSync, mkdirSync } from 'fs'
import path from 'path'
import {
  PRISMA_PACKAGES,
  PRISMA_VERSIONS,
  updatePrismaPackages,
  type PrismaVersion,
} from './prisma-versions'

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
  version: PrismaVersion
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
  sourceVersion: PrismaVersion
  source: BenchmarkTest
}

const RESULTS_DIR = path.join(process.cwd(), 'benchmark-results')

async function ensureResultsDir() {
  if (!existsSync(RESULTS_DIR)) {
    mkdirSync(RESULTS_DIR, { recursive: true })
  }
}

async function switchPrismaVersion(version: PrismaVersion) {
  console.log(`\n${'='.repeat(60)}`)
  console.log(`Switching to Prisma v${version}...`)
  console.log('='.repeat(60))

  const packageJsonPath = path.join(process.cwd(), 'package.json')
  const pkg = JSON.parse(readFileSync(packageJsonPath, 'utf-8'))
  const updatedPkg = updatePrismaPackages(pkg, version)

  writeFileSync(packageJsonPath, JSON.stringify(updatedPkg, null, 2) + '\n')

  console.log('Installing dependencies...')
  execSync('npm install', { stdio: 'inherit' })
  console.log(`✓ Switched to Prisma v${version}\n`)
}

async function runBenchmark(
  version: PrismaVersion,
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
      `./node_modules/.bin/vitest run ${testFile} --config vitest.config.e2e.ts --reporter=dot`,
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

function fmtMs(n: number): string {
  return `${n?.toFixed(3)}ms`
}

function fmtMsNoise(n: number): string {
  return `~${Math.round(n)}ms`
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

function printTableRow(cells: string[], widths: number[]) {
  console.log(
    `| ${cells.map((cell, index) => cell.padEnd(widths[index])).join(' | ')} |`,
  )
}

function printComparison(results: BenchmarkResult[]) {
  const NOISE_THRESHOLD_MS = 1.0
  const benchmarkLabels = PRISMA_VERSIONS.map(
    (version) => PRISMA_PACKAGES[version].label,
  ).join(' vs ')

  console.log('\n' + '='.repeat(140))
  console.log(`BENCHMARK RESULTS - ${benchmarkLabels} vs prisma-sql`)
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

    const versionResults = PRISMA_VERSIONS.flatMap((version) => {
      const result = dialectResults.find((candidate) => {
        return candidate.version === version
      })
      return result ? [result] : []
    })

    if (versionResults.length !== PRISMA_VERSIONS.length) continue

    const columns = [
      'Test',
      ...PRISMA_VERSIONS.map((version) => {
        return version === 8 ? 'Prisma v8 dev' : `Prisma v${version}`
      }),
      ...PRISMA_VERSIONS.map((version) => {
        return version === 8 ? 'SQL (v8 dev)' : `SQL (v${version})`
      }),
      'Drizzle',
      ...PRISMA_VERSIONS.map((version) => `v${version} Speedup`),
      'vs Drizzle',
    ]
    const tableRows: string[][] = []

    const testNames = new Set(
      versionResults.flatMap((result) => {
        return result.tests.map((test) => test.name)
      }),
    )

    const regressions: Regression[] = []

    for (const testName of testNames) {
      const versionTests = versionResults.flatMap((result) => {
        const test = result.tests.find((candidate) => {
          return candidate.name === testName
        })
        return test ? [test] : []
      })
      if (versionTests.length !== versionResults.length) continue

      const drizzleTest = versionTests[0]
      const drizzleTime =
        drizzleTest.drizzleMs > 0 ? fmtMs(drizzleTest.drizzleMs) : 'N/A'
      const drizzleSpeedup =
        drizzleTest.speedupVsDrizzle > 0
          ? fmtX(drizzleTest.speedupVsDrizzle)
          : 'N/A'

      tableRows.push([
        testName,
        ...versionTests.map((test) => fmtMs(test.prismaMs)),
        ...versionTests.map((test) => fmtMs(test.extendedMs)),
        drizzleTime,
        ...versionTests.map((test) => fmtX(test.speedupVsPrisma)),
        drizzleSpeedup,
      ])

      versionTests.forEach((test, index) => {
        const version = PRISMA_VERSIONS[index]
        if (
          test.speedupVsPrisma < 1.0 &&
          test.extendedMs - test.prismaMs >= NOISE_THRESHOLD_MS
        ) {
          regressions.push({
            name: testName,
            extendedMs: test.extendedMs,
            opponent: `Prisma v${version}`,
            opponentMs: test.prismaMs,
            speedup: test.speedupVsPrisma,
            sourceVersion: version,
            source: test,
          })
        }
      })

      if (
        drizzleTest.drizzleMs > 0 &&
        drizzleTest.speedupVsDrizzle < 1.0 &&
        drizzleTest.extendedMs - drizzleTest.drizzleMs >= NOISE_THRESHOLD_MS
      ) {
        regressions.push({
          name: testName,
          extendedMs: drizzleTest.extendedMs,
          opponent: 'Drizzle',
          opponentMs: drizzleTest.drizzleMs,
          speedup: drizzleTest.speedupVsDrizzle,
          sourceVersion: 6,
          source: drizzleTest,
        })
      }
    }

    const widths = columns.map((column, index) => {
      const minimumWidth = index === 0 ? 40 : 0
      return Math.max(
        minimumWidth,
        column.length,
        ...tableRows.map((row) => row[index].length),
      )
    })
    printTableRow(columns, widths)
    printTableRow(widths.map((width) => '-'.repeat(width)), widths)
    tableRows.forEach((row) => printTableRow(row, widths))

    console.log('\n' + '-'.repeat(140))
    console.log('Summary:')
    versionResults.forEach((result) => {
      console.log(
        `  prisma-sql vs ${PRISMA_PACKAGES[result.version].label}: ${fmtX(result.avgSpeedupVsPrisma)} faster`,
      )
    })
    const drizzleResult = versionResults[0]
    if (drizzleResult.avgSpeedupVsDrizzle > 0) {
      console.log(
        `  prisma-sql vs Drizzle: ${fmtX(drizzleResult.avgSpeedupVsDrizzle)} faster`,
      )
    }

    if (regressions.length > 0) {
      regressions.sort((a, b) => a.speedup - b.speedup)
      console.log(
        `\n⚠ ${dialect.toUpperCase()} — prisma-sql slower than baseline (${regressions.length}):`,
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
          `${fmtX(r.speedup)} (prisma-sql ${fmtMsNoise(r.extendedMs)} vs ${fmtMsNoise(r.opponentMs)})`,
          '    ',
        )

        console.log(`\n    PERF`)
        printKV('prisma_sql_ms', fmtMsNoise(r.extendedMs), '      ')
        printKV('opponent_ms', fmtMsNoise(r.opponentMs), '      ')

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
    'schema-postgres-v8.prisma',
    'schema-sqlite.prisma',
    'schema-sqlite-v7.prisma',
    'schema-sqlite-v8.prisma',
  ]

  for (const schema of schemasToClean) {
    try {
      await unlink(path.join(process.cwd(), 'tests', 'prisma', schema))
    } catch {}
  }
}

interface PackageState {
  packageJson: string
  packageLock: string
}

function capturePackageState(): PackageState {
  return {
    packageJson: readFileSync(path.join(process.cwd(), 'package.json'), 'utf-8'),
    packageLock: readFileSync(
      path.join(process.cwd(), 'package-lock.json'),
      'utf-8',
    ),
  }
}

function restorePackageState(state: PackageState) {
  const packageJsonPath = path.join(process.cwd(), 'package.json')
  const packageLockPath = path.join(process.cwd(), 'package-lock.json')

  writeFileSync(packageJsonPath, state.packageJson)
  writeFileSync(packageLockPath, state.packageLock)

  try {
    execSync('npm install', { stdio: 'inherit' })
  } finally {
    writeFileSync(packageJsonPath, state.packageJson)
    writeFileSync(packageLockPath, state.packageLock)
  }
}

async function main() {
  const packageState = capturePackageState()
  let benchmarkFailed = false
  let benchmarkError: unknown
  let finalizationError: unknown

  try {
    await ensureResultsDir()

    const dialects: Array<'postgres' | 'sqlite'> = ['postgres', 'sqlite']
    const allResults: BenchmarkResult[] = []

    for (const version of PRISMA_VERSIONS) {
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
  } catch (error) {
    benchmarkFailed = true
    benchmarkError = error
  }

  try {
    console.log('Cleaning up generated schemas...')
    await cleanupGeneratedSchemas()
  } catch (error) {
    finalizationError = error
    console.error('Schema cleanup failed:', error)
  }

  try {
    console.log('Restoring package state...')
    restorePackageState(packageState)
  } catch (error) {
    if (finalizationError === undefined) finalizationError = error
    console.error('Package state restoration failed:', error)
  }

  if (benchmarkFailed) throw benchmarkError
  if (finalizationError !== undefined) throw finalizationError

  console.log('✓ Cleanup complete\n')
}

main().catch((error) => {
  console.error('Benchmark failed:', error)
  process.exit(1)
})
