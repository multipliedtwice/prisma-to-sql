import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'

export type BenchmarkDialect = 'postgres' | 'sqlite'

export interface BenchmarkStats {
  mean: number
  median: number
  stdDev: number
  min: number
  max: number
  cv: number
  p95: number
  p99: number
  iterations: number
}

export interface BenchmarkTest {
  name: string
  prismaMs: number
  extendedMs: number
  drizzleMs: number
  speedupVsPrisma: number
  speedupVsDrizzle: number
  prismaStats: BenchmarkStats
  extendedStats: BenchmarkStats
  drizzleStats?: BenchmarkStats
}

export interface BenchmarkResult {
  version: number
  dialect: BenchmarkDialect
  tests: BenchmarkTest[]
  avgSpeedupVsPrisma: number
  avgSpeedupVsDrizzle: number
  timestamp: string
}

interface BenchmarkFileEntry {
  path: string
  mtime: number
  key: string
  version: number
  dialect: BenchmarkDialect
}

const PRISMA_PACKAGE_VERSIONS: Record<number, string> = {
  6: '6.19.3',
  7: '7.10.0',
  8: '8.1.0-dev.1',
}

const DIALECT_ORDER: Record<BenchmarkDialect, number> = {
  postgres: 0,
  sqlite: 1,
}

function isBenchmarkDialect(value: string): value is BenchmarkDialect {
  return value === 'postgres' || value === 'sqlite'
}

function findBenchmarkDir(): string {
  const candidatePaths = [
    resolve(process.cwd(), 'benchmark-results'),
    resolve(process.cwd(), '..', 'benchmark-results'),
    resolve(process.cwd(), '..', '..', 'benchmark-results'),
  ]

  return candidatePaths.find(path => existsSync(path)) ?? ''
}

export function dialectLabel(dialect: BenchmarkDialect): string {
  return dialect === 'postgres' ? 'PostgreSQL' : 'SQLite'
}

export function prismaVersionLabel(version: number): string {
  const packageVersion = PRISMA_PACKAGE_VERSIONS[version]
  return packageVersion ? `Prisma v${version} (${packageVersion})` : `Prisma v${version}`
}

export function loadBenchmarkResults(): BenchmarkResult[] {
  const benchmarkDir = findBenchmarkDir()
  if (!benchmarkDir) return []

  const latestPattern = /^v(\d+)-(\w+)-latest\.json$/
  const byKey = new Map<string, BenchmarkFileEntry>()

  for (const file of readdirSync(benchmarkDir)) {
    const match = file.match(latestPattern)
    if (!match) continue

    const dialect = match[2]
    if (!isBenchmarkDialect(dialect)) continue

    const version = Number(match[1])
    const fullPath = join(benchmarkDir, file)
    const entry = {
      path: fullPath,
      mtime: statSync(fullPath).mtimeMs,
      key: `${dialect}-v${version}`,
      version,
      dialect,
    }

    const existing = byKey.get(entry.key)
    if (!existing || entry.mtime > existing.mtime) byKey.set(entry.key, entry)
  }

  return [...byKey.values()]
    .sort((a, b) => DIALECT_ORDER[a.dialect] - DIALECT_ORDER[b.dialect] || a.version - b.version)
    .map(entry => JSON.parse(readFileSync(entry.path, 'utf-8')))
}
