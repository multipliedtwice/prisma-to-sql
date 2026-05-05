#!/usr/bin/env node
import { config } from 'dotenv'
import { readFile, writeFile, rename } from 'fs/promises'
import { resolve, isAbsolute } from 'path'
import {
  createDatabaseExecutor,
  collectPlannerArtifacts,
  emitPlannerGeneratedModule,
  parsePreviousArtifacts,
  GeneratePlannerArtifacts,
} from './cardinality-planner'

const CONNECT_TIMEOUT_MS = 10000
const DEFAULT_MAX_AGE_MS = 24 * 60 * 60 * 1000
const DEFAULT_SLOW_EDGE_MS = 10000
const DEFAULT_EDGE_TIMEOUT_MS = 30000
const DEFAULT_STALE_EDGE_HOURS = 168

function parseArgs(argv: string[]): { output: string; clientPath: string } {
  const outputIdx = argv.indexOf('--output')
  const clientIdx = argv.indexOf('--prisma-client')

  const output =
    outputIdx !== -1 && argv[outputIdx + 1]
      ? argv[outputIdx + 1]
      : './dist/prisma/generated/sql/planner.generated.js'

  const clientPath =
    clientIdx !== -1 && argv[clientIdx + 1]
      ? argv[clientIdx + 1]
      : '@prisma/client'

  return { output, clientPath }
}

function resolveOutput(output: string): string {
  return isAbsolute(output) ? output : resolve(process.cwd(), output)
}

function resolveClientPath(p: string): string {
  if (p.startsWith('.') || isAbsolute(p)) {
    return resolve(process.cwd(), p)
  }
  if (p.includes('/') && !p.startsWith('@')) {
    return resolve(process.cwd(), p)
  }
  return p
}

function emitCJS(artifacts: GeneratePlannerArtifacts): string {
  const ts = emitPlannerGeneratedModule(artifacts)
  return ts
    .replace(/^export const (\w+)/gm, 'exports.$1')
    .replace(/^import .*$/gm, '')
    .replace(/\bas const\b/g, '')
    .replace(/^export type .*$/gm, '')
    .trimStart()
}

function getEnvNumber(name: string, fallback: number): number {
  const raw = process.env[name]
  if (!raw) return fallback
  const parsed = Number(raw)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback
}

async function loadPreviousArtifacts(
  outputPath: string,
): Promise<GeneratePlannerArtifacts | null> {
  try {
    const content = await readFile(outputPath, 'utf8')
    const m: Record<string, unknown> = {}
    const fn = new Function('exports', content)
    fn(m)
    return parsePreviousArtifacts(m)
  } catch {
    return null
  }
}

async function connectWithTimeout(
  databaseUrl: string,
  dialect: 'postgres' | 'sqlite',
): Promise<{ executor: any; cleanup: () => Promise<void> }> {
  let settled = false

  const connectPromise = createDatabaseExecutor({ databaseUrl, dialect }).then(
    (conn) => {
      if (settled) {
        conn.cleanup().catch(() => {})
        throw new Error('Timed out')
      }
      return conn
    },
  )

  const timeoutPromise = new Promise<never>((_, reject) => {
    const id = setTimeout(() => {
      settled = true
      reject(new Error(`Connection timed out after ${CONNECT_TIMEOUT_MS}ms`))
    }, CONNECT_TIMEOUT_MS)
    id.unref?.()
  })

  try {
    const result = await Promise.race([connectPromise, timeoutPromise])
    settled = true
    return result
  } catch (err) {
    settled = true
    throw err
  }
}

async function main() {
  config()

  const skipPlanner =
    process.env.PRISMA_SQL_SKIP_PLANNER === '1' ||
    process.env.PRISMA_SQL_SKIP_PLANNER === 'true'

  if (skipPlanner) {
    console.log(
      '[prisma-sql] ⏭ Skipping planner stats (PRISMA_SQL_SKIP_PLANNER)',
    )
    process.exit(0)
  }

  const { output, clientPath } = parseArgs(process.argv.slice(2))
  const outputPath = resolveOutput(output)

  const maxAgeMs = getEnvNumber(
    'PRISMA_SQL_STATS_MAX_AGE_MS',
    DEFAULT_MAX_AGE_MS,
  )
  const slowEdgeMs = getEnvNumber(
    'PRISMA_SQL_SLOW_EDGE_MS',
    DEFAULT_SLOW_EDGE_MS,
  )
  const edgeTimeoutMs = getEnvNumber(
    'PRISMA_SQL_EDGE_TIMEOUT_MS',
    DEFAULT_EDGE_TIMEOUT_MS,
  )
  const staleEdgeHours = getEnvNumber(
    'PRISMA_SQL_STALE_EDGE_HOURS',
    DEFAULT_STALE_EDGE_HOURS,
  )
  const mode = (
    process.env.PRISMA_SQL_STATS_MODE === 'precise' ? 'precise' : 'fast'
  ) as 'fast' | 'precise'

  const previousArtifacts = await loadPreviousArtifacts(outputPath)

  if (previousArtifacts) {
    const ageMs = Date.now() - previousArtifacts.collectedAt
    if (ageMs < maxAgeMs) {
      const ageMinutes = Math.round(ageMs / 60000)
      const maxAgeHours = Math.round(maxAgeMs / 3600000)
      console.log(
        `[prisma-sql] ⏭ Stats are ${ageMinutes}m old (threshold: ${maxAgeHours}h), skipping`,
      )
      process.exit(0)
    }
    const ageHours = (ageMs / 3600000).toFixed(1)
    console.log(
      `[prisma-sql] Previous stats are ${ageHours}h old, recollecting`,
    )
  }

  const url = process.env.DATABASE_URL
  if (!url) {
    console.warn(
      '[prisma-sql] DATABASE_URL not set, skipping planner stats collection',
    )
    process.exit(0)
  }

  let executor: any
  let cleanup: (() => Promise<void>) | undefined

  try {
    const conn = await connectWithTimeout(url, 'postgres')
    executor = conn.executor
    cleanup = conn.cleanup
  } catch (err) {
    console.warn(
      '[prisma-sql] Failed to connect:',
      err instanceof Error ? err.message : err,
    )
    process.exit(0)
  }

  try {
    let dmmf: any
    try {
      const resolvedClientPath = resolveClientPath(clientPath)
      const client = require(resolvedClientPath)
      dmmf = client.Prisma?.dmmf ?? client.dmmf
      if (!dmmf?.datamodel) {
        throw new Error(`Could not read dmmf.datamodel from ${clientPath}`)
      }
    } catch (err) {
      throw new Error(
        `Failed to load Prisma client from "${clientPath}": ${err instanceof Error ? err.message : err}`,
      )
    }

    const startTime = Date.now()

    const artifacts = await collectPlannerArtifacts({
      executor,
      datamodel: dmmf.datamodel,
      dialect: 'postgres',
      mode,
      previousArtifacts: previousArtifacts ?? undefined,
      slowEdgeThresholdMs: slowEdgeMs,
      perEdgeTimeoutMs: edgeTimeoutMs,
      staleEdgeHours,
    })

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    const content = emitCJS(artifacts as any)
    const tmpPath = outputPath + '.tmp.' + process.pid
    await writeFile(tmpPath, content, 'utf8')
    await rename(tmpPath, outputPath)
    console.log(
      `[prisma-sql] ✓ Planner stats written to ${outputPath} (${elapsed}s)`,
    )
  } catch (err) {
    console.warn(
      '[prisma-sql] Failed to collect stats:',
      err instanceof Error ? err.message : err,
    )
  } finally {
    await cleanup?.()
  }

  process.exit(0)
}

main()
