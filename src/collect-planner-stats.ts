#!/usr/bin/env node
import { config } from 'dotenv'
import { writeFile } from 'fs/promises'
import { resolve, isAbsolute } from 'path'
import {
  createDatabaseExecutor,
  collectPlannerArtifacts,
  emitPlannerGeneratedModule,
  GeneratePlannerArtifacts,
} from './cardinality-planner'

const CONNECT_TIMEOUT_MS = 10000

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

function emitCJS(artifacts: GeneratePlannerArtifacts): string {
  const ts = emitPlannerGeneratedModule(artifacts)
  return ts
    .replace(/^export const (\w+)/gm, 'exports.$1')
    .replace(/^import .*$/gm, '')
    .trimStart()
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
      const client = require(clientPath)
      dmmf = client.Prisma?.dmmf ?? client.dmmf
      if (!dmmf?.datamodel) {
        throw new Error(`Could not read dmmf.datamodel from ${clientPath}`)
      }
    } catch (err) {
      throw new Error(
        `Failed to load Prisma client from "${clientPath}": ${err instanceof Error ? err.message : err}`,
      )
    }

    const artifacts = await collectPlannerArtifacts({
      executor,
      datamodel: dmmf.datamodel,
      dialect: 'postgres',
    })

    await writeFile(outputPath, emitCJS(artifacts as any), 'utf8')
    console.log('[prisma-sql] ✓ Planner stats written to', outputPath)
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
