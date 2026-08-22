#!/usr/bin/env node
import { readFileSync, existsSync } from 'fs'
import { resolve } from 'path'
import { pathToFileURL } from 'url'
import postgres from 'postgres'
import { compareResults, type VerifyDiff } from './verify-compare'

interface CorpusEntry {
  model: string
  method: string
  args?: Record<string, unknown>
}

interface CliArgs {
  corpus: string
  generated: string
  client: string
  sqlite?: string
  postgresUrl?: string
  clientInit?: string
  verbose?: boolean
}

const ALLOWED_METHODS = new Set([
  'findMany',
  'findFirst',
  'findUnique',
  'count',
  'aggregate',
  'groupBy',
])

function parseArgs(argv: string[]): CliArgs {
  const args: Record<string, string | boolean> = {}
  for (let i = 2; i < argv.length; i++) {
    const arg = argv[i]
    if (!arg.startsWith('--')) {
      throw new Error(`Unexpected argument: ${arg}`)
    }
    const key = arg.slice(2)
    if (key === 'verbose') {
      args.verbose = true
      continue
    }
    const next = argv[i + 1]
    if (next === undefined || next.startsWith('--')) {
      throw new Error(`Missing value for --${key}`)
    }
    args[key] = next
    i++
  }

  for (const required of ['corpus', 'generated', 'client'] as const) {
    if (!args[required]) throw new Error(`Missing --${required}`)
  }
  if (!args.sqlite && !args.postgresUrl && !process.env.DATABASE_URL) {
    throw new Error('Provide --sqlite <file> or --postgres-url <url> or DATABASE_URL')
  }

  return {
    corpus: resolve(String(args.corpus)),
    generated: resolve(String(args.generated)),
    client: String(args.client),
    sqlite: args.sqlite ? resolve(String(args.sqlite)) : undefined,
    postgresUrl: args.postgresUrl
      ? String(args.postgresUrl)
      : args.sqlite
        ? undefined
        : process.env.DATABASE_URL,
    clientInit: args.clientInit ? String(args.clientInit) : undefined,
    verbose: Boolean(args.verbose),
  }
}

function loadCorpus(path: string): CorpusEntry[] {
  if (!existsSync(path)) throw new Error(`Corpus file not found: ${path}`)
  const entries: CorpusEntry[] = []
  const lines = readFileSync(path, 'utf-8').split('\n')
  lines.forEach((line, idx) => {
    const trimmed = line.trim()
    if (!trimmed) return
    let parsed: CorpusEntry
    try {
      parsed = JSON.parse(trimmed)
    } catch (e) {
      throw new Error(`Corpus line ${idx + 1} is not valid JSON: ${(e as Error).message}`)
    }
    if (!parsed.model || !parsed.method) {
      throw new Error(`Corpus line ${idx + 1} needs "model" and "method"`)
    }
    if (!ALLOWED_METHODS.has(parsed.method)) {
      throw new Error(
        `Corpus line ${idx + 1}: method "${parsed.method}" is not a supported read method (${[...ALLOWED_METHODS].join(', ')})`,
      )
    }
    entries.push(parsed)
  })
  if (entries.length === 0) throw new Error('Corpus is empty')
  return entries
}

async function importModule(target: string): Promise<any> {
  const base = /\.(js|cjs|mjs|ts)$/.test(target)
    ? []
    : [
        resolve(target, 'index.js'),
        resolve(target, 'index.mjs'),
        resolve(target, 'index.cjs'),
        resolve(target, 'index.ts'),
        `${resolve(target)}.ts`,
        `${resolve(target)}.js`,
      ]
  for (const candidate of [...base, resolve(target)]) {
    if (!existsSync(candidate)) continue
    return import(pathToFileURL(candidate).toString())
  }
  throw new Error(`Cannot find module entry in: ${target}`)
}

async function main(): Promise<number> {
  let cli: CliArgs
  try {
    cli = parseArgs(process.argv)
  } catch (e) {
    console.error(`prisma-sql-verify: ${(e as Error).message}`)
    console.error(
      'Usage: prisma-sql-verify --corpus queries.jsonl --generated ./generated/sql \\\n' +
        '  --client @prisma/client (--sqlite ./data.db | --postgres-url $URL) [--client-init \'{...}\'] [--verbose]',
    )
    return 2
  }

  let corpus: CorpusEntry[]
  try {
    corpus = loadCorpus(cli.corpus)
  } catch (e) {
    console.error(`prisma-sql-verify: ${(e as Error).message}`)
    return 2
  }

  const generatedModule = await importModule(cli.generated)
  const speedExtension = generatedModule.speedExtension
  if (typeof speedExtension !== 'function') {
    console.error('prisma-sql-verify: generated module does not export speedExtension')
    return 2
  }

  const clientModule = await importModule(
    cli.client.includes('/') ? resolve(cli.client) : cli.client,
  )
  const PrismaClient = clientModule.PrismaClient
  if (typeof PrismaClient !== 'function') {
    console.error('prisma-sql-verify: client module does not export PrismaClient')
    return 2
  }

  const initArgs: unknown[] = []
  if (cli.clientInit) {
    initArgs.push(JSON.parse(cli.clientInit))
  } else if (cli.sqlite) {
    try {
      const { PrismaBetterSqlite3 } = await import('@prisma/adapter-better-sqlite3')
      initArgs.push({ adapter: new PrismaBetterSqlite3({ url: `file:${cli.sqlite}` }) })
    } catch {
      throw new Error(
        'Prisma 7 sqlite clients need @prisma/adapter-better-sqlite3 installed, or pass --client-init',
      )
    }
  } else if (cli.postgresUrl) {
    try {
      const { PrismaPg } = await import('@prisma/adapter-pg')
      initArgs.push({ adapter: new PrismaPg({ connectionString: cli.postgresUrl }) })
    } catch {
      throw new Error(
        'Prisma 7 postgres clients need @prisma/adapter-pg installed, or pass --client-init',
      )
    }
  }

  const base = new PrismaClient(...initArgs) as Record<string, any>
  const sqliteClient = cli.sqlite
    ? new ((await import('better-sqlite3')).default as typeof import('better-sqlite3'))(cli.sqlite)
    : undefined
  const pgClient = cli.sqlite ? undefined : postgres(cli.postgresUrl!)
  const extensionInput = cli.sqlite
    ? { sqlite: sqliteClient }
    : { postgres: pgClient }
  const accelerated = base.$extends(speedExtension(extensionInput)) as Record<string, any>

  let passed = 0
  const failures: { entry: CorpusEntry; diffs: VerifyDiff[]; error?: string }[] = []

  for (let i = 0; i < corpus.length; i++) {
    const entry = corpus[i]
    const label = `[${i + 1}/${corpus.length}] ${entry.model}.${entry.method}`

    const baseDelegate = base[entry.model]
    const fastDelegate = accelerated[entry.model]
    if (!baseDelegate || typeof baseDelegate[entry.method] !== 'function') {
      failures.push({ entry, diffs: [], error: `model/method not found on base client` })
      console.log(`${label} FAIL (model/method not found on base client)`)
      continue
    }
    if (!fastDelegate || typeof fastDelegate[entry.method] !== 'function') {
      failures.push({ entry, diffs: [], error: `model/method not found on extended client` })
      console.log(`${label} FAIL (model/method not found on extended client)`)
      continue
    }

    try {
      const prismaResult = await baseDelegate[entry.method](entry.args ?? {})
      const extendedResult = await fastDelegate[entry.method](entry.args ?? {})
      const diffs = compareResults(prismaResult, extendedResult)
      if (diffs.length === 0) {
        passed++
        if (cli.verbose) console.log(`${label} PASS`)
      } else {
        failures.push({ entry, diffs })
        console.log(`${label} FAIL`)
        for (const d of diffs) {
          console.log(`   ${d.path}: prisma=${d.prisma} extended=${d.extended}`)
        }
      }
    } catch (e) {
      failures.push({ entry, diffs: [], error: (e as Error).message })
      console.log(`${label} ERROR: ${(e as Error).message}`)
    }
  }

  if (cli.sqlite) {
    sqliteClient?.close()
  } else {
    await pgClient?.end({ timeout: 1 })
  }
  void base.$disconnect?.()

  console.log('')
  console.log(`${passed}/${corpus.length} queries match Prisma.`)
  if (failures.length > 0) {
    console.error(`${failures.length} mismatch(es). Paste the failing entries above into an issue.`)
    return 1
  }
  return 0
}

main().then(
  (code) => process.exit(code),
  (e) => {
    console.error(`prisma-sql-verify: ${(e as Error).stack ?? e}`)
    process.exit(2)
  },
)
