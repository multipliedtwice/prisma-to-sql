import postgres from 'postgres'
import Database from 'better-sqlite3'
import { promisify } from 'util'
import { exec } from 'child_process'
import fs from 'fs/promises'
import path from 'path'

const execAsync = promisify(exec)

export interface TestDB {
  prisma: any
  execute: (sql: string, params: unknown[]) => Promise<unknown[]>
  close: () => Promise<void>
}

const PG_URL = 'postgres://postgres:postgres@localhost:5433/prisma_test'
const SQLITE_DB_PATH = path.join(process.cwd(), 'tests', 'prisma', 'db.sqlite')
const SQLITE_URL = `file:${SQLITE_DB_PATH}`
const PRISMA_VERSION = parseInt(process.env.PRISMA_VERSION || '6', 10)

async function mergeSchema(
  dialect: 'postgres' | 'sqlite',
  version: number,
): Promise<string> {
  const prismaDir = path.join(process.cwd(), 'tests', 'prisma')

  if (version === 7) {
    const basePath = path.join(prismaDir, 'base.prisma')

    const base = await fs.readFile(basePath, 'utf-8')
    const header =
      dialect === 'postgres'
        ? 'generator client {\n  provider = "prisma-client"\n  output   = "../generated/postgres-v7"\n  previewFeatures = []\n}\n\ndatasource db {\n  provider = "postgresql"\n}\n'
        : 'generator client {\n  provider = "prisma-client"\n  output   = "../generated/sqlite-v7"\n  previewFeatures = []\n}\n\ndatasource db {\n  provider = "sqlite"\n}\n'

    const outputPath = path.join(prismaDir, `schema-${dialect}-v7.prisma`)
    await fs.writeFile(outputPath, `${header}\n${base}`)
    return outputPath
  } else {
    const headerPath = path.join(prismaDir, `${dialect}.prisma`)
    const basePath = path.join(prismaDir, 'base.prisma')
    const outputPath = path.join(prismaDir, `schema-${dialect}.prisma`)

    const [header, base] = await Promise.all([
      fs.readFile(headerPath, 'utf-8'),
      fs.readFile(basePath, 'utf-8'),
    ])

    await fs.writeFile(outputPath, `${header}\n\n${base}`)
    return outputPath
  }
}

async function generatePrismaClient(
  dialect: 'postgres' | 'sqlite',
  version?: number,
): Promise<void> {
  const prismaVersion = version ?? PRISMA_VERSION
  const schemaPath = await mergeSchema(dialect, prismaVersion)

  const env = { ...process.env }
  if (dialect === 'postgres') {
    env.DATABASE_URL = PG_URL
    env.DIRECT_URL = PG_URL
  } else {
    env.DATABASE_URL = SQLITE_URL
    delete env.DIRECT_URL
  }

  if (prismaVersion === 6) {
    const prismaPath = path.join(
      process.cwd(),
      'node_modules',
      'prisma',
      'build',
      'index.js',
    )

    const genCmd = `node ${prismaPath} generate --schema=${schemaPath}`
    await execAsync(genCmd, { env })

    if (process.env.CI !== 'true') {
      try {
        const pushCmd = `node ${prismaPath} db push --force-reset --skip-generate --schema=${schemaPath}`
        await execAsync(pushCmd, { env })
      } catch (e: any) {
        console.log('[DEBUG] db push failed (continuing)', e?.message ?? e)
      }
    }
  } else {
    const configFile =
      dialect === 'postgres' ? 'postgres-v7.config.ts' : 'sqlite-v7.config.ts'

    const prismaPath = path.join(
      process.cwd(),
      'node_modules',
      'prisma',
      'build',
      'index.js',
    )
    console.log('configFile :>> ', configFile)
    const genCmd = `node ${prismaPath} generate --config=${configFile}`
    await execAsync(genCmd, { env })

    if (process.env.CI !== 'true') {
      try {
        const url = dialect === 'postgres' ? PG_URL : SQLITE_URL
        const pushCmd = `node ${prismaPath} db push --force-reset --config=${configFile} --url="${url}"`
        await execAsync(pushCmd, { env })
      } catch (e: any) {
        console.log('[DEBUG] db push failed (continuing)', e?.message ?? e)
      }
    }
  }
}

async function createPostgresDB(version?: number): Promise<TestDB> {
  await generatePrismaClient('postgres', version)
  console.log('version || PRISMA_VERSION :>> ', version || PRISMA_VERSION)
  if ((version || PRISMA_VERSION) === 6) {
    const { PrismaClient } = await import('../generated/postgres/client')
    const prisma = new PrismaClient({
      datasources: { db: { url: PG_URL } },
    })
    const pgClient = postgres(PG_URL)

    return {
      prisma,
      execute: async (sql: string, params: unknown[]) => {
        return (await pgClient.unsafe(sql, params as any[])) as unknown[]
      },
      close: async () => {
        await prisma.$disconnect()
        await pgClient.end()
      },
    }
  } else {
    const { PrismaClient } = await import('../generated/postgres-v7/client')
    const { PrismaPg } = await import('@prisma/adapter-pg')

    const adapter = new PrismaPg({ connectionString: PG_URL })
    const prisma = new PrismaClient({ adapter })
    const pgClient = postgres(PG_URL)

    return {
      prisma,
      execute: async (sql: string, params: unknown[]) => {
        return (await pgClient.unsafe(sql, params as any[])) as unknown[]
      },
      close: async () => {
        await prisma.$disconnect()
        await pgClient.end()
      },
    }
  }
}

async function createSqliteDB(version?: number): Promise<TestDB> {
  const dbDir = path.dirname(SQLITE_DB_PATH)
  await fs.mkdir(dbDir, { recursive: true })

  await generatePrismaClient('sqlite', version)

  if ((version || PRISMA_VERSION) === 6) {
    const { PrismaClient } = await import('../generated/sqlite/client')
    const prisma = new PrismaClient({
      datasources: { db: { url: SQLITE_URL } },
    })
    const sqliteClient = new Database(SQLITE_DB_PATH)

    return {
      prisma,
      execute: async (sql: string, params: unknown[]) => {
        const stmt = sqliteClient.prepare(sql)
        const result = stmt.all(...(params as any[]))
        return Array.isArray(result) ? result : [result]
      },
      close: async () => {
        await prisma.$disconnect()
        sqliteClient.close()
      },
    }
  } else {
    const { PrismaClient } = await import('../generated/sqlite-v7/client')
    const { PrismaBetterSqlite3 } = await import(
      '@prisma/adapter-better-sqlite3'
    )

    const adapter = new PrismaBetterSqlite3({ url: SQLITE_URL })
    const prisma = new PrismaClient({ adapter })
    const sqliteClient = new Database(SQLITE_DB_PATH)

    return {
      prisma,
      execute: async (sql: string, params: unknown[]) => {
        const stmt = sqliteClient.prepare(sql)
        const result = stmt.all(...(params as any[]))
        return Array.isArray(result) ? result : [result]
      },
      close: async () => {
        await prisma.$disconnect()
        sqliteClient.close()
      },
    }
  }
}

export async function createTestDB(
  dialect: 'postgres' | 'sqlite',
  version?: number,
): Promise<TestDB> {
  if (dialect === 'postgres') return createPostgresDB(version)
  return createSqliteDB(version)
}
