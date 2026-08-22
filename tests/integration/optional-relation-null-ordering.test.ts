import Database from 'better-sqlite3'
import { getDMMF } from '@prisma/internals'
import { PrismaBetterSqlite3 } from '@prisma/adapter-better-sqlite3'
import { execFile } from 'node:child_process'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { promisify } from 'node:util'
import { generateClient } from '../../src/code-emitter'

const execFileAsync = promisify(execFile)
const plannerArtifacts = {
  relationStats: {},
  modelStats: {},
  roundtripRowEquivalent: 73,
  jsonRowFactor: 1.5,
  collectedAt: 0,
  edgeTimings: {},
}

describe('generated client optional relation null ordering', () => {
  const fixtureDir = mkdtempSync(join(tmpdir(), 'prisma-sql-order-e2e-'))
  const clientDir = join(fixtureDir, 'client')
  const sqlDir = join(fixtureDir, 'sql')
  const databasePath = join(fixtureDir, 'test.sqlite')
  let baseClient: { $disconnect(): Promise<void> } | undefined
  let sqlite: Database.Database | undefined

  afterAll(async () => {
    await baseClient?.$disconnect()
    sqlite?.close()
    rmSync(fixtureDir, { recursive: true, force: true })
  })

  it('orders absent related rows and preserves unique tie-break precedence', async () => {
    const schema = `generator client {
  provider = "prisma-client"
  output   = "./client"
}

datasource db {
  provider = "sqlite"
}

model Parent {
  id            Int            @id @default(autoincrement())
  label         String         @unique
  optionalChild OptionalChild?
}

model OptionalChild {
  id           Int      @id @default(autoincrement())
  requiredDate DateTime
  parentId     Int      @unique
  parent       Parent   @relation(fields: [parentId], references: [id])
}
`
    const schemaPath = join(fixtureDir, 'schema.prisma')
    writeFileSync(schemaPath, schema)

    await execFileAsync(process.execPath, [
      resolve('node_modules/prisma/build/index.js'),
      'generate',
      `--schema=${schemaPath}`,
    ])

    const dmmf = await getDMMF({ datamodel: schema })
    await generateClient({
      datamodel: dmmf.datamodel,
      outputDir: sqlDir,
      config: { dialect: 'sqlite', skipInvalid: false },
      runtimeImportPath: resolve('src/index'),
      clientImportPath: join(clientDir, 'client'),
      plannerArtifacts,
    })

    sqlite = new Database(databasePath)
    sqlite.exec(`
      PRAGMA foreign_keys = ON;
      CREATE TABLE "Parent" (
        "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        "label" TEXT NOT NULL UNIQUE
      );
      CREATE TABLE "OptionalChild" (
        "id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        "requiredDate" DATETIME NOT NULL,
        "parentId" INTEGER NOT NULL UNIQUE,
        CONSTRAINT "OptionalChild_parentId_fkey"
          FOREIGN KEY ("parentId") REFERENCES "Parent" ("id")
      );
    `)

    const { PrismaClient } = await import(join(clientDir, 'client.ts'))
    const { speedExtension } = await import(join(sqlDir, 'index.ts'))
    const adapter = new PrismaBetterSqlite3({ url: `file:${databasePath}` })
    const prisma = new PrismaClient({ adapter })
    baseClient = prisma

    const tiedDate = new Date('2025-01-01T00:00:00.000Z')
    const newestDate = new Date('2026-01-01T00:00:00.000Z')
    await prisma.parent.createMany({
      data: [{ label: 'null-b' }, { label: 'null-a' }],
    })
    const relatedParents: Array<[string, Date]> = [
      ['tie-b', tiedDate],
      ['tie-a', tiedDate],
      ['newest', newestDate],
    ]
    for (const [label, requiredDate] of relatedParents) {
      await prisma.parent.create({
        data: {
          label,
          optionalChild: { create: { requiredDate } },
        },
      })
    }

    let fallbackCalls = 0
    const acceleratedBase = prisma.$extends(
      speedExtension({ sqlite, fallbackOnError: false }),
    )
    const accelerated = acceleratedBase.$extends({
      query: {
        $allModels: {
          $allOperations({ args, query }) {
            fallbackCalls++
            return query(args)
          },
        },
      },
    })
    const descending = await accelerated.parent.findMany({
      select: { label: true },
      orderBy: [
        {
          optionalChild: {
            requiredDate: { sort: 'desc', nulls: 'last' },
          },
        },
        { label: 'asc' },
      ],
    })
    const ascending = await accelerated.parent.findMany({
      select: { label: true },
      orderBy: [
        {
          optionalChild: {
            requiredDate: { sort: 'asc', nulls: 'first' },
          },
        },
        { label: 'desc' },
      ],
    })
    const first = await accelerated.parent.findFirst({
      select: { label: true },
      orderBy: [
        {
          optionalChild: {
            requiredDate: { sort: 'asc', nulls: 'first' },
          },
        },
        { label: 'asc' },
      ],
    })

    expect(descending.map((row) => row.label)).toEqual([
      'newest',
      'tie-a',
      'tie-b',
      'null-a',
      'null-b',
    ])
    expect(ascending.map((row) => row.label)).toEqual([
      'null-b',
      'null-a',
      'tie-b',
      'tie-a',
      'newest',
    ])
    expect(first).toEqual({ label: 'null-a' })
    expect(fallbackCalls).toBe(0)

    await accelerated.parent.create({ data: { label: 'delegate-control' } })
    expect(fallbackCalls).toBe(1)
  }, 30000)
})
