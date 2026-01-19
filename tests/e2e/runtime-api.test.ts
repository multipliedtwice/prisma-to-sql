import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest'
import postgres from 'postgres'
import Database from 'better-sqlite3'

import { speedExtension, createPrismaSQL, createToSQL } from '../../src/index'
import { createTestDB, type TestDB } from '../helpers/db'
import { seedDatabase, type SeedResult } from '../helpers/seed-db'
import { normalizeValue, sortByField } from '../helpers/compare'
import { Prisma } from '../generated/client'

let pgDb: TestDB
let sqliteDb: TestDB
let seed: SeedResult
let pgClient: ReturnType<typeof postgres>
let sqliteClient: Database.Database

const dmmf = Prisma.dmmf

describe('Runtime API Tests', () => {
  describe('speedExtension - PostgreSQL', () => {
    beforeAll(async () => {
      pgDb = await createTestDB('postgres')
      seed = await seedDatabase(pgDb)
      pgClient = postgres(
        'postgresql://postgres:postgres@localhost:5433/prisma_test',
      )
    }, 120000)

    afterAll(async () => {
      // await pgClient.end()
      await pgDb?.close()
    })

    it('basic usage - findMany', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const users = await prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      const expected = await pgDb.prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(normalizeValue(users)).toEqual(normalizeValue(expected))
    })

    it('findFirst returns single result or null', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const user = await prisma.user.findFirst({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      const expected = await pgDb.prisma.user.findFirst({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(user).not.toBeInstanceOf(Array)
      if (expected) {
        expect(normalizeValue(user)).toEqual(normalizeValue(expected))
      } else {
        expect(user).toBeNull()
      }
    })

    it('findUnique returns single result or null', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const user = await prisma.user.findUnique({
        where: { id: seed.userIds[0] },
      })

      const expected = await pgDb.prisma.user.findUnique({
        where: { id: seed.userIds[0] },
      })

      expect(user).not.toBeInstanceOf(Array)
      expect(normalizeValue(user)).toEqual(normalizeValue(expected))
    })

    it('count returns number', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const count = await prisma.user.count({
        where: { status: 'ACTIVE' },
      })

      const expected = await pgDb.prisma.user.count({
        where: { status: 'ACTIVE' },
      })

      expect(typeof count).toBe('number')
      expect(count).toBe(expected)
    })

    it('aggregate returns object', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const result = await prisma.task.aggregate({
        _count: { _all: true },
        _sum: { position: true },
        _avg: { position: true },
      })

      const expected = await pgDb.prisma.task.aggregate({
        _count: { _all: true },
        _sum: { position: true },
        _avg: { position: true },
      })

      expect(result).not.toBeInstanceOf(Array)
      expect(result._count._all).toBe(expected._count._all)
    })

    it('groupBy returns array', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const results = await prisma.task.groupBy({
        by: ['status'],
        _count: { _all: true },
      })

      const expected = await pgDb.prisma.task.groupBy({
        by: ['status'],
        _count: { _all: true },
      })

      expect(Array.isArray(results)).toBe(true)
      expect(results.length).toBe(expected.length)
    })

    it('debug mode logs SQL and params', async () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})

      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf, debug: true }),
      )

      await prisma.user.findMany({ where: { status: 'ACTIVE' } })

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('[postgres] User.findMany'),
      )
      expect(consoleSpy).toHaveBeenCalledWith(
        'SQL:',
        expect.stringContaining('SELECT'),
      )
      expect(consoleSpy).toHaveBeenCalledWith(
        'Params:',
        expect.arrayContaining(['ACTIVE']),
      )

      consoleSpy.mockRestore()
    })

    it('onQuery callback receives query info', async () => {
      const queries: any[] = []

      const prisma = pgDb.prisma.$extends(
        speedExtension({
          postgres: pgClient,
          dmmf,
          onQuery: (info) => queries.push(info),
        }),
      )

      await prisma.user.findMany({ where: { status: 'ACTIVE' } })

      expect(queries.length).toBe(1)
      expect(queries[0]).toMatchObject({
        model: 'User',
        method: 'findMany',
        sql: expect.stringContaining('SELECT'),
        params: expect.arrayContaining(['ACTIVE']),
        duration: expect.any(Number),
      })
    })

    it('selective models - only accelerate specified models', async () => {
      const queries: string[] = []

      const prisma = pgDb.prisma.$extends(
        speedExtension({
          postgres: pgClient,
          dmmf,
          models: ['User'],
          onQuery: (info) => queries.push(info.model),
        }),
      )

      await prisma.user.findMany({ where: { status: 'ACTIVE' } })
      await prisma.task.findMany({ where: { status: 'TODO' } })

      expect(queries).toContain('User')
      expect(queries).not.toContain('Task')
    })

    it('access to $original bypasses extension', async () => {
      const queries: string[] = []

      const prisma = pgDb.prisma.$extends(
        speedExtension({
          postgres: pgClient,
          dmmf,
          onQuery: (info) => queries.push(info.model),
        }),
      )

      await (prisma as any).$original.user.findMany({
        where: { status: 'ACTIVE' },
      })

      expect(queries.length).toBe(0)
    })

    it('includes work correctly', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const projects = await prisma.project.findMany({
        include: { tasks: true },
        take: 3,
        orderBy: { id: 'asc' },
      })

      const expected = await pgDb.prisma.project.findMany({
        include: { tasks: true },
        take: 3,
        orderBy: { id: 'asc' },
      })

      expect(projects.length).toBe(expected.length)
      for (let i = 0; i < projects.length; i++) {
        expect(projects[i].id).toBe(expected[i].id)
        expect(Array.isArray(projects[i].tasks)).toBe(true)
        expect(projects[i].tasks.length).toBe(expected[i].tasks.length)
      }
    })

    it('nested includes work correctly', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const users = await prisma.user.findMany({
        include: {
          assignedTasks: {
            include: { project: true },
          },
        },
        take: 2,
        orderBy: { id: 'asc' },
      })

      expect(users.length).toBeGreaterThan(0)
      expect(Array.isArray(users[0].assignedTasks)).toBe(true)
      if (users[0].assignedTasks.length > 0) {
        expect(users[0].assignedTasks[0].project).toBeDefined()
      }
    })

    it('complex where clauses work', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const args = {
        where: {
          AND: [
            { status: { in: ['TODO', 'IN_PROGRESS'] } },
            { OR: [{ priority: 'URGENT' }, { priority: 'HIGH' }] },
            { NOT: { assigneeId: null } },
          ],
        },
        orderBy: { id: 'asc' },
        take: 10,
      }

      const tasks = await prisma.task.findMany(args as any)
      const expected = await pgDb.prisma.task.findMany(args as any)

      expect(normalizeValue(tasks)).toEqual(normalizeValue(expected))
    })

    it('relation filters work', async () => {
      const prisma = pgDb.prisma.$extends(
        speedExtension({ postgres: pgClient, dmmf }),
      )

      const users = await prisma.user.findMany({
        where: { assignedTasks: { some: { status: 'IN_PROGRESS' } } },
        orderBy: { id: 'asc' },
      })

      const expected = await pgDb.prisma.user.findMany({
        where: { assignedTasks: { some: { status: 'IN_PROGRESS' } } },
        orderBy: { id: 'asc' },
      })

      expect(normalizeValue(users)).toEqual(normalizeValue(expected))
    })

    it('falls back to Prisma on errors', async () => {
      const invalidClient = postgres('postgresql://invalid:5432/db')
      const prisma = pgDb.prisma.$extends(
        speedExtension({
          postgres: invalidClient,
          dmmf,
          debug: true,
        }),
      )

      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      const users = await prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(consoleSpy).toHaveBeenCalled()
      expect(Array.isArray(users)).toBe(true)

      consoleSpy.mockRestore()
      await invalidClient.end()
    })
  })

  describe('speedExtension - SQLite', () => {
    beforeAll(async () => {
      sqliteDb = await createTestDB('sqlite')
      await seedDatabase(sqliteDb)
      sqliteClient = new Database('./tests/prisma/db.sqlite')
    }, 120000)

    afterAll(async () => {
      sqliteClient?.close()
      await sqliteDb?.close()
    })

    it('basic usage - findMany', async () => {
      const prisma = sqliteDb.prisma.$extends(
        speedExtension({ sqlite: sqliteClient, dmmf }),
      )

      const users = await prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      const expected = await sqliteDb.prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(normalizeValue(users)).toEqual(normalizeValue(expected))
    })

    it('findFirst returns single result', async () => {
      const prisma = sqliteDb.prisma.$extends(
        speedExtension({ sqlite: sqliteClient, dmmf }),
      )

      const user = await prisma.user.findFirst({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(user).not.toBeInstanceOf(Array)
      expect(user).toBeDefined()
    })

    it('count returns number', async () => {
      const prisma = sqliteDb.prisma.$extends(
        speedExtension({ sqlite: sqliteClient, dmmf }),
      )

      const count = await prisma.task.count({
        where: { status: 'DONE' },
      })

      const expected = await sqliteDb.prisma.task.count({
        where: { status: 'DONE' },
      })

      expect(typeof count).toBe('number')
      expect(count).toBe(expected)
    })

    it('includes work correctly', async () => {
      const prisma = sqliteDb.prisma.$extends(
        speedExtension({ sqlite: sqliteClient, dmmf }),
      )

      const projects = await prisma.project.findMany({
        include: { tasks: true },
        take: 3,
        orderBy: { id: 'asc' },
      })

      expect(projects.length).toBeGreaterThan(0)
      expect(Array.isArray(projects[0].tasks)).toBe(true)
    })

    it('distinct with window function', async () => {
      const prisma = sqliteDb.prisma.$extends(
        speedExtension({ sqlite: sqliteClient, dmmf, debug: true }),
      )

      const tasks = await prisma.task.findMany({
        distinct: ['status'],
        orderBy: { status: 'asc' },
      })

      const expected = await sqliteDb.prisma.task.findMany({
        distinct: ['status'],
        orderBy: { status: 'asc' },
      })

      expect(tasks.length).toBe(expected.length)
    })

    it('debug mode shows ? placeholders', async () => {
      const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})

      const prisma = sqliteDb.prisma.$extends(
        speedExtension({ sqlite: sqliteClient, dmmf, debug: true }),
      )

      await prisma.user.findMany({ where: { status: 'ACTIVE' } })

      expect(consoleSpy).toHaveBeenCalledWith(
        expect.stringContaining('[sqlite] User.findMany'),
      )

      const sqlCall = consoleSpy.mock.calls.find((call) =>
        call[0]?.toString().includes('SQL:'),
      )
      expect(sqlCall).toBeDefined()
      expect(sqlCall![1]).toContain('?')
      expect(sqlCall![1]).not.toContain('$')

      consoleSpy.mockRestore()
    })
  })

  describe('createPrismaSQL (Legacy API)', () => {
    let legacyPgClient: ReturnType<typeof postgres>

    beforeAll(async () => {
      if (!pgDb) {
        pgDb = await createTestDB('postgres')
        seed = await seedDatabase(pgDb)
      }
      legacyPgClient = postgres(
        'postgresql://postgres:postgres@localhost:5433/prisma_test',
      )
    }, 120000)

    afterAll(async () => {
      await legacyPgClient?.end()
    })

    it('creates instance and generates valid SQL', () => {
      const db = createPrismaSQL({
        client: legacyPgClient,
        dmmf,
        dialect: 'postgres',
        execute: (c, sql, params) =>
          c.unsafe(sql, params as any[]) as Promise<unknown[]>,
      })

      const { sql, params } = db.toSQL('User', 'findMany', {
        where: { status: 'ACTIVE' },
      })

      expect(sql).toContain('SELECT')
      expect(sql).toContain('FROM')
      expect(sql).toContain('"public"."users"')
      expect(sql).toContain('status')
      expect(params).toContain('ACTIVE')
    })

    it('throws on unknown model', () => {
      const db = createPrismaSQL({
        client: legacyPgClient,
        dmmf,
        dialect: 'postgres',
        execute: (c, sql, params) =>
          c.unsafe(sql, params as any[]) as Promise<unknown[]>,
      })

      expect(() => db.toSQL('NonExistent', 'findMany', {})).toThrow(/not found/)
    })

    it('findMany matches Prisma output', async () => {
      const db = createPrismaSQL({
        client: legacyPgClient,
        dmmf,
        dialect: 'postgres',
        execute: (c, sql, params) =>
          c.unsafe(sql, params as any[]) as Promise<unknown[]>,
      })

      const generated = await db.query<any[]>('User', 'findMany', {
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      const prisma = await pgDb.prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(normalizeValue(sortByField(generated, 'id'))).toEqual(
        normalizeValue(sortByField(prisma, 'id')),
      )
    })
  })

  describe('createToSQL (Standalone API)', () => {
    it('generates SQL without client', () => {
      const toSQL = createToSQL(dmmf, 'postgres')

      const { sql, params } = toSQL('User', 'findMany', {
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(sql).toContain('SELECT')
      expect(sql).toContain('FROM')
      expect(sql).toContain('"users"')
      expect(params).toContain('ACTIVE')
    })

    it('generates SQLite SQL with ? placeholders', () => {
      const toSQL = createToSQL(dmmf, 'sqlite')

      const { sql, params } = toSQL('User', 'findMany', {
        where: { status: 'ACTIVE' },
      })

      expect(sql).toContain('SELECT')
      expect(sql).toContain('FROM')
      expect(sql).toContain('"users"')
      expect(sql).not.toContain('$1')
      expect(sql.match(/\?/g)?.length).toBeGreaterThan(0)
      expect(params).toContain('ACTIVE')
    })

    it('works with both dialects', () => {
      const pgSQL = createToSQL(dmmf, 'postgres')
      const sqliteSQL = createToSQL(dmmf, 'sqlite')

      const pgResult = pgSQL('User', 'findMany', { where: { id: 'test' } })
      const sqliteResult = sqliteSQL('User', 'findMany', {
        where: { id: 'test' },
      })

      expect(pgResult.sql).toContain('$1')
      expect(sqliteResult.sql).toContain('?')
      expect(sqliteResult.sql).not.toContain('$')
    })
  })

  describe('dmmf Compatibility', () => {
    it('datamodel structure is correct', () => {
      const datamodel = dmmf.datamodel

      expect(datamodel).toBeDefined()
      expect(datamodel.models).toBeInstanceOf(Array)
      expect(datamodel.models.length).toBeGreaterThan(0)

      const userModel = datamodel.models.find((m: any) => m.name === 'User')
      expect(userModel).toBeDefined()
      expect(userModel!.fields).toBeInstanceOf(Array)
    })

    it('createToSQL parses dmmf correctly', () => {
      const toSQL = createToSQL(dmmf, 'postgres')

      expect(() => toSQL('User', 'findMany', {})).not.toThrow()
      expect(() => toSQL('Task', 'findMany', {})).not.toThrow()
      expect(() => toSQL('Project', 'findMany', {})).not.toThrow()
      expect(() => toSQL('Organization', 'findMany', {})).not.toThrow()
    })
  })
})
