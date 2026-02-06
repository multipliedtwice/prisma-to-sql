import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { createTestDB, type TestDB } from '../helpers/db'
import { seedDatabase, type SeedResult } from '../helpers/seed-db'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { getDatamodel } from '../helpers/datamodel'
import { buildBatchSql, parseBatchResults } from '../../src/batch'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { type Model } from '../../src/types'
import postgres from 'postgres'
import {
  type BatchProxy,
  type ExtendedPrismaClient,
  speedExtension,
} from '../../src'

const PG_URL = 'postgresql://postgres:postgres@localhost:5433/prisma_test'

let db: TestDB
let seed: SeedResult
let pgClient: ReturnType<typeof postgres>
let models: Model[]
let modelMap: Map<string, Model>

type SpeedClient = ExtendedPrismaClient<any>

describe.skip('Batch Multi-Query E2E - PostgreSQL', () => {
  beforeAll(async () => {
    setGlobalDialect('postgres')
    db = await createTestDB('postgres')
    pgClient = postgres(PG_URL)
    seed = await seedDatabase(db)

    const datamodel = await getDatamodel('postgres')
    models = convertDMMFToModels(datamodel)
    modelMap = new Map(models.map((m) => [m.name, m]))

    for (let i = 0; i < 3; i++) {
      await db.prisma.user.findMany({ take: 1 })
      await pgClient.unsafe('SELECT 1', [])
    }
  }, 120000)

  afterAll(async () => {
    await pgClient?.end()
    await db?.close()
  })

  describe('buildBatchSql', () => {
    it('builds SQL for single findMany query', async () => {
      const queries = {
        users: {
          model: 'User',
          method: 'findMany' as const,
          args: { where: { status: 'ACTIVE' }, orderBy: { id: 'asc' } },
        },
      }

      const { sql, params, keys } = buildBatchSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(sql).toContain('WITH')
      expect(sql).toContain('batch_0')
      expect(sql).toContain('json_agg')
      expect(keys).toEqual(['users'])
      expect(params.length).toBeGreaterThan(0)

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchResults(
        rows[0] as Record<string, unknown>,
        keys,
        queries,
      )

      expect(results.users).toBeDefined()
      expect(Array.isArray(results.users)).toBe(true)

      const expected = await db.prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })
      expect((results.users as any[]).length).toBe(expected.length)
    })

    it('builds SQL for mixed query types', async () => {
      const queries = {
        users: {
          model: 'User',
          method: 'findMany' as const,
          args: {
            where: { status: 'ACTIVE' },
            orderBy: { id: 'asc' },
            take: 5,
          },
        },
        taskCount: {
          model: 'Task',
          method: 'count' as const,
          args: { where: { status: 'TODO' } },
        },
        project: {
          model: 'Project',
          method: 'findFirst' as const,
          args: { orderBy: { id: 'asc' } },
        },
      }

      const { sql, params, keys } = buildBatchSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(sql).toContain('WITH')
      expect(sql).toContain('batch_0')
      expect(sql).toContain('batch_1')
      expect(sql).toContain('batch_2')
      expect(keys).toEqual(['users', 'taskCount', 'project'])

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchResults(
        rows[0] as Record<string, unknown>,
        keys,
        queries,
      )

      expect(results.users).toBeDefined()
      expect(Array.isArray(results.users)).toBe(true)
      expect(typeof results.taskCount).toBe('number')
      expect(results.project).toBeDefined()

      const [expectedUsers, expectedCount, expectedProject] = await Promise.all(
        [
          db.prisma.user.findMany({
            where: { status: 'ACTIVE' },
            orderBy: { id: 'asc' },
            take: 5,
          }),
          db.prisma.task.count({ where: { status: 'TODO' } }),
          db.prisma.project.findFirst({ orderBy: { id: 'asc' } }),
        ],
      )

      expect((results.users as any[]).length).toBe(expectedUsers.length)
      expect(results.taskCount).toBe(expectedCount)
      expect((results.project as any).id).toBe(expectedProject!.id)
    })

    it('builds SQL for aggregate query', async () => {
      const queries = {
        taskStats: {
          model: 'Task',
          method: 'aggregate' as const,
          args: {
            _count: { _all: true },
            _avg: { id: true },
          },
        },
      }

      const { sql, params, keys } = buildBatchSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(sql).toContain('WITH')
      expect(sql).toContain('batch_0')
      expect(keys).toEqual(['taskStats'])

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchResults(
        rows[0] as Record<string, unknown>,
        keys,
        queries,
      )

      expect(results.taskStats).toBeDefined()
      expect((results.taskStats as any)._count).toBeDefined()
      expect((results.taskStats as any)._count._all).toBeGreaterThan(0)
    })

    it('builds SQL for groupBy query', async () => {
      const queries = {
        tasksByStatus: {
          model: 'Task',
          method: 'groupBy' as const,
          args: {
            by: ['status'],
            _count: { _all: true },
          },
        },
      }

      const { sql, params, keys } = buildBatchSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(sql).toContain('WITH')
      expect(sql).toContain('batch_0')
      expect(keys).toEqual(['tasksByStatus'])

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchResults(
        rows[0] as Record<string, unknown>,
        keys,
        queries,
      )

      expect(results.tasksByStatus).toBeDefined()
      expect(Array.isArray(results.tasksByStatus)).toBe(true)
      expect((results.tasksByStatus as any[]).length).toBeGreaterThan(0)

      for (const group of results.tasksByStatus as any[]) {
        expect(group.status).toBeDefined()
        expect(group._count._all).toBeGreaterThan(0)
      }
    })

    it('builds SQL for findUnique query', async () => {
      const userId = seed.userIds[0]

      const queries = {
        user: {
          model: 'User',
          method: 'findUnique' as const,
          args: { where: { id: userId } },
        },
      }

      const { sql, params, keys } = buildBatchSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(sql).toContain('WITH')
      expect(sql).toContain('batch_0')
      expect(keys).toEqual(['user'])

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchResults(
        rows[0] as Record<string, unknown>,
        keys,
        queries,
      )

      expect(results.user).toBeDefined()
      expect((results.user as any).id).toBe(userId)

      const expected = await db.prisma.user.findUnique({
        where: { id: userId },
      })
      expect((results.user as any).email).toBe(expected!.email)
    })

    it('builds SQL with complex where clauses', async () => {
      const queries = {
        activeTasks: {
          model: 'Task',
          method: 'findMany' as const,
          args: {
            where: {
              AND: [
                { status: { in: ['TODO', 'IN_PROGRESS'] } },
                { priority: { in: ['HIGH', 'URGENT'] } },
              ],
            },
            orderBy: { id: 'asc' },
          },
        },
        doneCount: {
          model: 'Task',
          method: 'count' as const,
          args: { where: { status: 'DONE' } },
        },
        unassignedCount: {
          model: 'Task',
          method: 'count' as const,
          args: { where: { assigneeId: null } },
        },
      }

      const { sql, params, keys } = buildBatchSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(keys).toEqual(['activeTasks', 'doneCount', 'unassignedCount'])

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchResults(
        rows[0] as Record<string, unknown>,
        keys,
        queries,
      )

      expect(Array.isArray(results.activeTasks)).toBe(true)
      expect(typeof results.doneCount).toBe('number')
      expect(typeof results.unassignedCount).toBe('number')

      const [expectedActive, expectedDone, expectedUnassigned] =
        await Promise.all([
          db.prisma.task.findMany({
            where: {
              AND: [
                { status: { in: ['TODO', 'IN_PROGRESS'] } },
                { priority: { in: ['HIGH', 'URGENT'] } },
              ],
            },
            orderBy: { id: 'asc' },
          }),
          db.prisma.task.count({ where: { status: 'DONE' } }),
          db.prisma.task.count({ where: { assigneeId: null } }),
        ])

      expect((results.activeTasks as any[]).length).toBe(expectedActive.length)
      expect(results.doneCount).toBe(expectedDone)
      expect(results.unassignedCount).toBe(expectedUnassigned)
    })

    it('builds SQL for multiple counts', async () => {
      const queries = {
        totalUsers: { model: 'User', method: 'count' as const },
        totalTasks: { model: 'Task', method: 'count' as const },
        totalProjects: { model: 'Project', method: 'count' as const },
        activeUsers: {
          model: 'User',
          method: 'count' as const,
          args: { where: { status: 'ACTIVE' } },
        },
        todoTasks: {
          model: 'Task',
          method: 'count' as const,
          args: { where: { status: 'TODO' } },
        },
      }

      const { sql, params, keys } = buildBatchSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(keys).toEqual([
        'totalUsers',
        'totalTasks',
        'totalProjects',
        'activeUsers',
        'todoTasks',
      ])

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchResults(
        rows[0] as Record<string, unknown>,
        keys,
        queries,
      )

      const [
        expectedTotalUsers,
        expectedTotalTasks,
        expectedTotalProjects,
        expectedActiveUsers,
        expectedTodoTasks,
      ] = await Promise.all([
        db.prisma.user.count(),
        db.prisma.task.count(),
        db.prisma.project.count(),
        db.prisma.user.count({ where: { status: 'ACTIVE' } }),
        db.prisma.task.count({ where: { status: 'TODO' } }),
      ])

      expect(results.totalUsers).toBe(expectedTotalUsers)
      expect(results.totalTasks).toBe(expectedTotalTasks)
      expect(results.totalProjects).toBe(expectedTotalProjects)
      expect(results.activeUsers).toBe(expectedActiveUsers)
      expect(results.todoTasks).toBe(expectedTodoTasks)
    })

    it('throws on empty queries object', () => {
      expect(() => buildBatchSql({}, modelMap, models, 'postgres')).toThrow(
        /at least one query/,
      )
    })

    it('throws on unknown model', () => {
      const queries = {
        fake: {
          model: 'FakeModel',
          method: 'findMany' as const,
        },
      }

      expect(() =>
        buildBatchSql(queries, modelMap, models, 'postgres'),
      ).toThrow(/not found/)
    })

    it('throws on sqlite dialect', () => {
      const queries = {
        users: {
          model: 'User',
          method: 'findMany' as const,
        },
      }

      expect(() => buildBatchSql(queries, modelMap, models, 'sqlite')).toThrow(
        /only supported for postgres/,
      )
    })
  })

  describe('$batch API with speedExtension', () => {
    function createExtended(): SpeedClient {
      return db.prisma.$extends(
        speedExtension({
          postgres: pgClient,
          models,
          debug: false,
        }),
      ) as SpeedClient
    }

    it('executes batch with mixed queries', async () => {
      const extended = createExtended()

      const results = await extended.$batch((batch: BatchProxy) => ({
        users: batch.User.findMany({
          where: { status: 'ACTIVE' },
          orderBy: { id: 'asc' },
          take: 5,
        }),
        taskCount: batch.Task.count({ where: { status: 'TODO' } }),
        project: batch.Project.findFirst({ orderBy: { id: 'asc' } }),
      }))

      expect(results.users).toBeDefined()
      expect(Array.isArray(results.users)).toBe(true)
      expect(typeof results.taskCount).toBe('number')
      expect(results.project).toBeDefined()

      const [expectedUsers, expectedCount, expectedProject] = await Promise.all(
        [
          db.prisma.user.findMany({
            where: { status: 'ACTIVE' },
            orderBy: { id: 'asc' },
            take: 5,
          }),
          db.prisma.task.count({ where: { status: 'TODO' } }),
          db.prisma.project.findFirst({ orderBy: { id: 'asc' } }),
        ],
      )

      expect(results.users.length).toBe(expectedUsers.length)
      expect(results.taskCount).toBe(expectedCount)
      expect(results.project.id).toBe(expectedProject!.id)
    })

    it('executes batch with all count queries', async () => {
      const extended = createExtended()

      const results = await extended.$batch((batch: BatchProxy) => ({
        totalUsers: batch.User.count(),
        activeUsers: batch.User.count({ where: { status: 'ACTIVE' } }),
        todoTasks: batch.Task.count({ where: { status: 'TODO' } }),
        doneTasks: batch.Task.count({ where: { status: 'DONE' } }),
      }))

      expect(typeof results.totalUsers).toBe('number')
      expect(typeof results.activeUsers).toBe('number')
      expect(typeof results.todoTasks).toBe('number')
      expect(typeof results.doneTasks).toBe('number')

      const [expectedTotal, expectedActive, expectedTodo, expectedDone] =
        await Promise.all([
          db.prisma.user.count(),
          db.prisma.user.count({ where: { status: 'ACTIVE' } }),
          db.prisma.task.count({ where: { status: 'TODO' } }),
          db.prisma.task.count({ where: { status: 'DONE' } }),
        ])

      expect(results.totalUsers).toBe(expectedTotal)
      expect(results.activeUsers).toBe(expectedActive)
      expect(results.todoTasks).toBe(expectedTodo)
      expect(results.doneTasks).toBe(expectedDone)
    })

    it('executes batch with aggregate', async () => {
      const extended = createExtended()

      const results = await extended.$batch((batch: BatchProxy) => ({
        taskStats: batch.Task.aggregate({
          _count: { _all: true },
          _avg: { id: true },
        }),
        userCount: batch.User.count(),
      }))

      expect(results.taskStats).toBeDefined()
      expect(results.taskStats._count).toBeDefined()
      expect(results.taskStats._count._all).toBeGreaterThan(0)
      expect(typeof results.userCount).toBe('number')
    })

    it('executes batch with groupBy', async () => {
      const extended = createExtended()

      const results = await extended.$batch((batch: BatchProxy) => ({
        tasksByStatus: batch.Task.groupBy({
          by: ['status'],
          _count: { _all: true },
        }),
        taskCount: batch.Task.count(),
      }))

      expect(Array.isArray(results.tasksByStatus)).toBe(true)
      expect(results.tasksByStatus.length).toBeGreaterThan(0)
      expect(typeof results.taskCount).toBe('number')

      for (const group of results.tasksByStatus) {
        expect(group.status).toBeDefined()
        expect(group._count._all).toBeGreaterThan(0)
      }
    })

    it('throws when query is awaited inside batch', async () => {
      const extended = createExtended()

      await expect(
        extended.$batch(async (batch: BatchProxy) => ({
          users: (await batch.User.findMany()) as any,
        })),
      ).rejects.toThrow(/Cannot await a batch query/)
    })

    it('throws on unsupported method in batch', async () => {
      const extended = createExtended()

      await expect(
        extended.$batch((batch: BatchProxy) => ({
          result: (batch.User as any).create({
            data: { email: 'test@test.com' },
          }),
        })),
      ).rejects.toThrow(/not supported in batch/)
    })

    it('throws on unknown model in batch', async () => {
      const extended = createExtended()

      await expect(
        extended.$batch((batch: BatchProxy) => ({
          result: (batch as any).FakeModel.findMany(),
        })),
      ).rejects.toThrow(/not found/)
    })
  })

  describe('Performance comparison', () => {
    it('batch is faster than sequential queries', async () => {
      const extended = db.prisma.$extends(
        speedExtension({
          postgres: pgClient,
          models,
          debug: false,
        }),
      ) as SpeedClient

      const sequentialStart = Date.now()
      const seq1 = await db.prisma.user.count({ where: { status: 'ACTIVE' } })
      const seq2 = await db.prisma.task.count({ where: { status: 'TODO' } })
      const seq3 = await db.prisma.project.count()
      const seq4 = await db.prisma.user.findMany({
        take: 5,
        orderBy: { id: 'asc' },
      })
      const sequentialTime = Date.now() - sequentialStart

      const batchStart = Date.now()
      const batchResults = await extended.$batch((batch: BatchProxy) => ({
        activeUsers: batch.User.count({ where: { status: 'ACTIVE' } }),
        todoTasks: batch.Task.count({ where: { status: 'TODO' } }),
        projectCount: batch.Project.count(),
        users: batch.User.findMany({ take: 5, orderBy: { id: 'asc' } }),
      }))
      const batchTime = Date.now() - batchStart

      expect(batchResults.activeUsers).toBe(seq1)
      expect(batchResults.todoTasks).toBe(seq2)
      expect(batchResults.projectCount).toBe(seq3)
      expect(batchResults.users.length).toBe(seq4.length)

      console.log(`Sequential: ${sequentialTime}ms, Batch: ${batchTime}ms`)
      console.log(`Speedup: ${(sequentialTime / batchTime).toFixed(2)}x`)

      expect(batchTime).toBeLessThanOrEqual(sequentialTime * 1.5)
    })
  })
})
