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

interface PerfMetrics {
  buildTime: number
  executeTime: number
  parseTime: number
  totalTime: number
  queryCount: number
}

function measurePerf<T>(
  fn: () => T,
  label: string,
): { result: T; time: number } {
  const start = performance.now()
  const result = fn()
  const time = performance.now() - start
  return { result, time }
}

async function measureAsync<T>(
  fn: () => Promise<T>,
  label: string,
): Promise<{ result: T; time: number }> {
  const start = performance.now()
  const result = await fn()
  const time = performance.now() - start
  return { result, time }
}

describe.skip('Batch Multi-Query E2E - PostgreSQL', () => {
  beforeAll(async () => {
    setGlobalDialect('postgres')
    db = await createTestDB('postgres', 7)
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

  async function executeBatchWithMetrics(
    queries: Record<string, any>,
  ): Promise<PerfMetrics & { results: any }> {
    const buildMeasure = measurePerf(
      () => buildBatchSql(queries, modelMap, models, 'postgres'),
      'buildBatchSql',
    )
    const { sql, params, keys, aliases } = buildMeasure.result // ← Add aliases
    const buildTime = buildMeasure.time

    const executeMeasure = await measureAsync(
      () => pgClient.unsafe(sql, params as any[]),
      'execute',
    )
    const rows = executeMeasure.result
    const executeTime = executeMeasure.time

    // Remove the debug logs since they're cluttering the output
    const parseMeasure = measurePerf(
      () =>
        parseBatchResults(
          rows[0] as Record<string, unknown>,
          keys,
          queries,
          aliases,
        ), // ← Add aliases
      'parse',
    )
    const results = parseMeasure.result
    const parseTime = parseMeasure.time

    const totalTime = buildTime + executeTime + parseTime

    return {
      buildTime,
      executeTime,
      parseTime,
      totalTime,
      queryCount: keys.length,
      results,
    }
  }

  describe('buildBatchSql', () => {
    it('builds SQL for single findMany query', async () => {
      const queries = {
        users: {
          model: 'User',
          method: 'findMany' as const,
          args: { where: { status: 'ACTIVE' }, orderBy: { id: 'asc' } },
        },
      }

      const metrics = await executeBatchWithMetrics(queries)

      console.log(
        `[Single findMany] Build: ${metrics.buildTime.toFixed(2)}ms, Execute: ${metrics.executeTime.toFixed(2)}ms, Parse: ${metrics.parseTime.toFixed(2)}ms, Total: ${metrics.totalTime.toFixed(2)}ms`,
      )

      expect(metrics.results.users).toBeDefined()
      expect(Array.isArray(metrics.results.users)).toBe(true)

      const expected = await db.prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })
      expect((metrics.results.users as any[]).length).toBe(expected.length)
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

      const metrics = await executeBatchWithMetrics(queries)

      console.log(
        `[Mixed queries] Build: ${metrics.buildTime.toFixed(2)}ms, Execute: ${metrics.executeTime.toFixed(2)}ms, Parse: ${metrics.parseTime.toFixed(2)}ms, Total: ${metrics.totalTime.toFixed(2)}ms`,
      )

      expect(metrics.results.users).toBeDefined()
      expect(Array.isArray(metrics.results.users)).toBe(true)
      expect(typeof metrics.results.taskCount).toBe('number')
      expect(metrics.results.project).toBeDefined()

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

      expect((metrics.results.users as any[]).length).toBe(expectedUsers.length)
      expect(metrics.results.taskCount).toBe(expectedCount)
      expect((metrics.results.project as any).id).toBe(expectedProject!.id)
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

      const metrics = await executeBatchWithMetrics(queries)

      console.log(
        `[Aggregate] Build: ${metrics.buildTime.toFixed(2)}ms, Execute: ${metrics.executeTime.toFixed(2)}ms, Parse: ${metrics.parseTime.toFixed(2)}ms, Total: ${metrics.totalTime.toFixed(2)}ms`,
      )

      expect(metrics.results.taskStats).toBeDefined()
      expect((metrics.results.taskStats as any)._count).toBeDefined()
      expect((metrics.results.taskStats as any)._count._all).toBeGreaterThan(0)
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

      const metrics = await executeBatchWithMetrics(queries)

      console.log(
        `[GroupBy] Build: ${metrics.buildTime.toFixed(2)}ms, Execute: ${metrics.executeTime.toFixed(2)}ms, Parse: ${metrics.parseTime.toFixed(2)}ms, Total: ${metrics.totalTime.toFixed(2)}ms`,
      )

      expect(metrics.results.tasksByStatus).toBeDefined()
      expect(Array.isArray(metrics.results.tasksByStatus)).toBe(true)
      expect((metrics.results.tasksByStatus as any[]).length).toBeGreaterThan(0)

      for (const group of metrics.results.tasksByStatus as any[]) {
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

      const metrics = await executeBatchWithMetrics(queries)

      console.log(
        `[FindUnique] Build: ${metrics.buildTime.toFixed(2)}ms, Execute: ${metrics.executeTime.toFixed(2)}ms, Parse: ${metrics.parseTime.toFixed(2)}ms, Total: ${metrics.totalTime.toFixed(2)}ms`,
      )

      expect(metrics.results.user).toBeDefined()
      expect((metrics.results.user as any).id).toBe(userId)

      const expected = await db.prisma.user.findUnique({
        where: { id: userId },
      })
      expect((metrics.results.user as any).email).toBe(expected!.email)
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

      const metrics = await executeBatchWithMetrics(queries)

      console.log(
        `[Complex where] Build: ${metrics.buildTime.toFixed(2)}ms, Execute: ${metrics.executeTime.toFixed(2)}ms, Parse: ${metrics.parseTime.toFixed(2)}ms, Total: ${metrics.totalTime.toFixed(2)}ms`,
      )

      expect(Array.isArray(metrics.results.activeTasks)).toBe(true)
      expect(typeof metrics.results.doneCount).toBe('number')
      expect(typeof metrics.results.unassignedCount).toBe('number')

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

      expect((metrics.results.activeTasks as any[]).length).toBe(
        expectedActive.length,
      )
      expect(metrics.results.doneCount).toBe(expectedDone)
      expect(metrics.results.unassignedCount).toBe(expectedUnassigned)
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

      const metrics = await executeBatchWithMetrics(queries)

      console.log(
        `[Multiple counts] Build: ${metrics.buildTime.toFixed(2)}ms, Execute: ${metrics.executeTime.toFixed(2)}ms, Parse: ${metrics.parseTime.toFixed(2)}ms, Total: ${metrics.totalTime.toFixed(2)}ms`,
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

      expect(metrics.results.totalUsers).toBe(expectedTotalUsers)
      expect(metrics.results.totalTasks).toBe(expectedTotalTasks)
      expect(metrics.results.totalProjects).toBe(expectedTotalProjects)
      expect(metrics.results.activeUsers).toBe(expectedActiveUsers)
      expect(metrics.results.todoTasks).toBe(expectedTodoTasks)
    })

    it('complex real-world dashboard query', async () => {
      const queries = {
        activeUsersWithTasks: {
          model: 'User',
          method: 'findMany' as const,
          args: {
            where: {
              AND: [
                { status: 'ACTIVE' },
                {
                  assignedTasks: {
                    some: { status: { in: ['TODO', 'IN_PROGRESS'] } },
                  },
                },
              ],
            },
            orderBy: { createdAt: 'desc' },
            take: 10,
          },
        },
        taskStats: {
          model: 'Task',
          method: 'aggregate' as const,
          args: {
            _count: { _all: true },
            _avg: { id: true },
            where: {
              OR: [{ status: 'TODO' }, { status: 'IN_PROGRESS' }],
            },
          },
        },
        tasksByPriority: {
          model: 'Task',
          method: 'groupBy' as const,
          args: {
            by: ['priority', 'status'],
            _count: { _all: true },
            where: {
              assigneeId: { not: null },
            },
            orderBy: {
              priority: 'desc',
            },
          },
        },
        urgentTasks: {
          model: 'Task',
          method: 'findMany' as const,
          args: {
            where: {
              AND: [
                { priority: 'URGENT' },
                { status: { notIn: ['DONE', 'CANCELLED'] } },
                {
                  dueDate: {
                    lte: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000),
                  },
                },
              ],
            },
            orderBy: { dueDate: 'asc' },
            take: 20,
          },
        },
        projectsWithOverdueTasks: {
          model: 'Project',
          method: 'findMany' as const,
          args: {
            where: {
              tasks: {
                some: {
                  AND: [
                    { status: { notIn: ['DONE', 'CANCELLED'] } },
                    { dueDate: { lt: new Date() } },
                  ],
                },
              },
            },
            orderBy: { createdAt: 'desc' },
          },
        },
        unassignedHighPriorityCount: {
          model: 'Task',
          method: 'count' as const,
          args: {
            where: {
              AND: [
                { assigneeId: null },
                { priority: { in: ['HIGH', 'URGENT'] } },
                { status: { notIn: ['DONE', 'CANCELLED'] } },
              ],
            },
          },
        },
        recentlyCompletedTasks: {
          model: 'Task',
          method: 'findMany' as const,
          args: {
            where: {
              AND: [
                { status: 'DONE' },
                {
                  updatedAt: {
                    gte: new Date(Date.now() - 24 * 60 * 60 * 1000),
                  },
                },
              ],
            },
            orderBy: { updatedAt: 'desc' },
            take: 15,
          },
        },
        userWorkloadStats: {
          model: 'User',
          method: 'findMany' as const,
          args: {
            where: {
              status: 'ACTIVE',
              assignedTasks: {
                some: {
                  status: { in: ['TODO', 'IN_PROGRESS'] },
                },
              },
            },
            take: 20,
          },
        },
      }

      const metrics = await executeBatchWithMetrics(queries)

      console.log(`
[COMPLEX DASHBOARD QUERY]
Queries: ${metrics.queryCount}
Build:   ${metrics.buildTime.toFixed(2)}ms
Execute: ${metrics.executeTime.toFixed(2)}ms
Parse:   ${metrics.parseTime.toFixed(2)}ms
Total:   ${metrics.totalTime.toFixed(2)}ms
Avg/query: ${(metrics.totalTime / metrics.queryCount).toFixed(2)}ms
      `)

      expect(Array.isArray(metrics.results.activeUsersWithTasks)).toBe(true)
      expect(metrics.results.taskStats).toBeDefined()
      expect(Array.isArray(metrics.results.tasksByPriority)).toBe(true)
      expect(Array.isArray(metrics.results.urgentTasks)).toBe(true)
      expect(Array.isArray(metrics.results.projectsWithOverdueTasks)).toBe(true)
      expect(typeof metrics.results.unassignedHighPriorityCount).toBe('number')
      expect(Array.isArray(metrics.results.recentlyCompletedTasks)).toBe(true)
      expect(Array.isArray(metrics.results.userWorkloadStats)).toBe(true)
    })

    it('stress test with many queries', async () => {
      const queries: Record<string, any> = {}

      for (let i = 0; i < 20; i++) {
        queries[`userCount${i}`] = {
          model: 'User',
          method: 'count' as const,
          args: { where: { id: { gte: i * 1000 } } },
        }
      }

      for (let i = 0; i < 15; i++) {
        queries[`taskList${i}`] = {
          model: 'Task',
          method: 'findMany' as const,
          args: {
            where: { id: { gte: i * 100 } },
            take: 5,
            orderBy: { id: 'asc' },
          },
        }
      }

      for (let i = 0; i < 10; i++) {
        queries[`projectAgg${i}`] = {
          model: 'Project',
          method: 'aggregate' as const,
          args: {
            _count: { _all: true },
            _avg: { id: true },
            where: { id: { gte: i * 50 } },
          },
        }
      }

      const metrics = await executeBatchWithMetrics(queries)

      console.log(`
[STRESS TEST - ${metrics.queryCount} queries]
Build:   ${metrics.buildTime.toFixed(2)}ms (${(metrics.buildTime / metrics.queryCount).toFixed(2)}ms per query)
Execute: ${metrics.executeTime.toFixed(2)}ms
Parse:   ${metrics.parseTime.toFixed(2)}ms (${(metrics.parseTime / metrics.queryCount).toFixed(2)}ms per query)
Total:   ${metrics.totalTime.toFixed(2)}ms
      `)

      expect(Object.keys(metrics.results).length).toBe(metrics.queryCount)
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

      const sequentialStart = performance.now()
      const seq1 = await db.prisma.user.count({ where: { status: 'ACTIVE' } })
      const seq2 = await db.prisma.task.count({ where: { status: 'TODO' } })
      const seq3 = await db.prisma.project.count()
      const seq4 = await db.prisma.user.findMany({
        take: 5,
        orderBy: { id: 'asc' },
      })
      const sequentialTime = performance.now() - sequentialStart

      const batchStart = performance.now()
      const batchResults = await extended.$batch((batch: BatchProxy) => ({
        activeUsers: batch.User.count({ where: { status: 'ACTIVE' } }),
        todoTasks: batch.Task.count({ where: { status: 'TODO' } }),
        projectCount: batch.Project.count(),
        users: batch.User.findMany({ take: 5, orderBy: { id: 'asc' } }),
      }))
      const batchTime = performance.now() - batchStart

      expect(batchResults.activeUsers).toBe(seq1)
      expect(batchResults.todoTasks).toBe(seq2)
      expect(batchResults.projectCount).toBe(seq3)
      expect(batchResults.users.length).toBe(seq4.length)

      console.log(`
[PERFORMANCE COMPARISON]
Sequential: ${sequentialTime.toFixed(2)}ms (${(sequentialTime / 4).toFixed(2)}ms per query)
Batch:      ${batchTime.toFixed(2)}ms (${(batchTime / 4).toFixed(2)}ms per query)
Speedup:    ${(sequentialTime / batchTime).toFixed(2)}x
      `)

      expect(batchTime).toBeLessThanOrEqual(sequentialTime * 1.5)
    })

    it('complex dashboard query performance', async () => {
      const extended = db.prisma.$extends(
        speedExtension({
          postgres: pgClient,
          models,
          debug: false,
        }),
      ) as SpeedClient

      const sequentialStart = performance.now()
      const seq1 = await db.prisma.user.findMany({
        where: {
          AND: [
            { status: 'ACTIVE' },
            {
              assignedTasks: {
                some: { status: { in: ['TODO', 'IN_PROGRESS'] } },
              },
            },
          ],
        },
        orderBy: { createdAt: 'desc' },
        take: 10,
      })
      const seq2 = await db.prisma.task.aggregate({
        _count: { _all: true },
        _avg: { id: true },
        where: {
          OR: [{ status: 'TODO' }, { status: 'IN_PROGRESS' }],
        },
      })
      const seq3 = await db.prisma.task.groupBy({
        by: ['priority', 'status'],
        _count: { _all: true },
        where: {
          assigneeId: { not: null },
        },
      })
      const seq4 = await db.prisma.task.count({
        where: {
          AND: [
            { assigneeId: null },
            { priority: { in: ['HIGH', 'URGENT'] } },
            { status: { notIn: ['DONE', 'CANCELLED'] } },
          ],
        },
      })
      const sequentialTime = performance.now() - sequentialStart

      const batchStart = performance.now()
      const batchResults = await extended.$batch((batch: BatchProxy) => ({
        activeUsersWithTasks: batch.User.findMany({
          where: {
            AND: [
              { status: 'ACTIVE' },
              {
                assignedTasks: {
                  some: { status: { in: ['TODO', 'IN_PROGRESS'] } },
                },
              },
            ],
          },
          orderBy: { createdAt: 'desc' },
          take: 10,
        }),
        taskStats: batch.Task.aggregate({
          _count: { _all: true },
          _avg: { id: true },
          where: {
            OR: [{ status: 'TODO' }, { status: 'IN_PROGRESS' }],
          },
        }),
        tasksByPriority: batch.Task.groupBy({
          by: ['priority', 'status'],
          _count: { _all: true },
          where: {
            assigneeId: { not: null },
          },
        }),
        unassignedHighPriorityCount: batch.Task.count({
          where: {
            AND: [
              { assigneeId: null },
              { priority: { in: ['HIGH', 'URGENT'] } },
              { status: { notIn: ['DONE', 'CANCELLED'] } },
            ],
          },
        }),
      }))
      const batchTime = performance.now() - batchStart

      console.log(`
[COMPLEX DASHBOARD PERFORMANCE]
Sequential: ${sequentialTime.toFixed(2)}ms
Batch:      ${batchTime.toFixed(2)}ms
Speedup:    ${(sequentialTime / batchTime).toFixed(2)}x
      `)

      expect(batchResults.activeUsersWithTasks.length).toBe(seq1.length)
      expect(batchResults.taskStats._count._all).toBe(seq2._count._all)
      expect(batchResults.tasksByPriority.length).toBe(seq3.length)
      expect(batchResults.unassignedHighPriorityCount).toBe(seq4)
    })
  })
})
