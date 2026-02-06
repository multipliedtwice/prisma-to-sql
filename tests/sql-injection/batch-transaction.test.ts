// tests/e2e/batch-transaction.test.ts

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { createTestDB, type TestDB } from '../helpers/db'
import { seedDatabase, type SeedResult } from '../helpers/seed-db'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { getDatamodel } from '../helpers/datamodel'
import {
  buildBatchCountSql,
  parseBatchCountResults,
  type BatchCountQuery,
} from '../../src/batch'
import {
  createTransactionExecutor,
  type TransactionQuery,
  type TransactionOptions,
  type TransactionExecutor,
} from '../../src/transaction'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { type Model } from '../../src/types'
import postgres from 'postgres'

const PRISMA_VERSION = parseInt(process.env.PRISMA_VERSION || '6', 10)
const PG_URL = 'postgresql://postgres:postgres@localhost:5433/prisma_test'

let db: TestDB
let seed: SeedResult
let pgClient: ReturnType<typeof postgres>
let models: Model[]
let modelMap: Map<string, Model>
let txExecutor: TransactionExecutor

describe('Batch Count & Transaction E2E - PostgreSQL', () => {
  beforeAll(async () => {
    setGlobalDialect('postgres')
    db = await createTestDB('postgres')
    pgClient = postgres(PG_URL)
    seed = await seedDatabase(db)

    const datamodel = await getDatamodel('postgres')
    models = convertDMMFToModels(datamodel)
    modelMap = new Map(models.map((m) => [m.name, m]))

    txExecutor = createTransactionExecutor({
      modelMap,
      allModels: models,
      dialect: 'postgres',
      executeRaw: async (sql: string, params?: unknown[]) => {
        return (await pgClient.unsafe(
          sql,
          (params || []) as any[],
        )) as unknown[]
      },
      postgresClient: pgClient,
    })

    for (let i = 0; i < 3; i++) {
      await db.prisma.user.findMany({ take: 1 })
      await pgClient.unsafe('SELECT 1', [])
    }
  }, 120000)

  afterAll(async () => {
    await pgClient?.end()
    await db?.close()
  })

  describe('buildBatchCountSql', () => {
    it('single count without where', async () => {
      const queries: BatchCountQuery[] = [{ model: 'User', method: 'count' }]

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(sql).toContain('SELECT')
      expect(sql).toContain('COUNT(*)')
      expect(sql).toContain('"0"')

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      const expected = await db.prisma.user.count()

      expect(results).toHaveLength(1)
      expect(results[0]).toBe(expected)
    })

    it('single count with where', async () => {
      const queries: BatchCountQuery[] = [
        { model: 'Task', method: 'count', args: { where: { status: 'DONE' } } },
      ]

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      expect(sql).toContain('COUNT(*)')
      expect(params.length).toBeGreaterThan(0)

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      const expected = await db.prisma.task.count({ where: { status: 'DONE' } })

      expect(results[0]).toBe(expected)
    })

    it('multiple counts different models', async () => {
      const queries: BatchCountQuery[] = [
        { model: 'User', method: 'count' },
        { model: 'Task', method: 'count' },
        { model: 'Project', method: 'count' },
        { model: 'Comment', method: 'count' },
        { model: 'Organization', method: 'count' },
      ]

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )

      for (let i = 0; i < queries.length; i++) {
        expect(sql).toContain(`"${i}"`)
      }

      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      const [userCount, taskCount, projectCount, commentCount, orgCount] =
        await Promise.all([
          db.prisma.user.count(),
          db.prisma.task.count(),
          db.prisma.project.count(),
          db.prisma.comment.count(),
          db.prisma.organization.count(),
        ])

      expect(results[0]).toBe(userCount)
      expect(results[1]).toBe(taskCount)
      expect(results[2]).toBe(projectCount)
      expect(results[3]).toBe(commentCount)
      expect(results[4]).toBe(orgCount)
    })

    it('multiple counts with mixed where clauses', async () => {
      const queries: BatchCountQuery[] = [
        {
          model: 'User',
          method: 'count',
          args: { where: { status: 'ACTIVE' } },
        },
        { model: 'Task', method: 'count', args: { where: { status: 'TODO' } } },
        {
          model: 'Task',
          method: 'count',
          args: { where: { priority: 'HIGH' } },
        },
        { model: 'User', method: 'count' },
      ]

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )
      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      const [activeUsers, todoTasks, highTasks, allUsers] = await Promise.all([
        db.prisma.user.count({ where: { status: 'ACTIVE' } }),
        db.prisma.task.count({ where: { status: 'TODO' } }),
        db.prisma.task.count({ where: { priority: 'HIGH' } }),
        db.prisma.user.count(),
      ])

      expect(results[0]).toBe(activeUsers)
      expect(results[1]).toBe(todoTasks)
      expect(results[2]).toBe(highTasks)
      expect(results[3]).toBe(allUsers)
    })

    it('count with complex where (AND, OR, NOT)', async () => {
      const queries: BatchCountQuery[] = [
        {
          model: 'Task',
          method: 'count',
          args: {
            where: {
              AND: [
                { status: { in: ['TODO', 'IN_PROGRESS'] } },
                { OR: [{ priority: 'URGENT' }, { priority: 'HIGH' }] },
              ],
            },
          },
        },
        {
          model: 'User',
          method: 'count',
          args: {
            where: { NOT: { status: 'DELETED' } },
          },
        },
      ]

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )
      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      const [complexTaskCount, nonDeletedUsers] = await Promise.all([
        db.prisma.task.count({
          where: {
            AND: [
              { status: { in: ['TODO', 'IN_PROGRESS'] } },
              { OR: [{ priority: 'URGENT' }, { priority: 'HIGH' }] },
            ],
          },
        }),
        db.prisma.user.count({ where: { NOT: { status: 'DELETED' } } }),
      ])

      expect(results[0]).toBe(complexTaskCount)
      expect(results[1]).toBe(nonDeletedUsers)
    })

    it('count with null checks', async () => {
      const queries: BatchCountQuery[] = [
        {
          model: 'Task',
          method: 'count',
          args: { where: { assigneeId: null } },
        },
        {
          model: 'Task',
          method: 'count',
          args: { where: { assigneeId: { not: null } } },
        },
        { model: 'User', method: 'count', args: { where: { name: null } } },
      ]

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )
      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      const [unassigned, assigned, noName] = await Promise.all([
        db.prisma.task.count({ where: { assigneeId: null } }),
        db.prisma.task.count({ where: { assigneeId: { not: null } } }),
        db.prisma.user.count({ where: { name: null } }),
      ])

      expect(results[0]).toBe(unassigned)
      expect(results[1]).toBe(assigned)
      expect(results[2]).toBe(noName)
    })

    it('same model counted multiple times with different filters', async () => {
      const statuses = ['TODO', 'IN_PROGRESS', 'DONE', 'CANCELLED']
      const queries: BatchCountQuery[] = statuses.map((s) => ({
        model: 'Task' as const,
        method: 'count' as const,
        args: { where: { status: s } },
      }))

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )
      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      for (let i = 0; i < statuses.length; i++) {
        const expected = await db.prisma.task.count({
          where: { status: statuses[i] },
        })
        expect(results[i]).toBe(expected)
      }
    })

    it('throws on empty queries array', () => {
      expect(() =>
        buildBatchCountSql([], modelMap, models, 'postgres'),
      ).toThrow(/at least one query/)
    })

    it('throws on non-count method', () => {
      const queries = [{ model: 'User', method: 'findMany' as any }]
      expect(() =>
        buildBatchCountSql(queries, modelMap, models, 'postgres'),
      ).toThrow(/only supports count/)
    })

    it('throws on unknown model', () => {
      const queries: BatchCountQuery[] = [
        { model: 'NonExistent', method: 'count' },
      ]
      expect(() =>
        buildBatchCountSql(queries, modelMap, models, 'postgres'),
      ).toThrow(/not found/)
    })

    it('throws on non-postgres dialect', () => {
      const queries: BatchCountQuery[] = [{ model: 'User', method: 'count' }]
      expect(() =>
        buildBatchCountSql(queries, modelMap, models, 'sqlite'),
      ).toThrow(/only supported for postgres/)
    })
  })

  describe('parseBatchCountResults', () => {
    it('parses string values', () => {
      const row = { '0': '42', '1': '100' }
      const results = parseBatchCountResults(row, 2)
      expect(results).toEqual([42, 100])
    })

    it('parses numeric values', () => {
      const row = { '0': 5, '1': 0, '2': 999 }
      const results = parseBatchCountResults(row, 3)
      expect(results).toEqual([5, 0, 999])
    })

    it('parses bigint values', () => {
      const row = { '0': BigInt(12345) }
      const results = parseBatchCountResults(row, 1)
      expect(results).toEqual([12345])
    })

    it('returns 0 for missing keys', () => {
      const row = { '0': 10 }
      const results = parseBatchCountResults(row, 3)
      expect(results).toEqual([10, 0, 0])
    })

    it('returns 0 for unexpected types', () => {
      const row = { '0': null, '1': undefined, '2': true } as any
      const results = parseBatchCountResults(row, 3)
      expect(results).toEqual([0, 0, 0])
    })
  })

  describe('Transaction Executor', () => {
    it('single findMany query', async () => {
      const queries: TransactionQuery[] = [
        {
          model: 'User',
          method: 'findMany',
          args: { where: { status: 'ACTIVE' }, orderBy: { id: 'asc' } },
        },
      ]

      const results = await txExecutor.execute(queries)
      const expected = await db.prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
      })

      expect(results).toHaveLength(1)
      expect(Array.isArray(results[0])).toBe(true)
      expect((results[0] as any[]).length).toBe(expected.length)
    })

    it('single count query', async () => {
      const queries: TransactionQuery[] = [
        { model: 'Task', method: 'count', args: { where: { status: 'DONE' } } },
      ]

      const results = await txExecutor.execute(queries)
      const expected = await db.prisma.task.count({ where: { status: 'DONE' } })

      expect(results).toHaveLength(1)
      expect(results[0]).toBe(expected)
    })

    it('multiple queries different methods', async () => {
      const queries: TransactionQuery[] = [
        {
          model: 'User',
          method: 'findMany',
          args: {
            where: { status: 'ACTIVE' },
            orderBy: { id: 'asc' },
            take: 5,
          },
        },
        { model: 'Task', method: 'count', args: { where: { status: 'TODO' } } },
        {
          model: 'Project',
          method: 'findFirst',
          args: { orderBy: { id: 'asc' } },
        },
        {
          model: 'Task',
          method: 'aggregate',
          args: { _count: { _all: true } },
        },
      ]

      const results = await txExecutor.execute(queries)

      expect(results).toHaveLength(4)

      const users = results[0] as any[]
      expect(Array.isArray(users)).toBe(true)
      const expectedUsers = await db.prisma.user.findMany({
        where: { status: 'ACTIVE' },
        orderBy: { id: 'asc' },
        take: 5,
      })
      expect(users.length).toBe(expectedUsers.length)

      const taskCount = results[1] as number
      const expectedCount = await db.prisma.task.count({
        where: { status: 'TODO' },
      })
      expect(taskCount).toBe(expectedCount)

      const project = results[2]
      expect(project).not.toBeNull()

      const agg = results[3] as any
      expect(agg._count._all).toBeDefined()
    })

    it('multiple queries same model', async () => {
      const queries: TransactionQuery[] = [
        { model: 'Task', method: 'count', args: { where: { status: 'TODO' } } },
        { model: 'Task', method: 'count', args: { where: { status: 'DONE' } } },
        {
          model: 'Task',
          method: 'count',
          args: { where: { status: 'IN_PROGRESS' } },
        },
        { model: 'Task', method: 'count' },
      ]

      const results = await txExecutor.execute(queries)

      const [todo, done, inProgress, total] = await Promise.all([
        db.prisma.task.count({ where: { status: 'TODO' } }),
        db.prisma.task.count({ where: { status: 'DONE' } }),
        db.prisma.task.count({ where: { status: 'IN_PROGRESS' } }),
        db.prisma.task.count(),
      ])

      expect(results[0]).toBe(todo)
      expect(results[1]).toBe(done)
      expect(results[2]).toBe(inProgress)
      expect(results[3]).toBe(total)
    })

    it('findUnique in transaction', async () => {
      const userId = seed.userIds[0]

      const queries: TransactionQuery[] = [
        {
          model: 'User',
          method: 'findUnique',
          args: { where: { id: userId } },
        },
      ]

      const results = await txExecutor.execute(queries)
      const expected = await db.prisma.user.findUnique({
        where: { id: userId },
      })

      expect(results).toHaveLength(1)
      const user = results[0] as any
      expect(user).not.toBeNull()
      expect(user.id).toBe(expected!.id)
      expect(user.email).toBe(expected!.email)
    })

    it('groupBy in transaction', async () => {
      const queries: TransactionQuery[] = [
        {
          model: 'Task',
          method: 'groupBy',
          args: { by: ['status'], _count: { _all: true } },
        },
      ]

      const results = await txExecutor.execute(queries)

      expect(results).toHaveLength(1)
      const groups = results[0] as any[]
      expect(Array.isArray(groups)).toBe(true)
      expect(groups.length).toBeGreaterThan(0)

      for (const g of groups) {
        expect(g.status).toBeDefined()
        expect(g._count._all).toBeDefined()
      }
    })

    it('empty queries array returns empty results', async () => {
      const results = await txExecutor.execute([])
      expect(results).toEqual([])
    })

    it('throws on unknown model', async () => {
      const queries: TransactionQuery[] = [
        { model: 'FakeModel', method: 'findMany' },
      ]

      await expect(txExecutor.execute(queries)).rejects.toThrow(/not found/)
    })

    it('transaction with isolation level ReadCommitted', async () => {
      const queries: TransactionQuery[] = [
        { model: 'User', method: 'count' },
        { model: 'Task', method: 'count' },
      ]

      const options: TransactionOptions = { isolationLevel: 'ReadCommitted' }
      const results = await txExecutor.execute(queries, options)

      const [userCount, taskCount] = await Promise.all([
        db.prisma.user.count(),
        db.prisma.task.count(),
      ])

      expect(results[0]).toBe(userCount)
      expect(results[1]).toBe(taskCount)
    })

    it('transaction with isolation level Serializable', async () => {
      const queries: TransactionQuery[] = [{ model: 'User', method: 'count' }]

      const options: TransactionOptions = { isolationLevel: 'Serializable' }
      const results = await txExecutor.execute(queries, options)

      const expected = await db.prisma.user.count()
      expect(results[0]).toBe(expected)
    })

    it('transaction with timeout', async () => {
      const queries: TransactionQuery[] = [{ model: 'User', method: 'count' }]

      const options: TransactionOptions = { timeout: 30000 }
      const results = await txExecutor.execute(queries, options)

      const expected = await db.prisma.user.count()
      expect(results[0]).toBe(expected)
    })

    it('transaction with isolation level and timeout combined', async () => {
      const queries: TransactionQuery[] = [
        {
          model: 'User',
          method: 'findMany',
          args: { orderBy: { id: 'asc' }, take: 3 },
        },
        { model: 'Task', method: 'count', args: { where: { status: 'DONE' } } },
      ]

      const options: TransactionOptions = {
        isolationLevel: 'RepeatableRead',
        timeout: 15000,
      }

      const results = await txExecutor.execute(queries, options)

      expect(results).toHaveLength(2)
      expect(Array.isArray(results[0])).toBe(true)
      expect(typeof results[1]).toBe('number')
    })

    it('transaction rolls back on query failure', async () => {
      const countBefore = await db.prisma.user.count()

      const badExecuteRaw = async (sql: string, params?: unknown[]) => {
        if (sql.includes('nonexistent_table')) {
          throw new Error('relation "nonexistent_table" does not exist')
        }
        return (await pgClient.unsafe(
          sql,
          (params || []) as any[],
        )) as unknown[]
      }

      const badModels = [
        ...models,
        {
          name: 'BadModel',
          tableName: 'nonexistent_table',
          fields: [
            {
              name: 'id',
              dbName: 'id',
              type: 'Int',
              isRequired: true,
              isRelation: false,
            },
          ],
        },
      ] as Model[]

      const badModelMap = new Map(badModels.map((m) => [m.name, m]))

      const badTxExecutor = createTransactionExecutor({
        modelMap: badModelMap,
        allModels: badModels,
        dialect: 'postgres',
        executeRaw: badExecuteRaw,
        postgresClient: pgClient,
      })

      const queries: TransactionQuery[] = [
        { model: 'User', method: 'count' },
        { model: 'BadModel', method: 'findMany' },
      ]

      await expect(badTxExecutor.execute(queries)).rejects.toThrow()

      const countAfter = await db.prisma.user.count()
      expect(countAfter).toBe(countBefore)
    })

    it('concurrent transactions produce consistent results', async () => {
      const makeQueries = (): TransactionQuery[] => [
        { model: 'User', method: 'count' },
        { model: 'Task', method: 'count' },
      ]

      const [r1, r2, r3] = await Promise.all([
        txExecutor.execute(makeQueries()),
        txExecutor.execute(makeQueries()),
        txExecutor.execute(makeQueries()),
      ])

      expect(r1[0]).toBe(r2[0])
      expect(r2[0]).toBe(r3[0])
      expect(r1[1]).toBe(r2[1])
      expect(r2[1]).toBe(r3[1])
    })
  })

  describe('Transaction Executor - sqlite rejection', () => {
    it('throws for sqlite dialect', async () => {
      const sqliteExecutor = createTransactionExecutor({
        modelMap,
        allModels: models,
        dialect: 'sqlite',
        executeRaw: async () => [],
      })

      await expect(
        sqliteExecutor.execute([{ model: 'User', method: 'count' }]),
      ).rejects.toThrow(/only supported for postgres/)
    })
  })

  describe('Batch + Transaction combined usage', () => {
    it('batch count results match individual transaction counts', async () => {
      const batchQueries: BatchCountQuery[] = [
        {
          model: 'User',
          method: 'count',
          args: { where: { status: 'ACTIVE' } },
        },
        { model: 'Task', method: 'count', args: { where: { status: 'TODO' } } },
        { model: 'Project', method: 'count' },
      ]

      const { sql, params } = buildBatchCountSql(
        batchQueries,
        modelMap,
        models,
        'postgres',
      )
      const batchRows = await pgClient.unsafe(sql, params as any[])
      const batchResults = parseBatchCountResults(
        batchRows[0] as Record<string, unknown>,
        batchQueries.length,
      )

      const txQueries: TransactionQuery[] = [
        {
          model: 'User',
          method: 'count',
          args: { where: { status: 'ACTIVE' } },
        },
        { model: 'Task', method: 'count', args: { where: { status: 'TODO' } } },
        { model: 'Project', method: 'count' },
      ]

      const txResults = await txExecutor.execute(txQueries)

      expect(batchResults[0]).toBe(txResults[0])
      expect(batchResults[1]).toBe(txResults[1])
      expect(batchResults[2]).toBe(txResults[2])
    })

    it('large batch count (10+ queries)', async () => {
      const statuses = ['TODO', 'IN_PROGRESS', 'DONE', 'CANCELLED']
      const priorities = ['LOW', 'MEDIUM', 'HIGH', 'URGENT']

      const queries: BatchCountQuery[] = [
        ...statuses.map((s) => ({
          model: 'Task' as const,
          method: 'count' as const,
          args: { where: { status: s } },
        })),
        ...priorities.map((p) => ({
          model: 'Task' as const,
          method: 'count' as const,
          args: { where: { priority: p } },
        })),
        { model: 'User', method: 'count' },
        { model: 'Project', method: 'count' },
        { model: 'Organization', method: 'count' },
      ]

      const { sql, params } = buildBatchCountSql(
        queries,
        modelMap,
        models,
        'postgres',
      )
      const rows = await pgClient.unsafe(sql, params as any[])
      const results = parseBatchCountResults(
        rows[0] as Record<string, unknown>,
        queries.length,
      )

      expect(results).toHaveLength(queries.length)

      for (const r of results) {
        expect(typeof r).toBe('number')
        expect(r).toBeGreaterThanOrEqual(0)
      }

      const totalByStatus = results.slice(0, 4).reduce((a, b) => a + b, 0)
      const totalTasks = await db.prisma.task.count({
        where: { status: { in: statuses } },
      })
      expect(totalByStatus).toBe(totalTasks)
    })
  })
})
