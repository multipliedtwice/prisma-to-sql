import { describe, it, beforeAll, afterAll } from 'vitest'
import { createTestDB, type TestDB } from '../helpers/db'
import { seedDatabase, type SeedResult } from '../helpers/seed-db'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import {
  createDrizzleDB,
  type SqliteDrizzleDB,
  eq,
  and,
  or,
  not,
  inArray,
  gte,
  desc,
  asc,
  isNull,
  isNotNull,
  like,
  sql,
  lt,
  lte,
  gt,
} from '../helpers/drizzle'
import * as schema from '../drizzle/schema'
import {
  runParityTest,
  outputBenchmarkResults,
  type BenchmarkResult,
} from '../helpers/benchmark-utils'

const SHOULD_OUTPUT_JSON = process.env.BENCHMARK_JSON_OUTPUT === '1'
const PRISMA_VERSION = parseInt(process.env.PRISMA_VERSION || '6', 10)

let db: TestDB
let seed: SeedResult
let drizzle: SqliteDrizzleDB
const benchmarkResults: BenchmarkResult[] = []

describe('Prisma Parity E2E - SQLite', () => {
  beforeAll(async () => {
    setGlobalDialect('sqlite')
    db = await createTestDB('sqlite')
    seed = await seedDatabase(db)
    drizzle = createDrizzleDB('sqlite', './tests/prisma/db.sqlite')
    await new Promise((resolve) => setTimeout(resolve, 100))

    for (let i = 0; i < 5; i++) {
      await db.prisma.user.findMany({ take: 1 })
      await db.execute('SELECT 1', [])
      await drizzle.db.select().from(schema.sqliteUsers).limit(1)
    }
  }, 120000)

  afterAll(async () => {
    await outputBenchmarkResults(benchmarkResults, {
      version: PRISMA_VERSION,
      dialect: 'sqlite',
      shouldOutputJson: SHOULD_OUTPUT_JSON,
    })
    await db?.close()
  })

  describe('findMany', () => {
    it('no filters', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany basic',
        'User',
        { method: 'findMany' },
        () => db.prisma.user.findMany({ orderBy: { id: 'asc' } }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('where equals', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany where =',
        'User',
        { method: 'findMany', where: { status: 'ACTIVE' } },
        () =>
          db.prisma.user.findMany({
            where: { status: 'ACTIVE' },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(eq(schema.sqliteUsers.status, 'ACTIVE'))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('where comparison operators', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany where >=',
        'Task',
        { method: 'findMany', where: { position: { gte: 5 } } },
        () =>
          db.prisma.task.findMany({
            where: { position: { gte: 5 } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(gte(schema.sqliteTasks.position, 5))
              .orderBy(asc(schema.sqliteTasks.id)),
        },
      ))

    it('where IN', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany where IN',
        'User',
        {
          method: 'findMany',
          where: { status: { in: ['ACTIVE', 'INACTIVE'] } },
        },
        () =>
          db.prisma.user.findMany({
            where: { status: { in: ['ACTIVE', 'INACTIVE'] } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(inArray(schema.sqliteUsers.status, ['ACTIVE', 'INACTIVE']))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('where null', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany where null',
        'User',
        { method: 'findMany', where: { name: null } },
        () =>
          db.prisma.user.findMany({
            where: { name: null },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(isNull(schema.sqliteUsers.name))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('where contains insensitive', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany LIKE',
        'User',
        {
          method: 'findMany',
          where: { email: { contains: 'example', mode: 'insensitive' } },
        },
        () =>
          db.prisma.user.findMany({
            where: { email: { contains: 'example' } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(like(schema.sqliteUsers.email, '%example%'))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('where AND', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany AND',
        'Task',
        {
          method: 'findMany',
          where: { AND: [{ status: 'TODO' }, { priority: 'HIGH' }] },
        },
        () =>
          db.prisma.task.findMany({
            where: { AND: [{ status: 'TODO' }, { priority: 'HIGH' }] },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(
                and(
                  eq(schema.sqliteTasks.status, 'TODO'),
                  eq(schema.sqliteTasks.priority, 'HIGH'),
                ),
              )
              .orderBy(asc(schema.sqliteTasks.id)),
        },
      ))

    it('where OR', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany OR',
        'Task',
        {
          method: 'findMany',
          where: { OR: [{ status: 'DONE' }, { status: 'CANCELLED' }] },
        },
        () =>
          db.prisma.task.findMany({
            where: { OR: [{ status: 'DONE' }, { status: 'CANCELLED' }] },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(
                or(
                  eq(schema.sqliteTasks.status, 'DONE'),
                  eq(schema.sqliteTasks.status, 'CANCELLED'),
                ),
              )
              .orderBy(asc(schema.sqliteTasks.id)),
        },
      ))

    it('where NOT', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany NOT',
        'User',
        { method: 'findMany', where: { NOT: { status: 'DELETED' } } },
        () =>
          db.prisma.user.findMany({
            where: { NOT: { status: 'DELETED' } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(not(eq(schema.sqliteUsers.status, 'DELETED')))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('orderBy desc', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany orderBy',
        'Task',
        { method: 'findMany', orderBy: { createdAt: 'desc' }, take: 20 },
        () =>
          db.prisma.task.findMany({ orderBy: { createdAt: 'desc' }, take: 20 }),
        {
          sortField: undefined,
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .orderBy(desc(schema.sqliteTasks.createdAt))
              .limit(20),
        },
      ))

    it('take and skip', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany pagination',
        'User',
        { method: 'findMany', take: 5, skip: 3, orderBy: { id: 'asc' } },
        () =>
          db.prisma.user.findMany({ take: 5, skip: 3, orderBy: { id: 'asc' } }),
        {
          sortField: undefined,
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .orderBy(asc(schema.sqliteUsers.id))
              .limit(5)
              .offset(3),
        },
      ))

    it('select specific fields', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany select',
        'User',
        {
          method: 'findMany',
          select: { id: true, email: true, name: true },
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.user.findMany({
            select: { id: true, email: true, name: true },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select({
                id: schema.sqliteUsers.id,
                email: schema.sqliteUsers.email,
                name: schema.sqliteUsers.name,
              })
              .from(schema.sqliteUsers)
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('relation filter some', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany relation some',
        'User',
        {
          method: 'findMany',
          where: { assignedTasks: { some: { status: 'IN_PROGRESS' } } },
        },
        () =>
          db.prisma.user.findMany({
            where: { assignedTasks: { some: { status: 'IN_PROGRESS' } } },
            orderBy: { id: 'asc' },
          }),
      ))

    it('relation filter every', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany relation every',
        'Project',
        { method: 'findMany', where: { tasks: { every: { status: 'DONE' } } } },
        () =>
          db.prisma.project.findMany({
            where: { tasks: { every: { status: 'DONE' } } },
            orderBy: { id: 'asc' },
          }),
      ))

    it(
      'relation filter none',
      () =>
        runParityTest(
          db,
          benchmarkResults,
          'findMany relation none',
          'Task',
          { method: 'findMany', where: { comments: { none: {} } } },
          () =>
            db.prisma.task.findMany({
              where: { comments: { none: {} } },
              orderBy: { id: 'asc' },
            }),
        ),
      60000,
    )

    it('nested relation filter', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany nested relation',
        'Organization',
        {
          method: 'findMany',
          where: {
            projects: {
              some: {
                tasks: {
                  some: { status: 'DONE' },
                },
              },
            },
          },
        },
        () =>
          db.prisma.organization.findMany({
            where: {
              projects: {
                some: {
                  tasks: {
                    some: { status: 'DONE' },
                  },
                },
              },
            },
            orderBy: { id: 'asc' },
          }),
      ))

    it('complex where', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany complex',
        'Task',
        {
          method: 'findMany',
          where: {
            AND: [
              { status: { in: ['TODO', 'IN_PROGRESS'] } },
              { OR: [{ priority: 'URGENT' }, { priority: 'HIGH' }] },
              { assigneeId: { not: null } },
            ],
          },
          orderBy: { createdAt: 'desc' },
          take: 10,
        },
        () =>
          db.prisma.task.findMany({
            where: {
              AND: [
                { status: { in: ['TODO', 'IN_PROGRESS'] } },
                { OR: [{ priority: 'URGENT' }, { priority: 'HIGH' }] },
                { assigneeId: { not: null } },
              ],
            },
            orderBy: { createdAt: 'desc' },
            take: 10,
          }),
        {
          sortField: undefined,
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(
                and(
                  inArray(schema.sqliteTasks.status, ['TODO', 'IN_PROGRESS']),
                  or(
                    eq(schema.sqliteTasks.priority, 'URGENT'),
                    eq(schema.sqliteTasks.priority, 'HIGH'),
                  ),
                  isNotNull(schema.sqliteTasks.assigneeId),
                ),
              )
              .orderBy(desc(schema.sqliteTasks.createdAt))
              .limit(10),
        },
      ))
  })

  describe('findFirst', () => {
    it('simple', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findFirst',
        'User',
        {
          method: 'findFirst',
          where: { status: 'ACTIVE' },
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.user.findFirst({
            where: { status: 'ACTIVE' },
            orderBy: { id: 'asc' },
          }),
        {
          transformPrisma: (r) => (r ? [r] : []),
          drizzleQuery: async () => {
            const result = await drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(eq(schema.sqliteUsers.status, 'ACTIVE'))
              .orderBy(asc(schema.sqliteUsers.id))
              .limit(1)
            return result
          },
        },
      ))

    it('with skip', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findFirst skip',
        'Task',
        {
          method: 'findFirst',
          where: { status: 'TODO' },
          skip: 5,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.task.findFirst({
            where: { status: 'TODO' },
            skip: 5,
            orderBy: { id: 'asc' },
          }),
        {
          transformPrisma: (r) => (r ? [r] : []),
          drizzleQuery: async () => {
            const result = await drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(eq(schema.sqliteTasks.status, 'TODO'))
              .orderBy(asc(schema.sqliteTasks.id))
              .limit(1)
              .offset(5)
            return result
          },
        },
      ))
  })

  describe('findUnique', () => {
    it('by id', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findUnique id',
        'User',
        { method: 'findUnique', where: { id: seed.userIds[0] } },
        () => db.prisma.user.findUnique({ where: { id: seed.userIds[0] } }),
        {
          transformPrisma: (r) => (r ? [r] : []),
          drizzleQuery: async () => {
            const result = await drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(eq(schema.sqliteUsers.id, seed.userIds[0]))
              .limit(1)
            return result
          },
        },
      ))

    it('by unique field', async () => {
      const user = await db.prisma.user.findFirst()
      if (!user) return

      await runParityTest(
        db,
        benchmarkResults,
        'findUnique email',
        'User',
        { method: 'findUnique', where: { email: user.email } },
        () => db.prisma.user.findUnique({ where: { email: user.email } }),
        {
          transformPrisma: (r) => (r ? [r] : []),
          drizzleQuery: async () => {
            const result = await drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(eq(schema.sqliteUsers.email, user.email))
              .limit(1)
            return result
          },
        },
      )
    })
  })

  describe('count', () => {
    it('simple', () =>
      runParityTest(
        db,
        benchmarkResults,
        'count',
        'User',
        { method: 'count' },
        () => db.prisma.user.count(),
        {
          transformPrisma: (c) => [{ '_count._all': BigInt(c as number) }],
          transformDrizzle: (r: any[]) => [
            { '_count._all': BigInt(r[0].count) },
          ],
          drizzleQuery: async () => {
            const result = await drizzle.db
              .select({ count: sql<number>`count(*)` })
              .from(schema.sqliteUsers)
            return result
          },
        },
      ))

    it('with where', () =>
      runParityTest(
        db,
        benchmarkResults,
        'count where',
        'Task',
        { method: 'count', where: { status: 'DONE' } },
        () => db.prisma.task.count({ where: { status: 'DONE' } }),
        {
          transformPrisma: (c) => [{ '_count._all': BigInt(c as number) }],
          transformDrizzle: (r: any[]) => [
            { '_count._all': BigInt(r[0].count) },
          ],
          drizzleQuery: async () => {
            const result = await drizzle.db
              .select({ count: sql<number>`count(*)` })
              .from(schema.sqliteTasks)
              .where(eq(schema.sqliteTasks.status, 'DONE'))
            return result
          },
        },
      ))
  })

  describe('aggregate', () => {
    it('count all', () =>
      runParityTest(
        db,
        benchmarkResults,
        'aggregate count',
        'Task',
        { method: 'aggregate', _count: { _all: true } },
        () => db.prisma.task.aggregate({ _count: { _all: true } }),
        {
          transformPrisma: (r: any) => [
            { '_count._all': BigInt(r._count._all) },
          ],
        },
      ))

    it('sum and avg', () =>
      runParityTest(
        db,
        benchmarkResults,
        'aggregate sum/avg',
        'Task',
        {
          method: 'aggregate',
          _sum: { position: true },
          _avg: { position: true },
        },
        () =>
          db.prisma.task.aggregate({
            _sum: { position: true },
            _avg: { position: true },
          }),
        {
          transformPrisma: (r: any) => [
            {
              '_sum.position': r._sum.position,
              '_avg.position': r._avg.position,
            },
          ],
        },
      ))

    it('with where', () =>
      runParityTest(
        db,
        benchmarkResults,
        'aggregate where',
        'Task',
        {
          method: 'aggregate',
          where: { status: 'DONE' },
          _count: { _all: true },
        },
        () =>
          db.prisma.task.aggregate({
            where: { status: 'DONE' },
            _count: { _all: true },
          }),
        {
          transformPrisma: (r: any) => [
            { '_count._all': BigInt(r._count._all) },
          ],
        },
      ))

    it('min and max', () =>
      runParityTest(
        db,
        benchmarkResults,
        'aggregate min/max',
        'Task',
        {
          method: 'aggregate',
          _min: { position: true },
          _max: { position: true },
        },
        () =>
          db.prisma.task.aggregate({
            _min: { position: true },
            _max: { position: true },
          }),
        {
          transformPrisma: (r: any) => [
            {
              '_min.position': r._min.position,
              '_max.position': r._max.position,
            },
          ],
        },
      ))

    it('all aggregate operations', () =>
      runParityTest(
        db,
        benchmarkResults,
        'aggregate complete',
        'Task',
        {
          method: 'aggregate',
          _count: { _all: true },
          _sum: { position: true },
          _avg: { position: true },
          _min: { position: true },
          _max: { position: true },
        },
        () =>
          db.prisma.task.aggregate({
            _count: { _all: true },
            _sum: { position: true },
            _avg: { position: true },
            _min: { position: true },
            _max: { position: true },
          }),
        {
          transformPrisma: (r: any) => [
            {
              '_count._all': BigInt(r._count._all),
              '_sum.position': r._sum.position,
              '_avg.position': r._avg.position,
              '_min.position': r._min.position,
              '_max.position': r._max.position,
            },
          ],
        },
      ))
  })

  describe('groupBy', () => {
    it('simple', () =>
      runParityTest(
        db,
        benchmarkResults,
        'groupBy',
        'Task',
        { method: 'groupBy', by: ['status'] },
        () =>
          db.prisma.task.groupBy({
            by: ['status'],
            orderBy: { status: 'asc' },
          }),
        {
          sortField: 'status',
        },
      ))

    it('with count', () =>
      runParityTest(
        db,
        benchmarkResults,
        'groupBy count',
        'Task',
        { method: 'groupBy', by: ['status'], _count: { _all: true } },
        () =>
          db.prisma.task.groupBy({
            by: ['status'],
            _count: { _all: true },
            orderBy: { status: 'asc' },
          }),
        {
          sortField: 'status',
          transformPrisma: (r: any[]) =>
            r.map((row) => ({
              status: row.status,
              '_count._all': BigInt(row._count._all),
            })),
        },
      ))

    it('multiple fields', () =>
      runParityTest(
        db,
        benchmarkResults,
        'groupBy multi',
        'Task',
        {
          method: 'groupBy',
          by: ['status', 'priority'],
          _count: { _all: true },
        },
        () =>
          db.prisma.task.groupBy({
            by: ['status', 'priority'],
            _count: { _all: true },
            orderBy: [{ status: 'asc' }, { priority: 'asc' }],
          }),
        {
          sortField: undefined,
          transformPrisma: (r: any[]) =>
            r
              .map((row) => ({
                status: row.status,
                priority: row.priority,
                '_count._all': BigInt(row._count._all),
              }))
              .sort((a, b) => {
                const s = a.status.localeCompare(b.status)
                return s !== 0 ? s : a.priority.localeCompare(b.priority)
              }),
          transformGenerated: (r: any[]) =>
            r.sort((a, b) => {
              const s = a.status.localeCompare(b.status)
              return s !== 0 ? s : a.priority.localeCompare(b.priority)
            }),
        },
      ))

    it('with having', () =>
      runParityTest(
        db,
        benchmarkResults,
        'groupBy having',
        'Task',
        {
          method: 'groupBy',
          by: ['status'],
          _count: { _all: true },
          having: { status: { _count: { gte: 5 } } },
        },
        () =>
          db.prisma.task.groupBy({
            by: ['status'],
            _count: { status: true },
            having: {
              status: {
                _count: {
                  gte: 5,
                },
              },
            },
            orderBy: { status: 'asc' },
          }),
        {
          sortField: 'status',
          transformPrisma: (r: any[]) =>
            r.map((row) => ({
              status: row.status,
              '_count._all': BigInt(row._count.status),
            })),
        },
      ))

    it('with where', () =>
      runParityTest(
        db,
        benchmarkResults,
        'groupBy + where',
        'Task',
        {
          method: 'groupBy',
          by: ['status'],
          where: { priority: 'HIGH' },
          _count: { _all: true },
        },
        () =>
          db.prisma.task.groupBy({
            by: ['status'],
            where: { priority: 'HIGH' },
            _count: { _all: true },
            orderBy: { status: 'asc' },
          }),
        {
          sortField: 'status',
          transformPrisma: (r: any[]) =>
            r.map((row) => ({
              status: row.status,
              '_count._all': BigInt(row._count._all),
            })),
        },
      ))

    it('with sum/avg', () =>
      runParityTest(
        db,
        benchmarkResults,
        'groupBy aggregates',
        'Task',
        {
          method: 'groupBy',
          by: ['status'],
          _sum: { position: true },
          _avg: { position: true },
        },
        () =>
          db.prisma.task.groupBy({
            by: ['status'],
            _sum: { position: true },
            _avg: { position: true },
            orderBy: { status: 'asc' },
          }),
        {
          sortField: 'status',
          transformPrisma: (r: any[]) =>
            r.map((row) => ({
              status: row.status,
              '_sum.position': row._sum.position,
              '_avg.position': row._avg.position,
            })),
        },
      ))

    it('with min/max', () =>
      runParityTest(
        db,
        benchmarkResults,
        'groupBy min/max',
        'Task',
        {
          method: 'groupBy',
          by: ['status'],
          _min: { position: true },
          _max: { position: true },
        },
        () =>
          db.prisma.task.groupBy({
            by: ['status'],
            _min: { position: true },
            _max: { position: true },
            orderBy: { status: 'asc' },
          }),
        {
          sortField: 'status',
          transformPrisma: (r: any[]) =>
            r.map((row) => ({
              status: row.status,
              '_min.position': row._min.position,
              '_max.position': row._max.position,
            })),
        },
      ))
  })

  describe('include (nested)', () => {
    it('include one-to-many', () =>
      runParityTest(
        db,
        benchmarkResults,
        'include posts',
        'Project',
        {
          method: 'findMany',
          include: { tasks: true },
          take: 5,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.project.findMany({
            include: { tasks: true },
            take: 5,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      ))

    it('include one-to-one', () =>
      runParityTest(
        db,
        benchmarkResults,
        'include profile',
        'Task',
        {
          method: 'findMany',
          include: { assignee: true },
          where: { assigneeId: { not: null } },
          take: 5,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.task.findMany({
            include: { assignee: true },
            where: { assigneeId: { not: null } },
            take: 5,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      ))

    it('deep nesting 3 levels', () =>
      runParityTest(
        db,
        benchmarkResults,
        'include 3 levels',
        'Organization',
        {
          method: 'findMany',
          include: {
            projects: {
              include: {
                tasks: {
                  include: { comments: true },
                  take: 3,
                },
              },
              take: 2,
            },
          },
          take: 2,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.organization.findMany({
            include: {
              projects: {
                include: {
                  tasks: {
                    include: { comments: true },
                    take: 3,
                  },
                },
                take: 2,
              },
            },
            take: 2,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      ))

    it('deep nesting 4 levels', () =>
      runParityTest(
        db,
        benchmarkResults,
        'include 4 levels',
        'Organization',
        {
          method: 'findMany',
          include: {
            projects: {
              include: {
                tasks: {
                  include: {
                    comments: {
                      include: { reactions: true },
                      take: 2,
                    },
                  },
                  take: 2,
                },
              },
              take: 2,
            },
          },
          take: 2,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.organization.findMany({
            include: {
              projects: {
                include: {
                  tasks: {
                    include: {
                      comments: {
                        include: { reactions: true },
                        take: 2,
                      },
                    },
                    take: 2,
                  },
                },
                take: 2,
              },
            },
            take: 2,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      ))

    it('include with where', () =>
      runParityTest(
        db,
        benchmarkResults,
        'include + where',
        'Project',
        {
          method: 'findMany',
          include: {
            tasks: {
              where: { status: 'DONE' },
              orderBy: { id: 'asc' },
              take: 5,
            },
          },
          take: 5,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.project.findMany({
            include: {
              tasks: {
                where: { status: 'DONE' },
                orderBy: { id: 'asc' },
                take: 5,
              },
            },
            take: 5,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      ))

    it('include with select', () =>
      runParityTest(
        db,
        benchmarkResults,
        'include + select nested',
        'User',
        {
          method: 'findMany',
          include: {
            assignedTasks: {
              select: { id: true, title: true, status: true },
              orderBy: { id: 'asc' },
            },
          },
          take: 5,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.user.findMany({
            include: {
              assignedTasks: {
                select: { id: true, title: true, status: true },
                orderBy: { id: 'asc' },
              },
            },
            take: 5,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      ))
  })

  describe('string operations', () => {
    it('startsWith', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany startsWith',
        'User',
        { method: 'findMany', where: { email: { startsWith: 'user' } } },
        () =>
          db.prisma.user.findMany({
            where: { email: { startsWith: 'user' } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(like(schema.sqliteUsers.email, 'user%'))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('endsWith', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany endsWith',
        'User',
        { method: 'findMany', where: { email: { endsWith: '.com' } } },
        () =>
          db.prisma.user.findMany({
            where: { email: { endsWith: '.com' } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(like(schema.sqliteUsers.email, '%.com'))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('not contains', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany NOT contains',
        'User',
        { method: 'findMany', where: { NOT: { email: { contains: 'test' } } } },
        () =>
          db.prisma.user.findMany({
            where: { NOT: { email: { contains: 'test' } } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(not(like(schema.sqliteUsers.email, '%test%')))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('contains case sensitive', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany LIKE',
        'User',
        {
          method: 'findMany',
          where: { email: { contains: 'Example' } },
        },
        () =>
          db.prisma.user.findMany({
            where: { email: { contains: 'Example' } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(like(schema.sqliteUsers.email, '%Example%'))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))
  })

  describe('comparison operators', () => {
    it('lt', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany <',
        'Task',
        { method: 'findMany', where: { position: { lt: 10 } } },
        () =>
          db.prisma.task.findMany({
            where: { position: { lt: 10 } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(lt(schema.sqliteTasks.position, 10))
              .orderBy(asc(schema.sqliteTasks.id)),
        },
      ))

    it('lte', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany <=',
        'Task',
        { method: 'findMany', where: { position: { lte: 10 } } },
        () =>
          db.prisma.task.findMany({
            where: { position: { lte: 10 } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(lte(schema.sqliteTasks.position, 10))
              .orderBy(asc(schema.sqliteTasks.id)),
        },
      ))

    it('gt', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany >',
        'Task',
        { method: 'findMany', where: { position: { gt: 5 } } },
        () =>
          db.prisma.task.findMany({
            where: { position: { gt: 5 } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .where(gt(schema.sqliteTasks.position, 5))
              .orderBy(asc(schema.sqliteTasks.id)),
        },
      ))

    it('notIn', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany NOT IN',
        'User',
        {
          method: 'findMany',
          where: { status: { notIn: ['DELETED', 'BANNED'] } },
        },
        () =>
          db.prisma.user.findMany({
            where: { status: { notIn: ['DELETED', 'BANNED'] } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(
                not(inArray(schema.sqliteUsers.status, ['DELETED', 'BANNED'])),
              )
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))

    it('isNot explicit', () =>
      runParityTest(
        db,
        benchmarkResults,
        'findMany isNot null',
        'User',
        { method: 'findMany', where: { name: { not: null } } },
        () =>
          db.prisma.user.findMany({
            where: { name: { not: null } },
            orderBy: { id: 'asc' },
          }),
        {
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteUsers)
              .where(isNotNull(schema.sqliteUsers.name))
              .orderBy(asc(schema.sqliteUsers.id)),
        },
      ))
  })

  describe('sorting', () => {
    it('multiple orderBy fields', () =>
      runParityTest(
        db,
        benchmarkResults,
        'orderBy multi-field',
        'Task',
        {
          method: 'findMany',
          orderBy: [
            { status: 'asc' },
            { priority: 'desc' },
            { createdAt: 'desc' },
          ],
          take: 20,
        },
        () =>
          db.prisma.task.findMany({
            orderBy: [
              { status: 'asc' },
              { priority: 'desc' },
              { createdAt: 'desc' },
            ],
            take: 20,
          }),
        {
          sortField: undefined,
          drizzleQuery: () =>
            drizzle.db
              .select()
              .from(schema.sqliteTasks)
              .orderBy(
                asc(schema.sqliteTasks.status),
                desc(schema.sqliteTasks.priority),
                desc(schema.sqliteTasks.createdAt),
              )
              .limit(20),
        },
      ))
  })

  describe('distinct', () => {
    it('single field', () =>
      runParityTest(
        db,
        benchmarkResults,
        'distinct status',
        'Task',
        {
          method: 'findMany',
          distinct: ['status'],
          orderBy: { status: 'asc' },
        },
        () =>
          db.prisma.task.findMany({
            distinct: ['status'],
            orderBy: { status: 'asc' },
          }),
        {
          sortField: 'status',
        },
      ))

    it('multiple fields', () =>
      runParityTest(
        db,
        benchmarkResults,
        'distinct multi',
        'Task',
        {
          method: 'findMany',
          distinct: ['status', 'priority'],
          orderBy: [{ status: 'asc' }, { priority: 'asc' }],
        },
        () =>
          db.prisma.task.findMany({
            distinct: ['status', 'priority'],
            orderBy: [{ status: 'asc' }, { priority: 'asc' }],
          }),
        {
          sortField: undefined,
        },
      ))
  })

  describe('cursor pagination', () => {
    it('basic cursor', async () => {
      const firstTask = await db.prisma.task.findFirst({
        orderBy: { id: 'asc' },
      })
      if (!firstTask) return

      await runParityTest(
        db,
        benchmarkResults,
        'cursor pagination',
        'Task',
        {
          method: 'findMany',
          cursor: { id: firstTask.id },
          take: 5,
          skip: 1,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.task.findMany({
            cursor: { id: firstTask.id },
            take: 5,
            skip: 1,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      )
    })
  })

  describe('select + include', () => {
    it('select fields + include relation', () =>
      runParityTest(
        db,
        benchmarkResults,
        'select + include',
        'User',
        {
          method: 'findMany',
          select: {
            id: true,
            email: true,
            assignedTasks: {
              select: { id: true, title: true, status: true },
              orderBy: { id: 'asc' },
              take: 3,
            },
          },
          take: 5,
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.user.findMany({
            select: {
              id: true,
              email: true,
              assignedTasks: {
                select: { id: true, title: true, status: true },
                orderBy: { id: 'asc' },
                take: 3,
              },
            },
            take: 5,
            orderBy: { id: 'asc' },
          }),
        {
          sortField: undefined,
        },
      ))
  })

  describe('relation count', () => {
    it('_count in select', () =>
      runParityTest(
        db,
        benchmarkResults,
        '_count relation',
        'Project',
        {
          method: 'findMany',
          select: {
            id: true,
            name: true,
            _count: { select: { tasks: true } },
          },
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.project.findMany({
            select: {
              id: true,
              name: true,
              _count: { select: { tasks: true } },
            },
            orderBy: { id: 'asc' },
          }),
      ))

    it('_count multiple relations', () =>
      runParityTest(
        db,
        benchmarkResults,
        '_count multi-relation',
        'Organization',
        {
          method: 'findMany',
          select: {
            id: true,
            name: true,
            _count: { select: { projects: true, members: true } },
          },
          orderBy: { id: 'asc' },
        },
        () =>
          db.prisma.organization.findMany({
            select: {
              id: true,
              name: true,
              _count: { select: { projects: true, members: true } },
            },
            orderBy: { id: 'asc' },
          }),
      ))
  })
})
