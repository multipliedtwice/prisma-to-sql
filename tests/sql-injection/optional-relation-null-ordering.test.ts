import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { Prisma } from '../generated/postgres/client'
import { createToSQL } from '../../src'

const models = convertDMMFToModels(Prisma.dmmf.datamodel)

describe('optional relation null ordering', () => {
  it('emits PostgreSQL relation ordering with explicit null placement', () => {
    const toSQL = createToSQL(models, 'postgres')
    const descending = toSQL('Task', 'findMany', {
      orderBy: {
        assignee: {
          createdAt: { sort: 'desc', nulls: 'last' },
        },
      },
    }).sql
    const ascending = toSQL('Task', 'findMany', {
      orderBy: {
        assignee: {
          createdAt: { sort: 'asc', nulls: 'first' },
        },
      },
    }).sql

    expect(descending).toContain(
      'LEFT JOIN "public"."users" ob_0 ON ob_0.id = tasks."assigneeId"',
    )
    expect(descending).toContain('ob_0."createdAt" DESC NULLS LAST')
    expect(ascending).toContain('ob_0."createdAt" ASC NULLS FIRST')
  })

  it('keeps order array precedence', () => {
    const sql = createToSQL(models, 'postgres')('Task', 'findMany', {
      orderBy: [
        {
          assignee: {
            createdAt: { sort: 'desc', nulls: 'last' },
          },
        },
        { title: 'asc' },
      ],
    }).sql

    expect(sql).toContain(
      'ORDER BY ob_0."createdAt" DESC NULLS LAST, tasks.title ASC',
    )
  })

  it('keeps SQLite explicit-null emulation', () => {
    const toSQL = createToSQL(models, 'sqlite')
    const descending = toSQL('Task', 'findMany', {
      orderBy: {
        assignee: {
          createdAt: { sort: 'desc', nulls: 'last' },
        },
      },
    }).sql
    const ascending = toSQL('Task', 'findMany', {
      orderBy: {
        assignee: {
          createdAt: { sort: 'asc', nulls: 'first' },
        },
      },
    }).sql

    expect(descending).toContain(
      'ORDER BY (ob_0."createdAt" IS NULL) ASC, ob_0."createdAt" DESC',
    )
    expect(ascending).toContain(
      'ORDER BY (ob_0."createdAt" IS NULL) DESC, ob_0."createdAt" ASC',
    )
  })
})
