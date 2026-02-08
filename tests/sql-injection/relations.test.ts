// tests/sql-injection/relations.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Relations', () => {
  let prisma: PrismaClient
  let toSQL: ReturnType<typeof createToSQL>

  beforeAll(() => {
    prisma = new PrismaClient()
    const models = convertDMMFToModels(Prisma.dmmf.datamodel as DMMF.Datamodel)
    setGlobalDialect('postgres')
    toSQL = createToSQL(models, 'postgres')
  })

  afterAll(async () => {
    await prisma.$disconnect()
  })

  it('should parameterize relation "some" filter values', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        memberships: {
          some: {
            organizationId: "999'; DROP TABLE users; --" as any,
          },
        },
      },
    })

    expect(params).toContain("999'; DROP TABLE users; --")
    expect(sql).not.toContain('DROP TABLE')
  })

  it('should parameterize relation "every" filter values', () => {
    const { sql, params } = toSQL('Organization', 'findMany', {
      where: {
        members: {
          every: {
            userId: "123' OR '1'='1" as any,
          },
        },
      },
    })

    expect(params).toContain("123' OR '1'='1")
    expect(sql).not.toMatch(/OR\s+'1'\s*=\s*'1'/)
  })

  it('should parameterize relation "is" filter values', () => {
    const { sql, params } = toSQL('Task', 'findMany', {
      where: {
        assignee: {
          is: {
            email: "admin@example.com' UNION SELECT * FROM users--",
          },
        },
      },
    })

    expect(params).toContain("admin@example.com' UNION SELECT * FROM users--")
    expect(sql).not.toContain('UNION SELECT')
  })

  it('should parameterize relation "isNot" filter values', () => {
    const { sql, params } = toSQL('Task', 'findMany', {
      where: {
        creator: {
          isNot: {
            name: "'; EXEC xp_cmdshell('rm -rf /'); --",
          },
        },
      },
    })

    expect(params).toContain("'; EXEC xp_cmdshell('rm -rf /'); --")
    expect(sql).not.toContain('EXEC')
  })

  it('should parameterize nested relation filters', () => {
    const { sql, params } = toSQL('Organization', 'findMany', {
      where: {
        projects: {
          some: {
            tasks: {
              some: {
                title: "Task'; DROP TABLE tasks; --",
              },
            },
          },
        },
      },
    })

    expect(params).toContain("Task'; DROP TABLE tasks; --")
    expect(sql).not.toContain('DROP TABLE')
  })

  it('should parameterize multiple relation conditions', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        memberships: {
          some: { organizationId: "999' OR 1=1--" as any },
        },
        assignedTasks: {
          some: { title: '; DELETE FROM tasks--' },
        },
      },
    })

    expect(params).toContain("999' OR 1=1--")
    expect(params).toContain('; DELETE FROM tasks--')
    expect(sql).not.toContain('DELETE FROM')
  })
})
