// tests/sql-injection/logical-operators.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Logical Operators', () => {
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

  it('should parameterize AND conditions', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        AND: [
          { email: "test@example.com'; DROP TABLE users; --" },
          { name: "admin' OR '1'='1" },
        ],
      },
    })

    expect(params).toContain("test@example.com'; DROP TABLE users; --")
    expect(params).toContain("admin' OR '1'='1")
    expect(sql).not.toContain('DROP TABLE')
  })

  it('should parameterize OR conditions', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        OR: [
          { email: "; DELETE FROM users WHERE '1'='1" },
          { name: "ACTIVE' UNION SELECT * FROM users--" },
        ],
      },
    })

    expect(params).toContain("; DELETE FROM users WHERE '1'='1")
    expect(params).toContain("ACTIVE' UNION SELECT * FROM users--")
    expect(sql).not.toContain('DELETE FROM')
    expect(sql).not.toContain('UNION SELECT')
  })

  it('should parameterize NOT conditions', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        NOT: {
          email: "test@example.com'; TRUNCATE TABLE users; --",
        },
      },
    })

    expect(params).toContain("test@example.com'; TRUNCATE TABLE users; --")
    expect(sql).not.toContain('TRUNCATE')
  })

  it('should parameterize nested logical operators', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        AND: [
          {
            OR: [
              { email: "'; DROP TABLE users; --" },
              { name: "admin' OR 1=1--" },
            ],
          },
          {
            NOT: {
              role: '; DELETE FROM users--',
            },
          },
        ],
      },
    })

    expect(params).toContain("'; DROP TABLE users; --")
    expect(params).toContain("admin' OR 1=1--")
    expect(params).toContain('; DELETE FROM users--')
    expect(sql).not.toContain('DROP TABLE')
    expect(sql).not.toContain('DELETE FROM')
  })

  it('should parameterize complex nested conditions', () => {
    const { sql, params } = toSQL('Task', 'findMany', {
      where: {
        OR: [
          {
            AND: [
              { title: "'; EXEC sp_executesql '--" },
              { description: "TODO' UNION ALL SELECT * FROM tasks--" },
            ],
          },
          {
            NOT: {
              description: "; INSERT INTO tasks VALUES('malicious')--",
            },
          },
        ],
      },
    })

    expect(params).toContain("'; EXEC sp_executesql '--")
    expect(params).toContain("TODO' UNION ALL SELECT * FROM tasks--")
    expect(params).toContain("; INSERT INTO tasks VALUES('malicious')--")
    expect(sql).not.toContain('EXEC')
    expect(sql).not.toContain('UNION ALL')
    expect(sql).not.toContain('INSERT INTO')
  })
})
