// tests/sql-injection/string-operators.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'
import { convertDMMFToModels } from '@dee-wan/schema-parser'

describe('SQL Injection - String Operators', () => {
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

  it('should parameterize contains operator', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        email: {
          contains: "'; DROP TABLE users; --",
        },
      },
    })

    expect(params[0]).toContain("'; DROP TABLE users; --")
    expect(sql).not.toContain('DROP TABLE')
    expect(sql).toContain('LIKE')
  })

  it('should parameterize startsWith operator', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        name: {
          startsWith: "admin' OR '1'='1",
        },
      },
    })

    expect(params[0]).toContain("admin' OR '1'='1")
    expect(sql).not.toMatch(/OR\s+'1'\s*=\s*'1'/)
    expect(sql).toContain('LIKE')
  })

  it('should parameterize endsWith operator', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        email: {
          endsWith: "; DELETE FROM users WHERE '1'='1",
        },
      },
    })

    expect(params[0]).toContain("; DELETE FROM users WHERE '1'='1")
    expect(sql).not.toContain('DELETE FROM')
    expect(sql).toContain('LIKE')
  })

  it('should parameterize case insensitive contains', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        email: {
          contains: "'; UNION SELECT password FROM users--",
          mode: 'insensitive',
        },
      },
    })

    expect(params[0]).toContain("'; UNION SELECT password FROM users--")
    expect(sql).not.toContain('UNION SELECT')
    expect(sql).toContain('ILIKE')
  })

  it('should parameterize multiple string operators', () => {
    const { sql, params } = toSQL('Task', 'findMany', {
      where: {
        title: {
          contains: "'; DROP--",
          startsWith: "admin'--",
        },
      },
    })

    expect(params).toContainEqual(expect.stringContaining("'; DROP--"))
    expect(params).toContainEqual(expect.stringContaining("admin'--"))
    expect(sql).not.toContain('DROP')
  })

  it('should escape wildcards in patterns', () => {
    const { sql, params } = toSQL('User', 'findMany', {
      where: {
        email: {
          contains: "%' OR '1'='1' OR email LIKE '%",
        },
      },
    })

    expect(params[0]).toContain("%' OR '1'='1' OR email LIKE '%")
    expect(sql).not.toMatch(/OR\s+'1'\s*=\s*'1'/)
  })
})
