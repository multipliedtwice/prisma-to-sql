// tests/sql-injection/like-patterns.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'
import { convertDMMFToModels } from '@dee-wan/schema-parser'

describe('SQL Injection - LIKE Pattern Safety', () => {
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

  describe('Wildcard Character Safety', () => {
    it('should handle percent wildcard in injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { contains: "admin%' OR '1'='1" },
        },
      })

      expect(params[0]).toBe("%admin%' OR '1'='1%")
      expect(sql).toContain('LIKE')
    })

    it('should handle underscore wildcard in injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          name: { startsWith: "test_' OR '1'='1" },
        },
      })

      expect(params[0]).toBe("test_' OR '1'='1%")
    })

    it('should handle backslash in patterns', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { contains: "test\\'; DROP--" },
        },
      })

      expect(params[0]).toBe("%test\\'; DROP--%")
    })

    it('should handle multiple wildcards in pattern', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { contains: "%_%'; DROP--" },
        },
      })

      expect(params[0]).toBe("%%_%'; DROP--%")
    })
  })

  describe('String Operator Injection', () => {
    it('should parameterize contains with SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { contains: "'; DROP TABLE users; --" },
        },
      })

      expect(params[0]).toBe("%'; DROP TABLE users; --%")
    })

    it('should parameterize startsWith with SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          name: { startsWith: "admin'; --" },
        },
      })

      expect(params[0]).toBe("admin'; --%")
    })

    it('should parameterize endsWith with SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { endsWith: "'; TRUNCATE TABLE--" },
        },
      })

      expect(params[0]).toBe("%'; TRUNCATE TABLE--")
    })
  })

  describe('Case Insensitive LIKE', () => {
    it('should handle injection in case insensitive contains', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: {
            contains: "'; UNION SELECT--",
            mode: 'insensitive',
          },
        },
      })

      expect(params[0]).toBe("%'; UNION SELECT--%")
      expect(sql).toContain('ILIKE')
    })

    it('should handle injection in case insensitive startsWith', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          name: {
            startsWith: "ADMIN' OR '1'='1",
            mode: 'insensitive',
          },
        },
      })

      expect(params[0]).toBe("ADMIN' OR '1'='1%")
    })
  })

  describe('Complex LIKE Patterns', () => {
    it('should handle nested OR with LIKE injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          OR: [
            { email: { contains: "'; DROP--" } },
            { name: { startsWith: "admin'; --" } },
          ],
        },
      })

      expect(params).toContainEqual("%'; DROP--%")
      expect(params).toContainEqual("admin'; --%")
    })

    it('should handle AND with multiple LIKE patterns', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          AND: [
            { email: { contains: "'; UNION--" } },
            { name: { endsWith: "'; DELETE--" } },
          ],
        },
      })

      expect(params).toContainEqual("%'; UNION--%")
      expect(params).toContainEqual("%'; DELETE--")
    })

    it('should handle NOT with LIKE injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          NOT: {
            email: { contains: "'; DROP TABLE--" },
          },
        },
      })

      expect(params).toContainEqual("%'; DROP TABLE--%")
    })
  })
})
