// tests/sql-injection/array-operators.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Array Operators', () => {
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

  describe('Array field parameterization', () => {
    it('should parameterize in operator with malicious arrays', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: {
            in: ["'; DROP--", 'UNION SELECT--'],
          },
        },
      })

      expect(params[0]).toEqual(["'; DROP--", 'UNION SELECT--'])
    })

    it('should parameterize notIn operator with malicious arrays', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: {
            notIn: ["'; TRUNCATE--", 'DELETE FROM--'],
          },
        },
      })

      expect(params[0]).toEqual(["'; TRUNCATE--", 'DELETE FROM--'])
    })

    it('should handle empty arrays in IN', () => {
      const { sql } = toSQL('User', 'findMany', {
        where: {
          email: { in: [] },
        },
      })

      expect(sql).toMatch(/0\s*=\s*1/i)
    })

    it('should handle empty arrays in NOT IN', () => {
      const { sql } = toSQL('User', 'findMany', {
        where: {
          email: { notIn: [] },
        },
      })

      expect(sql).toMatch(/1\s*=\s*1|SELECT/i)
    })

    it('should handle large arrays safely', () => {
      const largeArray = Array(100).fill("'; DROP--")
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { in: largeArray },
        },
      })

      expect(params[0]).toEqual(largeArray)
    })

    it('should handle nested arrays in OR conditions', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          OR: [
            { email: { in: ["'; DROP--"] } },
            { name: { in: ['UNION SELECT--'] } },
          ],
        },
      })

      expect(params).toHaveLength(2)
    })

    it('should parameterize array values in complex queries', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          AND: [
            { email: { in: ["'; DROP--", 'test@example.com'] } },
            { status: { notIn: ['UNION--', 'active'] } },
          ],
        },
      })

      expect(params).toHaveLength(2)
    })
  })

  describe('Array operator type safety', () => {
    it('should reject non-array values in IN', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            email: { in: 'not-an-array' as any },
          },
        })
      }).toThrow(/require.*array/i)
    })

    it('should handle null in array positions', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { in: [null, "'; DROP--"] as any },
        },
      })

      expect(params[0]).toContain(null)
      expect(params[0]).toContain("'; DROP--")
    })

    it('should handle mixed types in arrays', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { in: ['test', 123, "'; DROP--"] as any },
        },
      })

      expect(params[0]).toContain('test')
      expect(params[0]).toContain(123)
      expect(params[0]).toContain("'; DROP--")
    })

    it('should parameterize arrays with only injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: {
            in: ["' OR 1=1--", "'; DROP TABLE users; --", "admin'--"],
          },
        },
      })

      expect(params[0]).toEqual([
        "' OR 1=1--",
        "'; DROP TABLE users; --",
        "admin'--",
      ])
    })
  })
})
