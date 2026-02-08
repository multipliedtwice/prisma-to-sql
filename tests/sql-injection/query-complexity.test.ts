// tests/sql-injection/query-complexity.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Query Complexity DoS Protection', () => {
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

  describe('Large OR Conditions', () => {
    it('should handle moderate OR conditions', () => {
      const orConditions = Array(100)
        .fill(null)
        .map((_, i) => ({ id: { gt: i } }))

      const { sql, params } = toSQL('User', 'findMany', {
        where: { OR: orConditions },
      })

      expect(sql).toBeDefined()
      expect(params.length).toBe(100)
    })

    it('should handle large OR conditions without crashing', () => {
      const orConditions = Array(1000)
        .fill(null)
        .map((_, i) => ({ id: { gt: i } }))

      const { sql, params } = toSQL('User', 'findMany', {
        where: { OR: orConditions },
      })

      expect(sql).toBeDefined()
      expect(params.length).toBe(1000)
    })

    it('should handle very large OR with injection attempts', () => {
      const orConditions = Array(500)
        .fill(null)
        .map(() => ({ email: "'; DROP TABLE users; --" }))

      const { sql, params } = toSQL('User', 'findMany', {
        where: { OR: orConditions },
      })

      expect(sql).toBeDefined()
      expect(params.every((p) => p === "'; DROP TABLE users; --")).toBe(true)
      expect(sql).not.toContain('DROP TABLE')
    })
  })

  describe('Large AND Conditions', () => {
    it('should handle moderate AND conditions', () => {
      const andConditions = Array(100)
        .fill(null)
        .map((_, i) => ({ id: { lt: 1000 + i } }))

      const { sql, params } = toSQL('User', 'findMany', {
        where: { AND: andConditions },
      })

      expect(sql).toBeDefined()
      expect(params.length).toBe(100)
    })

    it('should handle large AND conditions without crashing', () => {
      const andConditions = Array(1000)
        .fill(null)
        .map((_, i) => ({ id: { lt: 1000 + i } }))

      const { sql, params } = toSQL('User', 'findMany', {
        where: { AND: andConditions },
      })

      expect(sql).toBeDefined()
      expect(params.length).toBe(1000)
    })
  })

  describe('Large IN Arrays', () => {
    it('should handle moderate IN arrays', () => {
      const inArray = Array(1000)
        .fill(null)
        .map((_, i) => i)

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          id: { in: inArray },
        },
      })

      expect(sql).toBeDefined()
      expect(params[0]).toEqual(inArray)
    })

    it('should handle large IN arrays', () => {
      const inArray = Array(10000)
        .fill(null)
        .map((_, i) => i)

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          id: { in: inArray },
        },
      })

      expect(sql).toBeDefined()
      expect(params[0]).toEqual(inArray)
    })

    it('should handle IN arrays with SQL injection attempts', () => {
      const inArray = Array(1000).fill("'; DROP TABLE users; --")

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: { in: inArray },
        },
      })

      expect(sql).toBeDefined()
      expect(params[0]).toEqual(inArray)
      expect(sql).not.toContain('DROP TABLE')
    })
  })

  describe('Deep Nesting', () => {
    it('should handle moderate nesting depth', () => {
      let nested: any = { email: 'test@example.com' }

      for (let i = 0; i < 10; i++) {
        nested = { AND: [nested, { id: { gt: i } }] }
      }

      const { sql, params } = toSQL('User', 'findMany', {
        where: nested,
      })

      expect(sql).toBeDefined()
      expect(params).toContain('test@example.com')
    })

    it('should handle deep nesting with injection attempts', () => {
      let nested: any = { email: "'; DROP TABLE users; --" }

      for (let i = 0; i < 20; i++) {
        nested = { AND: [nested, { status: 'active' }] }
      }

      const { sql, params } = toSQL('User', 'findMany', {
        where: nested,
      })

      expect(sql).toBeDefined()
      expect(params).toContain("'; DROP TABLE users; --")
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should reject or handle extremely deep nesting', () => {
      let nested: any = { email: "'; DROP--" }

      for (let i = 0; i < 100; i++) {
        nested = {
          assignedTasks: {
            some: {
              assignee: {
                is: nested,
              },
            },
          },
        }
      }

      const testFn = () => {
        return toSQL('User', 'findMany', { where: nested })
      }

      expect(() => {
        const result = testFn()
        expect(result.sql).not.toContain('DROP')
      }).not.toThrow(/DROP/)
    })
  })

  describe('Multiple Relations', () => {
    it('should handle many relation includes', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          assignedTasks: true,
          createdTasks: true,
          comments: true,
          activities: true,
          notifications: true,
          memberships: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).toContain('SELECT')
    })

    it('should handle many nested relations', () => {
      const { sql } = toSQL('Organization', 'findMany', {
        include: {
          members: {
            include: {
              user: {
                include: {
                  assignedTasks: true,
                  createdTasks: true,
                },
              },
            },
          },
          projects: {
            include: {
              tasks: {
                include: {
                  assignee: true,
                  creator: true,
                  comments: true,
                },
              },
            },
          },
        },
      })

      expect(sql).toBeDefined()
      expect(sql).toContain('SELECT')
    })
  })

  describe('Large Select Lists', () => {
    it('should handle selecting many fields', () => {
      const { sql } = toSQL('User', 'findMany', {
        select: {
          id: true,
          email: true,
          name: true,
          avatarUrl: true,
          role: true,
          status: true,
          metadata: true,
          tags: true,
          lastLoginAt: true,
          createdAt: true,
          updatedAt: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).toContain('SELECT')
    })
  })

  describe('Large Distinct Lists', () => {
    it('should handle many distinct fields', () => {
      const { sql } = toSQL('User', 'findMany', {
        distinct: ['email', 'status', 'role'],
      })

      expect(sql).toBeDefined()
    })
  })

  describe('Complex OrderBy', () => {
    it('should handle many orderBy fields', () => {
      const { sql } = toSQL('User', 'findMany', {
        orderBy: [
          { email: 'asc' },
          { status: 'desc' },
          { createdAt: 'asc' },
          { updatedAt: 'desc' },
          { id: 'asc' },
        ],
      })

      expect(sql).toBeDefined()
      expect(sql).toContain('ORDER BY')
    })
  })

  describe('Combined Complexity', () => {
    it('should handle complex query with multiple dimensions', () => {
      const orConditions = Array(50)
        .fill(null)
        .map((_, i) => ({ id: { gt: i } }))

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          OR: orConditions,
          AND: [{ status: 'active' }, { role: { in: ['USER', 'ADMIN'] } }],
        },
        include: {
          assignedTasks: {
            where: {
              title: { contains: 'test' },
            },
            orderBy: { createdAt: 'desc' },
            take: 10,
          },
          memberships: true,
        },
        orderBy: [{ createdAt: 'desc' }, { id: 'asc' }],
        take: 50,
        skip: 0,
      })

      expect(sql).toBeDefined()
      expect(params.length).toBeGreaterThan(0)
      expect(sql).toContain('SELECT')
      expect(sql).toContain('ORDER BY')

      // Verify no SQL injection in complex query
      expect(sql).not.toContain('DROP')
      expect(sql).not.toContain('TRUNCATE')
    })

    it('should handle complex query with injection attempts', () => {
      const orConditions = Array(50)
        .fill(null)
        .map(() => ({ email: "'; DROP TABLE users; --" }))

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          OR: orConditions,
        },
        include: {
          assignedTasks: {
            where: {
              title: "'; TRUNCATE TABLE tasks; --",
            },
          },
        },
        orderBy: { createdAt: 'desc' },
        take: 100,
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('DROP TABLE')
      expect(sql).not.toContain('TRUNCATE TABLE')
    })
  })
})
