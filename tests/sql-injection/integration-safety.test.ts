// tests/sql-injection/integration-safety.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Integration Safety Verification', () => {
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

  describe('SQL Generation Safety', () => {
    it('should never generate SQL with raw string concatenation of values', () => {
      const maliciousValues = [
        "'; DROP TABLE users; --",
        "admin' OR '1'='1",
        "' UNION SELECT * FROM users--",
        "; DELETE FROM users WHERE '1'='1",
        "' AND 1=0 UNION ALL SELECT * FROM users--",
      ]

      for (const value of maliciousValues) {
        const { sql, params } = toSQL('User', 'findMany', {
          where: { email: value },
        })

        expect(sql).toContain('$1')
        expect(params).toContain(value)
        expect(sql).not.toContain(value)
      }
    })

    it('should never generate SQL with unquoted field names', () => {
      const { sql } = toSQL('User', 'findMany', {
        select: {
          id: true,
          email: true,
          name: true,
        },
      })

      expect(sql).toMatch(/SELECT.*FROM/i)
      expect(sql).not.toMatch(/SELECT\s+\w+,\s*\w+,\s*\w+\s+FROM/i)
    })

    it('should never generate SQL with unsafe table references', () => {
      const { sql } = toSQL('User', 'findMany', {
        where: { id: { gt: 1 } },
      })

      expect(sql).toMatch(/FROM\s+"public"\."users"/i)
      expect(sql).not.toMatch(/FROM\s+users(?!\s*[a-z_])/i)
    })
  })

  describe('Parameter Binding Verification', () => {
    it('should bind all placeholders to parameters', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          AND: [
            { email: "'; DROP--" },
            { status: 'active' },
            { role: { in: ['USER', 'ADMIN'] } },
          ],
        },
      })

      const placeholderCount = (sql.match(/\$\d+/g) || []).length

      const totalParams = params.reduce((acc: any, p) => {
        return acc + (Array.isArray(p) ? 1 : 1)
      }, 0)

      expect(placeholderCount).toBe(totalParams)
    })

    it('should use sequential placeholder numbers', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          OR: [
            { email: 'a' },
            { email: 'b' },
            { email: 'c' },
            { email: 'd' },
            { email: 'e' },
          ],
        },
      })

      const placeholders = sql.match(/\$(\d+)/g) || []
      const numbers = placeholders.map((p) => parseInt(p.slice(1), 10))

      for (let i = 0; i < numbers.length; i++) {
        expect(numbers).toContain(i + 1)
      }

      expect(Math.max(...numbers)).toBe(params.length)
    })

    it('should never skip placeholder numbers', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          AND: [
            { email: 'test1@example.com' },
            { status: 'active' },
            { role: 'USER' },
          ],
        },
      })

      const placeholders = sql.match(/\$(\d+)/g) || []
      const numbers = placeholders.map((p) => parseInt(p.slice(1), 10)).sort()

      for (let i = 1; i <= params.length; i++) {
        expect(numbers).toContain(i)
      }
    })
  })

  describe('Field Validation Enforcement', () => {
    it('should reject all non-schema fields', () => {
      const maliciousFields = [
        'nonExistent',
        '__proto__',
        'constructor',
        "'; DROP TABLE--",
        'email; DROP TABLE users',
      ]

      for (const field of maliciousFields) {
        expect(() => {
          toSQL('User', 'findMany', {
            where: { [field as any]: 'value' },
          })
        }).toThrow()
      }
    })

    it('should safely handle non-existent relations in include', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: { nonExistentRelation: true },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('nonExistentRelation')
    })
  })

  describe('Operator Validation', () => {
    it('should reject unknown operators', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            email: {
              ['maliciousOperator' as any]: 'value',
            },
          },
        })
      }).toThrow()
    })

    it('should validate operators per field type', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            email: {
              ['has' as any]: 'value',
            },
          },
        })
      }).toThrow()
    })
  })

  describe('SQL Syntax Correctness', () => {
    it('should generate valid SQL for findMany', () => {
      const { sql } = toSQL('User', 'findMany', {
        where: { email: 'test@example.com' },
      })

      expect(sql).toMatch(/^SELECT\s+.*\s+FROM\s+.*\s+WHERE/i)
    })

    it('should generate valid SQL for findFirst', () => {
      const { sql } = toSQL('User', 'findFirst', {
        where: { id: { gt: 1 } },
      })

      expect(sql).toMatch(/^SELECT\s+.*\s+FROM\s+.*\s+WHERE/i)
      expect(sql).toContain('LIMIT 1')
    })

    it('should generate valid SQL for count', () => {
      const { sql } = toSQL('User', 'count', {
        where: { status: 'active' },
      })

      expect(sql).toMatch(/SELECT\s+COUNT\(\*\)/i)
    })

    it('should generate valid SQL for aggregate', () => {
      const { sql } = toSQL('Task', 'aggregate', {
        _count: { _all: true },
        _avg: { position: true },
      })

      expect(sql).toMatch(/SELECT.*COUNT\(\*\).*AVG/is)
    })

    it('should generate valid SQL for groupBy', () => {
      const { sql } = toSQL('Task', 'groupBy', {
        by: ['projectId'],
        _count: { _all: true },
      })

      expect(sql).toMatch(/GROUP\s+BY/i)
    })
  })

  describe('Cross-Method Safety', () => {
    const methods: Array<
      | 'findMany'
      | 'findFirst'
      | 'findUnique'
      | 'count'
      | 'aggregate'
      | 'groupBy'
    > = ['findMany', 'findFirst', 'findUnique', 'count']

    methods.forEach((method) => {
      it(`should safely handle injection attempts in ${method}`, () => {
        const args =
          method === 'aggregate'
            ? { _count: { _all: true }, where: { email: "'; DROP--" } }
            : { where: { email: "'; DROP TABLE users; --" } }

        const { sql, params } = toSQL('User', method, args)

        expect(params).toContain("'; DROP TABLE users; --")
        expect(sql).not.toContain('DROP TABLE')
      })
    })
  })

  describe('Real-World Attack Patterns', () => {
    it('should safely handle tautology-based authentication bypass', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "admin@example.com' OR '1'='1",
          status: 'active',
        },
      })

      expect(params).toContain("admin@example.com' OR '1'='1")
      expect(sql).not.toMatch(/OR\s+['"]1['"]\s*=\s*['"]1['"]/i)
    })

    it('should safely handle comment-based injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "admin@example.com'--",
          status: 'active',
        },
      })

      expect(params).toContain("admin@example.com'--")
      expect(sql).toContain('$1')
    })

    it('should safely handle UNION-based data extraction', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email:
            "test@example.com' UNION SELECT id, password, null FROM users--",
        },
      })

      expect(params).toContain(
        "test@example.com' UNION SELECT id, password, null FROM users--",
      )
      expect(sql).not.toContain('UNION')
    })

    it('should safely handle stacked query injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com'; DROP TABLE users; SELECT * FROM users--",
        },
      })

      expect(params).toContain(
        "test@example.com'; DROP TABLE users; SELECT * FROM users--",
      )
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should safely handle error-based injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com' AND CAST((SELECT version()) AS int)--",
        },
      })

      expect(params).toContain(
        "test@example.com' AND CAST((SELECT version()) AS int)--",
      )
      expect(sql).not.toContain('CAST')
      expect(sql).not.toContain('version()')
    })
  })
})
