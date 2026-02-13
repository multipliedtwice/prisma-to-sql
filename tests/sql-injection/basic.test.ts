// tests/sql-injection/basic.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { readFileSync, writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { getDatamodel } from '../helpers/datamodel'

const PRISMA_DIR = join(process.cwd(), 'tests', 'prisma')
const SCHEMA_PATH = join(PRISMA_DIR, 'schema-postgres.prisma')
const SCHEMA_PATH_V7 = join(PRISMA_DIR, 'schema-postgres-v7.prisma')

function mergeSchema(): void {
  const base = readFileSync(join(PRISMA_DIR, 'base.prisma'), 'utf-8')

  const header = `generator client {
  provider = "prisma-client-js"
  output   = "../generated/postgres"
}

datasource db {
  provider = "postgresql"
}`

  writeFileSync(SCHEMA_PATH, `${header}\n\n${base}`)
  writeFileSync(SCHEMA_PATH_V7, `${header}\n\n${base}`)
}

function cleanupSchema(): void {
  try {
    unlinkSync(SCHEMA_PATH)
  } catch {}
  try {
    unlinkSync(SCHEMA_PATH_V7)
  } catch {}
}

describe('SQL Injection - Basic Protection', () => {
  let toSQL: ReturnType<typeof createToSQL>

  beforeAll(async () => {
    mergeSchema()

    const datamodel = await getDatamodel('postgres')
    const models = convertDMMFToModels(datamodel)

    setGlobalDialect('postgres')
    toSQL = createToSQL(models, 'postgres')
  })

  afterAll(async () => {
    cleanupSchema()
  })

  describe('Value Parameterization', () => {
    it('should parameterize simple string values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'user@example.com' },
      })

      expect(sql).toContain('$1')
      expect(params).toEqual(['user@example.com'])
    })

    it('should parameterize strings with quotes', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "user'with'quotes" },
      })

      expect(params).toEqual(["user'with'quotes"])
    })

    it('should parameterize strings with semicolons', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'user;extra' },
      })

      expect(params).toEqual(['user;extra'])
    })

    it('should parameterize strings with SQL keywords', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'DROP TABLE users' },
      })

      expect(params).toEqual(['DROP TABLE users'])
    })

    it('should parameterize multiple values sequentially', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: 'user@example.com',
          status: 'active',
        },
      })

      expect(sql).toContain('$1')
      expect(sql).toContain('$2')
      expect(params).toEqual(['user@example.com', 'active'])
    })

    it('should handle null values without parameterization', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: null },
      })

      expect(sql).toContain('IS NULL')
      expect(params).toEqual([])
    })

    it('should parameterize array values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: {
            in: ['user1@example.com', 'user2@example.com'],
          },
        },
      })

      expect(params[0]).toEqual(['user1@example.com', 'user2@example.com'])
    })

    it('should parameterize numeric values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          id: { gt: 18, lt: 65 },
        },
      })

      expect(params).toContain(18)
      expect(params).toContain(65)
    })

    it('should maintain parameter order', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: 'first',
          status: 'second',
          id: { gt: 3 },
        },
      })

      expect(params[0]).toBe('first')
      expect(params[1]).toBe('second')
      expect(params[2]).toBe(3)
    })

    it('should use sequential placeholder numbers', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          OR: [{ email: 'a' }, { email: 'b' }, { email: 'c' }],
        },
      })

      expect(sql).toContain('$1')
      expect(sql).toContain('$2')
      expect(sql).toContain('$3')
      expect(params.length).toBe(3)
    })

    it('should parameterize SQL injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "'; DROP TABLE users; --",
        },
      })

      expect(params).toContain("'; DROP TABLE users; --")
      expect(sql).toMatch(/\$\d+/)
    })

    it('should parameterize complex injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "admin' OR '1'='1",
          name: "' UNION SELECT * FROM users--",
        },
      })

      expect(params).toContain("admin' OR '1'='1")
      expect(params).toContain("' UNION SELECT * FROM users--")
    })
  })

  describe('Field Name Protection', () => {
    it('should reject malicious field names in select', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: { ["'; DROP TABLE users--" as any]: true },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject SQL injection in field names', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: { ["email'; DELETE FROM users--" as any]: 'test' },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject malicious field names in orderBy', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          orderBy: { ["'; DROP TABLE--" as any]: 'asc' },
        })
      }).toThrow(/does not exist/i)
    })

    it('should reject non-existent fields', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: { nonExistentField: 'value' },
        })
      }).toThrow(/does not exist/i)
    })

    it('should only allow valid model fields', () => {
      const { sql } = toSQL('User', 'findMany', {
        where: { email: 'test@example.com' },
      })

      expect(sql).toMatch(/\w+\.(?:")?email(?:")?\b/)
    })
  })

  describe('Dynamic Parameter Safety', () => {
    it('should handle dynamic parameter markers safely', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test@example.com' },
        take: 10,
      })

      expect(sql).toContain('$')
      expect(params).toContain('test@example.com')
      expect(params).toContain(10)
    })

    it('should parameterize cursor values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "test'; DROP--" },
        cursor: { id: 1 },
        take: 10,
      })

      expect(params).toContain("test'; DROP--")
    })
  })

  describe('Field -> Column Mapping (@map)', () => {
    it('should use mapped column names in WHERE', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { isDeleted: false },
      })

      expect(params).toEqual([false])
      expect(sql).toMatch(/\b\w+\.(?:")?is_deleted(?:")?\b/)
    })

    it('should use mapped column names in SELECT', () => {
      const { sql } = toSQL('User', 'findMany', {
        select: { id: true, avatarUrl: true, isDeleted: true },
      })

      expect(sql).toMatch(/\b\w+\.(?:")?avatar_url(?:")?\b/)
      expect(sql).toMatch(/\b\w+\.(?:")?is_deleted(?:")?\b/)
    })

    it('should use mapped column names in ORDER BY', () => {
      const { sql } = toSQL('User', 'findMany', {
        orderBy: { isDeleted: 'asc' },
      })

      expect(sql).toMatch(/\bORDER BY\b/i)
      expect(sql).toMatch(/\b\w+\.(?:")?is_deleted(?:")?\s+ASC\b/i)
    })
  })
})
