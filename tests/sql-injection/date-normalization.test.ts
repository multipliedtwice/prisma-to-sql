import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { readFileSync, writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { getDatamodel } from '../helpers/datamodel'

const PRISMA_DIR = join(process.cwd(), 'tests', 'prisma')
const SCHEMA_PATH = join(PRISMA_DIR, 'schema-postgres.prisma')

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
}

function cleanupSchema(): void {
  try {
    unlinkSync(SCHEMA_PATH)
  } catch {}
}

describe('Date Parameter Normalization', () => {
  let toSQL: ReturnType<typeof createToSQL>

  beforeAll(async () => {
    mergeSchema()

    const datamodel = await getDatamodel('postgres')
    const models = convertDMMFToModels(datamodel)

    setGlobalDialect('postgres')
    toSQL = createToSQL(models, 'postgres')
  })

  afterAll(() => {
    cleanupSchema()
  })

  describe('Static Date Params', () => {
    it('should normalize Date objects to ISO strings', () => {
      const testDate = new Date('2024-01-15T10:30:00Z')
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdAt: { gte: testDate },
        },
      })

      expect(params).toContain('2024-01-15T10:30:00.000Z')
      expect(params).not.toContainEqual(testDate)
    })

    it('should handle multiple date params', () => {
      const startDate = new Date('2024-01-01T00:00:00Z')
      const endDate = new Date('2024-12-31T23:59:59Z')

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdAt: {
            gte: startDate,
            lte: endDate,
          },
        },
      })

      expect(params).toContain('2024-01-01T00:00:00.000Z')
      expect(params).toContain('2024-12-31T23:59:59.000Z')
    })

    it('should handle dates in complex queries', () => {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

      const { sql, params } = toSQL('User', 'count', {
        where: {
          createdAt: { gte: sevenDaysAgo },
          isDeleted: false,
        },
      })

      // Should be ISO string, not Date object
      const dateParam = params.find(
        (p) => typeof p === 'string' && p.includes('T'),
      )
      expect(dateParam).toBeTruthy()
      expect(dateParam).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)
    })

    it('should not break on valid dates', () => {
      const validDate = new Date('2024-01-15T10:30:00Z')

      expect(() => {
        toSQL('User', 'findMany', {
          where: { createdAt: { gte: validDate } },
        })
      }).not.toThrow()
    })
  })

  describe('Edge Cases', () => {
    it('should handle null date values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { lastLoginAt: null },
      })

      expect(sql).toContain('IS NULL')
      expect(params).toEqual([])
    })

    it('should handle date comparison operators', () => {
      const testDate = new Date('2024-01-15T10:30:00Z')

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdAt: {
            gt: testDate,
            lt: testDate,
          },
        },
      })

      const isoString = '2024-01-15T10:30:00.000Z'
      expect(params.filter((p) => p === isoString).length).toBe(2)
    })

    it('should handle dates in OR conditions', () => {
      const date1 = new Date('2024-01-01T00:00:00Z')
      const date2 = new Date('2024-12-31T23:59:59Z')

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          OR: [{ createdAt: { gte: date1 } }, { updatedAt: { gte: date2 } }],
        },
      })

      expect(params).toContain('2024-01-01T00:00:00.000Z')
      expect(params).toContain('2024-12-31T23:59:59.000Z')
    })

    it('should handle dates in nested queries', () => {
      const testDate = new Date('2024-01-15T10:30:00Z')

      const { sql, params } = toSQL('Task', 'findMany', {
        where: {
          createdAt: { gte: testDate },
          assignee: {
            createdAt: { gte: testDate },
          },
        },
      })

      const isoString = '2024-01-15T10:30:00.000Z'
      expect(params.filter((p) => p === isoString).length).toBeGreaterThan(0)
    })
  })

  describe('SQL Injection Prevention with Dates', () => {
    it('should not allow SQL injection via date string manipulation', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdAt: { gte: "2024-01-01'; DROP TABLE users; --" as any },
        },
      })

      // Should be parameterized, not injected
      expect(sql).toMatch(/\$\d+/)
      expect(params).toContain("2024-01-01'; DROP TABLE users; --")
    })

    it('should handle malicious date objects safely', () => {
      const maliciousDate = new Date('invalid')

      // Invalid dates should be caught during normalization
      const { params } = toSQL('User', 'findMany', {
        where: { createdAt: { gte: maliciousDate } },
      })

      // Should produce "Invalid Date" string or throw during toISOString
      const dateParam = params[0]
      expect(typeof dateParam).toBe('string')
    })
  })

  describe('Count Query with Dates (Reproducing Original Bug)', () => {
    it('should handle count queries with date filters', () => {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

      const { sql, params } = toSQL('User', 'count', {
        where: {
          isDeleted: false,
          createdAt: { gte: sevenDaysAgo },
        },
      })

      // Should have normalized date param
      const hasISOString = params.some(
        (p) => typeof p === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(p),
      )
      expect(hasISOString).toBe(true)
    })

    it('should handle multiple count queries with date filters', () => {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

      // This mimics the original failing code pattern
      const queries = [
        toSQL('Task', 'count', {
          where: { createdAt: { gte: sevenDaysAgo } },
        }),
        toSQL('Task', 'count', {
          where: { updatedAt: { gte: sevenDaysAgo } },
        }),
        toSQL('User', 'count', {
          where: { createdAt: { gte: sevenDaysAgo } },
        }),
        toSQL('Project', 'count', {
          where: { createdAt: { gte: sevenDaysAgo } },
        }),
      ]

      // All queries should have valid ISO string params
      for (const { params } of queries) {
        const hasValidDate = params.some(
          (p) =>
            typeof p === 'string' &&
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(p),
        )
        expect(hasValidDate).toBe(true)
      }
    })

    it('should handle aggregates with date filters', () => {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

      const { sql, params } = toSQL('Task', 'aggregate', {
        where: { createdAt: { gte: sevenDaysAgo } },
        _count: { _all: true },
      })

      const hasValidDate = params.some(
        (p) =>
          typeof p === 'string' &&
          /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(p),
      )
      expect(hasValidDate).toBe(true)
    })

    it('should handle groupBy with date filters', () => {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

      const { sql, params } = toSQL('Task', 'groupBy', {
        by: ['status'],
        where: { createdAt: { gte: sevenDaysAgo } },
        _count: { _all: true },
      })

      const hasValidDate = params.some(
        (p) =>
          typeof p === 'string' &&
          /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(p),
      )
      expect(hasValidDate).toBe(true)
    })
  })

  describe('Date Array Parameters', () => {
    it('should normalize dates in array values', () => {
      const dates = [
        new Date('2024-01-01T00:00:00Z'),
        new Date('2024-02-01T00:00:00Z'),
        new Date('2024-03-01T00:00:00Z'),
      ]

      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdAt: { in: dates },
        },
      })

      // For Postgres, array param is passed as-is (already normalized)
      // For SQLite, it's JSON stringified
      const dateArray = params[0]

      if (Array.isArray(dateArray)) {
        // Postgres dialect - array of ISO strings
        expect(dateArray).toEqual([
          '2024-01-01T00:00:00.000Z',
          '2024-02-01T00:00:00.000Z',
          '2024-03-01T00:00:00.000Z',
        ])
      } else if (typeof dateArray === 'string') {
        // SQLite dialect - JSON string
        const parsed = JSON.parse(dateArray)
        expect(parsed).toEqual([
          '2024-01-01T00:00:00.000Z',
          '2024-02-01T00:00:00.000Z',
          '2024-03-01T00:00:00.000Z',
        ])
      } else {
        throw new Error('Unexpected param type')
      }
    })

    it('should handle empty date arrays', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdAt: { in: [] },
        },
      })

      // Empty IN should produce 0=1 (always false)
      expect(sql).toContain('0=1')
    })
  })
})
