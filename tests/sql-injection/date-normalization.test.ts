import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { readFileSync, writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { getDatamodel } from '../helpers/datamodel'
import { queryCache } from '../../src/query-cache'

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

  beforeEach(() => {
    queryCache.clear()
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

      expect(sql).toMatch(/\$\d+/)
      expect(params).toContain("2024-01-01'; DROP TABLE users; --")
    })

    it('should throw on invalid date objects', () => {
      const maliciousDate = new Date('invalid')

      expect(() => {
        toSQL('User', 'findMany', {
          where: { createdAt: { gte: maliciousDate } },
        })
      }).toThrow(/Invalid time value/)
    })

    it('should throw on NaN date objects', () => {
      const nanDate = new Date(NaN)

      expect(() => {
        toSQL('User', 'findMany', {
          where: { createdAt: { gte: nanDate } },
        })
      }).toThrow(/Invalid time value/)
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

      const hasISOString = params.some(
        (p) => typeof p === 'string' && /^\d{4}-\d{2}-\d{2}T/.test(p),
      )
      expect(hasISOString).toBe(true)
    })

    it('should handle multiple count queries with date filters', () => {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

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

      const dateArray = params[0]

      if (Array.isArray(dateArray)) {
        expect(dateArray).toEqual([
          '2024-01-01T00:00:00.000Z',
          '2024-02-01T00:00:00.000Z',
          '2024-03-01T00:00:00.000Z',
        ])
      } else if (typeof dateArray === 'string') {
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

      expect(sql).toContain('0=1')
    })
  })
})
