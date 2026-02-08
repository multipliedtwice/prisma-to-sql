import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Aggregates', () => {
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

  describe('Aggregate Field Validation', () => {
    it('should reject malicious field names in _count', () => {
      expect(() => {
        toSQL('User', 'aggregate', {
          _count: {
            ["'; DROP TABLE--" as any]: true,
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject malicious field names in _sum', () => {
      expect(() => {
        toSQL('Task', 'aggregate', {
          _sum: {
            ["'; TRUNCATE--" as any]: true,
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject malicious field names in _avg', () => {
      expect(() => {
        toSQL('Task', 'aggregate', {
          _avg: {
            ["'; DELETE--" as any]: true,
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject non-numeric fields in _sum', () => {
      expect(() => {
        toSQL('User', 'aggregate', {
          _sum: {
            email: true,
          },
        })
      }).toThrow(/numeric field/i)
    })
  })

  it('should parameterize count where', () => {
    const { sql, params } = toSQL('User', 'count', {
      where: { email: 'test@example.com' },
    })

    expect(params).toContain('test@example.com')
    expect(sql).toContain('COUNT(*)')
  })

  it('should parameterize aggregate where', () => {
    const { sql, params } = toSQL('Task', 'aggregate', {
      where: { title: 'test' },
      _count: { _all: true },
    })

    expect(params).toContain('test')
    expect(sql).toContain('COUNT')
  })

  it('should parameterize groupBy where', () => {
    const { sql, params } = toSQL('Task', 'groupBy', {
      by: ['projectId'],
      where: { title: 'test' },
    })

    expect(params).toContain('test')
    expect(sql).toContain('GROUP BY')
  })

  it('should parameterize groupBy having', () => {
    const { sql, params } = toSQL('Task', 'groupBy', {
      by: ['projectId'],
      having: {
        id: { _avg: { gt: 10 } },
      },
    })

    expect(params).toContain(10)
    expect(sql).toContain('HAVING')
  })

  describe('Having Clause Injection', () => {
    it('should parameterize having with injection attempts', () => {
      const { sql, params } = toSQL('Task', 'groupBy', {
        by: ['projectId'],
        having: {
          id: {
            _avg: {
              gt: '10; DROP TABLE tasks; --' as any,
            },
          },
        },
      })

      expect(params).toContain('10; DROP TABLE tasks; --')
    })

    it('should handle complex having conditions', () => {
      const { sql, params } = toSQL('Task', 'groupBy', {
        by: ['projectId'],
        having: {
          AND: [
            { id: { _count: { gt: '5; DROP--' as any } } },
            { position: { _avg: { lt: '100; TRUNCATE--' as any } } },
          ],
        },
      })

      expect(params).toContain('5; DROP--')
      expect(params).toContain('100; TRUNCATE--')
    })
  })

  describe('GroupBy Field Validation', () => {
    it('should reject malicious fields in groupBy', () => {
      expect(() => {
        toSQL('Task', 'groupBy', {
          by: ["'; DROP--" as any],
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject relation fields in groupBy', () => {
      expect(() => {
        toSQL('User', 'groupBy', {
          by: ['memberships' as any],
        })
      }).toThrow(/does not support relation/i)
    })
  })
})
