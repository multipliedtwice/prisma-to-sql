// tests/sql-injection/include-select.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Include/Select Field Safety', () => {
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

  describe('Include Field Validation', () => {
    it('should parameterize valid include fields', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DROP--" },
        include: {
          assignedTasks: true,
        },
      })

      expect(params).toContain("'; DROP--")
      expect(sql).toBeDefined()
    })

    it('should handle include with injection in nested where', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        include: {
          assignedTasks: {
            where: {
              title: "'; UNION SELECT--",
            },
          },
        },
      })

      expect(params).toContain("'; UNION SELECT--")
    })

    it('should parameterize nested include where clauses', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        include: {
          assignedTasks: {
            where: {
              title: "'; DROP TABLE tasks; --",
            },
          },
        },
      })

      expect(params).toContain("'; DROP TABLE tasks; --")
    })

    it('should handle deeply nested includes with injection', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        include: {
          projects: {
            include: {
              tasks: {
                where: {
                  description: "'; TRUNCATE--",
                },
              },
            },
          },
        },
      })

      expect(params).toContain("'; TRUNCATE--")
    })
  })

  describe('Select Field Validation', () => {
    it('should reject malicious field names in select', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            ["'; DROP TABLE users--" as any]: true,
          },
        })
      }).toThrow(/unknown field|does not exist/i)
    })

    it('should reject control characters in select fields', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            ['email\x00' as any]: true,
          },
        })
      }).toThrow()
    })

    it('should handle nested select with where injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        select: {
          id: true,
          assignedTasks: {
            where: {
              title: "'; DELETE--",
            },
          },
        },
      })

      expect(params).toContain("'; DELETE--")
    })

    it('should handle deeply nested select with injection', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        select: {
          id: true,
          projects: {
            select: {
              id: true,
              tasks: {
                where: {
                  description: "'; EXEC--",
                },
              },
            },
          },
        },
      })

      expect(params).toContain("'; EXEC--")
    })
  })

  describe('Combined Include/Select', () => {
    it('should handle select field validation', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            ["'; DROP--" as any]: true,
          },
        })
      }).toThrow()
    })

    it('should parameterize values in both contexts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DROP--" },
        include: {
          assignedTasks: {
            where: {
              title: "'; UNION--",
            },
          },
        },
      })

      expect(params).toContain("'; DROP--")
      expect(params).toContain("'; UNION--")
    })
  })

  describe('Relation Count Injection', () => {
    it('should reject malicious field names in _count', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            _count: {
              select: {
                ["'; DROP--" as any]: true,
              },
            },
          },
        })
      }).toThrow()
    })

    it('should handle valid _count with injection in where', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DROP--" },
        select: {
          _count: {
            select: {
              assignedTasks: true,
            },
          },
        },
      })

      expect(params).toContain("'; DROP--")
    })
  })
})
