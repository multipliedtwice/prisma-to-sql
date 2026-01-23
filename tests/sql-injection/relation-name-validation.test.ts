// tests/sql-injection/relation-name-validation.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Relation Name Validation', () => {
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

  describe('Include Relation Name Validation', () => {
    it('should safely ignore malicious include relation names', () => {
      // Include silently ignores non-existent relations (safe behavior)
      const { sql, params } = toSQL('User', 'findMany', {
        include: { ["'; DROP TABLE--" as any]: true },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should safely ignore SQL injection in include relation names', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        include: {
          ["assignedTasks'; DELETE FROM users--" as any]: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('DELETE FROM')
    })

    it('should safely ignore non-existent relation names', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: { nonExistentRelation: true },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('nonExistentRelation')
    })

    it('should safely ignore control characters in relation names', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          ['assignedTasks\x00' as any]: true,
        },
      })

      expect(sql).toBeDefined()
    })

    it('should safely ignore UNION attempts in relation names', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          ["' UNION SELECT * FROM users--" as any]: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('UNION')
    })

    it('should allow only valid relation names', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          assignedTasks: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).toContain('SELECT')
    })

    it('should safely ignore relation names with semicolons', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          ['assignedTasks; DROP TABLE users' as any]: true,
        },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should safely ignore relation names with SQL keywords', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          ['SELECT FROM WHERE' as any]: true,
        },
      })

      expect(sql).toBeDefined()
    })
  })

  describe('Select Relation Name Validation', () => {
    it('should reject malicious select relation names', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            id: true,
            ["'; DROP TABLE--" as any]: true,
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject nested select with malicious relation names', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          select: {
            id: true,
            ["assignedTasks'; DELETE FROM users--" as any]: {
              select: {
                id: true,
              },
            },
          },
        })
      }).toThrow(/unknown field/i)
    })

    it('should allow only valid relation names in select', () => {
      const { sql } = toSQL('User', 'findMany', {
        select: {
          id: true,
          assignedTasks: {
            select: {
              id: true,
            },
          },
        },
      })

      expect(sql).toBeDefined()
      expect(sql).toContain('SELECT')
    })
  })

  describe('Where Relation Name Validation', () => {
    it('should reject malicious where relation names', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ["'; DROP TABLE users--" as any]: {
              some: {
                title: 'test',
              },
            },
          },
        })
      }).toThrow(/does not exist|unknown/i)
    })

    it('should reject UNION attempts in where relation names', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            ["' UNION SELECT * FROM users--" as any]: {
              some: {
                title: 'test',
              },
            },
          },
        })
      }).toThrow(/does not exist|unknown/i)
    })

    it('should allow only valid relation names in where', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          assignedTasks: {
            some: {
              title: 'test',
            },
          },
        },
      })

      expect(sql).toBeDefined()
      expect(params).toContain('test')
    })
  })

  describe('Nested Relation Name Validation', () => {
    it('should safely ignore malicious names in deeply nested includes', () => {
      // Include ignores non-existent relations at any depth
      const { sql } = toSQL('Organization', 'findMany', {
        include: {
          projects: {
            include: {
              ["'; DROP TABLE--" as any]: true,
            },
          },
        },
      })

      expect(sql).toBeDefined()
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should validate all levels of nested relation names in where', () => {
      expect(() => {
        toSQL('Organization', 'findMany', {
          where: {
            projects: {
              some: {
                ["'; DROP TABLE--" as any]: {
                  some: {
                    title: 'test',
                  },
                },
              },
            },
          },
        })
      }).toThrow(/does not exist|unknown/i)
    })
  })

  describe('Relation Filter Name Validation', () => {
    it('should reject malicious names in relation filter keys', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            assignedTasks: {
              ["'; DROP TABLE--" as any]: {
                title: 'test',
              },
            },
          },
        })
      }).toThrow()
    })

    it('should validate is/isNot relation filter names in where', () => {
      expect(() => {
        toSQL('Task', 'findMany', {
          where: {
            ["'; DROP TABLE--" as any]: {
              is: {
                email: 'test@example.com',
              },
            },
          },
        })
      }).toThrow(/does not exist|unknown/i)
    })
  })

  describe('Include Safety Verification', () => {
    it('should only process valid relations from include', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: {
          assignedTasks: true,
          ["'; DROP TABLE--" as any]: true,
          nonExistent: true,
          createdTasks: true,
        },
      })

      // Should only include the two valid relations
      expect(sql).toBeDefined()
      expect(sql).not.toContain('DROP TABLE')
      expect(sql).not.toContain('nonExistent')
    })

    it('should generate safe SQL even with all malicious include keys', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { id: { gt: 1 } },
        include: {
          ["'; DROP TABLE--" as any]: true,
          ["' UNION SELECT--" as any]: true,
          ["'; DELETE FROM--" as any]: true,
        },
      })

      expect(sql).toBeDefined()
      expect(params).toContain(1)
      expect(sql).not.toContain('DROP TABLE')
      expect(sql).not.toContain('UNION SELECT')
      expect(sql).not.toContain('DELETE FROM')
    })
  })
})
