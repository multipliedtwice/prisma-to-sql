// tests/sql-injection/nested-queries.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Nested Query Safety', () => {
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

  describe('Deep Relation Injection', () => {
    it('should parameterize deeply nested relation filters', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        where: {
          projects: {
            some: {
              tasks: {
                some: {
                  title: "'; DROP TABLE tasks; --",
                },
              },
            },
          },
        },
      })

      expect(params).toContain("'; DROP TABLE tasks; --")
    })

    it('should handle injection in multiple nested levels', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        where: {
          projects: {
            some: {
              tasks: {
                some: {
                  title: "'; DROP--",
                  assignee: {
                    is: {
                      email: "admin' OR '1'='1",
                    },
                  },
                },
              },
            },
          },
        },
      })

      expect(params).toContain("'; DROP--")
      expect(params).toContain("admin' OR '1'='1")
    })

    it('should handle injection in relation filters with every', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        where: {
          members: {
            every: {
              user: {
                is: {
                  email: "'; TRUNCATE TABLE--",
                },
              },
            },
          },
        },
      })

      expect(params).toContain("'; TRUNCATE TABLE--")
    })

    it('should handle injection in relation filters with some', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdTasks: {
            some: {
              title: "'; DELETE FROM--",
            },
          },
        },
      })

      expect(params).toContain("'; DELETE FROM--")
    })
  })

  describe('Nested Logical Operators', () => {
    it('should parameterize deeply nested AND/OR', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          AND: [
            {
              OR: [
                {
                  AND: [{ email: "'; DROP--" }, { name: "' UNION--" }],
                },
              ],
            },
          ],
        },
      })

      expect(params).toContain("'; DROP--")
      expect(params).toContain("' UNION--")
    })

    it('should handle NOT with nested conditions', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          NOT: {
            AND: [{ email: "'; EXEC--" }, { role: "' OR 1=1--" }],
          },
        },
      })

      expect(params).toContain("'; EXEC--")
      expect(params).toContain("' OR 1=1--")
    })
  })

  describe('Include with Nested Where', () => {
    it('should parameterize include where clauses', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          createdTasks: {
            some: {
              title: "'; DROP--",
            },
          },
        },
        include: {
          createdTasks: {
            where: {
              title: "'; TRUNCATE--",
            },
          },
        },
      })

      expect(params).toContain("'; DROP--")
      expect(params).toContain("'; TRUNCATE--")
    })

    it('should handle nested include with orderBy', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        include: {
          assignedTasks: {
            where: {
              title: "'; DELETE--",
            },
            orderBy: { createdAt: 'desc' },
          },
        },
      })

      expect(params).toContain("'; DELETE--")
    })
  })

  describe('Complex Nested Scenarios', () => {
    it('should handle injection across multiple relation types', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          memberships: {
            some: {
              organization: {
                is: {
                  name: "'; DROP--",
                },
              },
            },
          },
          assignedTasks: {
            some: {
              title: "' UNION--",
            },
          },
        },
      })

      expect(params).toContain("'; DROP--")
      expect(params).toContain("' UNION--")
    })

    it('should parameterize deeply nested select queries', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        where: {
          projects: {
            some: {
              tasks: {
                some: {
                  comments: {
                    some: {
                      content: "'; EXEC xp_cmdshell--",
                    },
                  },
                },
              },
            },
          },
        },
      })

      expect(params).toContain("'; EXEC xp_cmdshell--")
    })
  })
})
