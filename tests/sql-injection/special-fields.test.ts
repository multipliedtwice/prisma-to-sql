// tests/sql-injection/special-fields.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Special Fields', () => {
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

  describe('orderBy Protection', () => {
    it('should reject SQL injection in orderBy field names', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          orderBy: { ["'; DROP TABLE users--" as any]: 'asc' },
        })
      }).toThrow()
    })

    it('should reject malicious orderBy direction attempts', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          orderBy: { email: 'asc; DROP TABLE--' as any },
        })
      }).toThrow(/invalid.*direction/i)
    })

    it('should handle valid orderBy with injection in where', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DROP--" },
        orderBy: { id: 'desc' },
      })

      expect(params).toContain("'; DROP--")
      expect(sql).toContain('ORDER BY')
    })
  })

  describe('Pagination Protection', () => {
    it('should parameterize cursor values with injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        cursor: { id: 1 },
        where: { email: "test'; DROP--" },
        take: 10,
      })

      expect(params).toContain("test'; DROP--")
      expect(params).toContain(1)
      expect(params).toContain(10)
    })

    it('should parameterize skip value', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test@example.com' },
        skip: 10,
      })

      expect(params).toContain('test@example.com')
      expect(params).toContain(10)
      expect(sql).toContain('OFFSET')
    })

    it('should parameterize take value', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test@example.com' },
        take: 10,
      })

      expect(params).toContain('test@example.com')
      expect(params).toContain(10)
      expect(sql).toContain('LIMIT')
    })

    it('should reject negative skip', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          skip: -1,
        })
      }).toThrow(/skip.*>=.*0/i)
    })

    it('should reject extremely large skip', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          skip: Number.MAX_SAFE_INTEGER + 1,
        })
      }).toThrow(/skip/i)
    })

    it('should reject non-integer skip', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          skip: 10.5,
        })
      }).toThrow(/skip.*integer/i)
    })

    it('should reject string skip values', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          skip: "'; DROP TABLE--" as any,
        })
      }).toThrow(/skip.*integer/i)
    })

    it('should reject non-integer take', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          take: 10.5,
        })
      }).toThrow(/take.*integer/i)
    })

    it('should reject string take values', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          take: "'; DROP TABLE--" as any,
        })
      }).toThrow(/take.*integer/i)
    })

    it('should handle zero skip safely', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        skip: 0,
      })

      expect(sql).toBeDefined()
    })

    it('should handle zero take safely', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        take: 0,
      })

      expect(sql).toBeDefined()
      expect(params).toContain(0)
    })

    it('should handle negative take with orderBy', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        orderBy: { id: 'asc' },
        take: -10,
      })

      expect(params).toContain(10)
      expect(sql).toMatch(/ORDER BY/i)
    })
  })

  describe('distinct Protection', () => {
    it('should handle distinct with injection in where', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { role: "'; DROP TABLE users--" },
        distinct: ['email'],
      })

      expect(params).toContain("'; DROP TABLE users--")
      expect(sql).toMatch(/DISTINCT|PARTITION BY/i)
    })

    it('should reject non-existent fields in distinct', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          distinct: ["'; DROP TABLE--" as any],
        })
      }).toThrow(/unknown field/i)
    })

    it('should reject duplicate fields in distinct', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          distinct: ['email', 'email'],
        })
      }).toThrow(/duplicate/i)
    })

    it('should reject relation fields in distinct', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          distinct: ['memberships' as any],
        })
      }).toThrow(/does not support relation/i)
    })
  })

  describe('Cursor Protection', () => {
    it('should reject malicious cursor field names', () => {
      expect(() => {
        toSQL('User', 'findMany', {
          cursor: { ["'; DROP--" as any]: 1 },
          take: 10,
        })
      }).toThrow()
    })

    it('should parameterize cursor values with SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        cursor: { id: 1 },
        where: { email: "'; TRUNCATE TABLE--" },
        orderBy: { id: 'asc' },
        take: 10,
      })

      expect(params).toContain("'; TRUNCATE TABLE--")
    })

    it('should handle cursor with multiple fields', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        cursor: { id: 1, email: 'test@example.com' },
        orderBy: { id: 'asc' },
        take: 10,
      })

      expect(params).toContain(1)
      expect(params).toContain('test@example.com')
    })
  })

  describe('Combined Special Fields', () => {
    it('should handle multiple special fields with injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DROP--" },
        orderBy: { id: 'desc' },
        skip: 10,
        take: 20,
      })

      expect(params).toContain("'; DROP--")
      expect(sql).toContain('ORDER BY')
      expect(sql).toContain('LIMIT')
      expect(sql).toContain('OFFSET')
    })

    it('should handle cursor with orderBy and where', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { status: 'active' },
        cursor: { id: 100 },
        orderBy: { createdAt: 'desc' },
        take: 50,
      })

      expect(params).toContain('active')
      expect(params).toContain(100)
      expect(params).toContain(50)
      expect(sql).toContain('ORDER BY')
    })
  })
})
