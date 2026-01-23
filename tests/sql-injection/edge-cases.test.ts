// tests/sql-injection/edge-cases.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Edge Cases', () => {
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

  describe('Comment Injection', () => {
    it('should handle SQL comments', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com' -- comment",
        },
      })

      expect(params).toContain("test@example.com' -- comment")
      expect(sql).toContain('$')
    })

    it('should handle multi-line SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com';\nDROP TABLE users;\n--",
        },
      })

      expect(params).toContain("test@example.com';\nDROP TABLE users;\n--")
      expect(sql).not.toContain('DROP TABLE')
    })
  })

  describe('Encoding Attacks', () => {
    it('should handle unicode SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: 'test@example.com\u0027 OR \u00271\u0027=\u00271',
        },
      })

      expect(params).toContain(
        'test@example.com\u0027 OR \u00271\u0027=\u00271',
      )
      expect(sql).toContain('$')
    })

    it('should handle hex-encoded SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com' OR 0x31=0x31--",
        },
      })

      expect(params).toContain("test@example.com' OR 0x31=0x31--")
      expect(sql).not.toContain('0x31')
    })

    it('should handle URL encoded injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: '%27%3B%20DROP%20TABLE%20users%3B--',
        },
      })

      expect(params).toContain('%27%3B%20DROP%20TABLE%20users%3B--')
      expect(sql).not.toContain('DROP')
    })

    it('should handle base64-like strings safely', () => {
      const base64Evil = Buffer.from("'; DROP TABLE users--").toString('base64')
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: base64Evil },
      })

      expect(params).toContain(base64Evil)
      expect(sql).not.toContain('DROP TABLE')
    })
  })

  describe('Blind SQL Injection Attempts', () => {
    it('should handle stacked queries', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email:
            "test@example.com'; DROP TABLE users; SELECT * FROM users WHERE '1'='1",
        },
      })

      expect(params).toContain(
        "test@example.com'; DROP TABLE users; SELECT * FROM users WHERE '1'='1",
      )
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should handle time-based blind SQL injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com'; WAITFOR DELAY '00:00:05'--",
        },
      })

      expect(params).toContain("test@example.com'; WAITFOR DELAY '00:00:05'--")
      expect(sql).not.toContain('WAITFOR')
    })

    it('should handle boolean-based blind SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com' AND (SELECT COUNT(*) FROM users) > 0--",
        },
      })

      expect(params).toContain(
        "test@example.com' AND (SELECT COUNT(*) FROM users) > 0--",
      )
      expect(sql).not.toContain('SELECT COUNT')
    })

    it('should handle error-based SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test@example.com' AND 1=CONVERT(int, (SELECT @@version))--",
        },
      })

      expect(params).toContain(
        "test@example.com' AND 1=CONVERT(int, (SELECT @@version))--",
      )
      expect(sql).not.toContain('@@version')
    })

    it('should handle UNION-based SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email:
            "test@example.com' UNION ALL SELECT null, password, null FROM users--",
        },
      })

      expect(params).toContain(
        "test@example.com' UNION ALL SELECT null, password, null FROM users--",
      )
      expect(sql).not.toContain('UNION ALL SELECT')
    })

    it('should handle second-order SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "admin'--",
          name: "' OR '1'='1",
        },
      })

      expect(params).toContain("admin'--")
      expect(params).toContain("' OR '1'='1")
      expect(sql).not.toMatch(/OR\s+'1'\s*=\s*'1'/)
    })
  })

  describe('JSON Field Injection', () => {
    it('should handle JSON field with SQL injection in equals', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        where: {
          settings: {
            equals: "'; DROP TABLE organizations--",
          },
        },
      })

      expect(params).toContain("'; DROP TABLE organizations--")
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should reject malicious JSON path segments', () => {
      expect(() => {
        toSQL('Organization', 'findMany', {
          where: {
            settings: {
              path: {
                path: ['../../etc/passwd', 'malicious'],
                equals: 'value',
              },
            },
          },
        })
      }).toThrow()
    })

    it('should reject SQL in JSON path', () => {
      expect(() => {
        toSQL('Organization', 'findMany', {
          where: {
            settings: {
              path: {
                path: ["'; DROP TABLE--"],
                equals: 'value',
              },
            },
          },
        })
      }).toThrow(/invalid.*path/i)
    })

    it('should reject non-alphanumeric JSON path segments', () => {
      expect(() => {
        toSQL('Organization', 'findMany', {
          where: {
            settings: {
              path: {
                path: ['valid', 'path.with.dots'],
                equals: 'value',
              },
            },
          },
        })
      }).toThrow(/invalid.*path/i)
    })

    it('should parameterize JSON string_contains', () => {
      const { sql, params } = toSQL('Organization', 'findMany', {
        where: {
          settings: {
            string_contains: "'; DROP--",
          },
        },
      })

      expect(params.some((p) => String(p).includes("'; DROP--"))).toBe(true)
      expect(sql).not.toContain('DROP')
    })
  })

  describe('Type Confusion', () => {
    it('should handle unexpected object in scalar field', () => {
      // Unknown operators should be ignored or throw
      expect(() => {
        toSQL('User', 'findMany', {
          where: {
            email: { maliciousOperator: 'object' } as any,
          },
        })
      }).toThrow()
    })

    it('should handle array where string expected', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: ["'; DROP--", 'malicious'] as any,
        },
      })

      // Should treat as invalid but not execute SQL
      expect(sql).not.toContain('DROP')
    })

    it('should handle number where string expected', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: 12345 as any,
        },
      })

      expect(params).toContain(12345)
      expect(sql).toContain('$')
    })

    it('should reject function objects', () => {
      const { sql } = toSQL('User', 'findMany', {
        where: {
          email: (() => "'; DROP TABLE--") as any,
        },
      })

      expect(sql).not.toContain('DROP TABLE')
    })
  })

  describe('Nested Object Injection', () => {
    it('should handle nested object SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          AND: [
            {
              OR: [
                { email: "'; DROP TABLE users; --" },
                { name: "admin' UNION SELECT * FROM users--" },
              ],
            },
            {
              NOT: {
                status: '; DELETE FROM users--',
              },
            },
          ],
        },
      })

      expect(params).toContain("'; DROP TABLE users; --")
      expect(params).toContain("admin' UNION SELECT * FROM users--")
      expect(params).toContain('; DELETE FROM users--')
      expect(sql).not.toContain('DROP TABLE')
      expect(sql).not.toContain('UNION SELECT')
      expect(sql).not.toContain('DELETE FROM')
    })

    it('should handle deeply nested malicious objects', () => {
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
      expect(sql).not.toContain('DROP')
      expect(sql).not.toContain('UNION')
    })
  })

  describe('Special Characters', () => {
    it('should handle null bytes', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: 'test\x00@example.com',
        },
      })

      expect(params).toContain('test\x00@example.com')
    })

    it('should handle backslashes', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test\\'; DROP TABLE--",
        },
      })

      expect(params).toContain("test\\'; DROP TABLE--")
      expect(sql).not.toContain('DROP TABLE')
    })

    it('should handle control characters', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "test\r\n@example.com'; DROP--",
        },
      })

      expect(params).toContain("test\r\n@example.com'; DROP--")
      expect(sql).not.toContain('DROP')
    })

    it('should handle mixed quotes', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: `test"'; DROP TABLE users; --`,
        },
      })

      expect(params).toContain(`test"'; DROP TABLE users; --`)
      expect(sql).not.toContain('DROP TABLE')
    })
  })
})
