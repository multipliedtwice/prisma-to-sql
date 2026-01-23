// tests/sql-injection/unicode-edge-cases.test.ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/client'
import { createToSQL, convertDMMFToModels } from '../../src'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

describe('SQL Injection - Unicode Edge Cases', () => {
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

  describe('Zero-Width Characters', () => {
    it('should parameterize zero-width space in values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u200B@example.com' }, // Zero-width space
      })

      expect(params).toContain('test\u200B@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize zero-width non-joiner in values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u200C@example.com' }, // Zero-width non-joiner
      })

      expect(params).toContain('test\u200C@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize zero-width joiner in values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u200D@example.com' }, // Zero-width joiner
      })

      expect(params).toContain('test\u200D@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize word joiner in values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u2060@example.com' }, // Word joiner
      })

      expect(params).toContain('test\u2060@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize zero-width no-break space in values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\uFEFF@example.com' }, // Zero-width no-break space
      })

      expect(params).toContain('test\uFEFF@example.com')
      expect(sql).toContain('$1')
    })
  })

  describe('Combining Characters', () => {
    it('should parameterize combining diacritical marks', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: 'test\u0301\u0302\u0303' }, // Combining marks
      })

      expect(params).toContain('test\u0301\u0302\u0303')
      expect(sql).toContain('$1')
    })

    it('should parameterize combining characters with SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DROP\u0301 TABLE\u0302 users; --" },
      })

      expect(params).toContain("'; DROP\u0301 TABLE\u0302 users; --")
      expect(sql).not.toContain('DROP')
    })
  })

  describe('Right-to-Left Override', () => {
    it('should parameterize right-to-left override', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u202E@example.com' }, // Right-to-left override
      })

      expect(params).toContain('test\u202E@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize left-to-right override', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u202D@example.com' }, // Left-to-right override
      })

      expect(params).toContain('test\u202D@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize right-to-left override with SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; \u202EELBAT PORD ;'--" },
      })

      expect(params).toContain("'; \u202EELBAT PORD ;'--")
      expect(sql).not.toContain('DROP')
    })
  })

  describe('Homoglyphs', () => {
    it('should parameterize Cyrillic homoglyphs', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'аdmіn@example.com' }, // Cyrillic а, і
      })

      expect(params).toContain('аdmіn@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize Greek homoglyphs', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'αdmin@example.com' }, // Greek α
      })

      expect(params).toContain('αdmin@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize homoglyph SQL injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DRΟP TАBLE users; --" }, // Greek Ο, Cyrillic А
      })

      expect(params).toContain("'; DRΟP TАBLE users; --")
      expect(sql).not.toContain('DROP')
    })
  })

  describe('Normalization Attacks', () => {
    it('should parameterize NFC normalized strings', () => {
      const nfc = '\u00E9' // é (single character)
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: `test${nfc}` },
      })

      expect(params).toContain(`test${nfc}`)
      expect(sql).toContain('$1')
    })

    it('should parameterize NFD normalized strings', () => {
      const nfd = 'e\u0301' // é (e + combining acute)
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: `test${nfd}` },
      })

      expect(params).toContain(`test${nfd}`)
      expect(sql).toContain('$1')
    })

    it('should parameterize both NFC and NFD in complex injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          AND: [
            { name: "'; DROP\u00E9--" }, // NFC
            { email: "'; DROP\u0065\u0301--" }, // NFD
          ],
        },
      })

      expect(params).toContain("'; DROP\u00E9--")
      expect(params).toContain("'; DROP\u0065\u0301--")
      expect(sql).not.toContain('DROP')
    })
  })

  describe('Emoji and Special Characters', () => {
    it('should parameterize emoji in values', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: 'test 👨‍💻 user' },
      })

      expect(params).toContain('test 👨‍💻 user')
      expect(sql).toContain('$1')
    })

    it('should parameterize emoji with SQL injection', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: "'; DROP TABLE users 💣; --" },
      })

      expect(params).toContain("'; DROP TABLE users 💣; --")
      expect(sql).not.toContain('DROP')
    })

    it('should parameterize regional indicator symbols', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: '🇺🇸 user' }, // US flag
      })

      expect(params).toContain('🇺🇸 user')
      expect(sql).toContain('$1')
    })
  })

  describe('Variation Selectors', () => {
    it('should parameterize emoji variation selectors', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: 'test\u2764\uFE0F' }, // ❤️ with variation selector
      })

      expect(params).toContain('test\u2764\uFE0F')
      expect(sql).toContain('$1')
    })

    it('should parameterize text variation selectors', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: 'test\u2764\uFE0E' }, // ❤︎ text style
      })

      expect(params).toContain('test\u2764\uFE0E')
      expect(sql).toContain('$1')
    })
  })

  describe('Surrogates', () => {
    it('should parameterize high surrogates', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: 'test\uD800\uDC00' }, // Valid surrogate pair
      })

      expect(params).toContain('test\uD800\uDC00')
      expect(sql).toContain('$1')
    })

    it('should parameterize mathematical alphanumeric symbols', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { name: '𝕥𝕖𝕤𝕥' }, // Mathematical double-struck
      })

      expect(params).toContain('𝕥𝕖𝕤𝕥')
      expect(sql).toContain('$1')
    })
  })

  describe('Whitespace Variations', () => {
    it('should parameterize non-breaking space', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u00A0@example.com' }, // Non-breaking space
      })

      expect(params).toContain('test\u00A0@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize em space', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u2003@example.com' }, // Em space
      })

      expect(params).toContain('test\u2003@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize thin space', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u2009@example.com' }, // Thin space
      })

      expect(params).toContain('test\u2009@example.com')
      expect(sql).toContain('$1')
    })

    it('should parameterize hair space', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: { email: 'test\u200A@example.com' }, // Hair space
      })

      expect(params).toContain('test\u200A@example.com')
      expect(sql).toContain('$1')
    })
  })

  describe('Multiple Unicode Techniques Combined', () => {
    it('should parameterize complex unicode injection attempts', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "'; \u200BDROP\u0301 TABLE\u202E users\uFEFF; --",
        },
      })

      expect(params).toContain(
        "'; \u200BDROP\u0301 TABLE\u202E users\uFEFF; --",
      )
      expect(sql).not.toContain('DROP')
    })

    it('should parameterize homoglyph with zero-width characters', () => {
      const { sql, params } = toSQL('User', 'findMany', {
        where: {
          email: "'; \u200BDR\u039FP\u200C TАBLE\u200D users; --",
        },
      })

      expect(params).toContain("'; \u200BDR\u039FP\u200C TАBLE\u200D users; --")
      expect(sql).not.toContain('DROP')
    })
  })
})
