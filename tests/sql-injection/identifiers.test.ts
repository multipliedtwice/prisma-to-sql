// tests/sql-injection/identifiers.test.ts
import { describe, it, expect } from 'vitest'
import {
  buildTableReference,
  quote,
  assertSafeAlias,
  assertSafeTableRef,
} from '../../src/builder/shared/sql-utils'

describe('SQL Injection - Identifier Safety', () => {
  describe('buildTableReference', () => {
    it('should escape quotes in table names', () => {
      const result = buildTableReference(
        'public',
        'users"; DROP TABLE users--',
        'postgres',
      )

      expect(result).toBe('"public"."users""; DROP TABLE users--"')
      expect(result).toContain('""')
    })

    it('should reject control characters in table names', () => {
      expect(() => {
        buildTableReference('public', 'users\x00', 'postgres')
      }).toThrow(/invalid characters/)
    })

    it('should reject control characters in schema names', () => {
      expect(() => {
        buildTableReference('public\n', 'users', 'postgres')
      }).toThrow(/invalid characters/)
    })

    it('should reject newlines in table names', () => {
      expect(() => {
        buildTableReference('public', 'users\n\r', 'postgres')
      }).toThrow(/invalid characters/)
    })

    it('should handle table names that are SQL keywords', () => {
      const result = buildTableReference('public', 'select', 'postgres')
      expect(result).toBe('"public"."select"')
    })

    it('should handle schema names that are SQL keywords', () => {
      const result = buildTableReference('order', 'users', 'postgres')
      expect(result).toBe('"order"."users"')
    })

    it('should handle sqlite without schema', () => {
      const result = buildTableReference('', 'users', 'sqlite')
      expect(result).toBe('"users"')
    })

    it('should reject empty table names', () => {
      expect(() => {
        buildTableReference('public', '', 'postgres')
      }).toThrow(/required.*cannot be empty/)
    })

    it('should reject empty schema names', () => {
      expect(() => {
        buildTableReference('', 'users', 'postgres')
      }).toThrow(/required.*cannot be empty/)
    })
  })

  describe('assertSafeTableRef', () => {
    it('should accept valid table references', () => {
      expect(() => assertSafeTableRef('users')).not.toThrow()
      expect(() => assertSafeTableRef('"users"')).not.toThrow()
      expect(() => assertSafeTableRef('public.users')).not.toThrow()
      expect(() => assertSafeTableRef('"public"."users"')).not.toThrow()
    })

    it('should reject table refs with semicolons', () => {
      expect(() => {
        assertSafeTableRef('users; DROP TABLE--')
      }).toThrow(/must not contain/)
    })

    it('should reject table refs with parentheses', () => {
      expect(() => {
        assertSafeTableRef('users()')
      }).toThrow(/must not contain parentheses/)
    })

    it('should reject table refs with control characters', () => {
      expect(() => {
        assertSafeTableRef('users\x00')
      }).toThrow(/invalid characters/)
    })

    it('should reject table refs with whitespace', () => {
      expect(() => {
        assertSafeTableRef('users table')
      }).toThrow(/must not contain whitespace/)
    })

    it('should reject table refs with leading/trailing whitespace', () => {
      expect(() => {
        assertSafeTableRef(' users ')
      }).toThrow(/leading.*trailing.*whitespace/)
    })

    it('should reject more than 2 parts', () => {
      expect(() => {
        assertSafeTableRef('schema.public.users')
      }).toThrow(/max 2 parts/)
    })

    it('should reject empty identifier parts', () => {
      expect(() => {
        assertSafeTableRef('.users')
      }).toThrow(/empty identifier/)
    })

    it('should reject ending with dot', () => {
      expect(() => {
        assertSafeTableRef('users.')
      }).toThrow(/cannot end with/)
    })
  })

  describe('assertSafeAlias', () => {
    it('should accept valid aliases', () => {
      expect(() => assertSafeAlias('user_alias')).not.toThrow()
      expect(() => assertSafeAlias('_t1')).not.toThrow()
      expect(() => assertSafeAlias('alias123')).not.toThrow()
    })

    it('should reject aliases with semicolons', () => {
      expect(() => {
        assertSafeAlias('alias; DROP--')
      }).toThrow(/unsafe characters/)
    })

    it('should reject aliases with control characters', () => {
      expect(() => {
        assertSafeAlias('alias\x00')
      }).toThrow(/unsafe characters/)
    })

    it('should reject aliases with special characters', () => {
      expect(() => {
        assertSafeAlias('ali@s')
      }).toThrow(/simple identifier/)
    })

    it('should reject empty aliases', () => {
      expect(() => {
        assertSafeAlias('')
      }).toThrow(/required.*cannot be empty/)
    })

    it('should reject aliases with spaces', () => {
      expect(() => {
        assertSafeAlias('user alias')
      }).toThrow(/simple identifier/)
    })
  })

  describe('quote', () => {
    it('should quote identifiers with special characters', () => {
      expect(quote('user-name')).toBe('"user-name"')
      expect(quote('user.name')).toBe('"user.name"')
    })

    it('should quote SQL keywords', () => {
      expect(quote('select')).toBe('"select"')
      expect(quote('from')).toBe('"from"')
      expect(quote('where')).toBe('"where"')
    })

    it('should not quote simple identifiers', () => {
      expect(quote('username')).toBe('username')
      expect(quote('user_name')).toBe('user_name')
      expect(quote('_user')).toBe('_user')
    })

    it('should escape quotes in identifiers', () => {
      expect(quote('user"name')).toBe('"user""name"')
    })

    it('should reject control characters', () => {
      expect(() => {
        quote('user\x00name')
      }).toThrow(/invalid characters/)
    })

    it('should reject empty identifiers', () => {
      expect(() => {
        quote('')
      }).toThrow(/required.*cannot be empty/)
    })
  })
})
