import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Prisma, PrismaClient } from '../generated/postgres/client'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { DMMF } from '@prisma/generator-helper'

/**
 * Prisma's `take: -N` returns the LAST N rows in the requested order. The
 * builder captures the correct set by reversing the orderBy + LIMIT, then must
 * re-reverse to the original order. These tests lock that re-reversal so the
 * "correct set, wrong order" regression cannot come back.
 */
describe('Negative take ordering', () => {
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

  it('wraps and restores the original order for a single-key orderBy', () => {
    const { sql } = toSQL('User', 'findMany', {
      orderBy: { id: 'asc' },
      take: -2,
    })
    // inner selects the last N rows via reversed order + limit
    expect(sql).toMatch(/ORDER BY users_t\.id DESC LIMIT/i)
    // outer wrapper restores the requested (ascending) order
    expect(sql).toMatch(/\) AS __tp_negtake ORDER BY __tp_negtake\.id ASC\s*$/i)
  })

  it('restores each key of a multi-key orderBy', () => {
    const { sql } = toSQL('User', 'findMany', {
      where: { role: 'ADMIN' },
      orderBy: [{ createdAt: 'desc' }, { id: 'asc' }],
      take: -5,
    })
    // inner is reversed on both keys
    expect(sql).toMatch(/ORDER BY users_t\."createdAt" ASC, users_t\.id DESC LIMIT/i)
    // outer restores both keys to the requested directions
    expect(sql).toMatch(
      /ORDER BY __tp_negtake\."createdAt" DESC, __tp_negtake\.id ASC\s*$/i,
    )
  })

  it('leaves positive take untouched (no wrapper)', () => {
    const { sql } = toSQL('User', 'findMany', {
      orderBy: { id: 'asc' },
      take: 2,
    })
    expect(sql).not.toContain('__tp_negtake')
    expect(sql).toMatch(/ORDER BY users_t\.id ASC LIMIT/i)
  })

  it('wraps when an explicit select includes the orderBy field', () => {
    const { sql } = toSQL('User', 'findMany', {
      select: { name: true, createdAt: true },
      orderBy: { createdAt: 'asc' },
      take: -3,
    })
    expect(sql).toMatch(
      /\) AS __tp_negtake ORDER BY __tp_negtake\."createdAt" ASC\s*$/i,
    )
  })

  it('throws (not silently mis-orders) when the orderBy field is excluded by select', () => {
    expect(() =>
      toSQL('User', 'findMany', {
        select: { name: true },
        orderBy: { createdAt: 'asc' },
        take: -3,
      }),
    ).toThrow(/orderBy field to be selected/i)
  })

  it('does not change parameters (only wraps SQL text)', () => {
    const { params } = toSQL('User', 'findMany', {
      orderBy: { id: 'asc' },
      take: -4,
    })
    // single LIMIT param, abs value
    expect(params).toContain(4)
  })

  describe('nested list include', () => {
    it('restores original order for a negative take on an included relation', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: { comments: { take: -3, orderBy: { createdAt: 'asc' } } },
      })
      // inner numbers rows over the reversed order...
      expect(sql).toMatch(/ROW_NUMBER\(\) OVER \(ORDER BY comments_0\."createdAt" DESC/i)
      // ...then re-orders them descending to restore the requested ascending order
      expect(sql).toMatch(/ORDER BY "__tp_ord" DESC/i)
    })

    it('leaves a positive take on an included relation untouched', () => {
      const { sql } = toSQL('User', 'findMany', {
        include: { comments: { take: 3, orderBy: { createdAt: 'asc' } } },
      })
      expect(sql).not.toContain('__tp_ord')
      expect(sql).not.toContain('ROW_NUMBER')
    })
  })
})
