import { describe, it, expect } from 'vitest'
import { canonicalize, compareResults, findDifferences } from '../../src/verify-compare'

describe('canonicalize', () => {
  it('converts Date to ISO string', () => {
    const d = new Date('2024-01-01T00:00:00.000Z')
    expect(canonicalize(d)).toBe('2024-01-01T00:00:00.000Z')
  })

  it('converts BigInt to string', () => {
    expect(canonicalize(42n)).toBe('42')
  })

  it('sorts object keys', () => {
    expect(canonicalize({ b: 1, a: 2 })).toEqual({ a: 2, b: 1 })
  })

  it('drops undefined values', () => {
    expect(canonicalize({ a: undefined, b: 1 })).toEqual({ b: 1 })
  })

  it('handles nested structures', () => {
    const input = { posts: [{ createdAt: new Date(0), id: 1n }] }
    expect(canonicalize(input)).toEqual({
      posts: [{ createdAt: '1970-01-01T00:00:00.000Z', id: '1' }],
    })
  })
})

describe('findDifferences', () => {
  it('treats missing key and undefined as equal', () => {
    const diffs = findDifferences({ a: 1 }, { a: 1, b: undefined })
    expect(diffs).toEqual([])
  })

  it('reports null vs value', () => {
    const diffs = findDifferences({ p: null }, { p: 5 })
    expect(diffs).toHaveLength(1)
    expect(diffs[0].path).toBe('$.p')
  })

  it('reports array length mismatch with path', () => {
    const diffs = findDifferences([1, 2, 3], [1, 2])
    expect(diffs[0]).toMatchObject({ path: '$', prisma: 'array(length=3)' })
  })

  it('reports element order differences (order matters)', () => {
    const diffs = findDifferences([{ id: 1 }, { id: 2 }], [{ id: 2 }, { id: 1 }])
    expect(diffs.length).toBeGreaterThan(0)
    expect(diffs[0].path).toContain('$[0]')
  })

  it('reports type mismatches', () => {
    const diffs = findDifferences({ n: 5 }, { n: '5' })
    expect(diffs[0].path).toBe('$.n')
  })

  it('stops at maxDiffs', () => {
    const diffs = findDifferences({ a: 1, b: 2, c: 3 }, { a: 9, b: 9, c: 9 }, '$', [], 2)
    expect(diffs).toHaveLength(2)
  })
})

describe('compareResults', () => {
  it('passes for identical Prisma-like include trees', () => {
    const prisma = {
      id: 1,
      email: 'a@b.c',
      posts: [{ id: 10, authorId: 1, published: true }],
    }
    expect(compareResults(prisma, structuredClone(prisma))).toEqual([])
  })

  it('matches Decimal object against plain number of same value', () => {
    const decimalLike = {
      constructor: { name: 'Decimal' },
      toString: () => '1.50',
      toFixed: () => '1.50',
    }
    expect(compareResults({ avg: decimalLike }, { avg: 1.5 })).toEqual([])
  })

  it('catches nested scalar drift inside includes', () => {
    const prisma = { id: 1, profile: { bio: 'hello' } }
    const extended = { id: 1, profile: { bio: 'hell0' } }
    const diffs = compareResults(prisma, extended)
    expect(diffs[0].path).toBe('$.profile.bio')
  })

  it('catches DateTime drift', () => {
    const diffs = compareResults(
      { at: new Date('2024-06-01T12:00:00.000Z') },
      { at: new Date('2024-06-01T11:59:59.999Z') },
    )
    expect(diffs).toHaveLength(1)
  })
})
