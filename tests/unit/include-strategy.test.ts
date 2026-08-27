import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { Model } from '../../src/types'
import {
  pickIncludeStrategy,
  setModelStats,
  setRelationStats,
  setStrategyConfig,
} from '../../src/builder/select/strategy-estimator'

const DEFAULT_CONFIG = {
  roundtripRowEquivalent: 73,
  jsonRowFactor: 1.5,
  correlatedBoundedFactor: 0.5,
  correlatedUnboundedFactor: 3,
  correlatedWherePenalty: 3,
  defaultFanOut: 10,
  defaultParentCount: 50,
  singleParentMaxFlatJoinDepth: 2,
  minStatsCoverage: 0.1,
  dynamicTakeEstimate: 10,
  largeChildTableRows: 100_000,
  smallParentCountThreshold: 1000,
}

function scalar(name: string, isId = false): Model['fields'][number] {
  return {
    name,
    dbName: name,
    type: 'Int',
    isId,
    isRequired: true,
    isRelation: false,
  }
}

function relation(
  name: string,
  relatedModel: string,
  isList: boolean,
): Model['fields'][number] {
  return {
    name,
    dbName: name,
    type: isList ? `${relatedModel}[]` : relatedModel,
    isRequired: !isList,
    isRelation: true,
    relatedModel,
    relationName: `${name}Relation`,
    foreignKey: `${relatedModel.toLowerCase()}Id`,
    references: 'id',
    isForeignKeyLocal: false,
  }
}

const root: Model = {
  name: 'Root',
  tableName: 'roots',
  fields: [
    scalar('id', true),
    relation('children', 'Child', true),
    relation('profile', 'Profile', false),
  ],
}
const child: Model = {
  name: 'Child',
  tableName: 'children',
  fields: [scalar('id', true), relation('grands', 'Grand', true)],
}
const grand: Model = {
  name: 'Grand',
  tableName: 'grands',
  fields: [scalar('id', true)],
}
const profile: Model = {
  name: 'Profile',
  tableName: 'profiles',
  fields: [scalar('id', true)],
}
const schemas = [root, child, grand, profile]

function pick(overrides: Partial<Parameters<typeof pickIncludeStrategy>[0]>) {
  return pickIncludeStrategy({
    includeSpec: { children: { include: { grands: true } } },
    model: root,
    schemas,
    method: 'findMany',
    args: {},
    takeValue: 1,
    hasPagination: true,
    canFlatJoin: false,
    hasChildPagination: false,
    ...overrides,
  })
}

beforeEach(() => {
  setStrategyConfig(DEFAULT_CONFIG)
  setRelationStats({})
  setModelStats({
    Root: { rowCount: 10, tableName: 'roots' },
    Child: { rowCount: 10, tableName: 'children' },
    Grand: { rowCount: 10, tableName: 'grands' },
  })
})

describe('include strategy decisions', () => {
  it('keeps deep unpaginated includes on the empirical where-in guard', () => {
    setRelationStats({
      Root: {
        children: { avg: 1, p95: 1, p99: 1, max: 1, coverage: 1 },
      },
      Child: {
        grands: { avg: 1, p95: 1, p99: 1, max: 1, coverage: 1 },
      },
    })

    expect(pick({ hasChildPagination: false })).toBe('where-in')
  })

  it('uses conservative fan-out defaults when relation stats are absent', () => {
    expect(pick({ hasChildPagination: false })).toBe('where-in')
  })

  it('keeps deep child pagination correlated', () => {
    expect(pick({ hasChildPagination: true })).toBe('correlated')
  })

  it('keeps shallow child pagination with a where clause on where-in', () => {
    expect(
      pick({
        includeSpec: {
          children: { take: 5, where: { id: { gt: 1 } } },
        },
        hasChildPagination: true,
      }),
    ).toBe('where-in')
  })

  it('uses flat join for one-to-one includes', () => {
    expect(
      pick({
        includeSpec: { profile: true },
        canFlatJoin: true,
      }),
    ).toBe('flat-join')
  })

  it('uses flat join for a single parent through depth two', () => {
    expect(
      pick({
        method: 'findUnique',
        canFlatJoin: true,
      }),
    ).toBe('flat-join')
  })

  it('runs the large-child guard before the paginated rule', () => {
    setModelStats({
      Root: { rowCount: 10, tableName: 'roots' },
      Child: { rowCount: 100_001, tableName: 'children' },
      Grand: { rowCount: 10, tableName: 'grands' },
    })

    expect(pick({ hasChildPagination: true })).toBe('where-in')
  })

  it('keeps the guard inactive without model stats', () => {
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => {})
    setModelStats({})

    expect(pick({ hasChildPagination: true })).toBe('correlated')
    expect(warning).toHaveBeenCalledOnce()
    warning.mockRestore()
  })

  it('does not suggest unsupported stats collection for SQLite', () => {
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => {})
    setModelStats({})

    expect(pick({ dialect: 'sqlite', hasChildPagination: true })).toBe(
      'correlated',
    )
    expect(warning).not.toHaveBeenCalled()
    warning.mockRestore()
  })
})
