import { describe, it, expect, beforeEach } from 'vitest'
import { buildWhereClause } from '../../src/builder/where'
import { buildCountSql } from '../../src/builder/aggregates'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { queryCache } from '../../src/query-cache'
import type { Model } from '../../src/types'

const UserModel: Model = {
  name: 'User',
  tableName: 'users',
  fields: [
    {
      name: 'id',
      dbName: 'id',
      type: 'Int',
      isRequired: true,
      isRelation: false,
    },
    {
      name: 'email',
      dbName: 'email',
      type: 'String',
      isRequired: true,
      isRelation: false,
    },
    {
      name: 'isDeleted',
      dbName: 'is_deleted',
      type: 'Boolean',
      isRequired: true,
      isRelation: false,
    },
    {
      name: 'createdAt',
      dbName: 'createdAt',
      type: 'DateTime',
      isRequired: true,
      isRelation: false,
    },
    {
      name: 'updatedAt',
      dbName: 'updatedAt',
      type: 'DateTime',
      isRequired: true,
      isRelation: false,
    },
    {
      name: 'lastLoginAt',
      dbName: 'lastLoginAt',
      type: 'DateTime?',
      isRequired: false,
      isRelation: false,
    },
  ],
}

describe('Date Normalization - Direct Bug Detection', () => {
  beforeEach(() => {
    setGlobalDialect('postgres')
    queryCache.clear()
  })

  it('Catches Date object leak in count queries', () => {
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

    const whereResult = buildWhereClause(
      {
        isDeleted: false,
        createdAt: { gte: sevenDaysAgo },
      },
      {
        alias: 'user',
        schemaModels: [UserModel],
        model: UserModel,
        path: ['where'],
        isSubquery: false,
      },
    )

    const result = buildCountSql(whereResult, '"public"."users"', 'user')

    console.log('SQL:', result.sql)
    console.log('Params:', result.params)
    console.log(
      'Param types:',
      result.params.map((p) =>
        p instanceof Date
          ? 'Date'
          : typeof p === 'object' && p !== null
            ? `Object{${Object.keys(p).length}}`
            : typeof p,
      ),
    )

    result.params.forEach((param, index) => {
      if (param instanceof Date) {
        throw new Error(
          `🚨 BUG DETECTED!\n\n` +
            `Date instance found at params[${index}]\n` +
            `  Value: ${param}\n` +
            `  Expected: ISO string like "${param.toISOString()}"\n\n` +
            `This will cause:\n` +
            `  RangeError: Invalid time value\n` +
            `  at Date.toISOString (<anonymous>)\n` +
            `  at Object.serialize (postgres/cjs/src/types.js:31:59)\n\n` +
            `SQL: ${result.sql}\n` +
            `All params: ${JSON.stringify(result.params, null, 2)}`,
        )
      }

      if (
        typeof param === 'object' &&
        param !== null &&
        !Array.isArray(param) &&
        Object.keys(param).length === 0 &&
        Object.getPrototypeOf(param) === Object.prototype
      ) {
        throw new Error(
          `🚨 BUG DETECTED!\n\n` +
            `Empty object {} found at params[${index}]\n` +
            `This is likely a Date that failed to serialize!\n\n` +
            `SQL: ${result.sql}\n` +
            `All params: ${JSON.stringify(result.params, null, 2)}`,
        )
      }
    })

    result.params.forEach((param, index) => {
      if (typeof param === 'string' && /\d{4}-\d{2}-\d{2}T/.test(param)) {
        expect(() => new Date(param).toISOString()).not.toThrow()
      }
    })
  })

  it('should handle multiple Date filters', () => {
    const date1 = new Date('2024-01-01T00:00:00Z')
    const date2 = new Date('2024-12-31T23:59:59Z')

    const whereResult = buildWhereClause(
      {
        createdAt: { gte: date1 },
        updatedAt: { lte: date2 },
      },
      {
        alias: 'user',
        schemaModels: [UserModel],
        model: UserModel,
        path: ['where'],
        isSubquery: false,
      },
    )

    const result = buildCountSql(whereResult, '"public"."users"', 'user')

    result.params.forEach((param, index) => {
      expect(param, `params[${index}]`).not.toBeInstanceOf(Date)
    })
  })

  it('should handle OR conditions with Dates', () => {
    const date1 = new Date('2024-01-01T00:00:00Z')
    const date2 = new Date('2024-12-31T23:59:59Z')

    const whereResult = buildWhereClause(
      {
        OR: [{ createdAt: { gte: date1 } }, { updatedAt: { gte: date2 } }],
      },
      {
        alias: 'user',
        schemaModels: [UserModel],
        model: UserModel,
        path: ['where'],
        isSubquery: false,
      },
    )

    const result = buildCountSql(whereResult, '"public"."users"', 'user')

    result.params.forEach((param, index) => {
      expect(param, `params[${index}]`).not.toBeInstanceOf(Date)
    })
  })

  it('simulates exact postgres driver error scenario', () => {
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000)

    const whereResult = buildWhereClause(
      {
        isDeleted: false,
        lastLoginAt: { gte: sevenDaysAgo },
      },
      {
        alias: 'user',
        schemaModels: [UserModel],
        model: UserModel,
        path: ['where'],
        isSubquery: false,
      },
    )

    const result = buildCountSql(whereResult, '"public"."users"', 'user')

    result.params.forEach((param, index) => {
      if (param instanceof Date) {
        try {
          param.toISOString()
        } catch (error) {
          throw new Error(
            `🚨 This is the exact error from your production logs!\n` +
              `  params[${index}]: ${param}\n` +
              `  Error: ${error}\n\n` +
              `The postgres driver cannot serialize this Date object.`,
          )
        }
      }

      if (
        typeof param === 'object' &&
        param !== null &&
        !Array.isArray(param)
      ) {
        if (Object.keys(param).length === 0) {
          try {
            JSON.stringify(param)
          } catch (error) {
            throw new Error(
              `🚨 Serialization would fail!\n` +
                `  params[${index}]: ${JSON.stringify(param)}\n` +
                `  Error: ${error}`,
            )
          }
        }
      }
    })
  })
})
