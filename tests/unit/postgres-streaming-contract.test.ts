import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { executePostgresQuery } from '../../src/generated-runtime'
import type { Model } from '../../src/types'

const model: Model = {
  name: 'Item',
  tableName: 'items',
  fields: [
    {
      name: 'id',
      dbName: 'id',
      type: 'Int',
      isId: true,
      isRequired: true,
      isRelation: false,
    },
  ],
}

describe('postgres streaming contract', () => {
  it('consumes shared runtime rows through postgres.js query forEach', async () => {
    let streamed = false
    const query = {
      async forEach(onRow: (row: { id: number }) => void) {
        streamed = true
        onRow({ id: 1 })
        onRow({ id: 2 })
      },
      then() {
        throw new Error('query result was buffered')
      },
    }
    const client = { unsafe: () => query }

    const rows = await executePostgresQuery({
      client,
      sql: 'SELECT id FROM items ORDER BY id',
      params: [],
      method: 'findMany',
      requiresReduction: false,
      model,
      allModels: [model],
    })

    expect(streamed).toBe(true)
    expect(rows).toEqual([{ id: 1 }, { id: 2 }])
  })

  it('keeps generated where-in parent and child paths on query forEach', () => {
    const source = readFileSync(
      resolve(process.cwd(), 'src/code-emitter.ts'),
      'utf8',
    )

    expect(source).toContain(
      'await client.unsafe(sql, normalizedParams).forEach',
    )
    expect(source).toContain(
      'await client.unsafe(sql, normalizeParams(params)).forEach',
    )
  })
})
