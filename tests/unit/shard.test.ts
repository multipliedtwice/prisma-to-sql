import { describe, it, expect, vi } from 'vitest'

import { createShardedReader, type ShardController, type ShardExecutor, type ToSQLFn } from '../../src/shard'

const fakeToSQL = (result: { sql: string; params: unknown[] }): ToSQLFn =>
  ((model: string, method: string, args?: Record<string, unknown>) => {
    void model
    void method
    void args
    return result
  }) as unknown as ToSQLFn

const recordingExecutor = (name: string, log: string[]): ShardExecutor => ({
  execute: async (sql, params) => {
    log.push(`${name}:${sql}:${JSON.stringify(params)}`)
    return [{ executed_on: name }]
  },
})

describe('createShardedReader', () => {
  it('executes generated SQL on the executor the controller resolves for the key', async () => {
    const log: string[] = []
    const executors: Record<string, ShardExecutor> = {
      shard_a: recordingExecutor('shard_a', log),
      shard_b: recordingExecutor('shard_b', log),
    }
    const controller: ShardController<string> = {
      resolve: async (key) => {
        const executor = executors[key]
        if (!executor) throw new Error(`no shard for ${key}`)
        return executor
      },
    }
    const read = createShardedReader(
      fakeToSQL({ sql: 'SELECT * FROM t WHERE id = ?', params: ['x'] }),
      controller,
    )

    await expect(read('shard_a', { model: 't', method: 'findMany' })).resolves.toEqual([{ executed_on: 'shard_a' }])
    await expect(read('shard_b', { model: 't', method: 'findMany' })).resolves.toEqual([{ executed_on: 'shard_b' }])
    expect(log).toEqual([
      'shard_a:SELECT * FROM t WHERE id = ?:["x"]',
      'shard_b:SELECT * FROM t WHERE id = ?:["x"]',
    ])
  })

  it('forwards args to SQL generation and params to the executor', async () => {
    const seen: Array<Record<string, unknown>> = []
    const toSQL = ((model: string, method: string, args?: Record<string, unknown>) => {
      seen.push({ model, method, ...(args ?? {}) })
      return { sql: 'SELECT 1', params: [args?.take ?? 0] }
    }) as unknown as ToSQLFn
    const execute = vi.fn(async () => [])
    const read = createShardedReader(toSQL, { resolve: async () => ({ execute }) })

    await read('k', { model: 'user', method: 'findMany', args: { take: 5 } })
    expect(seen).toEqual([{ model: 'user', method: 'findMany', take: 5 }])
    expect(execute).toHaveBeenCalledWith('SELECT 1', [5])
  })

  it('defaults missing args to an empty object', async () => {
    let receivedArgs: Record<string, unknown> | undefined
    const toSQL = ((_m: string, _me: string, args?: Record<string, unknown>) => {
      receivedArgs = args
      return { sql: 'SELECT 1', params: [] }
    }) as unknown as ToSQLFn
    const read = createShardedReader(toSQL, { resolve: async () => ({ execute: async () => [] }) })

    await read('k', { model: 'user', method: 'count' })
    expect(receivedArgs).toEqual({})
  })

  it('fails closed when the controller refuses the key — SQL never executes', async () => {
    const execute = vi.fn(async () => [])
    const controller: ShardController<'a' | 'b'> = {
      resolve: async (key) => {
        if (key !== 'a') throw new Error('unroutable-shard-key')
        return { execute }
      },
    }
    const read = createShardedReader(fakeToSQL({ sql: 'DELETE FROM everything', params: [] }), controller)

    await expect(read('b' as 'a' | 'b', { model: 'x', method: 'findMany' })).rejects.toThrow('unroutable-shard-key')
    expect(execute).not.toHaveBeenCalled()
  })
})
