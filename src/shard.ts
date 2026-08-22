import type { createToSQL } from './index'
import type { PrismaMethod } from './types'

/**
 * Injectable shard-control slot.
 *
 * prisma-sql generates SQL; it never decides WHERE that SQL runs. A host
 * application with sharded storage injects a ShardController that maps an
 * EXPLICIT shard key to an executor. The key is always supplied by the call
 * site — prisma-sql does not inspect query args to infer tenancy. Call sites
 * that compose with prisma-guard get their keys from guard-validated shapes,
 * so tenancy stays enforced in exactly one place.
 */
export interface ShardExecutor {
  execute(sql: string, params: unknown[]): Promise<Record<string, unknown>[]>
}

export interface ShardController<TKey> {
  /** Fail closed: throw rather than return a default executor. */
  resolve(key: TKey): Promise<ShardExecutor>
}

export type ToSQLFn = ReturnType<typeof createToSQL>

export interface ShardRead<TKey> {
  model: string
  method: PrismaMethod
  args?: Record<string, unknown>
}

/**
 * A read runner over prerendered SQL: generate, resolve the shard for the
 * explicit key, execute there. No fallback executor exists by design — if the
 * controller cannot resolve a key the read fails loudly instead of running
 * somewhere wrong.
 */
export const createShardedReader = <TKey>(
  toSQL: ToSQLFn,
  controller: ShardController<TKey>,
) => async (
  key: TKey,
  read: ShardRead<TKey>,
): Promise<Record<string, unknown>[]> => {
  const { sql, params } = toSQL(read.model, read.method, read.args ?? {})
  const executor = await controller.resolve(key)
  return executor.execute(sql, params)
}
