import type { Model, PrismaMethod } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import { buildSQLWithCache } from './query-cache'
import { transformQueryResults } from './result-transformers'

export interface TransactionQuery {
  model: string
  method: PrismaMethod
  args?: Record<string, unknown>
}

export interface TransactionOptions {
  isolationLevel?: 'ReadCommitted' | 'RepeatableRead' | 'Serializable'
  timeout?: number
}

interface TransactionExecutor {
  execute(
    queries: TransactionQuery[],
    options?: TransactionOptions,
  ): Promise<unknown[]>
}

function isolationLevelToPostgresKeyword(
  level: TransactionOptions['isolationLevel'],
): string | undefined {
  switch (level) {
    case 'ReadCommitted':
      return 'read committed'
    case 'RepeatableRead':
      return 'repeatable read'
    case 'Serializable':
      return 'serializable'
    default:
      return undefined
  }
}

function validateTimeout(timeout: unknown): number {
  if (typeof timeout !== 'number') {
    throw new Error(
      `Transaction timeout must be a number, got ${typeof timeout}`,
    )
  }
  if (!Number.isFinite(timeout)) {
    throw new Error(`Transaction timeout must be finite, got ${timeout}`)
  }
  if (timeout < 0) {
    throw new Error(`Transaction timeout must be non-negative, got ${timeout}`)
  }
  return Math.floor(timeout)
}

export function createTransactionExecutor(deps: {
  modelMap: Map<string, Model>
  allModels: Model[]
  dialect: SqlDialect
  executeRaw: (sql: string, params?: unknown[]) => Promise<unknown[]>
  postgresClient?: any
}): TransactionExecutor {
  const { modelMap, allModels, dialect, postgresClient } = deps

  return {
    async execute(
      queries: TransactionQuery[],
      options?: TransactionOptions,
    ): Promise<unknown[]> {
      if (queries.length === 0) return []

      if (dialect !== 'postgres') {
        throw new Error('$transaction is only supported for postgres dialect')
      }

      if (!postgresClient) {
        throw new Error('postgresClient is required for transactions')
      }

      const transactionCallback = async (sql: any) => {
        const results: unknown[] = []

        const isolationLevel = isolationLevelToPostgresKeyword(
          options?.isolationLevel,
        )

        if (isolationLevel) {
          await sql.unsafe(
            `SET TRANSACTION ISOLATION LEVEL ${isolationLevel.toUpperCase()}`,
          )
        }

        if (options?.timeout !== undefined && options.timeout !== null) {
          const validatedTimeout = validateTimeout(options.timeout)
          await sql.unsafe(`SET LOCAL statement_timeout = $1`, [
            validatedTimeout,
          ])
        }

        for (const q of queries) {
          const model = modelMap.get(q.model)
          if (!model) {
            throw new Error(
              `Model '${q.model}' not found. Available: ${[...modelMap.keys()].join(', ')}`,
            )
          }

          const { sql: sqlStr, params } = buildSQLWithCache(
            model,
            allModels,
            q.method,
            q.args || {},
            dialect,
          )

          const rawResults = await sql.unsafe(sqlStr, params as any[])
          results.push(transformQueryResults(q.method, rawResults))
        }

        return results
      }

      return await postgresClient.begin(transactionCallback)
    },
  }
}
