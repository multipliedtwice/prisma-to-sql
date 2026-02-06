// src/index.ts
import {
  type Model,
  DirectiveProps,
  convertDMMFToModels,
} from '@dee-wan/schema-parser'
import { DMMF } from '@prisma/generator-helper'
import { SQL_RESERVED_WORDS } from './builder/shared/constants'
import { setGlobalDialect, SqlDialect } from './sql-builder-dialect'
import {
  generateSQL as generateSQLInternal,
  SQLDirective,
} from './sql-generator'
import { transformQueryResults, type PrismaMethod } from './result-transformers'
import { buildSQLWithCache } from './query-cache'
import { buildBatchSql, parseBatchResults, type BatchQuery } from './batch'
import {
  createTransactionExecutor,
  type TransactionQuery,
  type TransactionOptions,
  type TransactionExecutor,
} from './transaction'

interface SqlResult {
  sql: string
  params: unknown[]
}

interface SpeedExtensionConfig {
  postgres?: any
  sqlite?: any
  models?: Model[]
  dmmf?: DMMF.Document
  debug?: boolean
  allowedModels?: string[]
  onQuery?: (info: QueryInfo) => void
}

interface QueryInfo {
  model: string
  method: string
  sql: string
  params: unknown[]
  duration: number
}

interface DeferredQueryLike {
  readonly model: string
  readonly method: PrismaMethod
  readonly args: any
}

interface BatchProxy {
  [modelName: string]: {
    findMany: (args?: any) => DeferredQueryLike
    findFirst: (args?: any) => DeferredQueryLike
    findUnique: (args?: any) => DeferredQueryLike
    count: (args?: any) => DeferredQueryLike
    aggregate: (args?: any) => DeferredQueryLike
    groupBy: (args?: any) => DeferredQueryLike
  }
}

interface SpeedExtensionClient {
  $original: any
  $batch: <T extends Record<string, DeferredQueryLike>>(
    callback: (batch: BatchProxy) => T | Promise<T>,
  ) => Promise<{ [K in keyof T]: any }>
  $transaction: (
    queries: TransactionQuery[],
    options?: TransactionOptions,
  ) => Promise<unknown[]>
}

type ExtendedPrismaClient<T> = T & SpeedExtensionClient

const ACCELERATED_METHODS = new Set<PrismaMethod>([
  'findMany',
  'findFirst',
  'findUnique',
  'count',
  'aggregate',
  'groupBy',
])

function makeAlias(name: string): string {
  const base = name
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .slice(0, 50)
  const safe = /^[a-z_]/.test(base) ? base : `_${base}`
  return SQL_RESERVED_WORDS.has(safe) ? `${safe}_t` : safe
}

function toSqliteParams(sql: string, params: readonly unknown[]): SqlResult {
  const positions: number[] = []

  const converted = sql.replace(/\$(\d+)/g, (_, num) => {
    positions.push(parseInt(num, 10))
    return '?'
  })

  const reordered = positions.map((pos) => {
    const idx = pos - 1
    if (idx < 0 || idx >= params.length) {
      throw new Error(`Param $${pos} out of bounds (have ${params.length})`)
    }
    return params[idx]
  })

  return { sql: converted, params: reordered }
}

export function buildSQL(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult {
  return buildSQLWithCache(model, models, method, args, dialect)
}

async function executePostgres(
  client: any,
  sql: string,
  params: unknown[],
): Promise<unknown[]> {
  return await client.unsafe(sql, params as any[])
}

function executeSqlite(db: any, sql: string, params: unknown[]): unknown[] {
  const stmt = db.prepare(sql)

  if (sql.toUpperCase().includes('COUNT(*) AS')) {
    return [stmt.get(...params)]
  }

  return stmt.all(...params)
}

interface ExecuteWithTimingInput {
  modelName: string
  method: PrismaMethod
  model: Model
  allModels: Model[]
  args: Record<string, unknown>
  dialect: SqlDialect
  debug: boolean
  executeQuery: (sql: string, params: unknown[]) => Promise<unknown[]>
  onQuery?: (info: QueryInfo) => void
}

async function executeWithTiming(
  input: ExecuteWithTimingInput,
): Promise<unknown[]> {
  const startTime = Date.now()

  const { sql, params } = buildSQL(
    input.model,
    input.allModels,
    input.method,
    input.args,
    input.dialect,
  )

  if (input.debug) {
    console.log(`[${input.dialect}] ${input.modelName}.${input.method}`)
    console.log('SQL:', sql)
    console.log('Params:', params)
  }

  const results = await input.executeQuery(sql, params)
  const duration = Date.now() - startTime

  input.onQuery?.({
    model: input.modelName,
    method: input.method,
    sql,
    params,
    duration,
  })

  return results
}

function resolveModelName(ctx: any): string {
  return ctx?.name || ctx?.$name
}

function isAllowedModel(
  allowedModels: string[] | undefined,
  modelName: string,
): boolean {
  if (!allowedModels) return true
  return allowedModels.includes(modelName)
}

function getModelOrNull(
  modelMap: Map<string, Model>,
  modelName: string,
): Model | null {
  return modelMap.get(modelName) ?? null
}

function fallbackToPrisma(
  ctx: any,
  modelName: string,
  method: PrismaMethod,
  args: any,
): Promise<any> {
  return ctx.$parent[modelName][method](args)
}

function createExecuteQuery(
  client: any,
  dialect: SqlDialect,
): (sql: string, params: unknown[]) => Promise<unknown[]> {
  return async (sql: string, params: unknown[]): Promise<unknown[]> => {
    if (dialect === 'postgres') {
      return await executePostgres(client, sql, params)
    }
    return executeSqlite(client, sql, params)
  }
}

function logAcceleratedError(
  debug: boolean,
  dialect: SqlDialect,
  modelName: string,
  method: PrismaMethod,
  error: unknown,
): void {
  if (!debug) return
  console.error(`[${dialect}] ${modelName}.${method} failed:`, error)
}

interface AccelerationDeps {
  dialect: SqlDialect
  debug: boolean
  onQuery?: (info: QueryInfo) => void
  allowedModels?: string[]
  allModels: Model[]
  modelMap: Map<string, Model>
  executeQuery: (sql: string, params: unknown[]) => Promise<unknown[]>
}

function canAccelerate(
  deps: AccelerationDeps,
  modelName: string,
  method: PrismaMethod,
): boolean {
  if (!ACCELERATED_METHODS.has(method)) return false
  if (!isAllowedModel(deps.allowedModels, modelName)) return false
  return deps.modelMap.has(modelName)
}

async function runAccelerated(
  deps: AccelerationDeps,
  modelName: string,
  method: PrismaMethod,
  model: Model,
  args: any,
): Promise<unknown> {
  const results = await executeWithTiming({
    modelName,
    method,
    model,
    allModels: deps.allModels,
    args: (args || {}) as Record<string, unknown>,
    dialect: deps.dialect,
    debug: deps.debug,
    executeQuery: deps.executeQuery,
    onQuery: deps.onQuery,
  })

  return transformQueryResults(method, results)
}

async function handleMethodCall(
  ctx: any,
  method: PrismaMethod,
  args: any,
  deps: AccelerationDeps,
): Promise<any> {
  const modelName = resolveModelName(ctx)

  if (!canAccelerate(deps, modelName, method)) {
    return fallbackToPrisma(ctx, modelName, method, args)
  }

  const model = getModelOrNull(deps.modelMap, modelName)
  if (!model) {
    return fallbackToPrisma(ctx, modelName, method, args)
  }

  try {
    return await runAccelerated(deps, modelName, method, model, args)
  } catch (error) {
    logAcceleratedError(deps.debug, deps.dialect, modelName, method, error)
    return fallbackToPrisma(ctx, modelName, method, args)
  }
}

class DeferredQuery implements DeferredQueryLike {
  constructor(
    public readonly model: string,
    public readonly method: PrismaMethod,
    public readonly args: any,
  ) {}

  then(onfulfilled?: any, onrejected?: any): any {
    throw new Error(
      'Cannot await a batch query. Batch queries must not be awaited inside the $batch callback.',
    )
  }
}

function createBatchProxy(
  modelMap: Map<string, Model>,
  allowedModels?: string[],
): BatchProxy {
  return new Proxy(
    {},
    {
      get(_target, modelName: string): any {
        if (typeof modelName === 'symbol') return undefined

        const model = modelMap.get(modelName)
        if (!model) {
          throw new Error(
            `Model '${modelName}' not found. Available: ${[...modelMap.keys()].join(', ')}`,
          )
        }

        if (allowedModels && !allowedModels.includes(modelName)) {
          throw new Error(
            `Model '${modelName}' not allowed. Allowed: ${allowedModels.join(', ')}`,
          )
        }

        return new Proxy(
          {},
          {
            get(_target, method: string): (args?: any) => DeferredQuery {
              if (!ACCELERATED_METHODS.has(method as PrismaMethod)) {
                throw new Error(
                  `Method '${method}' not supported in batch. Supported: ${[...ACCELERATED_METHODS].join(', ')}`,
                )
              }

              return (args?: any): DeferredQuery => {
                return new DeferredQuery(
                  modelName,
                  method as PrismaMethod,
                  args,
                )
              }
            },
          },
        )
      },
    },
  ) as BatchProxy
}

export function speedExtension(config: SpeedExtensionConfig) {
  const {
    postgres,
    sqlite,
    models: providedModels,
    dmmf,
    debug = false,
    allowedModels,
    onQuery,
  } = config

  if (!postgres && !sqlite) {
    throw new Error('speedExtension requires either postgres or sqlite client')
  }

  if (postgres && sqlite) {
    throw new Error(
      'speedExtension cannot use both postgres and sqlite clients',
    )
  }

  let models: Model[]

  if (providedModels) {
    models = providedModels
  } else if (dmmf) {
    models = convertDMMFToModels(dmmf.datamodel)
  } else {
    throw new Error('speedExtension requires either models or dmmf parameter.')
  }

  if (!Array.isArray(models) || models.length === 0) {
    throw new Error('speedExtension: models array is empty or invalid')
  }

  const dialect: SqlDialect = postgres ? 'postgres' : 'sqlite'
  const client = postgres || sqlite

  setGlobalDialect(dialect)

  return (prisma: any) => {
    const modelMap = new Map(models.map((m) => [m.name, m]))
    const executeQuery = createExecuteQuery(client, dialect)

    const deps: AccelerationDeps = {
      dialect,
      debug,
      onQuery,
      allowedModels,
      allModels: models,
      modelMap,
      executeQuery,
    }

    const createMethodHandler = (
      method: PrismaMethod,
    ): ((this: any, args?: any) => Promise<any>) => {
      return async function (this: any, args?: any): Promise<any> {
        return handleMethodCall(this, method, args, deps)
      }
    }

    const methodHandlers: Record<string, (args?: any) => Promise<any>> = {}
    for (const method of ACCELERATED_METHODS) {
      methodHandlers[method] = createMethodHandler(method)
    }

    const executeRaw = async (
      sql: string,
      params?: unknown[],
    ): Promise<unknown[]> => {
      if (dialect === 'postgres') {
        return await client.unsafe(sql, params as any[])
      }
      throw new Error('Raw execution for sqlite not supported in transactions')
    }

    const txExecutor: TransactionExecutor = createTransactionExecutor({
      modelMap,
      allModels: models,
      dialect,
      executeRaw,
      postgresClient: postgres,
    })

    async function batch<T extends Record<string, DeferredQueryLike>>(
      callback: (batch: BatchProxy) => T | Promise<T>,
    ): Promise<{ [K in keyof T]: any }> {
      const batchProxy = createBatchProxy(modelMap, allowedModels)
      const queries = await callback(batchProxy)

      const batchQueries: Record<string, BatchQuery> = {}

      for (const [key, deferred] of Object.entries(queries)) {
        if (!(deferred instanceof DeferredQuery)) {
          throw new Error(
            `Batch query '${key}' must be a deferred query. Did you await it?`,
          )
        }

        batchQueries[key] = {
          model: deferred.model,
          method: deferred.method,
          args: deferred.args || {},
        }
      }

      const startTime = Date.now()
      const { sql, params, keys } = buildBatchSql(
        batchQueries,
        modelMap,
        models,
        dialect,
      )

      if (debug) {
        console.log(`[${dialect}] $batch (${keys.length} queries)`)
        console.log('SQL:', sql)
        console.log('Params:', params)
      }

      const rows = await executeQuery(sql, params)
      const row = rows[0] as Record<string, unknown>
      const results = parseBatchResults(row, keys, batchQueries)
      const duration = Date.now() - startTime

      onQuery?.({
        model: '_batch',
        method: 'batch',
        sql,
        params,
        duration,
      })

      return results as { [K in keyof T]: any }
    }

    async function transaction(
      queries: TransactionQuery[],
      options?: TransactionOptions,
    ): Promise<unknown[]> {
      const startTime = Date.now()

      if (debug) {
        console.log(`[${dialect}] $transaction (${queries.length} queries)`)
      }

      const results = await txExecutor.execute(queries, options)
      const duration = Date.now() - startTime

      onQuery?.({
        model: '_transaction',
        method: 'count',
        sql: `TRANSACTION(${queries.length})`,
        params: [],
        duration,
      })

      return results
    }

    return prisma.$extends({
      name: 'prisma-sql-speed',

      client: {
        $original: prisma,
        $batch: batch,
        $transaction: transaction,
      },

      model: {
        $allModels: methodHandlers,
      },
    })
  }
}

export function extendPrisma<T>(
  prisma: T,
  config: SpeedExtensionConfig,
): ExtendedPrismaClient<T> {
  const extension = speedExtension(config)
  return extension(prisma) as ExtendedPrismaClient<T>
}

function createToSQLFunction(
  models: Model[],
  dialect: SqlDialect,
): (
  model: string,
  method: PrismaMethod,
  args?: Record<string, unknown>,
) => SqlResult {
  if (!models || !Array.isArray(models) || models.length === 0) {
    throw new Error('createToSQL requires non-empty models array')
  }

  const modelMap = new Map(models.map((m) => [m.name, m]))

  setGlobalDialect(dialect)

  return function toSQL(
    model: string,
    method: PrismaMethod,
    args: Record<string, unknown> = {},
  ): SqlResult {
    const m = modelMap.get(model)
    if (!m) {
      throw new Error(
        `Model '${model}' not found. Available: ${[...modelMap.keys()].join(', ')}`,
      )
    }
    return buildSQL(m, models, method, args, dialect)
  }
}

export function createToSQL(
  modelsOrDmmf: Model[] | DMMF.Document,
  dialect: SqlDialect,
): (
  model: string,
  method: PrismaMethod,
  args?: Record<string, unknown>,
) => SqlResult {
  const models = Array.isArray(modelsOrDmmf)
    ? modelsOrDmmf
    : convertDMMFToModels((modelsOrDmmf as DMMF.Document).datamodel)

  return createToSQLFunction(models, dialect)
}

interface PrismaSQLConfig<TClient> {
  client: TClient
  models?: Model[]
  dmmf?: DMMF.Document
  dialect: SqlDialect
  execute: (
    client: TClient,
    sql: string,
    params: unknown[],
  ) => Promise<unknown[]>
}

interface PrismaSQLResult<TClient> {
  toSQL: (
    model: string,
    method: PrismaMethod,
    args?: Record<string, unknown>,
  ) => SqlResult
  query: <T = unknown[]>(
    model: string,
    method: PrismaMethod,
    args?: Record<string, unknown>,
  ) => Promise<T>
  batchSql: (queries: Record<string, BatchQuery>) => SqlResult
  client: TClient
}

export function createPrismaSQL<TClient>(
  config: PrismaSQLConfig<TClient>,
): PrismaSQLResult<TClient> {
  const { client, models: providedModels, dmmf, dialect, execute } = config

  let models: Model[]

  if (providedModels) {
    models = providedModels
  } else if (dmmf) {
    models = convertDMMFToModels(dmmf.datamodel)
  } else {
    throw new Error('createPrismaSQL requires either models or dmmf parameter')
  }

  if (!Array.isArray(models) || models.length === 0) {
    throw new Error('createPrismaSQL: models array is empty or invalid')
  }

  const toSQL = createToSQLFunction(models, dialect)
  const modelMap = new Map(models.map((m) => [m.name, m]))

  async function query<T = unknown[]>(
    model: string,
    method: PrismaMethod,
    args: Record<string, unknown> = {},
  ): Promise<T> {
    const { sql, params } = toSQL(model, method, args)
    return execute(client, sql, params) as Promise<T>
  }

  function batchSql(queries: Record<string, BatchQuery>): SqlResult {
    const { sql, params } = buildBatchSql(queries, modelMap, models, dialect)
    return { sql, params }
  }

  return { toSQL, query, batchSql, client }
}

export function generateSQL(directive: DirectiveProps): SQLDirective {
  return generateSQLInternal(directive)
}

export function generateAllSQL(directives: DirectiveProps[]): SQLDirective[] {
  const results: SQLDirective[] = []
  const errors: Array<{ directive: DirectiveProps; error: Error }> = []

  for (const directive of directives) {
    try {
      results.push(generateSQL(directive))
    } catch (error) {
      errors.push({
        directive,
        error: error instanceof Error ? error : new Error(String(error)),
      })
    }
  }

  if (errors.length > 0) {
    console.warn(
      `[generateAllSQL] ${errors.length} directive(s) failed SQL generation`,
    )
    for (const { directive, error } of errors) {
      console.warn(
        `  - ${directive.modelName}.${directive.method}: ${error.message}`,
      )
    }
  }

  return results
}

export function generateSQLByModel(
  directives: DirectiveProps[],
): Map<string, SQLDirective[]> {
  const byModel = new Map<string, SQLDirective[]>()

  for (const directive of directives) {
    const sql = generateSQL(directive)

    if (!byModel.has(directive.modelName)) {
      byModel.set(directive.modelName, [])
    }

    byModel.get(directive.modelName)!.push(sql)
  }

  return byModel
}

export type {
  SpeedExtensionConfig,
  SpeedExtensionClient,
  ExtendedPrismaClient,
  QueryInfo,
  SqlResult,
  PrismaMethod,
  PrismaSQLConfig,
  PrismaSQLResult,
  BatchQuery,
  BatchProxy,
  DeferredQueryLike,
  TransactionQuery,
  TransactionOptions,
  TransactionExecutor,
}

export type { SqlDialect, SQLDirective }
export { setGlobalDialect, getGlobalDialect } from './sql-builder-dialect'
export type { Model, Field, PrismaQueryArgs } from './types'
export { convertDMMFToModels } from '@dee-wan/schema-parser'
export { transformQueryResults }
export { buildBatchSql, parseBatchResults } from './batch'
export { createTransactionExecutor } from './transaction'
