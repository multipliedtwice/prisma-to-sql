import {
  convertDMMFToModels,
  type Model,
  DirectiveProps,
} from '@dee-wan/schema-parser'
import { buildWhereClause } from './builder/where'
import { buildSelectSql } from './builder/select'
import {
  buildAggregateSql,
  buildCountSql,
  buildGroupBySql,
} from './builder/aggregates'
import { buildTableReference } from './builder/shared/sql-utils'
import { SQL_TEMPLATES, SQL_RESERVED_WORDS } from './builder/shared/constants'
import { setGlobalDialect, SqlDialect } from './sql-builder-dialect'
import {
  generateSQL as generateSQLInternal,
  SQLDirective,
} from './sql-generator'
import { transformQueryResults, type PrismaMethod } from './result-transformers'

interface SqlResult {
  sql: string
  params: unknown[]
}

interface SpeedExtensionConfig {
  postgres?: any
  sqlite?: any
  models: Model[]
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
  const tableName = buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    model.tableName,
    dialect,
  )
  const alias = makeAlias(model.tableName)

  const whereResult = buildWhereClause(
    (args.where || {}) as Record<string, unknown>,
    {
      alias,
      schemaModels: models,
      model,
      path: ['where'],
      isSubquery: false,
      dialect,
    },
  )

  const withMethod = { ...args, method }
  let result: { sql: string; params: readonly unknown[] }

  switch (method) {
    case 'aggregate':
      result = buildAggregateSql(
        withMethod,
        whereResult,
        tableName,
        alias,
        model,
      )
      break
    case 'groupBy':
      result = buildGroupBySql(
        withMethod,
        whereResult,
        tableName,
        alias,
        model,
        dialect,
      )
      break
    case 'count':
      result = buildCountSql(
        whereResult,
        tableName,
        alias,
        args.skip as number,
        dialect,
      )
      break
    default:
      result = buildSelectSql({
        method,
        args: withMethod,
        model,
        schemas: models,
        from: { tableName, alias },
        whereResult,
        dialect,
      })
  }

  return dialect === 'sqlite'
    ? toSqliteParams(result.sql, result.params)
    : { sql: result.sql, params: [...result.params] }
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

type ExecuteWithTimingInput = {
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
) {
  return ctx.$parent[modelName][method](args)
}

function createExecuteQuery(client: any, dialect: SqlDialect) {
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
) {
  if (!debug) return
  console.error(`[${dialect}] ${modelName}.${method} failed:`, error)
}

type AccelerationDeps = {
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

export function speedExtension(config: SpeedExtensionConfig) {
  const {
    postgres,
    sqlite,
    models,
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

  if (!models || !Array.isArray(models) || models.length === 0) {
    throw new Error(
      'speedExtension requires models parameter. ' +
        'Convert DMMF first: speedExtension({ models: convertDMMFToModels(Prisma.dmmf.datamodel) })',
    )
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

    const createMethodHandler = (method: PrismaMethod) => {
      return async function (this: any, args: any) {
        return handleMethodCall(this, method, args, deps)
      }
    }

    const methodHandlers: Record<string, any> = {}
    for (const method of ACCELERATED_METHODS) {
      methodHandlers[method] = createMethodHandler(method)
    }

    return prisma.$extends({
      name: 'speed-extension',

      client: {
        $original: prisma,
      },

      model: {
        $allModels: methodHandlers,
      },
    })
  }
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

export function createToSQL(models: Model[], dialect: SqlDialect) {
  return createToSQLFunction(models, dialect)
}

interface PrismaSQLConfig<TClient> {
  client: TClient
  models: Model[]
  dialect: SqlDialect
  execute: (
    client: TClient,
    sql: string,
    params: unknown[],
  ) => Promise<unknown[]>
}

export function createPrismaSQL<TClient>(config: PrismaSQLConfig<TClient>) {
  const { client, models, dialect, execute } = config

  if (!models || !Array.isArray(models) || models.length === 0) {
    throw new Error('createPrismaSQL requires non-empty models array')
  }

  const toSQL = createToSQLFunction(models, dialect)

  async function query<T = unknown[]>(
    model: string,
    method: PrismaMethod,
    args: Record<string, unknown> = {},
  ): Promise<T> {
    const { sql, params } = toSQL(model, method, args)
    return execute(client, sql, params) as Promise<T>
  }

  return { toSQL, query, client }
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
        `  - ${directive.modelName}.${directive.header}: ${error.message}`,
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
  QueryInfo,
  SqlResult,
  PrismaMethod,
  PrismaSQLConfig,
}

export type { SqlDialect, SQLDirective }
export { setGlobalDialect, getGlobalDialect } from './sql-builder-dialect'
export type { Model, Field, PrismaQueryArgs } from './types'
export { convertDMMFToModels } from '@dee-wan/schema-parser'
export { transformQueryResults }
