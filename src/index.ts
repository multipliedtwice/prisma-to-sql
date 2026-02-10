import { DirectiveProps, convertDMMFToModels } from '@dee-wan/schema-parser'
import { DMMF } from '@prisma/generator-helper'
import { setGlobalDialect, SqlDialect } from './sql-builder-dialect'
import {
  generateSQL as generateSQLInternal,
  SQLDirective,
} from './sql-generator'
import { buildSQLWithCache } from './query-cache'
import {
  buildBatchSql,
  parseBatchResults,
  buildBatchCountSql,
  parseBatchCountResults,
  type BatchQuery,
  type BatchCountQuery,
} from './batch'
import {
  createTransactionExecutor,
  type TransactionQuery,
  type TransactionOptions,
} from './transaction'
import { transformQueryResults } from './result-transformers'
import { buildReducerConfig, reduceFlatRows } from './builder/select/reducer'
import {
  Model,
  PrismaMethod,
  PrismaSQLConfig,
  PrismaSQLResult,
  SqlResult,
} from './types'

export function buildSQL(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult {
  return buildSQLWithCache(model, models, method, args, dialect)
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
    : convertDMMFToModels(modelsOrDmmf.datamodel)

  return createToSQLFunction(models, dialect)
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
    const sqlResult = toSQL(model, method, args)
    let results = await execute(client, sqlResult.sql, [...sqlResult.params])

    if (sqlResult.requiresReduction && sqlResult.includeSpec) {
      const modelDef = modelMap.get(model)
      if (modelDef) {
        const config = buildReducerConfig(
          modelDef,
          sqlResult.includeSpec,
          models,
        )
        results = reduceFlatRows(results as any[], config)
      }
    }

    return transformQueryResults(method, results) as T
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

export {
  buildBatchSql,
  parseBatchResults,
  buildBatchCountSql,
  parseBatchCountResults,
  type BatchQuery,
  type BatchCountQuery,
}

export {
  createTransactionExecutor,
  type TransactionQuery,
  type TransactionOptions,
}

export { transformQueryResults }
export { buildReducerConfig, reduceFlatRows } from './builder/select/reducer'
export type { ReducerConfig } from './builder/select/reducer'

export type { Model, PrismaMethod, PrismaSQLConfig, PrismaSQLResult, SqlResult }
export { normalizeValue } from './utils/normalize-value'
