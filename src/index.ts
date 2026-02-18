import { DirectiveProps, convertDMMFToModels } from '@dee-wan/schema-parser'
import { DMMF } from '@prisma/generator-helper'
import { setGlobalDialect, SqlDialect } from './sql-builder-dialect'
import {
  generateSQL as generateSQLInternal,
  SQLDirective,
} from './sql-generator'
import { buildSQLWithCache } from './query-cache'

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
import { planQueryStrategy } from './builder/select/segment-planner'
import { executeWhereInSegments } from './builder/where-in-executor'
import { executeWithPreFetchedParents } from './builder/select/streaming-where-in-executor'
import { getPrimaryKeyField } from './builder/shared/primary-key-utils'
import {
  buildLateralReducerConfig,
  reduceLateralRows,
} from './builder/select/lateral-reducer'
import { LateralRelationMeta } from './builder/select/lateral-join'
import {
  BatchCountQuery,
  BatchQuery,
  buildBatchCountSql,
  buildBatchSql,
} from './batch/batch-builder'
import { parseBatchCountResults, parseBatchResults } from './batch/batch-result'

export function buildSQL(
  model: Model,
  models: Model[],
  method: PrismaMethod,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult {
  return buildSQLWithCache(model, models, method, args, dialect) as any
}

export function generateSQL(directive: DirectiveProps): SQLDirective {
  return generateSQLInternal(directive)
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
  type TransactionQuery,
  type TransactionOptions,
  type Model,
  type PrismaMethod,
  type PrismaSQLConfig,
  type PrismaSQLResult,
  type SqlResult,
  type LateralRelationMeta,
}

export {
  transformQueryResults,
  planQueryStrategy,
  createTransactionExecutor,
  executeWhereInSegments,
  executeWithPreFetchedParents,
  getPrimaryKeyField,
  buildLateralReducerConfig,
  reduceLateralRows,
}
export { buildReducerConfig, reduceFlatRows } from './builder/select/reducer'
export type { ReducerConfig } from './builder/select/reducer'
export {
  normalizeValue,
  setNormalizeDateMode,
  detectSqliteDateMode,
} from './utils/normalize-value'
export { createStreamingReducer } from './builder/select/streaming-reducer'
export { createProgressiveReducer } from './builder/select/streaming-progressive-reducer'
export { executeWhereInSegmentsStreaming } from './builder/select/streaming-where-in-executor'
export {
  transformAggregateRow,
  extractCountValue,
  getRowTransformer,
} from './builder/select/row-transformers'
export {
  getOrPrepareStatement,
  shouldSqliteUseGet,
  normalizeParams,
  executePostgresQuery,
  executeSqliteQuery,
  executeRaw,
} from './generated-runtime'
export {
  setRelationStats,
  getRelationStats,
  setRoundtripRowEquivalent,
  setJsonRowFactor,
  countIncludeDepth,
} from './builder/select/strategy-estimator'
