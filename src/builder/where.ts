import { SqlDialect, getGlobalDialect } from '../sql-builder-dialect'
import { Model } from '../types'
import { whereBuilderInstance } from './where/builder'
import { createAliasGenerator } from './shared/alias-generator'
import { createParamStore, ParamStore } from './shared/param-store'
import { toPublicResult } from './shared/state'
import { WhereClauseResult, BuildContext } from './shared/types'
import {
  validateParamConsistency,
  validateParamConsistencyFragment,
} from './shared/validators/sql-validators'
import { assertSafeAlias } from './shared/sql-utils'

interface BuildWhereOptions {
  alias: string
  model: Model
  schemaModels?: Model[]
  path?: string[]
  params?: ParamStore
  isSubquery?: boolean
  aliasGen?: any
  dialect?: SqlDialect
}

export function buildWhereClause(
  where: Record<string, unknown>,
  options: BuildWhereOptions,
): WhereClauseResult {
  assertSafeAlias(options.alias)

  const dialect = options.dialect || getGlobalDialect()
  const params = options.params ?? createParamStore()

  const ctx: BuildContext = {
    alias: options.alias,
    model: options.model,
    schemaModels: options.schemaModels ?? [],
    path: options.path ?? [],
    isSubquery: options.isSubquery ?? false,
    aliasGen: options.aliasGen ?? createAliasGenerator(),
    dialect,
    params,
    depth: 0,
  }

  const result = whereBuilderInstance.build(where, ctx)
  const publicResult = toPublicResult(result.clause, result.joins, params)

  if (!options.isSubquery) {
    const nums = [...publicResult.clause.matchAll(/\$(\d+)/g)].map((m) =>
      parseInt(m[1], 10),
    )
    if (nums.length > 0) {
      const min = Math.min(...nums)
      if (min === 1) {
        validateParamConsistency(publicResult.clause, publicResult.params)
      } else {
        validateParamConsistencyFragment(
          publicResult.clause,
          publicResult.params,
        )
      }
    }
  }

  return publicResult
}
