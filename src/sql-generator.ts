import { getModelByName } from './builder/joins'
import { buildSelectSql } from './builder/select'
import {
  buildAggregateSql,
  buildCountSql,
  buildGroupBySql,
} from './builder/aggregates'
import { getGlobalDialect, SqlDialect } from './sql-builder-dialect'
import { buildWhereClause } from './builder/where'
import { buildTableReference } from './builder/shared/sql-utils'
import { SQL_TEMPLATES, SQL_RESERVED_WORDS } from './builder/shared/constants'
import { validateSqlPositions } from './builder/shared/validators/sql-validators'
import {
  ParamMap,
  DirectiveProps,
  convertDMMFToModels,
} from '@dee-wan/schema-parser'

export interface SQLDirective {
  method: PrismaMethod
  sql: string
  staticParams: any[]
  dynamicKeys: string[]
  paramMappings: readonly ParamMap[]
  originalDirective: DirectiveProps
}

type PrismaMethod =
  | 'findMany'
  | 'findFirst'
  | 'findUnique'
  | 'aggregate'
  | 'groupBy'
  | 'count'

function safeAlias(input: string): string {
  const raw = String(input).toLowerCase()
  const cleaned = raw.replace(/[^a-z0-9_]/g, '_')
  const startsOk = /^[a-z_]/.test(cleaned)
  let base = startsOk ? cleaned : `_${cleaned}`
  base = base.length > 0 ? base : '_t'

  if (SQL_RESERVED_WORDS.has(base)) {
    base = `_${base}`
  }

  return base
}

function isPrismaMethod(v: unknown): v is PrismaMethod {
  return (
    v === 'findMany' ||
    v === 'findFirst' ||
    v === 'findUnique' ||
    v === 'aggregate' ||
    v === 'groupBy' ||
    v === 'count'
  )
}

function getMethodFromProcessed(processed: Record<string, any>): PrismaMethod {
  const maybe = processed?.method
  if (isPrismaMethod(maybe)) return maybe
  return 'findMany'
}

function buildSqlResult(args: {
  method: PrismaMethod
  processed: Record<string, any>
  whereResult: ReturnType<typeof buildWhereClause>
  tableName: string
  alias: string
  modelDef: any
  schemaModels: any
  dialect: SqlDialect
}): { sql: string; paramMappings: readonly ParamMap[] } {
  const {
    method,
    processed,
    whereResult,
    tableName,
    alias,
    modelDef,
    schemaModels,
    dialect,
  } = args

  if (method === 'aggregate') {
    return buildAggregateSql(processed, whereResult, tableName, alias, modelDef)
  }

  if (method === 'groupBy') {
    return buildGroupBySql(processed, whereResult, tableName, alias, modelDef)
  }

  if (method === 'count') {
    return buildCountSql(
      whereResult,
      tableName,
      alias,
      processed.skip as number,
    )
  }

  return buildSelectSql({
    method,
    args: processed,
    model: modelDef,
    schemas: schemaModels,
    from: { tableName, alias },
    whereResult,
    dialect,
  })
}

function normalizeSqlAndMappingsForDialect(
  sql: string,
  paramMappings: readonly ParamMap[],
  dialect: SqlDialect,
): { sql: string; paramMappings: readonly ParamMap[] } {
  if (dialect !== 'sqlite') return { sql, paramMappings }

  const byIndex = new Map<number, ParamMap>()
  for (const m of paramMappings) byIndex.set(m.index, m)

  const placeholderPositions: number[] = []
  const normalizedSql = sql.replace(/\$(\d+)/g, (_, num) => {
    placeholderPositions.push(parseInt(num, 10))
    return '?'
  })

  const expandedMappings: ParamMap[] = placeholderPositions.map(
    (originalIndex, i) => {
      const originalMapping = byIndex.get(originalIndex)
      if (!originalMapping) {
        throw new Error(
          `CRITICAL: No mapping found for parameter $${originalIndex}`,
        )
      }
      return {
        index: i + 1,
        value: originalMapping.value,
        dynamicName: originalMapping.dynamicName,
      }
    },
  )

  return { sql: normalizedSql, paramMappings: expandedMappings }
}

function buildParamsFromMappings(mappings: readonly ParamMap[]): {
  staticParams: any[]
  dynamicKeys: string[]
} {
  const sorted = [...mappings].sort((a, b) => a.index - b.index)

  return sorted.reduce(
    (acc, m) => {
      if (m.dynamicName !== undefined) {
        acc.dynamicKeys.push(m.dynamicName)
        return acc
      }
      if (m.value !== undefined) {
        acc.staticParams.push(m.value)
        return acc
      }
      throw new Error(
        `CRITICAL: ParamMap ${m.index} has neither dynamicName nor value`,
      )
    },
    { staticParams: [] as any[], dynamicKeys: [] as string[] },
  )
}

function resolveModelContext(directive: DirectiveProps): {
  schemaModels: any
  modelDef: any
} {
  const { model, datamodel } = directive.context

  const schemaModels = convertDMMFToModels(datamodel)
  const modelDef = getModelByName(schemaModels, model.name)
  if (!modelDef) throw new Error(`Model ${model.name} not found in schema`)

  return { schemaModels, modelDef }
}

function buildMainTableAndAlias(args: { modelDef: any; dialect: SqlDialect }): {
  tableName: string
  alias: string
} {
  const { modelDef, dialect } = args
  const baseName = modelDef.tableName || modelDef.name

  return {
    tableName: buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      baseName,
      dialect,
    ),
    alias: safeAlias(`${baseName}_main`),
  }
}

function buildMainWhere(args: {
  processed: Record<string, any>
  alias: string
  schemaModels: any
  modelDef: any
  dialect: SqlDialect
}): ReturnType<typeof buildWhereClause> {
  const { processed, alias, schemaModels, modelDef, dialect } = args

  return buildWhereClause((processed.where || {}) as Record<string, unknown>, {
    alias,
    schemaModels,
    model: modelDef,
    path: ['where'],
    isSubquery: false,
    dialect,
  })
}

function buildAndNormalizeSql(args: {
  method: PrismaMethod
  processed: Record<string, any>
  whereResult: ReturnType<typeof buildWhereClause>
  tableName: string
  alias: string
  modelDef: any
  schemaModels: any
  dialect: SqlDialect
}): { sql: string; paramMappings: readonly ParamMap[] } {
  const {
    method,
    processed,
    whereResult,
    tableName,
    alias,
    modelDef,
    schemaModels,
    dialect,
  } = args

  const sqlResult = buildSqlResult({
    method,
    processed,
    whereResult,
    tableName,
    alias,
    modelDef,
    schemaModels,
    dialect,
  })

  return normalizeSqlAndMappingsForDialect(
    sqlResult.sql,
    sqlResult.paramMappings,
    dialect,
  )
}

function finalizeDirective(args: {
  directive: DirectiveProps
  normalizedSql: string
  normalizedMappings: readonly ParamMap[]
}): SQLDirective {
  const { directive, normalizedSql, normalizedMappings } = args

  validateSqlPositions(normalizedSql, normalizedMappings, getGlobalDialect())

  const { staticParams, dynamicKeys } =
    buildParamsFromMappings(normalizedMappings)

  return {
    method: directive.method as PrismaMethod,
    sql: normalizedSql,
    staticParams,
    dynamicKeys,
    paramMappings: normalizedMappings,
    originalDirective: directive,
  }
}

export function generateSQL(directive: DirectiveProps): SQLDirective {
  const { query } = directive

  const { schemaModels, modelDef } = resolveModelContext(directive)

  const dialect = getGlobalDialect()

  const { tableName, alias } = buildMainTableAndAlias({ modelDef, dialect })

  const whereResult = buildMainWhere({
    processed: query.processed,
    alias,
    schemaModels,
    modelDef,
    dialect,
  })

  const method = directive.method as PrismaMethod

  const normalized = buildAndNormalizeSql({
    method,
    processed: query.processed,
    whereResult,
    tableName,
    alias,
    modelDef,
    schemaModels,
    dialect,
  })

  return finalizeDirective({
    directive,
    normalizedSql: normalized.sql,
    normalizedMappings: normalized.paramMappings,
  })
}
