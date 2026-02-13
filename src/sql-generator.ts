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
import { validateParamConsistencyByDialect } from './builder/shared/validators/sql-validators'
import {
  ParamMap,
  DirectiveProps,
  convertDMMFToModels,
} from '@dee-wan/schema-parser'
import { PrismaMethod } from './types'
import { isPlainObject } from './builder/shared/validators/type-guards'
import { SqlResult } from './builder/shared/types'

export interface SQLDirective {
  method: PrismaMethod
  sql: string
  staticParams: any[]
  dynamicKeys: string[]
  paramOrder: string
  paramMappings: readonly ParamMap[]
  requiresReduction: boolean
  includeSpec: Record<string, any>
  originalDirective: DirectiveProps
}

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

function resolveMethod(directive: DirectiveProps): PrismaMethod {
  const m = (directive as any)?.method
  if (isPrismaMethod(m)) return m
  const pm = (directive as any)?.query?.processed?.method
  if (isPrismaMethod(pm)) return pm
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
}): SqlResult {
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
    return buildGroupBySql(
      processed,
      whereResult,
      tableName,
      alias,
      modelDef,
      dialect,
    )
  }

  if (method === 'count') {
    return buildCountSql(
      whereResult,
      tableName,
      alias,
      processed,
      dialect,
      modelDef,
      schemaModels,
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
  paramOrder: string
} {
  const sorted = [...mappings].sort((a, b) => a.index - b.index)
  const staticParams: any[] = []
  const dynamicKeys: string[] = []
  let paramOrder = ''

  for (const m of sorted) {
    if (m.dynamicName !== undefined) {
      dynamicKeys.push(m.dynamicName)
      paramOrder += 'd'
    } else if (m.value !== undefined) {
      staticParams.push(m.value)
      paramOrder += 's'
    } else {
      throw new Error(
        `CRITICAL: ParamMap ${m.index} has neither dynamicName nor value`,
      )
    }
  }

  return { staticParams, dynamicKeys, paramOrder }
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

function extractIncludeSpec(
  processed: Record<string, any>,
  modelDef: any,
): Record<string, any> {
  const includeSpec: Record<string, any> = {}
  const relationSet = new Set<string>(
    Array.isArray(modelDef?.fields)
      ? modelDef.fields
          .filter((f: any) => f && f.isRelation && typeof f.name === 'string')
          .map((f: any) => f.name)
      : [],
  )

  if (processed.include && isPlainObject(processed.include)) {
    for (const [key, value] of Object.entries(processed.include)) {
      if (!relationSet.has(key)) continue
      if (value !== false) {
        includeSpec[key] = value
      }
    }
  }

  if (processed.select && isPlainObject(processed.select)) {
    for (const [key, value] of Object.entries(processed.select)) {
      if (!relationSet.has(key)) continue
      if (value === false) continue

      if (value === true) {
        includeSpec[key] = true
        continue
      }

      if (isPlainObject(value)) {
        const selectVal = value as Record<string, any>
        if (selectVal.include || selectVal.select) {
          includeSpec[key] = value
        } else {
          includeSpec[key] = true
        }
      } else {
        includeSpec[key] = true
      }
    }
  }

  return includeSpec
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
}): {
  sql: string
  paramMappings: readonly ParamMap[]
  requiresReduction: boolean
  includeSpec: Record<string, any>
} {
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

  const normalized = normalizeSqlAndMappingsForDialect(
    sqlResult.sql,
    sqlResult.paramMappings!,
    dialect,
  )

  const includeSpec =
    (sqlResult.includeSpec && isPlainObject(sqlResult.includeSpec)
      ? (sqlResult.includeSpec as Record<string, any>)
      : null) ?? extractIncludeSpec(processed, modelDef)

  const requiresReduction = sqlResult.requiresReduction === true

  return {
    sql: normalized.sql,
    paramMappings: normalized.paramMappings,
    requiresReduction,
    includeSpec,
  }
}

function finalizeDirective(args: {
  directive: DirectiveProps
  method: PrismaMethod
  normalizedSql: string
  normalizedMappings: readonly ParamMap[]
  dialect: SqlDialect
  requiresReduction: boolean
  includeSpec: Record<string, any>
}): SQLDirective {
  const {
    directive,
    method,
    normalizedSql,
    normalizedMappings,
    dialect,
    requiresReduction,
    includeSpec,
  } = args

  const params = normalizedMappings.map((m) => m.value ?? undefined)
  validateParamConsistencyByDialect(normalizedSql, params, dialect)

  const { staticParams, dynamicKeys, paramOrder } =
    buildParamsFromMappings(normalizedMappings)

  return {
    method,
    sql: normalizedSql,
    staticParams,
    dynamicKeys,
    paramOrder,
    paramMappings: normalizedMappings,
    requiresReduction,
    includeSpec,
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

  const method = resolveMethod(directive)

  const built = buildAndNormalizeSql({
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
    method,
    normalizedSql: built.sql,
    normalizedMappings: built.paramMappings,
    dialect,
    requiresReduction: built.requiresReduction,
    includeSpec: built.includeSpec,
  })
}
