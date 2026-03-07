import { Model, ParamMapping } from '../../types'
import { SQL_TEMPLATES, SQL_SEPARATORS, LIMITS } from '../shared/constants'
import { quote, buildTableReference, quoteColumn } from '../shared/sql-utils'
import { joinCondition, isValidRelationField } from '../joins'
import { SelectQuerySpec } from '../shared/types'
import { getFieldIndices, getJsonFieldSet } from '../shared/model-field-cache'
import { isPlainObject } from '../shared/validators/type-guards'
import { SqlDialect } from '../../sql-builder-dialect'
import {
  getPrimaryKeyField,
  getPrimaryKeyFields,
} from '../shared/primary-key-utils'
import { extractRelationEntries } from '../shared/relation-extraction-utils'
import {
  extractScalarSelection,
  extractNestedIncludeSpec,
} from '../shared/relation-utils'
import { resolveRelationKeys } from '../shared/relation-key-utils'
import { ParamStore } from '../shared/param-store'
import { buildScalarColumnSelect } from '../shared/relation-query-context'
import { resolveIncludeRelations } from '../shared/include-tree-walker'
import { scanSqlPlaceholders } from '../shared/sql-param-scanner'
import { isDynamicParameter } from '@dee-wan/schema-parser'

interface FlatJoinBuildResult {
  sql: string
  params: any[]
  paramMappings: ParamMapping[]
  requiresReduction: boolean
  includeSpec: Record<string, any>
}

interface AliasCounter {
  count: number
  next(): number
}

function createAliasCounter(): AliasCounter {
  return {
    count: 0,
    next(): number {
      if (
        this.count >=
        Number.MAX_SAFE_INTEGER - LIMITS.MAX_ALIAS_COUNTER_THRESHOLD
      ) {
        throw new Error(
          'Alias counter overflow. This indicates an extremely complex query ' +
            'or a potential infinite loop in relation traversal.',
        )
      }
      const current = this.count
      this.count++
      return current
    },
  }
}

function getRelationModel(
  parentModel: Model,
  relationName: string,
  schemas: readonly Model[],
  modelMap?: Map<string, Model>,
): Model {
  const indices = getFieldIndices(parentModel)
  const field = indices.allFieldsByName.get(relationName)
  if (!field?.isRelation || !field.relatedModel) {
    throw new Error(`Invalid relation ${relationName} on ${parentModel.name}`)
  }

  const relModel = modelMap
    ? modelMap.get(field.relatedModel)
    : schemas.find((m) => m.name === field.relatedModel)
  if (!relModel) {
    throw new Error(`Related model ${field.relatedModel} not found`)
  }

  return relModel
}

function buildChildColumns(args: {
  relModel: Model
  relationName: string
  childAlias: string
  prefix: string
  relArgs: unknown
}): string[] {
  const { relModel, relationName, childAlias, prefix, relArgs } = args
  const fullPrefix = prefix ? `${prefix}.${relationName}` : relationName

  const indices = getFieldIndices(relModel)
  const jsonSet = getJsonFieldSet(relModel)
  const scalarSelection = extractScalarSelection(relArgs, relModel)

  const required = indices.pkFields.concat(
    scalarSelection.selectedScalarFields.filter(
      (f) => !indices.pkFields.includes(f),
    ),
  )

  const columns = new Array<string>(required.length)
  let idx = 0

  for (const fieldName of required) {
    const field = indices.scalarFields.get(fieldName)
    if (!field) continue

    const colName = field.dbName || field.name
    const quotedCol = quote(colName)
    const colRef = jsonSet.has(fieldName)
      ? `${childAlias}.${quotedCol}::text`
      : `${childAlias}.${quotedCol}`

    columns[idx++] = `${colRef} AS "${fullPrefix}.${field.name}"`
  }

  columns.length = idx
  return columns
}

function countActiveEntries(spec: Record<string, any>): number {
  let count = 0
  for (const value of Object.values(spec)) {
    if (value !== false) count++
  }
  return count
}

function hasUnsafeFlatJoinArgs(relValue: unknown): boolean {
  if (!isPlainObject(relValue)) return false
  const obj = relValue as Record<string, unknown>
  if ('where' in obj && obj.where != null) return true
  if ('orderBy' in obj && obj.orderBy != null) return true
  if ('_count' in obj && obj._count != null) return true
  if ('cursor' in obj && obj.cursor != null) return true
  return false
}

export function canUseFlatJoinForAll(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
  debug?: boolean,
  modelMap?: Map<string, Model>,
): boolean {
  const relations = resolveIncludeRelations(
    includeSpec,
    model,
    schemas,
    modelMap,
  )

  if (relations.length < countActiveEntries(includeSpec)) {
    return false
  }

  for (const rel of relations) {
    if (isPlainObject(rel.value)) {
      const obj = rel.value as Record<string, unknown>
      if ('take' in obj && obj.take != null) {
        return false
      }
      if ('skip' in obj && typeof obj.skip === 'number' && obj.skip > 0) {
        return false
      }
    }

    if (hasUnsafeFlatJoinArgs(rel.value)) {
      if (debug)
        console.log(
          `    [canFlatJoin] ${model.name}.${rel.relName}: has where/orderBy/_count/cursor`,
        )
      return false
    }

    const keys = resolveRelationKeys(rel.field as any, 'include')
    if (!keys || keys.parentKeys.length === 0 || keys.childKeys.length === 0) {
      if (debug)
        console.log(
          `    [canFlatJoin] ${model.name}.${rel.relName}: no join keys resolved`,
        )
      return false
    }

    if (keys.parentKeys.length > 1 || keys.childKeys.length > 1) {
      if (debug)
        console.log(
          `    [canFlatJoin] ${model.name}.${rel.relName}: composite keys (${keys.parentKeys.length} parent, ${keys.childKeys.length} child)`,
        )
      return false
    }

    if (Object.keys(rel.nestedSpec).length > 0) {
      if (
        !canUseFlatJoinForAll(
          rel.nestedSpec,
          rel.relModel,
          schemas,
          debug,
          modelMap,
        )
      ) {
        return false
      }
    }
  }

  return true
}

function buildNestedJoins(
  parentModel: Model,
  parentAlias: string,
  includeSpec: Record<string, any>,
  schemas: readonly Model[],
  dialect: SqlDialect,
  prefix: string,
  aliasCounter: AliasCounter,
  depth: number = 0,
  modelMap?: Map<string, Model>,
): { joins: string[]; selects: string[]; orderBy: string[] } {
  if (depth > LIMITS.MAX_NESTED_JOIN_DEPTH) {
    throw new Error(
      `Nested joins exceeded maximum depth of ${LIMITS.MAX_NESTED_JOIN_DEPTH} at prefix '${prefix}'`,
    )
  }

  const joins: string[] = []
  const selects: string[] = []
  const orderBy: string[] = []

  for (const [relName, relValue] of Object.entries(includeSpec)) {
    if (relValue === false) continue

    const indices = getFieldIndices(parentModel)
    const field = indices.allFieldsByName.get(relName)
    if (!isValidRelationField(field as any)) continue

    const relModel = getRelationModel(parentModel, relName, schemas, modelMap)
    const relTable = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      relModel.tableName,
      dialect,
    )

    const childAlias = `fj_${aliasCounter.next()}`
    const joinCond = joinCondition(
      field as any,
      parentModel,
      relModel,
      parentAlias,
      childAlias,
    )

    joins.push(`LEFT JOIN ${relTable} ${childAlias} ON ${joinCond}`)
    selects.push(
      ...buildChildColumns({
        relModel,
        relationName: relName,
        childAlias,
        prefix,
        relArgs: relValue,
      }),
    )

    const childPkFields = getPrimaryKeyFields(relModel)
    for (const pkField of childPkFields) {
      orderBy.push(
        `${childAlias}.${quoteColumn(relModel, pkField)} ASC NULLS LAST`,
      )
    }

    const nested = extractNestedIncludeSpec(relValue, relModel)
    if (Object.keys(nested).length > 0) {
      const nestedPrefix = prefix ? `${prefix}.${relName}` : relName
      const deeper = buildNestedJoins(
        relModel,
        childAlias,
        nested,
        schemas,
        dialect,
        nestedPrefix,
        aliasCounter,
        depth + 1,
        modelMap,
      )

      joins.push(...deeper.joins)
      selects.push(...deeper.selects)
      orderBy.push(...deeper.orderBy)
    }
  }

  return { joins, selects, orderBy }
}

const PARENT_ORD_COL = '"__tp_parent_ord"'
const PARENT_ORD_INNER_ALIAS = '"__tp_inner"'

function pushParam(
  params: any[],
  paramMappings: ParamMapping[],
  value: unknown,
): string {
  params.push(value)
  const index = params.length
  if (isDynamicParameter(value)) {
    paramMappings.push({ index, dynamicName: value as string })
  } else {
    paramMappings.push({ index, value })
  }
  return `$${index}`
}

function extractReferencedParams(
  whereClause: string | undefined,
  specParams: ParamStore | readonly unknown[],
): { cleanWhere: string; params: any[]; paramMappings: ParamMapping[] } {
  if (!whereClause || whereClause === '1=1') {
    return { cleanWhere: '', params: [], paramMappings: [] }
  }

  let allParams: readonly unknown[]
  const mappingByIndex = new Map<
    number,
    { value?: unknown; dynamicName?: string }
  >()

  if (Array.isArray(specParams)) {
    allParams = specParams
  } else if (typeof (specParams as any).snapshot === 'function') {
    const snap = (specParams as ParamStore).snapshot()
    allParams = snap.params
    for (const m of snap.mappings) {
      mappingByIndex.set(m.index, {
        value: m.value,
        dynamicName: m.dynamicName,
      })
    }
  } else {
    allParams = []
  }

  const params: any[] = []
  const paramMappings: ParamMapping[] = []
  const indexMap = new Map<number, number>()
  let hasAny = false

  const cleanWhere = scanSqlPlaceholders(
    whereClause,
    (oldIndex) => {
      hasAny = true
      const existing = indexMap.get(oldIndex)
      if (existing !== undefined) return `$${existing}`

      const pos = oldIndex - 1
      if (pos >= allParams.length) {
        throw new Error(
          `Param placeholder $${oldIndex} exceeds params length (${allParams.length})`,
        )
      }

      params.push(allParams[pos])
      const newIndex = params.length
      indexMap.set(oldIndex, newIndex)

      const origMapping = mappingByIndex.get(oldIndex)
      if (origMapping?.dynamicName !== undefined) {
        paramMappings.push({
          index: newIndex,
          dynamicName: origMapping.dynamicName,
        })
      } else {
        paramMappings.push({ index: newIndex, value: allParams[pos] })
      }

      return `$${newIndex}`
    },
    { pgAware: true, strictPlaceholders: true },
  )

  if (!hasAny) {
    return { cleanWhere: whereClause, params: [], paramMappings: [] }
  }

  return { cleanWhere, params, paramMappings }
}

export function buildFlatJoinSql(spec: SelectQuerySpec): FlatJoinBuildResult {
  const {
    select,
    from,
    whereClause,
    whereJoins,
    orderBy,
    dialect,
    model,
    schemas,
    args,
    pagination,
  } = spec

  const emptyResult: FlatJoinBuildResult = {
    sql: '',
    params: [],
    paramMappings: [],
    requiresReduction: false,
    includeSpec: {},
  }

  const modelMap = new Map<string, Model>()
  for (const m of schemas) modelMap.set(m.name, m)

  const includeSpec = extractRelationEntries(args, model).reduce(
    (acc, { name, value }) => {
      acc[name] = value
      return acc
    },
    {} as Record<string, any>,
  )

  if (Object.keys(includeSpec).length === 0) {
    return emptyResult
  }

  if (!canUseFlatJoinForAll(includeSpec, model, schemas, false, modelMap)) {
    return emptyResult
  }

  const { cleanWhere, params, paramMappings } = extractReferencedParams(
    whereClause,
    spec.params,
  )

  const baseJoins = whereJoins.length > 0 ? whereJoins.join(' ') : ''
  const baseWhere = cleanWhere ? `WHERE ${cleanWhere}` : ''
  const baseOrderBy = orderBy ? `ORDER BY ${orderBy}` : ''

  const subqueryScalarCols = buildScalarColumnSelect(model, from.alias)
  let innerSubquery = `
    SELECT ${subqueryScalarCols} FROM ${from.table} ${from.alias}
    ${baseJoins}
    ${baseWhere}
    ${baseOrderBy}
  `.trim()

  const take =
    pagination.take !== undefined && pagination.take !== null
      ? pagination.take
      : null
  const skip =
    pagination.skip !== undefined && pagination.skip !== null
      ? pagination.skip
      : null

  if (take !== null) {
    const ph = pushParam(params, paramMappings, take)
    innerSubquery += ` LIMIT ${ph}`
  }
  if (skip !== null) {
    const ph = pushParam(params, paramMappings, skip)
    innerSubquery += ` OFFSET ${ph}`
  }

  const baseSubquery = `SELECT *, ROW_NUMBER() OVER () AS ${PARENT_ORD_COL} FROM (${innerSubquery}) ${PARENT_ORD_INNER_ALIAS}`

  const aliasCounter = createAliasCounter()
  const built = buildNestedJoins(
    model,
    from.alias,
    includeSpec,
    schemas,
    dialect,
    '',
    aliasCounter,
    0,
    modelMap,
  )

  if (built.joins.length === 0) {
    return emptyResult
  }

  const baseSelect = (select ?? '').trim()
  const allSelects = [baseSelect, ...built.selects]
    .filter((s) => s && s.trim().length > 0)
    .join(SQL_SEPARATORS.FIELD_LIST)

  if (!allSelects) {
    throw new Error('Flat-join SELECT requires at least one selected field')
  }

  const orderByParts: string[] = []
  orderByParts.push(`${from.alias}.${PARENT_ORD_COL} ASC`)
  orderByParts.push(...built.orderBy)

  const finalOrderBy = orderByParts.join(', ')

  const sql = `
    SELECT ${allSelects}
    FROM (${baseSubquery}) ${from.alias}
    ${built.joins.join(' ')}
    ORDER BY ${finalOrderBy}
  `.trim()

  return { sql, params, paramMappings, requiresReduction: true, includeSpec }
}
