import { Model } from '../../types'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { quote, buildTableReference, quoteColumn } from '../shared/sql-utils'
import { joinCondition, isValidRelationField } from '../joins'
import { SelectQuerySpec } from '../shared/types'
import {
  getFieldIndices,
  getRelationFieldSet,
} from '../shared/model-field-cache'
import { isPlainObject } from '../shared/validators/type-guards'
import { SqlDialect } from '../../sql-builder-dialect'
import {
  getPrimaryKeyField,
  getPrimaryKeyFields,
} from '../shared/primary-key-utils'
import { extractRelationEntries } from '../shared/relation-extraction-utils'
import {
  hasChildPagination,
  extractScalarSelection,
  extractNestedIncludeSpec,
} from '../shared/relation-utils'
import { deduplicatePreserveOrder } from '../shared/array-utils'
import { Field } from '@dee-wan/schema-parser'
import { resolveRelationKeys } from '../shared/relation-key-utils'
import { ParamStore } from '../shared/param-store'

export interface FlatJoinBuildResult {
  sql: string
  params: any[]
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
      if (this.count >= Number.MAX_SAFE_INTEGER - 1000) {
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
): Model {
  const field = parentModel.fields.find((f) => f.name === relationName)
  if (!field?.isRelation || !field.relatedModel) {
    throw new Error(`Invalid relation ${relationName} on ${parentModel.name}`)
  }

  const relModel = schemas.find((m) => m.name === field.relatedModel)
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

    columns[idx++] =
      `${childAlias}.${quotedCol} AS "${fullPrefix}.${field.name}"`
  }

  columns.length = idx
  return columns
}

function canUseNestedFlatJoin(relArgs: unknown, depth: number): boolean {
  if (depth > 10) return false
  if (!isPlainObject(relArgs)) return true
  if (hasChildPagination(relArgs)) return false

  const obj = relArgs as Record<string, unknown>

  if (obj.include && isPlainObject(obj.include)) {
    for (const childValue of Object.values(
      obj.include as Record<string, unknown>,
    )) {
      if (childValue !== false && !canUseNestedFlatJoin(childValue, depth + 1))
        return false
    }
  }

  if (obj.select && isPlainObject(obj.select)) {
    for (const childValue of Object.values(
      obj.select as Record<string, unknown>,
    )) {
      if (childValue !== false && !canUseNestedFlatJoin(childValue, depth + 1))
        return false
    }
  }

  return true
}

export function canUseFlatJoinForAll(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
  debug?: boolean,
): boolean {
  const modelMap = new Map(schemas.map((m) => [m.name, m]))

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = model.fields.find((f) => f.name === relName)
    if (!field?.isRelation || !field.relatedModel) {
      return false
    }

    if (isPlainObject(value)) {
      const obj = value as Record<string, unknown>
      if ('take' in obj && obj.take != null) {
        return false
      }
      if ('skip' in obj && typeof obj.skip === 'number' && obj.skip > 0) {
        return false
      }
    }

    const relModel = modelMap.get(field.relatedModel)
    if (!relModel) {
      if (debug)
        console.log(
          `    [canFlatJoin] ${model.name}.${relName}: relModel not found for ${field.relatedModel}`,
        )
      return false
    }

    const keys = resolveRelationKeys(field as any, 'include')
    if (!keys || keys.parentKeys.length === 0 || keys.childKeys.length === 0) {
      if (debug)
        console.log(
          `    [canFlatJoin] ${model.name}.${relName}: no join keys resolved`,
        )
      return false
    }

    if (keys.parentKeys.length > 1 || keys.childKeys.length > 1) {
      if (debug)
        console.log(
          `    [canFlatJoin] ${model.name}.${relName}: composite keys (${keys.parentKeys.length} parent, ${keys.childKeys.length} child)`,
        )
      return false
    }

    const nestedSpec = isPlainObject(value)
      ? extractNestedIncludeSpec(value, relModel)
      : {}

    if (Object.keys(nestedSpec).length > 0) {
      if (!canUseFlatJoinForAll(nestedSpec, relModel, schemas, debug)) {
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
): { joins: string[]; selects: string[]; orderBy: string[] } {
  if (depth > 10) {
    throw new Error(
      `Nested joins exceeded maximum depth of 10 at prefix '${prefix}'`,
    )
  }

  const joins: string[] = []
  const selects: string[] = []
  const orderBy: string[] = []

  for (const [relName, relValue] of Object.entries(includeSpec)) {
    if (relValue === false) continue

    const field = parentModel.fields.find((f) => f.name === relName)
    if (!isValidRelationField(field as any)) continue

    const relModel = getRelationModel(parentModel, relName, schemas)
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
      )

      joins.push(...deeper.joins)
      selects.push(...deeper.selects)
      orderBy.push(...deeper.orderBy)
    }
  }

  return { joins, selects, orderBy }
}

function buildSubqueryRawSelect(model: Model, alias: string): string {
  const cols: string[] = []
  for (const f of model.fields) {
    if (f.isRelation) continue
    cols.push(`${alias}.${quoteColumn(model, f.name)}`)
  }
  return cols.length > 0 ? cols.join(SQL_SEPARATORS.FIELD_LIST) : '*'
}

function extractReferencedParams(
  whereClause: string | undefined,
  specParams: ParamStore | readonly unknown[],
): { cleanWhere: string; params: any[] } {
  if (!whereClause || whereClause === '1=1') {
    return { cleanWhere: '', params: [] }
  }

  const refSet = new Set<number>()
  const re = /\$(\d+)/g
  let match: RegExpExecArray | null
  while ((match = re.exec(whereClause)) !== null) {
    refSet.add(Number(match[1]))
  }

  if (refSet.size === 0) {
    return { cleanWhere: whereClause, params: [] }
  }

  const allParams: readonly unknown[] = Array.isArray(specParams)
    ? specParams
    : typeof (specParams as any).snapshot === 'function'
      ? (specParams as ParamStore).snapshot().params
      : []

  const refs = Array.from(refSet).sort((a, b) => a - b)
  const params: any[] = []
  const indexMap = new Map<number, number>()

  for (const oldIdx of refs) {
    params.push(allParams[oldIdx - 1])
    indexMap.set(oldIdx, params.length)
  }

  let cleanWhere = whereClause
  const sorted = Array.from(indexMap.entries()).sort((a, b) => b[0] - a[0])
  for (const [oldIdx, newIdx] of sorted) {
    cleanWhere = cleanWhere.split(`$${oldIdx}`).join(`$${newIdx}`)
  }

  return { cleanWhere, params }
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
    requiresReduction: false,
    includeSpec: {},
  }

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

  if (!canUseFlatJoinForAll(includeSpec, model, schemas)) {
    return emptyResult
  }

  const { cleanWhere, params } = extractReferencedParams(
    whereClause,
    spec.params,
  )

  const baseJoins = whereJoins.length > 0 ? whereJoins.join(' ') : ''
  const baseWhere = cleanWhere ? `WHERE ${cleanWhere}` : ''
  const baseOrderBy = orderBy ? `ORDER BY ${orderBy}` : ''

  const subqueryScalarCols = buildSubqueryRawSelect(model, from.alias)
  let baseSubquery = `
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
    params.push(take)
    baseSubquery += ` LIMIT $${params.length}`
  }
  if (skip !== null) {
    params.push(skip)
    baseSubquery += ` OFFSET $${params.length}`
  }

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

  const pkField = getPrimaryKeyField(model)

  const orderByParts: string[] = []
  orderByParts.push(`${from.alias}.${quoteColumn(model, pkField)} ASC`)
  orderByParts.push(...built.orderBy)

  const finalOrderBy = orderByParts.join(', ')

  const sql = `
    SELECT ${allSelects}
    FROM (${baseSubquery}) ${from.alias}
    ${built.joins.join(' ')}
    ORDER BY ${finalOrderBy}
  `.trim()

  return { sql, params, requiresReduction: true, includeSpec }
}
