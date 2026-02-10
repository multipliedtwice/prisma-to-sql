import { Model } from '../../types'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { quote, buildTableReference, quoteColumn } from '../shared/sql-utils'
import { joinCondition, isValidRelationField } from '../joins'
import { SelectQuerySpec } from '../shared/types'
import {
  getScalarFieldSet,
  getRelationFieldSet,
} from '../shared/model-field-cache'
import { appendPagination } from './assembly'
import { isPlainObject } from '../shared/validators/type-guards'
import { SqlDialect } from '../../sql-builder-dialect'

export interface FlatJoinBuildResult {
  sql: string
  requiresReduction: boolean
  includeSpec: Record<string, any>
}

function getPrimaryKeyField(model: Model): string {
  const scalarSet = getScalarFieldSet(model)

  for (const f of model.fields) {
    if (f.isId && !f.isRelation && scalarSet.has(f.name)) {
      return f.name
    }
  }

  if (scalarSet.has('id')) return 'id'

  throw new Error(
    `Model ${model.name} has no primary key field. Models must have either a field with isId=true or a field named 'id'.`,
  )
}

function findPrimaryKeyFields(model: Model): string[] {
  const pkFields = model.fields.filter((f) => f.isId && !f.isRelation)
  if (pkFields.length > 0) return pkFields.map((f) => f.name)

  const idField = model.fields.find((f) => f.name === 'id' && !f.isRelation)
  if (idField) return ['id']

  throw new Error(`Model ${model.name} has no primary key field`)
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

function extractIncludeSpecFromArgs(
  args: SelectQuerySpec['args'],
  model: Model,
): Record<string, any> {
  const includeSpec: Record<string, any> = {}
  const relationSet = getRelationFieldSet(model)

  if (args.include && isPlainObject(args.include)) {
    for (const [key, value] of Object.entries(args.include)) {
      if (value !== false) includeSpec[key] = value
    }
  }

  if (args.select && isPlainObject(args.select)) {
    for (const [key, value] of Object.entries(args.select)) {
      if (!relationSet.has(key)) continue
      if (value === false) continue
      if (value === true) {
        includeSpec[key] = true
        continue
      }
      if (isPlainObject(value)) {
        const v = value as Record<string, unknown>
        if (isPlainObject(v.include) || isPlainObject(v.select)) {
          includeSpec[key] = value
        }
      }
    }
  }

  return includeSpec
}

function hasChildPagination(relArgs: unknown): boolean {
  if (!isPlainObject(relArgs)) return false
  const args = relArgs as Record<string, unknown>
  if (args.take !== undefined && args.take !== null) return true
  if (args.skip !== undefined && args.skip !== null) return true
  return false
}

function extractNestedIncludeSpec(
  relArgs: unknown,
  relModel: Model,
): Record<string, any> {
  const relationSet = getRelationFieldSet(relModel)
  const out: Record<string, any> = {}

  if (!isPlainObject(relArgs)) return out
  const obj = relArgs as Record<string, unknown>

  if (isPlainObject(obj.include)) {
    for (const [k, v] of Object.entries(
      obj.include as Record<string, unknown>,
    )) {
      if (!relationSet.has(k)) continue
      if (v === false) continue
      out[k] = v
    }
  }

  if (isPlainObject(obj.select)) {
    for (const [k, v] of Object.entries(
      obj.select as Record<string, unknown>,
    )) {
      if (!relationSet.has(k)) continue
      if (v === false) continue
      if (v === true) {
        out[k] = true
        continue
      }
      if (isPlainObject(v)) {
        const vv = v as Record<string, unknown>
        if (isPlainObject(vv.include) || isPlainObject(vv.select)) {
          out[k] = v
        }
      }
    }
  }

  return out
}

function extractSelectedScalarFields(
  relArgs: unknown,
  relModel: Model,
): { includeAllScalars: boolean; selected: string[] } {
  const scalarFields = relModel.fields
    .filter((f) => !f.isRelation)
    .map((f) => f.name)
  const scalarSet = new Set(scalarFields)

  if (relArgs === true || !isPlainObject(relArgs)) {
    return { includeAllScalars: true, selected: scalarFields }
  }

  const obj = relArgs as Record<string, unknown>
  if (!isPlainObject(obj.select)) {
    return { includeAllScalars: true, selected: scalarFields }
  }

  const sel = obj.select as Record<string, unknown>
  const selected: string[] = []
  for (const [k, v] of Object.entries(sel)) {
    if (!scalarSet.has(k)) continue
    if (v === true) selected.push(k)
  }

  return { includeAllScalars: false, selected }
}

function uniqPreserveOrder(items: string[]): string[] {
  const seen = new Set<string>()
  const out: string[] = []
  for (const it of items) {
    if (seen.has(it)) continue
    seen.add(it)
    out.push(it)
  }
  return out
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

  const pkFields = findPrimaryKeyFields(relModel)
  const scalarSelection = extractSelectedScalarFields(relArgs, relModel)
  const selectedScalar = scalarSelection.selected

  const required = uniqPreserveOrder([...pkFields, ...selectedScalar])

  const columns: string[] = []
  for (const fieldName of required) {
    const field = relModel.fields.find(
      (f) => f.name === fieldName && !f.isRelation,
    )
    if (!field) continue

    const colName = field.dbName || field.name
    const quotedCol = quote(colName)

    columns.push(`${childAlias}.${quotedCol} AS "${fullPrefix}.${field.name}"`)
  }

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
): boolean {
  for (const value of Object.values(includeSpec)) {
    if (value === false) continue
    if (!canUseNestedFlatJoin(value, 0)) return false
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
  aliasCounter: { count: number },
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
    if (!isValidRelationField(field)) continue

    const relModel = getRelationModel(parentModel, relName, schemas)
    const relTable = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      relModel.tableName,
      dialect,
    )

    const childAlias = `fj_${aliasCounter.count++}`
    const joinCond = joinCondition(
      field,
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

    const childPkFields = findPrimaryKeyFields(relModel)
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
  } = spec

  const includeSpec = extractIncludeSpecFromArgs(args, model)

  if (Object.keys(includeSpec).length === 0) {
    return { sql: '', requiresReduction: false, includeSpec: {} }
  }

  if (!canUseFlatJoinForAll(includeSpec)) {
    return { sql: '', requiresReduction: false, includeSpec: {} }
  }

  const baseJoins = whereJoins.length > 0 ? whereJoins.join(' ') : ''
  const baseWhere =
    whereClause && whereClause !== '1=1' ? `WHERE ${whereClause}` : ''
  const baseOrderBy = orderBy ? `ORDER BY ${orderBy}` : ''

  let baseSubquery = `
    SELECT * FROM ${from.table} ${from.alias}
    ${baseJoins}
    ${baseWhere}
    ${baseOrderBy}
  `.trim()

  baseSubquery = appendPagination(baseSubquery, spec)

  const aliasCounter = { count: 0 }
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
    return { sql: '', requiresReduction: false, includeSpec: {} }
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
  if (orderBy) orderByParts.push(orderBy)
  orderByParts.push(`${from.alias}.${quoteColumn(model, pkField)} ASC`)
  orderByParts.push(...built.orderBy)

  const finalOrderBy = orderByParts.join(', ')

  const sql = `
    SELECT ${allSelects}
    FROM (${baseSubquery}) ${from.alias}
    ${built.joins.join(' ')}
    ORDER BY ${finalOrderBy}
  `.trim()

  return { sql, requiresReduction: true, includeSpec }
}
