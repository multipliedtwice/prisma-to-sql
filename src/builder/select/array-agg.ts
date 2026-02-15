import { Model, Field } from '../../types'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { quote, buildTableReference, quoteColumn } from '../shared/sql-utils'
import { joinCondition, isValidRelationField } from '../joins'
import { SelectQuerySpec } from '../shared/types'
import { getFieldIndices } from '../shared/model-field-cache'
import { appendPagination } from './assembly'
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
import { resolveRelationKeys } from '../shared/relation-key-utils'
import { buildWhereClause } from '../where'
import { isValidWhereClause } from '../shared/validators/sql-validators'
import { createAliasGenerator } from '../shared/alias-generator'

export interface ArrayAggBuildResult {
  sql: string
  requiresReduction: boolean
  includeSpec: Record<string, any>
  isArrayAgg: boolean
}

export function canUseArrayAggForAll(
  includeSpec: Record<string, any>,
  parentModel: Model,
  schemas: readonly Model[],
): boolean {
  const modelMap = new Map(schemas.map((m) => [m.name, m]))

  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue

    const field = parentModel.fields.find((f) => f.name === relName)
    if (!field || !field.isRelation) continue

    if (isPlainObject(value) && hasChildPagination(value)) return false

    const relModel = modelMap.get(field.relatedModel!)
    if (!relModel) continue

    const nestedSpec = extractNestedIncludeSpec(value, relModel)
    if (Object.keys(nestedSpec).length > 0) return false
  }

  return true
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

function buildSubqueryRawSelect(model: Model, alias: string): string {
  const cols: string[] = []
  for (const f of model.fields) {
    if (f.isRelation) continue
    cols.push(`${alias}.${quoteColumn(model, f.name)}`)
  }
  return cols.length > 0 ? cols.join(SQL_SEPARATORS.FIELD_LIST) : '*'
}

interface ArrayAggRelationBuild {
  joinSql: string
  selectExprs: string[]
  relationName: string
  isList: boolean
  scalarFieldNames: string[]
}

function readWhereInput(relArgs: unknown): Record<string, unknown> {
  if (!isPlainObject(relArgs)) return {}
  const obj = relArgs as Record<string, unknown>
  if (!('where' in obj)) return {}
  const w = obj.where
  return isPlainObject(w) ? w : {}
}

function buildArrayAggRelation(args: {
  relationName: string
  relArgs: unknown
  field: Field
  relModel: Model
  parentModel: Model
  parentAlias: string
  schemas: readonly Model[]
  dialect: SqlDialect
  aliasCounter: { count: number }
  params: SelectQuerySpec['params']
}): ArrayAggRelationBuild | null {
  const {
    relationName,
    relArgs,
    field,
    relModel,
    parentModel,
    parentAlias,
    schemas,
    dialect,
    aliasCounter,
    params,
  } = args

  const isList = typeof field.type === 'string' && field.type.endsWith('[]')
  const { childKeys: relKeyFields, parentKeys: parentKeyFields } =
    resolveRelationKeys(field, 'include')

  if (relKeyFields.length === 0) return null

  const innerAlias = `__aa_r${aliasCounter.count++}`
  const joinAlias = `__aa_j${aliasCounter.count++}`

  const indices = getFieldIndices(relModel)
  const scalarSel = extractScalarSelection(relArgs, relModel)
  const pkFields = getPrimaryKeyFields(relModel)

  const selectedFields = scalarSel.includeAllScalars
    ? Array.from(indices.scalarFields.keys())
    : [...new Set([...pkFields, ...scalarSel.selectedScalarFields])]

  const pkOrderExpr = pkFields
    .map((f) => `${innerAlias}.${quoteColumn(relModel, f)}`)
    .join(SQL_SEPARATORS.FIELD_LIST)

  const pkFilterExpr = `${innerAlias}.${quoteColumn(relModel, pkFields[0])}`

  const fkSelectParts = relKeyFields.map(
    (f, i) => `${innerAlias}.${quoteColumn(relModel, f)} AS "__fk${i}"`,
  )

  const aggParts = selectedFields
    .map((fieldName) => {
      const f = indices.scalarFields.get(fieldName)
      if (!f) return null
      const colRef = `${innerAlias}.${quoteColumn(relModel, fieldName)}`
      const alias = `"${relationName}.${f.name}"`
      return `array_agg(${colRef} ORDER BY ${pkOrderExpr}) FILTER (WHERE ${pkFilterExpr} IS NOT NULL) AS ${alias}`
    })
    .filter(Boolean)

  const fkGroupByParts = relKeyFields.map(
    (f) => `${innerAlias}.${quoteColumn(relModel, f)}`,
  )

  const relTable = buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    relModel.tableName,
    dialect,
  )

  const whereInput = readWhereInput(relArgs)
  let whereJoinsSql = ''
  let whereClauseSql = ''

  if (Object.keys(whereInput).length > 0) {
    const aliasGen = createAliasGenerator()
    const whereResult = buildWhereClause(whereInput, {
      alias: innerAlias,
      schemaModels: schemas as Model[],
      model: relModel,
      params,
      isSubquery: true,
      aliasGen,
      dialect,
    })

    if (whereResult.joins.length > 0) {
      whereJoinsSql = ' ' + whereResult.joins.join(' ')
    }
    if (isValidWhereClause(whereResult.clause)) {
      whereClauseSql = ` ${SQL_TEMPLATES.WHERE} ${whereResult.clause}`
    }
  }

  const subquery =
    `SELECT ${fkSelectParts.join(SQL_SEPARATORS.FIELD_LIST)}${SQL_SEPARATORS.FIELD_LIST}` +
    `${aggParts.join(SQL_SEPARATORS.FIELD_LIST)}` +
    ` FROM ${relTable} ${innerAlias}${whereJoinsSql}${whereClauseSql}` +
    ` GROUP BY ${fkGroupByParts.join(SQL_SEPARATORS.FIELD_LIST)}`

  const onParts = parentKeyFields.map(
    (f, i) =>
      `${joinAlias}."__fk${i}" = ${parentAlias}.${quoteColumn(parentModel, f)}`,
  )
  const onCondition =
    onParts.length === 1 ? onParts[0] : `(${onParts.join(' AND ')})`

  const joinSql = `LEFT JOIN (${subquery}) ${joinAlias} ON ${onCondition}`

  const selectExprs = selectedFields
    .map((fieldName) => {
      const f = indices.scalarFields.get(fieldName)
      if (!f) return null
      return `${joinAlias}."${relationName}.${f.name}"`
    })
    .filter(Boolean) as string[]

  return {
    joinSql,
    selectExprs,
    relationName,
    isList,
    scalarFieldNames: selectedFields,
  }
}

export function buildArrayAggSql(spec: SelectQuerySpec): ArrayAggBuildResult {
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
    params,
  } = spec

  const entries = extractRelationEntries(args, model)
  const includeSpec: Record<string, any> = {}
  for (const e of entries) {
    includeSpec[e.name] = e.value
  }

  if (Object.keys(includeSpec).length === 0) {
    return {
      sql: '',
      requiresReduction: false,
      includeSpec: {},
      isArrayAgg: false,
    }
  }

  if (!canUseArrayAggForAll(includeSpec, model, schemas)) {
    return {
      sql: '',
      requiresReduction: false,
      includeSpec: {},
      isArrayAgg: false,
    }
  }

  const baseJoins = whereJoins.length > 0 ? whereJoins.join(' ') : ''
  const baseWhere =
    whereClause && whereClause !== '1=1' ? `WHERE ${whereClause}` : ''
  const baseOrderBy = orderBy ? `ORDER BY ${orderBy}` : ''

  const subqueryScalarCols = buildSubqueryRawSelect(model, from.alias)
  let baseSubquery =
    `SELECT ${subqueryScalarCols} FROM ${from.table} ${from.alias}` +
    (baseJoins ? ` ${baseJoins}` : '') +
    (baseWhere ? ` ${baseWhere}` : '') +
    (baseOrderBy ? ` ${baseOrderBy}` : '')

  baseSubquery = appendPagination(baseSubquery.trim(), spec)

  const aliasCounter = { count: 0 }
  const joins: string[] = []
  const arraySelectExprs: string[] = []

  for (const [relName, relValue] of Object.entries(includeSpec)) {
    if (relValue === false) continue

    const field = model.fields.find((f) => f.name === relName)
    if (!field || !isValidRelationField(field)) continue

    const relModel = getRelationModel(model, relName, schemas)

    const built = buildArrayAggRelation({
      relationName: relName,
      relArgs: relValue,
      field,
      relModel,
      parentModel: model,
      parentAlias: from.alias,
      schemas,
      dialect,
      aliasCounter,
      params,
    })

    if (!built) continue

    joins.push(built.joinSql)
    arraySelectExprs.push(...built.selectExprs)
  }

  if (joins.length === 0) {
    return {
      sql: '',
      requiresReduction: false,
      includeSpec: {},
      isArrayAgg: false,
    }
  }

  const baseSelect = (select ?? '').trim()
  const allSelects = [baseSelect, ...arraySelectExprs]
    .filter((s) => s && s.trim().length > 0)
    .join(SQL_SEPARATORS.FIELD_LIST)

  if (!allSelects) {
    throw new Error('Array-agg SELECT requires at least one selected field')
  }

  const pkField = getPrimaryKeyField(model)
  const pkOrder = `${from.alias}.${quoteColumn(model, pkField)} ASC`

  const sql = `
    SELECT ${allSelects}
    FROM (${baseSubquery}) ${from.alias}
    ${joins.join(' ')}
    ORDER BY ${pkOrder}
  `.trim()

  return { sql, requiresReduction: true, includeSpec, isArrayAgg: true }
}
