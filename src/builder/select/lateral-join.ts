import { Model, Field } from '../../types'
import { SQL_TEMPLATES, LIMITS } from '../shared/constants'
import { quote, buildTableReference, quoteColumn } from '../shared/sql-utils'
import { SelectQuerySpec } from '../shared/types'
import { getFieldIndices } from '../shared/model-field-cache'
import { isPlainObject, isNotNullish } from '../shared/validators/type-guards'
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
import { isValidRelationField } from '../joins'
import { buildWhereClause } from '../where'
import { isValidWhereClause } from '../shared/validators/sql-validators'
import { createAliasGenerator } from '../shared/alias-generator'
import { ParamStore } from '../shared/param-store'
import {
  extractWhereInput,
  buildScalarColumnSelect,
} from '../shared/relation-query-context'
import { isListRelation } from '../shared/field-type-utils'
import { readSkipTake } from '../pagination'
import { maybeReverseNegativeTake } from '../shared/negative-take-utils'
import { resolveIncludeRelations } from '../shared/include-tree-walker'

interface LateralJoinBuildResult {
  sql: string
  params: any[]
  requiresReduction: boolean
  includeSpec: Record<string, any>
  isLateral: boolean
  lateralMeta: LateralRelationMeta[]
}

export interface LateralRelationMeta {
  name: string
  isList: boolean
  fieldTypes: Array<{ fieldName: string; type: string }>
  nestedRelations: LateralRelationMeta[]
}

interface ParamCollector {
  values: any[]
  add(value: unknown): string
}

function createParamCollector(): ParamCollector {
  const values: any[] = []
  return {
    values,
    add(value: unknown): string {
      values.push(value)
      return `$${values.length}`
    },
  }
}

function createParamAdapter(collector: ParamCollector): any {
  return {
    add(value: unknown): string {
      return collector.add(value)
    },
    snapshot() {
      return { params: [...collector.values] }
    },
    get length() {
      return collector.values.length
    },
  }
}

function reindexWhereParams(
  whereClause: string | undefined,
  specParams: ParamStore | readonly unknown[],
  collector: ParamCollector,
): string {
  if (!whereClause || whereClause === '1=1') return ''

  const refSet = new Set<number>()
  const re = /\$(\d+)/g
  let match: RegExpExecArray | null
  while ((match = re.exec(whereClause)) !== null) {
    refSet.add(Number(match[1]))
  }

  if (refSet.size === 0) return whereClause

  const allParams: readonly unknown[] = Array.isArray(specParams)
    ? specParams
    : typeof (specParams as any).snapshot === 'function'
      ? (specParams as ParamStore).snapshot().params
      : []

  const refs = Array.from(refSet).sort((a, b) => a - b)
  const indexMap = new Map<number, number>()

  for (const oldIdx of refs) {
    collector.values.push(allParams[oldIdx - 1])
    indexMap.set(oldIdx, collector.values.length)
  }

  let clean = whereClause
  const sorted = Array.from(indexMap.entries()).sort((a, b) => b[0] - a[0])
  for (const [oldIdx, newIdx] of sorted) {
    clean = clean.split(`$${oldIdx}`).join(`$${newIdx}`)
  }

  return clean
}

function getRelationModel(
  parentModel: Model,
  relationName: string,
  schemas: readonly Model[],
): Model | null {
  const indices = getFieldIndices(parentModel)
  const field = indices.allFieldsByName.get(relationName)
  if (!field?.isRelation || !field.relatedModel) return null
  return schemas.find((m) => m.name === field.relatedModel) ?? null
}

function extractOrderByInput(relArgs: unknown): unknown {
  if (!isPlainObject(relArgs)) return undefined
  const obj = relArgs as Record<string, unknown>
  if ('orderBy' in obj) return obj.orderBy
  return undefined
}

function buildChildOrderBy(
  relModel: Model,
  alias: string,
  orderByInput: unknown,
  pkFields: string[],
): string {
  if (isNotNullish(orderByInput)) {
    const entries = Array.isArray(orderByInput) ? orderByInput : [orderByInput]
    const parts: string[] = []
    for (const entry of entries) {
      if (!isPlainObject(entry)) continue
      for (const [field, dir] of Object.entries(
        entry as Record<string, unknown>,
      )) {
        const direction = String(dir).toUpperCase() === 'DESC' ? 'DESC' : 'ASC'
        parts.push(`${alias}.${quoteColumn(relModel, field)} ${direction}`)
      }
    }
    if (parts.length > 0) return parts.join(', ')
  }
  return pkFields
    .map((f) => `${alias}.${quoteColumn(relModel, f)} ASC`)
    .join(', ')
}

interface LateralBuildResult {
  joinSql: string
  latAlias: string
  meta: LateralRelationMeta
}

interface LateralBuildContext {
  schemas: readonly Model[]
  dialect: SqlDialect
  aliasCounter: { count: number }
  collector: ParamCollector
}

function buildLateralForRelation(
  relationName: string,
  relArgs: unknown,
  field: Field,
  relModel: Model,
  parentModel: Model,
  parentAlias: string,
  ctx: LateralBuildContext,
  depth: number,
): LateralBuildResult | null {
  if (depth > LIMITS.MAX_NESTED_JOIN_DEPTH) return null

  const isList = isListRelation(field)
  const keys = resolveRelationKeys(field as any, 'include')
  if (!keys || keys.childKeys.length === 0 || keys.parentKeys.length === 0)
    return null

  const latAlias = `_l${ctx.aliasCounter.count++}`
  const subAlias = `_s${ctx.aliasCounter.count++}`
  const childAlias = `_c${ctx.aliasCounter.count++}`

  const indices = getFieldIndices(relModel)
  const scalarSel = extractScalarSelection(relArgs, relModel)
  const pkFields = getPrimaryKeyFields(relModel)

  const selectedFields = scalarSel.includeAllScalars
    ? Array.from(indices.scalarFields.keys())
    : [...new Set([...pkFields, ...scalarSel.selectedScalarFields])]

  const nestedSpec = isPlainObject(relArgs)
    ? extractNestedIncludeSpec(relArgs, relModel)
    : {}

  const nestedResults: Array<{ name: string; result: LateralBuildResult }> = []

  for (const [nestedName, nestedValue] of Object.entries(nestedSpec)) {
    if (nestedValue === false) continue
    const nestedIndices = getFieldIndices(relModel)
    const nestedField = nestedIndices.allFieldsByName.get(nestedName)
    if (!nestedField || !isValidRelationField(nestedField as any)) continue

    const nestedModel = getRelationModel(relModel, nestedName, ctx.schemas)
    if (!nestedModel) continue

    const nested = buildLateralForRelation(
      nestedName,
      nestedValue,
      nestedField as Field,
      nestedModel,
      relModel,
      childAlias,
      ctx,
      depth + 1,
    )

    if (nested) {
      nestedResults.push({ name: nestedName, result: nested })
    }
  }

  const innerSelectCols: string[] = []
  for (const fieldName of selectedFields) {
    const f = indices.scalarFields.get(fieldName)
    if (!f) continue
    const colName = f.dbName || f.name
    innerSelectCols.push(`${childAlias}.${quote(colName)} AS ${quote(f.name)}`)
  }

  for (const { name, result } of nestedResults) {
    innerSelectCols.push(
      `${result.latAlias}.data AS ${quote(`__nested_${name}`)}`,
    )
  }

  const nestedJoinsSql = nestedResults.map((n) => n.result.joinSql).join(' ')

  const fkParts = keys.childKeys.map(
    (ck, i) =>
      `${childAlias}.${quoteColumn(relModel, ck)} = ${parentAlias}.${quoteColumn(parentModel, keys.parentKeys[i])}`,
  )
  const fkCondition =
    fkParts.length === 1 ? fkParts[0] : `(${fkParts.join(' AND ')})`

  let childWhereSql = ''
  let childWhereJoinsSql = ''

  const whereInput = extractWhereInput(relArgs)
  if (Object.keys(whereInput).length > 0) {
    const aliasGen = createAliasGenerator()
    const whereResult = buildWhereClause(whereInput, {
      alias: childAlias,
      schemaModels: ctx.schemas as Model[],
      model: relModel,
      params: createParamAdapter(ctx.collector),
      isSubquery: true,
      aliasGen,
      dialect: ctx.dialect,
    })

    if (whereResult.joins.length > 0) {
      childWhereJoinsSql = ' ' + whereResult.joins.join(' ')
    }
    if (isValidWhereClause(whereResult.clause)) {
      childWhereSql = ` AND ${whereResult.clause}`
    }
  }

  const rawOrderBy = extractOrderByInput(relArgs)
  const hasOrderBy = isNotNullish(rawOrderBy)
  const { hasTake, takeVal: rawTakeVal, skipVal } = readSkipTake(relArgs)

  if (!isList && typeof rawTakeVal === 'number' && rawTakeVal < 0) {
    throw new Error('Negative take is only supported for list relations')
  }

  const { takeVal, orderByInput: adjustedOrderBy } = maybeReverseNegativeTake(
    rawTakeVal,
    hasOrderBy,
    rawOrderBy,
  )

  const orderBySql = buildChildOrderBy(
    relModel,
    childAlias,
    adjustedOrderBy,
    pkFields,
  )

  let paginationSql = ''
  if (isNotNullish(takeVal)) {
    paginationSql += ` LIMIT ${ctx.collector.add(takeVal)}`
  }
  if (isNotNullish(skipVal)) {
    paginationSql += ` OFFSET ${ctx.collector.add(skipVal)}`
  }

  const relTable = buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    relModel.tableName,
    ctx.dialect,
  )

  const innerSql =
    `SELECT ${innerSelectCols.join(', ')}` +
    ` FROM ${relTable} ${childAlias}` +
    childWhereJoinsSql +
    (nestedJoinsSql ? ` ${nestedJoinsSql}` : '') +
    ` WHERE ${fkCondition}${childWhereSql}` +
    ` ORDER BY ${orderBySql}` +
    paginationSql

  const jsonScalarParts: string[] = []
  for (const fieldName of selectedFields) {
    const f = indices.scalarFields.get(fieldName)
    if (!f) continue
    jsonScalarParts.push(`'${f.name}', ${subAlias}.${quote(f.name)}`)
  }

  const jsonNestedParts: string[] = nestedResults.map(
    ({ name }) => `'${name}', ${subAlias}.${quote(`__nested_${name}`)}`,
  )

  const jsonExpr = `json_build_object(${[...jsonScalarParts, ...jsonNestedParts].join(', ')})`

  let outerSql: string
  if (isList) {
    const aggOrderParts = pkFields
      .map((f) => `${subAlias}.${quote(f)} ASC`)
      .join(', ')
    outerSql =
      `SELECT coalesce(json_agg(${jsonExpr} ORDER BY ${aggOrderParts}), '[]'::json) AS data` +
      ` FROM (${innerSql}) ${subAlias}`
  } else {
    outerSql = `SELECT ${jsonExpr} AS data FROM (${innerSql}) ${subAlias}`
  }

  const joinSql = `LEFT JOIN LATERAL (${outerSql}) ${latAlias} ON true`

  const fieldTypes = selectedFields
    .map((fieldName) => {
      const f = indices.scalarFields.get(fieldName)
      if (!f) return null
      return {
        fieldName: f.name,
        type: String((f as any).type ?? '').toLowerCase(),
      }
    })
    .filter(Boolean) as LateralRelationMeta['fieldTypes']

  const meta: LateralRelationMeta = {
    name: relationName,
    isList,
    fieldTypes,
    nestedRelations: nestedResults.map((n) => n.result.meta),
  }

  return { joinSql, latAlias, meta }
}

function countActiveEntries(spec: Record<string, any>): number {
  let count = 0
  for (const value of Object.values(spec)) {
    if (value !== false) count++
  }
  return count
}

export function canUseLateralJoin(
  includeSpec: Record<string, any>,
  parentModel: Model,
  schemas: readonly Model[],
): boolean {
  const relations = resolveIncludeRelations(includeSpec, parentModel, schemas)

  if (relations.length < countActiveEntries(includeSpec)) {
    return false
  }

  for (const rel of relations) {
    const keys = resolveRelationKeys(rel.field as any, 'include')
    if (!keys || keys.childKeys.length === 0 || keys.parentKeys.length === 0)
      return false

    if (Object.keys(rel.nestedSpec).length > 0) {
      if (!canUseLateralJoin(rel.nestedSpec, rel.relModel, schemas))
        return false
    }
  }

  return true
}

export function buildLateralJoinSql(
  spec: SelectQuerySpec,
): LateralJoinBuildResult {
  const {
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

  const emptyResult: LateralJoinBuildResult = {
    sql: '',
    params: [],
    requiresReduction: false,
    includeSpec: {},
    isLateral: false,
    lateralMeta: [],
  }

  const entries = extractRelationEntries(args, model)
  const includeSpec: Record<string, any> = {}
  for (const e of entries) {
    includeSpec[e.name] = e.value
  }

  if (Object.keys(includeSpec).length === 0) return emptyResult

  const collector = createParamCollector()

  const cleanWhere = reindexWhereParams(whereClause, spec.params, collector)

  const parentScalarCols = buildScalarColumnSelect(model, from.alias)
  const baseJoins = whereJoins.length > 0 ? ` ${whereJoins.join(' ')}` : ''
  const baseWhere = cleanWhere ? ` WHERE ${cleanWhere}` : ''
  const baseOrderBy = orderBy ? ` ORDER BY ${orderBy}` : ''

  let parentSub =
    `SELECT ${parentScalarCols} FROM ${from.table} ${from.alias}` +
    baseJoins +
    baseWhere +
    baseOrderBy

  const take =
    pagination.take !== undefined && pagination.take !== null
      ? pagination.take
      : null
  const skip =
    pagination.skip !== undefined && pagination.skip !== null
      ? pagination.skip
      : null

  if (take !== null) {
    parentSub += ` LIMIT ${collector.add(take)}`
  }
  if (skip !== null) {
    parentSub += ` OFFSET ${collector.add(skip)}`
  }

  const aliasCounter = { count: 0 }
  const ctx: LateralBuildContext = {
    schemas,
    dialect,
    aliasCounter,
    collector,
  }

  const lateralJoins: string[] = []
  const lateralSelects: string[] = []
  const lateralMeta: LateralRelationMeta[] = []

  for (const [relName, relValue] of Object.entries(includeSpec)) {
    if (relValue === false) continue

    const indices = getFieldIndices(model)
    const field = indices.allFieldsByName.get(relName)
    if (!field || !isValidRelationField(field as any)) continue

    const relModel = getRelationModel(model, relName, schemas)
    if (!relModel) continue

    const result = buildLateralForRelation(
      relName,
      relValue,
      field as Field,
      relModel,
      model,
      from.alias,
      ctx,
      0,
    )

    if (!result) continue

    lateralJoins.push(result.joinSql)
    lateralSelects.push(`${result.latAlias}.data AS ${quote(relName)}`)
    lateralMeta.push(result.meta)
  }

  if (lateralJoins.length === 0) return emptyResult

  const baseSelect = (spec.select ?? '').trim()
  const allSelects = [baseSelect, ...lateralSelects]
    .filter((s) => s && s.trim().length > 0)
    .join(', ')

  if (!allSelects) {
    return emptyResult
  }

  const pkField = getPrimaryKeyField(model)
  const pkOrder = `${from.alias}.${quoteColumn(model, pkField)} ASC`

  const sql =
    `SELECT ${allSelects}` +
    ` FROM (${parentSub}) ${from.alias}` +
    ` ${lateralJoins.join(' ')}` +
    ` ORDER BY ${pkOrder}`

  return {
    sql: sql.trim(),
    params: collector.values,
    requiresReduction: true,
    includeSpec,
    isLateral: true,
    lateralMeta,
  }
}
