import { PrismaQueryArgs, Model } from '../types'
import {
  SQL_TEMPLATES,
  SQL_SEPARATORS,
  Ops,
  LogicalOps,
} from './shared/constants'
import {
  assertSafeAlias,
  assertSafeTableRef,
  col,
  quote,
} from './shared/sql-utils'
import { createParamStore, ParamStore } from './shared/param-store'
import { WhereClauseResult, SqlResult } from './shared/types'
import {
  isValidWhereClause,
  validateSelectQuery,
  validateParamConsistency,
} from './shared/validators/sql-validators'
import {
  isNotNullish,
  isNonEmptyArray,
  isPlainObject,
} from './shared/validators/type-guards'
import {
  SqlDialect,
  getGlobalDialect,
  inArray,
  notInArray,
  prepareArrayParam,
} from '../sql-builder-dialect'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import { normalizeSkipLike } from './pagination'
import { addAutoScoped } from './shared/dynamic-params'
import { buildNotComposite } from './where/operators-scalar'

type AggregateKey = '_count' | '_sum' | '_avg' | '_min' | '_max'
type LogicalKey = 'AND' | 'OR' | 'NOT'
type ScalarFieldInfo = { name: string; type: string; isRelation: boolean }

const MODEL_FIELD_CACHE = new WeakMap<Model, Map<string, ScalarFieldInfo>>()
const NUMERIC_TYPES = new Set(['Int', 'Float', 'Decimal', 'BigInt'])
const AGGREGATES: ReadonlyArray<[AggregateKey, string]> = [
  ['_sum', 'SUM'],
  ['_avg', 'AVG'],
  ['_min', 'MIN'],
  ['_max', 'MAX'],
]
const COMPARISON_OPS: Record<string, string> = {
  [Ops.EQUALS]: '=',
  [Ops.NOT]: '<>',
  [Ops.GT]: '>',
  [Ops.GTE]: '>=',
  [Ops.LT]: '<',
  [Ops.LTE]: '<=',
}

function getModelFieldMap(model: Model): Map<string, ScalarFieldInfo> {
  const cached = MODEL_FIELD_CACHE.get(model)
  if (cached) return cached

  const m = new Map<string, ScalarFieldInfo>()
  for (const f of model.fields) {
    m.set(f.name, { name: f.name, type: f.type, isRelation: !!f.isRelation })
  }
  MODEL_FIELD_CACHE.set(model, m)
  return m
}

function isTruthySelection(v: unknown): boolean {
  return v === true
}

function aggExprForField(
  aggKey: AggregateKey,
  field: string,
  alias: string,
): string {
  if (aggKey === '_count') {
    return field === '_all' ? `COUNT(*)` : `COUNT(${col(alias, field)})`
  }
  if (field === '_all') {
    throw new Error(`'${aggKey}' does not support '_all'`)
  }
  if (aggKey === '_sum') return `SUM(${col(alias, field)})`
  if (aggKey === '_avg') return `AVG(${col(alias, field)})`
  if (aggKey === '_min') return `MIN(${col(alias, field)})`
  return `MAX(${col(alias, field)})`
}

function buildComparisonOp(op: string): string {
  const sqlOp = COMPARISON_OPS[op]
  if (!sqlOp) {
    throw new Error(`Unsupported HAVING operator: ${op}`)
  }
  return sqlOp
}

function addHavingParam(
  params: ParamStore,
  op: string,
  value: unknown,
): string {
  return addAutoScoped(params, value, `having.${op}`)
}

function normalizeLogicalValue(
  operator: LogicalKey,
  value: unknown,
): Record<string, unknown>[] {
  if (Array.isArray(value)) {
    const out: Record<string, unknown>[] = []
    for (const v of value) {
      if (!isPlainObject(v)) {
        throw new Error(`${operator} entries must be objects in HAVING`)
      }
      out.push(v)
    }
    return out
  }

  if (isPlainObject(value)) {
    return [value]
  }

  throw new Error(`${operator} must be an object or array of objects in HAVING`)
}

function assertScalarField(
  model: Model,
  fieldName: string,
  ctx: string,
): { name: string; type: string } {
  const m = getModelFieldMap(model)
  const field = m.get(fieldName)
  if (!field) {
    throw new Error(
      `${ctx} references unknown field '${fieldName}' on model ${model.name}. ` +
        `Available fields: ${model.fields.map((f) => f.name).join(', ')}`,
    )
  }
  if (field.isRelation) {
    throw new Error(`${ctx} does not support relation field '${fieldName}'`)
  }
  return { name: field.name, type: field.type }
}

function assertAggregateFieldType(
  aggKey: AggregateKey,
  fieldType: string,
  fieldName: string,
  modelName: string,
): void {
  const baseType = fieldType.replace(/\[\]|\?/g, '')

  if (
    (aggKey === '_sum' || aggKey === '_avg') &&
    !NUMERIC_TYPES.has(baseType)
  ) {
    throw new Error(
      `Cannot use ${aggKey} on non-numeric field '${fieldName}' (type: ${fieldType}) on model ${modelName}`,
    )
  }
}

function buildNullComparison(expr: string, op: string): string {
  if (op === Ops.EQUALS) return `${expr} ${SQL_TEMPLATES.IS_NULL}`
  if (op === Ops.NOT) return `${expr} ${SQL_TEMPLATES.IS_NOT_NULL}`
  throw new Error(`Operator '${op}' doesn't support null in HAVING`)
}

function buildInComparison(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
): string {
  if (isDynamicParameter(val)) {
    const placeholder = addHavingParam(params, op, val)
    return op === Ops.IN
      ? inArray(expr, placeholder, dialect)
      : notInArray(expr, placeholder, dialect)
  }

  if (!Array.isArray(val)) {
    throw new Error(`HAVING '${op}' requires array value`)
  }

  if (val.length === 0) {
    return op === Ops.IN ? '0=1' : '1=1'
  }

  const paramValue = prepareArrayParam(val, dialect)
  const placeholder = params.add(paramValue)
  return op === Ops.IN
    ? inArray(expr, placeholder, dialect)
    : notInArray(expr, placeholder, dialect)
}

function buildBinaryComparison(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
): string {
  const sqlOp = buildComparisonOp(op)
  const placeholder = addHavingParam(params, op, val)
  return `${expr} ${sqlOp} ${placeholder}`
}

function buildSimpleComparison(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
): string {
  if (val === null) return buildNullComparison(expr, op)

  if (op === Ops.NOT && isPlainObject(val)) {
    return buildNotComposite(
      expr,
      val,
      params,
      dialect,
      buildSimpleComparison,
      SQL_SEPARATORS.CONDITION_AND,
    )
  }

  if (op === Ops.IN || op === Ops.NOT_IN) {
    return buildInComparison(expr, op, val, params, dialect)
  }

  return buildBinaryComparison(expr, op, val, params)
}

function isLogicalKey(key: string): key is LogicalKey {
  return (
    key === LogicalOps.AND || key === LogicalOps.OR || key === LogicalOps.NOT
  )
}

function isAggregateKey(key: string): key is AggregateKey {
  return (
    key === '_count' ||
    key === '_sum' ||
    key === '_avg' ||
    key === '_min' ||
    key === '_max'
  )
}

function negateClauses(subClauses: string[]): string {
  if (subClauses.length === 1) return `${SQL_TEMPLATES.NOT} ${subClauses[0]}`
  return `${SQL_TEMPLATES.NOT} (${subClauses.join(SQL_SEPARATORS.CONDITION_AND)})`
}

function combineLogical(key: LogicalKey, subClauses: string[]): string {
  if (key === LogicalOps.NOT) return negateClauses(subClauses)
  return subClauses.join(` ${key} `)
}

function buildLogicalClause(
  key: LogicalKey,
  value: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string {
  const items = normalizeLogicalValue(key, value)
  const subClauses: string[] = []

  for (const it of items) {
    const c = buildHavingNode(it, alias, params, dialect, model)
    if (c && c !== '') subClauses.push(`(${c})`)
  }

  if (subClauses.length === 0) return ''
  return combineLogical(key, subClauses)
}

function buildHavingEntry(
  key: string,
  value: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string[] {
  if (isLogicalKey(key)) {
    const logical = buildLogicalClause(
      key,
      value,
      alias,
      params,
      dialect,
      model,
    )
    return logical ? [logical] : []
  }

  if (isAggregateKey(key)) {
    return buildHavingForAggregateFirstShape(
      key,
      value,
      alias,
      params,
      dialect,
      model,
    )
  }

  return buildHavingForFieldFirstShape(
    key,
    value,
    alias,
    params,
    dialect,
    model,
  )
}

function buildHavingNode(
  node: Record<string, unknown>,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string {
  const clauses: string[] = []

  for (const [key, value] of Object.entries(node)) {
    const built = buildHavingEntry(key, value, alias, params, dialect, model)
    for (const c of built) {
      if (c && c.trim().length > 0) clauses.push(c)
    }
  }

  return clauses.join(SQL_SEPARATORS.CONDITION_AND)
}

function assertHavingAggTarget(
  aggKey: AggregateKey,
  field: string,
  model: Model,
): void {
  if (field === '_all') {
    if (aggKey !== '_count') {
      throw new Error(`HAVING '${aggKey}' does not support '_all'`)
    }
    return
  }

  const f = assertScalarField(model, field, 'HAVING')
  assertAggregateFieldType(aggKey, f.type, f.name, model.name)
}

function buildHavingOpsForExpr(
  expr: string,
  filter: Record<string, unknown>,
  params: ParamStore,
  dialect: SqlDialect,
): string[] {
  const out: string[] = []
  for (const [op, val] of Object.entries(filter)) {
    if (op === 'mode') continue
    const built = buildSimpleComparison(expr, op, val, params, dialect)
    if (built && built.trim().length > 0) out.push(built)
  }
  return out
}

function buildHavingForAggregateFirstShape(
  aggKey: AggregateKey,
  target: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string[] {
  if (!isPlainObject(target)) return []

  const out: string[] = []

  for (const [field, filter] of Object.entries(target)) {
    assertHavingAggTarget(aggKey, field, model)
    if (!isPlainObject(filter) || Object.keys(filter).length === 0) continue

    const expr = aggExprForField(aggKey, field, alias)
    out.push(...buildHavingOpsForExpr(expr, filter, params, dialect))
  }

  return out
}

function buildHavingForFieldFirstShape(
  fieldName: string,
  target: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string[] {
  if (!isPlainObject(target)) return []

  const field = assertScalarField(model, fieldName, 'HAVING')
  const out: string[] = []
  const obj = target

  const keys: AggregateKey[] = ['_count', '_sum', '_avg', '_min', '_max']
  for (const aggKey of keys) {
    const aggFilter = obj[aggKey]
    if (!isPlainObject(aggFilter)) continue

    assertAggregateFieldType(aggKey, field.type, field.name, model.name)

    const entries = Object.entries(aggFilter)
    if (entries.length === 0) continue

    const expr = aggExprForField(aggKey, fieldName, alias)
    for (const [op, val] of entries) {
      if (op === 'mode') continue
      const built = buildSimpleComparison(expr, op, val, params, dialect)
      if (built && built.trim().length > 0) out.push(built)
    }
  }

  return out
}

function buildHavingClause(
  having: Record<string, unknown> | undefined,
  alias: string,
  params: ParamStore,
  model: Model,
  dialect?: SqlDialect,
): string {
  if (!isNotNullish(having)) return ''

  const d = dialect ?? getGlobalDialect()
  if (!isPlainObject(having)) return ''

  return buildHavingNode(having, alias, params, d, model)
}

function normalizeCountArg(
  v: unknown,
): Record<string, unknown> | true | undefined {
  if (!isNotNullish(v)) return undefined
  if (v === true) return true
  if (isPlainObject(v)) return v
  return undefined
}

function pushCountAllField(fields: string[]): void {
  fields.push(
    `${SQL_TEMPLATES.COUNT_ALL} ${SQL_TEMPLATES.AS} ${quote('_count._all')}`,
  )
}

function assertCountableScalarField(
  fieldMap: Map<string, ScalarFieldInfo>,
  model: Model,
  fieldName: string,
): void {
  const field = fieldMap.get(fieldName)
  if (!field) {
    throw new Error(
      `Field '${fieldName}' does not exist on model ${model.name}`,
    )
  }
  if (field.isRelation) {
    throw new Error(
      `Cannot use _count on relation field '${fieldName}' on model ${model.name}`,
    )
  }
}

function pushCountField(
  fields: string[],
  alias: string,
  fieldName: string,
): void {
  const outAlias = `_count.${fieldName}`
  fields.push(
    `COUNT(${col(alias, fieldName)}) ${SQL_TEMPLATES.AS} ${quote(outAlias)}`,
  )
}

function addCountFields(
  fields: string[],
  countArg: Record<string, unknown> | true | undefined,
  alias: string,
  model: Model,
  fieldMap: Map<string, ScalarFieldInfo>,
): void {
  if (!isNotNullish(countArg)) return

  if (countArg === true) {
    pushCountAllField(fields)
    return
  }

  if (!isPlainObject(countArg)) return

  if (countArg._all === true) {
    pushCountAllField(fields)
  }

  const selected = Object.entries(countArg).filter(
    ([f, v]) => f !== '_all' && isTruthySelection(v),
  )

  for (const [f] of selected) {
    assertCountableScalarField(fieldMap, model, f)
    pushCountField(fields, alias, f)
  }
}

function getAggregateSelectionObject(
  args: PrismaQueryArgs,
  agg: AggregateKey,
): Record<string, unknown> | undefined {
  const obj = (args as Record<string, unknown>)[agg]
  return isPlainObject(obj) ? obj : undefined
}

function assertAggregatableScalarField(
  fieldMap: Map<string, ScalarFieldInfo>,
  model: Model,
  agg: AggregateKey,
  fieldName: string,
): ScalarFieldInfo {
  const field = fieldMap.get(fieldName)
  if (!field) {
    throw new Error(
      `Field '${fieldName}' does not exist on model ${model.name}`,
    )
  }
  if (field.isRelation) {
    throw new Error(
      `Cannot use ${agg} on relation field '${fieldName}' on model ${model.name}`,
    )
  }
  return field
}

function pushAggregateFieldSql(
  fields: string[],
  aggFn: string,
  alias: string,
  agg: AggregateKey,
  fieldName: string,
): void {
  const outAlias = `${agg}.${fieldName}`
  fields.push(
    `${aggFn}(${col(alias, fieldName)}) ${SQL_TEMPLATES.AS} ${quote(outAlias)}`,
  )
}

function addAggregateFields(
  fields: string[],
  args: PrismaQueryArgs,
  alias: string,
  model: Model,
  fieldMap: Map<string, ScalarFieldInfo>,
): void {
  for (const [agg, aggFn] of AGGREGATES) {
    const obj = getAggregateSelectionObject(args, agg)
    if (!obj) continue

    for (const [fieldName, selection] of Object.entries(obj)) {
      if (fieldName === '_all')
        throw new Error(`'${agg}' does not support '_all'`)
      if (!isTruthySelection(selection)) continue

      const field = assertAggregatableScalarField(
        fieldMap,
        model,
        agg,
        fieldName,
      )
      assertAggregateFieldType(agg, field.type, fieldName, model.name)
      pushAggregateFieldSql(fields, aggFn, alias, agg, fieldName)
    }
  }
}

function buildAggregateFields(
  args: PrismaQueryArgs,
  alias: string,
  model: Model,
): string[] {
  const fields: string[] = []
  const fieldMap = getModelFieldMap(model)

  const countArg = normalizeCountArg(args._count)
  addCountFields(fields, countArg, alias, model, fieldMap)
  addAggregateFields(fields, args, alias, model, fieldMap)

  return fields
}

export function buildAggregateSql(
  args: PrismaQueryArgs,
  whereResult: WhereClauseResult,
  tableName: string,
  alias: string,
  model: Model,
): SqlResult {
  assertSafeAlias(alias)
  assertSafeTableRef(tableName)

  const aggFields = buildAggregateFields(args, alias, model)
  if (!isNonEmptyArray(aggFields)) {
    throw new Error('buildAggregateSql requires at least one aggregate field')
  }

  const selectClause = aggFields.join(SQL_SEPARATORS.FIELD_LIST)
  const whereClause = isValidWhereClause(whereResult.clause)
    ? `${SQL_TEMPLATES.WHERE} ${whereResult.clause}`
    : ''

  const sql = [
    SQL_TEMPLATES.SELECT,
    selectClause,
    SQL_TEMPLATES.FROM,
    tableName,
    alias,
    whereClause,
  ]
    .filter((x) => x && String(x).trim().length > 0)
    .join(' ')
    .trim()

  validateSelectQuery(sql)
  validateParamConsistency(sql, whereResult.params)

  return Object.freeze({
    sql,
    params: Object.freeze([...whereResult.params]),
    paramMappings: Object.freeze([...whereResult.paramMappings]),
  })
}

function assertGroupByBy(args: PrismaQueryArgs, model: Model): string[] {
  if (!isNotNullish(args.by) || !isNonEmptyArray(args.by)) {
    throw new Error('buildGroupBySql: by is required and cannot be empty')
  }

  const byFields = args.by.map((f: string) => String(f))
  const bySet = new Set(byFields)
  if (bySet.size !== byFields.length) {
    throw new Error('buildGroupBySql: by must not contain duplicates')
  }

  const modelFieldMap = getModelFieldMap(model)

  for (const f of byFields) {
    const field = modelFieldMap.get(f)
    if (!field) {
      throw new Error(
        `groupBy.by references unknown field '${f}' on model ${model.name}`,
      )
    }
    if (field.isRelation) {
      throw new Error(
        `groupBy.by does not support relation field '${f}' on model ${model.name}`,
      )
    }
  }

  return byFields
}

function buildGroupBySelectParts(
  args: PrismaQueryArgs,
  alias: string,
  model: Model,
  byFields: string[],
): { groupCols: string[]; groupFields: string; selectFields: string } {
  const groupCols = byFields.map((f) => col(alias, f))
  const groupFields = groupCols.join(SQL_SEPARATORS.FIELD_LIST)

  const aggFields = buildAggregateFields(args, alias, model)
  const selectFields = isNonEmptyArray(aggFields)
    ? groupCols.concat(aggFields).join(SQL_SEPARATORS.FIELD_LIST)
    : groupCols.join(SQL_SEPARATORS.FIELD_LIST)

  return { groupCols, groupFields, selectFields }
}

function buildGroupByHaving(
  args: PrismaQueryArgs,
  alias: string,
  params: ParamStore,
  model: Model,
  dialect: SqlDialect,
): string {
  if (!isNotNullish(args.having)) return ''
  if (!isPlainObject(args.having)) return ''

  const h = buildHavingClause(args.having, alias, params, model, dialect)
  if (!h || h.trim().length === 0) return ''
  return `${SQL_TEMPLATES.HAVING} ${h}`
}

export function buildGroupBySql(
  args: PrismaQueryArgs,
  whereResult: WhereClauseResult,
  tableName: string,
  alias: string,
  model: Model,
  dialect?: SqlDialect,
): SqlResult {
  assertSafeAlias(alias)
  assertSafeTableRef(tableName)

  const byFields = assertGroupByBy(args, model)

  const d = dialect ?? getGlobalDialect()
  const params = createParamStore(whereResult.nextParamIndex)

  const { groupFields, selectFields } = buildGroupBySelectParts(
    args,
    alias,
    model,
    byFields,
  )

  const havingClause = buildGroupByHaving(args, alias, params, model, d)

  const whereClause = isValidWhereClause(whereResult.clause)
    ? `${SQL_TEMPLATES.WHERE} ${whereResult.clause}`
    : ''

  const sql = [
    SQL_TEMPLATES.SELECT,
    selectFields,
    SQL_TEMPLATES.FROM,
    tableName,
    alias,
    whereClause,
    SQL_TEMPLATES.GROUP_BY,
    groupFields,
    havingClause,
  ]
    .filter((x) => x && String(x).trim().length > 0)
    .join(' ')
    .trim()

  const snapshot = params.snapshot()

  validateSelectQuery(sql)
  validateParamConsistency(sql, [...whereResult.params, ...snapshot.params])

  return Object.freeze({
    sql,
    params: Object.freeze([...whereResult.params, ...snapshot.params]),
    paramMappings: Object.freeze([
      ...whereResult.paramMappings,
      ...snapshot.mappings,
    ]),
  })
}

export function buildCountSql(
  whereResult: WhereClauseResult,
  tableName: string,
  alias: string,
  skip?: number | string,
  dialect?: SqlDialect,
): SqlResult {
  assertSafeAlias(alias)
  assertSafeTableRef(tableName)

  const d = dialect ?? getGlobalDialect()

  const whereClause = isValidWhereClause(whereResult.clause)
    ? `${SQL_TEMPLATES.WHERE} ${whereResult.clause}`
    : ''

  const params = createParamStore(whereResult.nextParamIndex)

  const baseSubSelect = [
    SQL_TEMPLATES.SELECT,
    '1',
    SQL_TEMPLATES.FROM,
    tableName,
    alias,
    whereClause,
  ]
    .filter((x) => x && String(x).trim().length > 0)
    .join(' ')
    .trim()

  const normalizedSkip = normalizeSkipLike(skip)
  const subSelect = applyCountSkip(baseSubSelect, normalizedSkip, params, d)

  const sql = [
    SQL_TEMPLATES.SELECT,
    SQL_TEMPLATES.COUNT_ALL,
    SQL_TEMPLATES.AS,
    quote('_count._all'),
    SQL_TEMPLATES.FROM,
    `(${subSelect})`,
    SQL_TEMPLATES.AS,
    `"sub"`,
  ]
    .filter((x) => x && String(x).trim().length > 0)
    .join(' ')
    .trim()

  validateSelectQuery(sql)

  const snapshot = params.snapshot()
  const mergedParams = [...whereResult.params, ...snapshot.params]

  validateParamConsistency(sql, mergedParams)

  return Object.freeze({
    sql,
    params: Object.freeze(mergedParams),
    paramMappings: Object.freeze([
      ...whereResult.paramMappings,
      ...snapshot.mappings,
    ]),
  })
}

function applyCountSkip(
  subSelect: string,
  normalizedSkip: unknown,
  params: ParamStore,
  dialect: SqlDialect,
): string {
  const shouldApply =
    isDynamicParameter(normalizedSkip) ||
    (typeof normalizedSkip === 'number' && normalizedSkip > 0)

  if (!shouldApply) return subSelect

  const placeholder = addAutoScoped(params, normalizedSkip, 'count.skip')

  if (dialect === 'sqlite') {
    return `${subSelect} ${SQL_TEMPLATES.LIMIT} -1 ${SQL_TEMPLATES.OFFSET} ${placeholder}`
  }

  return `${subSelect} ${SQL_TEMPLATES.OFFSET} ${placeholder}`
}
