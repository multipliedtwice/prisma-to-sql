import { PrismaQueryArgs, Model } from '../types'
import {
  SQL_TEMPLATES,
  SQL_SEPARATORS,
  Ops,
  LogicalOps,
  LIMITS,
} from './shared/constants'
import {
  assertSafeAlias,
  assertSafeTableRef,
  col,
  quote,
  colWithAlias,
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
import { SqlDialect, getGlobalDialect } from '../sql-builder-dialect'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import { addAutoScoped } from './shared/dynamic-params'
import { buildNotComposite } from './where/operators-scalar'
import {
  assertScalarField,
  assertNumericField,
} from './shared/validators/field-assertions'
import { buildComparisons } from './shared/comparison-builder'
import { buildOrderBy } from './pagination'
import { buildSelectSql } from './select'
import { getPrimaryKeyFields } from './shared/primary-key-utils'
import { tryBuildNullComparison } from './shared/null-comparison'
import { buildInCondition } from './shared/in-operator-builder'

type AggregateKey = '_count' | '_sum' | '_avg' | '_min' | '_max'
type LogicalKey = 'AND' | 'OR' | 'NOT'

const MAX_NOT_DEPTH = 50

const AGGREGATES: ReadonlyArray<[Exclude<AggregateKey, '_count'>, string]> = [
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

const HAVING_ALLOWED_OPS = new Set<string>([
  Ops.EQUALS,
  Ops.NOT,
  Ops.GT,
  Ops.GTE,
  Ops.LT,
  Ops.LTE,
  Ops.IN,
  Ops.NOT_IN,
])

const HAVING_FIELD_FIRST_AGG_KEYS: readonly AggregateKey[] = Object.freeze([
  '_count',
  '_sum',
  '_avg',
  '_min',
  '_max',
] as const)

function hasAnyOwnKey(obj: Record<string, unknown>): boolean {
  for (const k in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, k)) return true
  }
  return false
}

function isTruthySelection(v: unknown): boolean {
  return v === true
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

function assertHavingOp(op: string): void {
  if (!HAVING_ALLOWED_OPS.has(op)) {
    throw new Error(
      `Unsupported HAVING operator '${op}'. Allowed: ${[...HAVING_ALLOWED_OPS].join(', ')}`,
    )
  }
}

function aggExprForField(
  aggKey: AggregateKey,
  field: string,
  alias: string,
  model?: Model,
): string {
  if (aggKey === '_count') {
    return field === '_all'
      ? 'COUNT(*)'
      : 'COUNT(' + col(alias, field, model) + ')'
  }
  if (field === '_all') {
    throw new Error(`'${aggKey}' does not support '_all'`)
  }
  if (aggKey === '_sum') return 'SUM(' + col(alias, field, model) + ')'
  if (aggKey === '_avg') return 'AVG(' + col(alias, field, model) + ')'
  if (aggKey === '_min') return 'MIN(' + col(alias, field, model) + ')'
  return 'MAX(' + col(alias, field, model) + ')'
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

  if (isPlainObject(value)) return [value]

  throw new Error(`${operator} must be an object or array of objects in HAVING`)
}

function buildBinaryComparison(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
): string {
  const sqlOp = buildComparisonOp(op)
  const placeholder = addHavingParam(params, op, val)
  return expr + ' ' + sqlOp + ' ' + placeholder
}

function buildSimpleComparison(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
  depth: number = 0,
): string {
  assertHavingOp(op)

  if (depth > MAX_NOT_DEPTH) {
    throw new Error(
      `NOT operator nesting too deep in HAVING (max ${MAX_NOT_DEPTH} levels).`,
    )
  }

  const nullCheck = tryBuildNullComparison(expr, op, val, 'HAVING')
  if (nullCheck) return nullCheck

  if (op === Ops.NOT && isPlainObject(val)) {
    return buildNotComposite(
      expr,
      val,
      params,
      dialect,
      (e, subOp, subVal, p, d) =>
        buildSimpleComparison(e, subOp, subVal, p, d, depth + 1),
      SQL_SEPARATORS.CONDITION_AND,
    )
  }

  if (op === Ops.IN || op === Ops.NOT_IN) {
    return buildInCondition(
      expr,
      op === Ops.IN ? 'in' : 'notIn',
      val,
      params,
      dialect,
      'HAVING',
    )
  }

  return buildBinaryComparison(expr, op, val, params)
}

function negateClauses(subClauses: string[]): string {
  if (subClauses.length === 1) return SQL_TEMPLATES.NOT + ' ' + subClauses[0]
  return (
    SQL_TEMPLATES.NOT +
    ' (' +
    subClauses.join(SQL_SEPARATORS.CONDITION_AND) +
    ')'
  )
}

function combineLogical(key: LogicalKey, subClauses: string[]): string {
  if (key === LogicalOps.NOT) return negateClauses(subClauses)
  return subClauses.join(' ' + key + ' ')
}

function buildHavingNode(
  node: Record<string, unknown>,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
  depth: number = 0,
): string {
  if (depth > LIMITS.MAX_HAVING_DEPTH) {
    throw new Error(
      `HAVING clause nesting too deep (max ${LIMITS.MAX_HAVING_DEPTH} levels). This usually indicates a circular reference.`,
    )
  }

  const clauses: string[] = []

  for (const key in node) {
    if (!Object.prototype.hasOwnProperty.call(node, key)) continue
    const value = node[key]
    const built = buildHavingEntry(
      key,
      value,
      alias,
      params,
      dialect,
      model,
      depth,
    )
    for (const c of built) {
      if (c && c.length > 0) clauses.push(c)
    }
  }

  return clauses.join(SQL_SEPARATORS.CONDITION_AND)
}

function buildLogicalClause(
  key: LogicalKey,
  value: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
  depth: number = 0,
): string {
  const items = normalizeLogicalValue(key, value)
  const subClauses: string[] = []

  for (const it of items) {
    const c = buildHavingNode(it, alias, params, dialect, model, depth + 1)
    if (c && c.length > 0) subClauses.push('(' + c + ')')
  }

  if (subClauses.length === 0) return ''

  return combineLogical(key, subClauses)
}

function assertHavingAggTarget(
  aggKey: AggregateKey,
  field: string,
  model: Model,
): void {
  if (field === '_all') {
    if (aggKey !== '_count')
      throw new Error(`HAVING '${aggKey}' does not support '_all'`)
    return
  }

  if (aggKey === '_sum' || aggKey === '_avg') {
    assertNumericField(model, field, 'HAVING')
  } else {
    assertScalarField(model, field, 'HAVING')
  }
}

function buildHavingOpsForExpr(
  expr: string,
  filter: Record<string, unknown>,
  params: ParamStore,
  dialect: SqlDialect,
): string[] {
  return buildComparisons(expr, filter, params, dialect, buildSimpleComparison)
}

type HavingShape = 'aggregate-first' | 'field-first'

interface NormalizedHavingEntry {
  aggKey: AggregateKey
  fieldName: string
  filter: Record<string, unknown>
}

function normalizeHavingTarget(
  target: Record<string, unknown>,
  shape: HavingShape,
  model: Model,
): NormalizedHavingEntry[] {
  const entries: NormalizedHavingEntry[] = []

  if (shape === 'aggregate-first') {
    for (const aggKey in target) {
      if (!Object.prototype.hasOwnProperty.call(target, aggKey)) continue
      if (!isAggregateKey(aggKey)) continue

      const aggTarget = target[aggKey]
      if (!isPlainObject(aggTarget)) continue

      for (const fieldName in aggTarget) {
        if (!Object.prototype.hasOwnProperty.call(aggTarget, fieldName))
          continue

        const filter = aggTarget[fieldName]
        if (!isPlainObject(filter)) continue
        if (!hasAnyOwnKey(filter)) continue

        entries.push({ aggKey: aggKey as AggregateKey, fieldName, filter })
      }
    }
  } else {
    for (const fieldName in target) {
      if (!Object.prototype.hasOwnProperty.call(target, fieldName)) continue

      const fieldTarget = target[fieldName]
      if (!isPlainObject(fieldTarget)) continue

      for (const aggKey of HAVING_FIELD_FIRST_AGG_KEYS) {
        const aggFilter = (fieldTarget as any)[aggKey]
        if (!isPlainObject(aggFilter)) continue
        if (!hasAnyOwnKey(aggFilter)) continue

        entries.push({ aggKey, fieldName, filter: aggFilter })
      }
    }
  }

  return entries
}

function validateHavingTarget(
  aggKey: AggregateKey,
  fieldName: string,
  model: Model,
): void {
  if (fieldName === '_all') {
    if (aggKey !== '_count')
      throw new Error(`HAVING '${aggKey}' does not support '_all'`)
    return
  }

  if (aggKey === '_sum' || aggKey === '_avg') {
    assertNumericField(model, fieldName, 'HAVING')
  } else {
    assertScalarField(model, fieldName, 'HAVING')
  }
}

function buildHavingForShape(
  shape: HavingShape,
  target: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string[] {
  if (!isPlainObject(target)) {
    const shapeLabel = shape === 'aggregate-first' ? 'aggregate key' : 'field'
    throw new Error(`HAVING '${shapeLabel}' must be an object`)
  }

  const entries = normalizeHavingTarget(target, shape, model)
  const out: string[] = []

  for (const { aggKey, fieldName, filter } of entries) {
    validateHavingTarget(aggKey, fieldName, model)
    const expr = aggExprForField(aggKey, fieldName, alias, model)
    out.push(...buildHavingOpsForExpr(expr, filter, params, dialect))
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
  return buildHavingForShape(
    'aggregate-first',
    { [aggKey]: target },
    alias,
    params,
    dialect,
    model,
  )
}

function buildHavingEntry(
  key: string,
  value: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
  depth: number = 0,
): string[] {
  if (isLogicalKey(key)) {
    const logical = buildLogicalClause(
      key,
      value,
      alias,
      params,
      dialect,
      model,
      depth,
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

  return buildHavingForShape(
    'field-first',
    { [key]: value },
    alias,
    params,
    dialect,
    model,
  )
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
  if (!isPlainObject(having)) throw new Error('having must be an object')
  return buildHavingNode(having, alias, params, d, model, 0)
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
    SQL_TEMPLATES.COUNT_ALL +
      ' ' +
      SQL_TEMPLATES.AS +
      ' ' +
      quote('_count._all'),
  )
}

function pushCountField(
  fields: string[],
  alias: string,
  fieldName: string,
  model?: Model,
): void {
  const outAlias = '_count.' + fieldName
  fields.push(
    'COUNT(' +
      col(alias, fieldName, model) +
      ') ' +
      SQL_TEMPLATES.AS +
      ' ' +
      quote(outAlias),
  )
}

function addCountFields(
  fields: string[],
  countArg: Record<string, unknown> | true | undefined,
  alias: string,
  model: Model,
): void {
  if (!isNotNullish(countArg)) return

  if (countArg === true) {
    pushCountAllField(fields)
    return
  }

  if (!isPlainObject(countArg)) {
    throw new Error(`_count must be true or an object, got ${typeof countArg}`)
  }

  if (countArg._all === true) {
    pushCountAllField(fields)
  }

  for (const f in countArg) {
    if (!Object.prototype.hasOwnProperty.call(countArg, f)) continue
    if (f === '_all') continue
    const v = countArg[f]
    if (isTruthySelection(v)) {
      assertScalarField(model, f, '_count')
      pushCountField(fields, alias, f, model)
    }
  }
}

function getAggregateSelectionObject(
  args: PrismaQueryArgs,
  agg: AggregateKey,
): Record<string, unknown> | undefined {
  const obj = args[agg]
  return isPlainObject(obj) ? obj : undefined
}

function assertAggregatableScalarField(
  model: Model,
  agg: AggregateKey,
  fieldName: string,
): void {
  if (agg === '_sum' || agg === '_avg') {
    assertNumericField(model, fieldName, agg)
  } else {
    assertScalarField(model, fieldName, agg)
  }
}

function pushAggregateFieldSql(
  fields: string[],
  aggFn: string,
  alias: string,
  agg: AggregateKey,
  fieldName: string,
  model?: Model,
): void {
  const outAlias = agg + '.' + fieldName
  fields.push(
    aggFn +
      '(' +
      col(alias, fieldName, model) +
      ') ' +
      SQL_TEMPLATES.AS +
      ' ' +
      quote(outAlias),
  )
}

function addAggregateFields(
  fields: string[],
  args: PrismaQueryArgs,
  alias: string,
  model: Model,
): void {
  for (const [agg, aggFn] of AGGREGATES) {
    const obj = getAggregateSelectionObject(args, agg)
    if (!obj) continue

    for (const fieldName in obj) {
      if (!Object.prototype.hasOwnProperty.call(obj, fieldName)) continue

      const selection = obj[fieldName]
      if (fieldName === '_all')
        throw new Error(`'${agg}' does not support '_all'`)
      if (!isTruthySelection(selection)) continue

      assertAggregatableScalarField(model, agg, fieldName)
      pushAggregateFieldSql(fields, aggFn, alias, agg, fieldName, model)
    }
  }
}

function buildAggregateFields(
  args: PrismaQueryArgs,
  alias: string,
  model: Model,
): string[] {
  const fields: string[] = []

  const countArg = normalizeCountArg(args._count)
  addCountFields(fields, countArg, alias, model)
  addAggregateFields(fields, args, alias, model)

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

  const joinsPart =
    whereResult.joins && whereResult.joins.length > 0
      ? whereResult.joins.join(' ')
      : ''

  const whereClause = isValidWhereClause(whereResult.clause)
    ? SQL_TEMPLATES.WHERE + ' ' + whereResult.clause
    : ''

  const parts: string[] = [
    SQL_TEMPLATES.SELECT,
    selectClause,
    SQL_TEMPLATES.FROM,
    tableName,
    alias,
  ]
  if (joinsPart) parts.push(joinsPart)
  if (whereClause) parts.push(whereClause)

  const sql = parts.join(' ').trim()

  validateSelectQuery(sql)
  validateParamConsistency(sql, whereResult.params)

  return {
    sql,
    params: [...whereResult.params],
    paramMappings: [...whereResult.paramMappings],
  }
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

  for (const f of byFields) {
    assertScalarField(model, f, 'groupBy.by')
  }

  return byFields
}

function buildGroupBySelectParts(
  args: PrismaQueryArgs,
  alias: string,
  model: Model,
  byFields: string[],
): { groupCols: string[]; groupFields: string; selectFields: string } {
  const groupCols = byFields.map((f) => col(alias, f, model))
  const selectCols = byFields.map((f) => colWithAlias(alias, f, model))
  const groupFields = groupCols.join(SQL_SEPARATORS.FIELD_LIST)

  const aggFields = buildAggregateFields(args, alias, model)
  const selectFields = isNonEmptyArray(aggFields)
    ? selectCols.concat(aggFields).join(SQL_SEPARATORS.FIELD_LIST)
    : selectCols.join(SQL_SEPARATORS.FIELD_LIST)

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
  if (!isPlainObject(args.having)) throw new Error('having must be an object')

  const h = buildHavingClause(args.having, alias, params, model, dialect)
  if (!h || h.length === 0) return ''
  return SQL_TEMPLATES.HAVING + ' ' + h
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
  const params = createParamStore(whereResult.nextParamIndex, d)

  const { groupFields, selectFields } = buildGroupBySelectParts(
    args,
    alias,
    model,
    byFields,
  )
  const havingClause = buildGroupByHaving(args, alias, params, model, d)

  const joinsPart =
    whereResult.joins && whereResult.joins.length > 0
      ? whereResult.joins.join(' ')
      : ''

  const whereClause = isValidWhereClause(whereResult.clause)
    ? SQL_TEMPLATES.WHERE + ' ' + whereResult.clause
    : ''

  const orderBySql = isNotNullish(args.orderBy)
    ? buildOrderBy(args.orderBy, alias, d, model)
    : ''

  const parts: string[] = [
    SQL_TEMPLATES.SELECT,
    selectFields,
    SQL_TEMPLATES.FROM,
    tableName,
    alias,
  ]
  if (joinsPart) parts.push(joinsPart)
  if (whereClause) parts.push(whereClause)
  parts.push(SQL_TEMPLATES.GROUP_BY, groupFields)
  if (havingClause) parts.push(havingClause)

  if (orderBySql) {
    parts.push(SQL_TEMPLATES.ORDER_BY, orderBySql)
  }

  if (isNotNullish(args.take)) {
    const ph = addAutoScoped(params, args.take, 'groupBy.take')
    parts.push(SQL_TEMPLATES.LIMIT, ph)
  }

  if (isNotNullish(args.skip)) {
    const ph = addAutoScoped(params, args.skip, 'groupBy.skip')
    parts.push(SQL_TEMPLATES.OFFSET, ph)
  }

  const sql = parts.join(' ').trim()

  const snapshot = params.snapshot()

  const allParams = [...whereResult.params, ...snapshot.params]
  const allMappings = [...whereResult.paramMappings, ...snapshot.mappings]

  validateSelectQuery(sql)
  validateParamConsistency(sql, allParams)

  return {
    sql,
    params: allParams,
    paramMappings: allMappings,
  }
}

function normalizeCountArgs(argsOrSkip: unknown): PrismaQueryArgs {
  if (isPlainObject(argsOrSkip)) return argsOrSkip as PrismaQueryArgs
  if (argsOrSkip === undefined || argsOrSkip === null) return {}
  return { skip: argsOrSkip as any }
}

function assertNoNegativeTake(args: PrismaQueryArgs): void {
  if (typeof args.take === 'number' && args.take < 0) {
    throw new Error('Negative take is not supported for count()')
  }
}

function buildSimpleCountSql(
  whereResult: WhereClauseResult,
  tableName: string,
  alias: string,
): SqlResult {
  const joinsPart =
    whereResult.joins && whereResult.joins.length > 0
      ? whereResult.joins.join(' ')
      : ''

  const whereClause = isValidWhereClause(whereResult.clause)
    ? SQL_TEMPLATES.WHERE + ' ' + whereResult.clause
    : ''

  const parts: string[] = [
    SQL_TEMPLATES.SELECT,
    SQL_TEMPLATES.COUNT_ALL,
    SQL_TEMPLATES.AS,
    quote('_count._all'),
    SQL_TEMPLATES.FROM,
    tableName,
    alias,
  ]
  if (joinsPart) parts.push(joinsPart)
  if (whereClause) parts.push(whereClause)

  const sql = parts.join(' ').trim()

  validateSelectQuery(sql)
  validateParamConsistency(sql, whereResult.params)

  return {
    sql,
    params: [...whereResult.params],
    paramMappings: [...whereResult.paramMappings],
  }
}

export function buildCountSql(
  whereResult: WhereClauseResult,
  tableName: string,
  alias: string,
  argsOrSkip?: PrismaQueryArgs | number | string,
  dialect?: SqlDialect,
  model?: Model,
  schemas?: Model[],
): SqlResult {
  assertSafeAlias(alias)
  assertSafeTableRef(tableName)

  const args = normalizeCountArgs(argsOrSkip)
  assertNoNegativeTake(args)

  if (!model) {
    return buildSimpleCountSql(whereResult, tableName, alias)
  }

  const pkFields = getPrimaryKeyFields(model)
  const distinctFields = isNonEmptyArray((args as any).distinct)
    ? (args as any).distinct.map((x: any) => String(x)).filter((x: string) => x)
    : []

  const selectFields = distinctFields.length > 0 ? distinctFields : pkFields
  if (selectFields.length === 0) {
    return buildSimpleCountSql(whereResult, tableName, alias)
  }

  const select: Record<string, boolean> = {}
  for (const f of selectFields) select[f] = true

  const subArgs: PrismaQueryArgs = {
    ...(args as any),
    include: undefined,
    select,
  }

  const d = dialect ?? getGlobalDialect()
  const subSchemas =
    Array.isArray(schemas) && schemas.length > 0 ? schemas : [model]

  const sub = buildSelectSql({
    method: 'findMany',
    args: subArgs,
    model,
    schemas: subSchemas,
    from: { tableName, alias },
    whereResult,
    dialect: d,
  })

  const countAlias = quote('__count_sub')
  const sql =
    `${SQL_TEMPLATES.SELECT} ${SQL_TEMPLATES.COUNT_ALL} ${SQL_TEMPLATES.AS} ${quote(
      '_count._all',
    )} ` +
    `${SQL_TEMPLATES.FROM} (${sub.sql}) ${SQL_TEMPLATES.AS} ${countAlias}`

  validateSelectQuery(sql)
  validateParamConsistency(sql, sub.params)

  return {
    sql,
    params: sub.params,
    paramMappings: sub.paramMappings,
  }
}
