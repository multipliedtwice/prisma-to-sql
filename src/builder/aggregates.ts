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
import {
  SqlDialect,
  getGlobalDialect,
  inArray,
  notInArray,
  prepareArrayParam,
} from '../sql-builder-dialect'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import { addAutoScoped } from './shared/dynamic-params'
import { buildNotComposite } from './where/operators-scalar'
import {
  assertScalarField,
  assertNumericField,
} from './shared/validators/field-assertions'
import { buildComparisons } from './shared/comparison-builder'

type AggregateKey = '_count' | '_sum' | '_avg' | '_min' | '_max'
type LogicalKey = 'AND' | 'OR' | 'NOT'

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
    return field === '_all' ? `COUNT(*)` : `COUNT(${col(alias, field, model)})`
  }
  if (field === '_all') {
    throw new Error(`'${aggKey}' does not support '_all'`)
  }
  if (aggKey === '_sum') return `SUM(${col(alias, field, model)})`
  if (aggKey === '_avg') return `AVG(${col(alias, field, model)})`
  if (aggKey === '_min') return `MIN(${col(alias, field, model)})`
  return `MAX(${col(alias, field, model)})`
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
  assertHavingOp(op)

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

function negateClauses(subClauses: string[]): string {
  if (subClauses.length === 1) return `${SQL_TEMPLATES.NOT} ${subClauses[0]}`
  return `${SQL_TEMPLATES.NOT} (${subClauses.join(SQL_SEPARATORS.CONDITION_AND)})`
}

function combineLogical(key: LogicalKey, subClauses: string[]): string {
  if (key === LogicalOps.NOT) return negateClauses(subClauses)
  return subClauses.join(` ${key} `)
}

function buildHavingNode(
  node: Record<string, unknown>,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string {
  const clauses: string[] = []

  for (const key in node) {
    if (!Object.prototype.hasOwnProperty.call(node, key)) continue
    const value = (node as Record<string, unknown>)[key]
    const built = buildHavingEntry(key, value, alias, params, dialect, model)
    for (const c of built) {
      if (c && c.trim().length > 0) clauses.push(c)
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
): string {
  const items = normalizeLogicalValue(key, value)
  const subClauses: string[] = []

  for (const it of items) {
    const c = buildHavingNode(it, alias, params, dialect, model)
    if (c && c.trim().length > 0) subClauses.push(`(${c})`)
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

function buildHavingForAggregateFirstShape(
  aggKey: AggregateKey,
  target: unknown,
  alias: string,
  params: ParamStore,
  dialect: SqlDialect,
  model: Model,
): string[] {
  if (!isPlainObject(target)) {
    throw new Error(`HAVING '${aggKey}' must be an object`)
  }

  const out: string[] = []
  for (const field in target as Record<string, unknown>) {
    if (!Object.prototype.hasOwnProperty.call(target, field)) continue

    assertHavingAggTarget(aggKey, field, model)

    const filter = (target as Record<string, unknown>)[field]
    if (!isPlainObject(filter)) continue

    let hasAny = false
    for (const k in filter as Record<string, unknown>) {
      if (Object.prototype.hasOwnProperty.call(filter, k)) {
        hasAny = true
        break
      }
    }
    if (!hasAny) continue

    const expr = aggExprForField(aggKey, field, alias, model)
    out.push(
      ...buildHavingOpsForExpr(
        expr,
        filter as Record<string, unknown>,
        params,
        dialect,
      ),
    )
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
  if (!isPlainObject(target)) {
    throw new Error(`HAVING '${fieldName}' must be an object`)
  }

  assertScalarField(model, fieldName, 'HAVING')

  const out: string[] = []
  const obj = target as Record<string, unknown>

  for (const aggKey of HAVING_FIELD_FIRST_AGG_KEYS) {
    const aggFilter = obj[aggKey]
    if (!isPlainObject(aggFilter)) continue

    let hasAny = false
    for (const k in aggFilter as Record<string, unknown>) {
      if (Object.prototype.hasOwnProperty.call(aggFilter, k)) {
        hasAny = true
        break
      }
    }
    if (!hasAny) continue

    if (aggKey === '_sum' || aggKey === '_avg') {
      assertNumericField(model, fieldName, 'HAVING')
    }

    const expr = aggExprForField(aggKey, fieldName, alias, model)
    out.push(...buildHavingOpsForExpr(expr, aggFilter, params, dialect))
  }

  return out
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

function pushCountField(
  fields: string[],
  alias: string,
  fieldName: string,
  model?: Model,
): void {
  const outAlias = `_count.${fieldName}`
  fields.push(
    `COUNT(${col(alias, fieldName, model)}) ${SQL_TEMPLATES.AS} ${quote(outAlias)}`,
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

  if (!isPlainObject(countArg)) return

  if (countArg._all === true) {
    pushCountAllField(fields)
  }

  const selected = Object.entries(countArg).filter(
    ([f, v]) => f !== '_all' && isTruthySelection(v),
  )
  for (const [f] of selected) {
    assertScalarField(model, f, '_count')
    pushCountField(fields, alias, f, model)
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
  const outAlias = `${agg}.${fieldName}`
  fields.push(
    `${aggFn}(${col(alias, fieldName, model)}) ${SQL_TEMPLATES.AS} ${quote(outAlias)}`,
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

    for (const [fieldName, selection] of Object.entries(obj)) {
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
  _dialect?: SqlDialect,
): SqlResult {
  assertSafeAlias(alias)
  assertSafeTableRef(tableName)

  if (skip !== undefined && skip !== null) {
    if (isDynamicParameter(skip)) {
      throw new Error(
        'count() with skip is not supported because it produces nondeterministic results. ' +
          'Dynamic skip cannot be validated at build time. ' +
          'Use findMany().length or add explicit orderBy + cursor/skip logic in a deterministic query.',
      )
    }

    if (typeof skip === 'string') {
      const s = skip.trim()
      if (s.length > 0) {
        const n = Number(s)
        if (Number.isFinite(n) && Number.isInteger(n) && n > 0) {
          throw new Error(
            'count() with skip is not supported because it produces nondeterministic results. ' +
              'Use findMany().length or add explicit orderBy to ensure deterministic behavior.',
          )
        }
      }
    }

    if (
      typeof skip === 'number' &&
      Number.isFinite(skip) &&
      Number.isInteger(skip) &&
      skip > 0
    ) {
      throw new Error(
        'count() with skip is not supported because it produces nondeterministic results. ' +
          'Use findMany().length or add explicit orderBy to ensure deterministic behavior.',
      )
    }
  }

  const whereClause = isValidWhereClause(whereResult.clause)
    ? `${SQL_TEMPLATES.WHERE} ${whereResult.clause}`
    : ''

  const sql = [
    SQL_TEMPLATES.SELECT,
    SQL_TEMPLATES.COUNT_ALL,
    SQL_TEMPLATES.AS,
    quote('_count._all'),
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
