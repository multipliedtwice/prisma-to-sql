import { isRelationField } from '../joins'
import { buildScalarOperator } from './operators-scalar'
import { buildArrayOperator } from './operators-array'
import { buildJsonOperator } from './operators-json'
import {
  buildTopLevelRelation,
  buildNestedRelation,
  IWhereBuilder,
} from './relations'
import {
  DEFAULT_WHERE_CLAUSE,
  LogicalOps,
  SQL_SEPARATORS,
  SQL_TEMPLATES,
  Ops,
} from '../shared/constants'
import { createError } from '../shared/errors'
import { col } from '../shared/sql-utils'
import { BuildContext, QueryResult } from '../shared/types'
import {
  assertFieldExists,
  assertValidOperator,
} from '../shared/validators/field-validators'
import {
  isEmptyWhere,
  isValidWhereClause,
} from '../shared/validators/sql-validators'
import {
  isNonEmptyArray,
  isArrayType,
  isJsonType,
  isPlainObject,
} from '../shared/validators/type-guards'

type LogicalOperator = 'AND' | 'OR' | 'NOT'

class WhereBuilder implements IWhereBuilder {
  build(where: Record<string, unknown>, ctx: BuildContext): QueryResult {
    if (!isPlainObject(where)) {
      throw createError('where must be an object', {
        path: ctx.path,
        modelName: ctx.model.name,
      })
    }
    return buildWhereInternal(where, ctx, this)
  }
}

const MAX_QUERY_DEPTH = 50
const EMPTY_JOINS: readonly string[] = Object.freeze([])

export const whereBuilderInstance = new WhereBuilder()

function freezeResult(
  clause: string,
  joins: readonly string[] = EMPTY_JOINS,
): QueryResult {
  return Object.freeze({ clause, joins })
}

function dedupePreserveOrder(items: readonly string[]): readonly string[] {
  if (items.length <= 1) return Object.freeze([...items])
  const seen = new Set<string>()
  const out: string[] = []
  for (const s of items) {
    if (!seen.has(s)) {
      seen.add(s)
      out.push(s)
    }
  }
  return Object.freeze(out)
}

function appendResult(
  result: QueryResult,
  clauses: string[],
  allJoins: string[],
): void {
  if (isValidWhereClause(result.clause)) clauses.push(result.clause)
  if (isNonEmptyArray(result.joins)) allJoins.push(...result.joins)
}

function asLogicalOperator(key: string): LogicalOperator | null {
  if (key === LogicalOps.AND) return 'AND'
  if (key === LogicalOps.OR) return 'OR'
  if (key === LogicalOps.NOT) return 'NOT'
  return null
}

function nextContext(ctx: BuildContext): BuildContext {
  return { ...ctx, depth: ctx.depth + 1 }
}

function buildRelationFilter(
  fieldName: string,
  value: Record<string, unknown>,
  ctx: BuildContext,
  builder: IWhereBuilder,
): QueryResult {
  const ctx2 = nextContext(ctx)
  if (ctx.isSubquery) {
    return buildNestedRelation(fieldName, value, ctx2, builder)
  }
  return buildTopLevelRelation(fieldName, value, ctx2, builder)
}

function buildWhereEntry(
  key: string,
  value: unknown,
  ctx: BuildContext,
  builder: IWhereBuilder,
): QueryResult {
  const op = asLogicalOperator(key)
  if (op) return buildLogical(op, value, ctx, builder)

  if (isRelationField(key, ctx.model)) {
    if (!isPlainObject(value)) {
      throw createError(`Relation filter '${key}' must be an object`, {
        path: [...ctx.path, key],
        field: key,
        modelName: ctx.model.name,
        value,
      })
    }
    return buildRelationFilter(key, value, ctx, builder)
  }

  return buildScalarField(key, value, ctx)
}

function buildWhereInternal(
  where: Record<string, unknown>,
  ctx: BuildContext,
  builder: IWhereBuilder,
): QueryResult {
  if (ctx.depth > MAX_QUERY_DEPTH) {
    throw createError(
      `Query nesting too deep (max ${MAX_QUERY_DEPTH} levels). This usually indicates a circular reference.`,
      { path: ctx.path, modelName: ctx.model.name },
    )
  }

  if (isEmptyWhere(where)) {
    return freezeResult(DEFAULT_WHERE_CLAUSE, EMPTY_JOINS)
  }

  const allJoins: string[] = []
  const clauses: string[] = []

  for (const [key, value] of Object.entries(where)) {
    if (value === undefined) continue
    const result = buildWhereEntry(key, value, ctx, builder)
    appendResult(result, clauses, allJoins)
  }

  const finalClause =
    clauses.length > 0
      ? clauses.join(SQL_SEPARATORS.CONDITION_AND)
      : DEFAULT_WHERE_CLAUSE

  return freezeResult(finalClause, dedupePreserveOrder(allJoins))
}

function normalizeLogicalValue(
  operator: LogicalOperator,
  value: unknown,
  ctx: BuildContext,
): Record<string, unknown>[] {
  if (Array.isArray(value)) {
    const out: Record<string, unknown>[] = []
    for (let i = 0; i < value.length; i++) {
      const v = value[i]
      if (v === undefined) continue
      if (!isPlainObject(v)) {
        throw createError(`${operator} entries must be objects`, {
          path: [...ctx.path, operator, String(i)],
          modelName: ctx.model.name,
          value: v,
        })
      }
      out.push(v)
    }
    return out
  }

  if (isPlainObject(value)) {
    return [value]
  }

  throw createError(`${operator} must be an object or array of objects`, {
    path: [...ctx.path, operator],
    modelName: ctx.model.name,
    value,
  })
}

function collectLogicalParts(
  operator: LogicalOperator,
  conditions: Record<string, unknown>[],
  ctx: BuildContext,
  builder: IWhereBuilder,
): { joins: readonly string[]; clauses: readonly string[] } {
  const allJoins: string[] = []
  const clauses: string[] = []

  for (let i = 0; i < conditions.length; i++) {
    const result = builder.build(conditions[i], {
      ...ctx,
      path: [...ctx.path, operator, String(i)],
      depth: ctx.depth + 1,
    })

    if (isNonEmptyArray(result.joins)) allJoins.push(...result.joins)

    if (result.clause && result.clause !== DEFAULT_WHERE_CLAUSE) {
      clauses.push(`(${result.clause})`)
    }
  }

  return {
    joins: dedupePreserveOrder(allJoins),
    clauses: Object.freeze(clauses),
  }
}

function buildLogicalClause(
  operator: LogicalOperator,
  clauses: readonly string[],
): string {
  if (clauses.length === 0) return DEFAULT_WHERE_CLAUSE

  if (operator === 'NOT') {
    if (clauses.length === 1) return `${SQL_TEMPLATES.NOT} ${clauses[0]}`
    return `${SQL_TEMPLATES.NOT} (${clauses.join(SQL_SEPARATORS.CONDITION_AND)})`
  }

  return clauses.join(` ${operator} `)
}

function buildLogical(
  operator: LogicalOperator,
  value: unknown,
  ctx: BuildContext,
  builder: IWhereBuilder,
): QueryResult {
  const conditions = normalizeLogicalValue(operator, value, ctx)

  if (conditions.length === 0) {
    const clause = operator === 'OR' ? '0=1' : DEFAULT_WHERE_CLAUSE
    return freezeResult(clause, EMPTY_JOINS)
  }

  const { joins, clauses } = collectLogicalParts(
    operator,
    conditions,
    ctx,
    builder,
  )
  const clause = buildLogicalClause(operator, clauses)
  return freezeResult(clause, joins)
}

function buildScalarField(
  fieldName: string,
  value: unknown,
  ctx: BuildContext,
): QueryResult {
  const field = assertFieldExists(fieldName, ctx.model, ctx.path)
  const expr = col(ctx.alias, fieldName)

  if (value === null) {
    return freezeResult(`${expr} ${SQL_TEMPLATES.IS_NULL}`, EMPTY_JOINS)
  }

  if (isPlainObject(value)) {
    const mode = value.mode as 'insensitive' | 'default' | undefined
    const ops = Object.entries(value).filter(
      ([k, v]) => k !== 'mode' && v !== undefined,
    )

    if (ops.length === 0) {
      return freezeResult(DEFAULT_WHERE_CLAUSE, EMPTY_JOINS)
    }

    const parts: string[] = []
    for (const [op, val] of ops) {
      assertValidOperator(fieldName, op, field.type, ctx.path, ctx.model.name)
      const clause = buildOperator(expr, op, val, ctx, mode, field.type)
      if (isValidWhereClause(clause)) parts.push(clause)
    }

    const clause =
      parts.length > 0
        ? parts.join(SQL_SEPARATORS.CONDITION_AND)
        : DEFAULT_WHERE_CLAUSE

    return freezeResult(clause, EMPTY_JOINS)
  }

  const clause = buildOperator(
    expr,
    Ops.EQUALS,
    value,
    ctx,
    undefined,
    field.type,
  )
  return freezeResult(clause || DEFAULT_WHERE_CLAUSE, EMPTY_JOINS)
}

function buildOperator(
  expr: string,
  op: string,
  val: unknown,
  ctx: BuildContext,
  mode?: 'insensitive' | 'default',
  fieldType?: string,
): string {
  if (fieldType && isArrayType(fieldType)) {
    return buildArrayOperator(expr, op, val, ctx.params, fieldType, ctx.dialect)
  }

  if (fieldType && isJsonType(fieldType)) {
    const JSON_OPS = new Set([
      Ops.PATH,
      Ops.STRING_CONTAINS,
      Ops.STRING_STARTS_WITH,
      Ops.STRING_ENDS_WITH,
    ])

    if (JSON_OPS.has(op as any)) {
      return buildJsonOperator(expr, op, val, ctx.params, ctx.dialect)
    }
  }

  return buildScalarOperator(
    expr,
    op,
    val,
    ctx.params,
    mode,
    fieldType,
    ctx.dialect,
  )
}
