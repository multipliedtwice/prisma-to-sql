import { isDynamicParameter } from '@dee-wan/schema-parser'
import {
  caseInsensitiveLike,
  caseInsensitiveEquals,
  inArray,
  notInArray,
  prepareArrayParam,
  SqlDialect,
} from '../../sql-builder-dialect'
import { Ops, Modes, SQL_TEMPLATES, Wildcards } from '../shared/constants'
import { createError } from '../shared/errors'
import { ParamStore } from '../shared/param-store'
import { isNotNullish, isPlainObject } from '../shared/validators/type-guards'

const MAX_NOT_DEPTH = 50

interface ScalarOperatorOptions {
  mode?: ModeType
  fieldType?: string
  dialect?: SqlDialect
  depth?: number
}

type ModeType = 'insensitive' | 'default' | undefined

export function buildNotComposite(
  expr: string,
  val: Record<string, unknown>,
  params: ParamStore,
  dialect: SqlDialect,
  buildOp: (
    expr: string,
    op: string,
    subVal: unknown,
    params: ParamStore,
    dialect: SqlDialect,
  ) => string,
  separator: string,
): string {
  const entries = Object.entries(val).filter(
    ([k, v]) => k !== 'mode' && v !== undefined,
  )
  if (entries.length === 0) return ''

  const clauses: string[] = []
  for (const [subOp, subVal] of entries) {
    const sub = buildOp(expr, subOp, subVal, params, dialect)
    if (sub && sub.trim().length > 0) clauses.push(`(${sub})`)
  }

  if (clauses.length === 0) return ''
  if (clauses.length === 1) return `${SQL_TEMPLATES.NOT} ${clauses[0]}`
  return `${SQL_TEMPLATES.NOT} (${clauses.join(separator)})`
}

function validateOperatorRequirements(
  op: string,
  mode: ModeType,
  dialect: SqlDialect | undefined,
): void {
  const STRING_LIKE_OPS = new Set([
    Ops.CONTAINS,
    Ops.STARTS_WITH,
    Ops.ENDS_WITH,
  ])

  if (STRING_LIKE_OPS.has(op as any) && !isNotNullish(dialect)) {
    throw createError(`Like operators require a SQL dialect`, { operator: op })
  }

  if ((op === Ops.IN || op === Ops.NOT_IN) && !isNotNullish(dialect)) {
    throw createError(`IN operators require a SQL dialect`, { operator: op })
  }

  if (
    op === Ops.EQUALS &&
    mode === Modes.INSENSITIVE &&
    !isNotNullish(dialect)
  ) {
    throw createError(`Insensitive equals requires a SQL dialect`, {
      operator: op,
    })
  }
}

function routeOperatorHandler(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  mode: ModeType,
  dialect: SqlDialect | undefined,
): string | null {
  const STRING_LIKE_OPS = new Set([
    Ops.CONTAINS,
    Ops.STARTS_WITH,
    Ops.ENDS_WITH,
  ])

  if (STRING_LIKE_OPS.has(op as any)) {
    return handleLikeOperator(expr, op, val, params, mode, dialect!)
  }

  if (op === Ops.IN || op === Ops.NOT_IN) {
    return handleInOperator(expr, op, val, params, dialect!)
  }

  if (
    op === Ops.EQUALS &&
    mode === Modes.INSENSITIVE &&
    isNotNullish(dialect)
  ) {
    const placeholder = params.addAuto(val)
    return caseInsensitiveEquals(expr, placeholder, dialect)
  }

  return null
}

export function buildScalarOperator(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  options: ScalarOperatorOptions = {},
): string {
  const { mode, fieldType, dialect, depth = 0 } = options

  if (val === undefined) return ''

  if (depth > MAX_NOT_DEPTH) {
    throw new Error(
      `NOT operator nesting too deep (max ${MAX_NOT_DEPTH} levels). This usually indicates a circular reference or adversarial input.`,
    )
  }

  if (val === null) {
    return handleNullValue(expr, op)
  }

  if (op === Ops.NOT && isPlainObject(val)) {
    return handleNotOperator(expr, val, params, mode, fieldType, dialect, depth)
  }

  if (op === Ops.NOT) {
    const placeholder = params.addAuto(val)
    return `${expr} <> ${placeholder}`
  }

  validateOperatorRequirements(op, mode, dialect)

  const specialHandler = routeOperatorHandler(
    expr,
    op,
    val,
    params,
    mode,
    dialect,
  )
  if (specialHandler !== null) {
    return specialHandler
  }

  return handleComparisonOperator(expr, op, val, params)
}

function handleNullValue(expr: string, op: string): string {
  if (op === Ops.EQUALS) return `${expr} ${SQL_TEMPLATES.IS_NULL}`
  if (op === Ops.NOT) return `${expr} ${SQL_TEMPLATES.IS_NOT_NULL}`
  throw createError(`Operator '${op}' doesn't support null`, { operator: op })
}

function normalizeMode(v: unknown): ModeType {
  if (v === Modes.INSENSITIVE) return Modes.INSENSITIVE
  if (v === Modes.DEFAULT) return Modes.DEFAULT
  return undefined
}

function handleNotOperator(
  expr: string,
  val: Record<string, unknown>,
  params: ParamStore,
  outerMode?: ModeType,
  fieldType?: string,
  dialect?: SqlDialect,
  depth: number = 0,
): string {
  const innerMode = normalizeMode(val.mode)
  const effectiveMode = innerMode ?? outerMode

  const entries = Object.entries(val).filter(
    ([k, v]) => k !== 'mode' && v !== undefined,
  )
  if (entries.length === 0) return ''

  if (!isNotNullish(dialect)) {
    const clauses: string[] = []
    for (const [subOp, subVal] of entries) {
      const sub = buildScalarOperator(expr, subOp, subVal, params, {
        mode: effectiveMode,
        fieldType,
        dialect: undefined,
        depth: depth + 1,
      })
      if (sub && sub.trim().length > 0) clauses.push(`(${sub})`)
    }

    if (clauses.length === 0) return ''
    if (clauses.length === 1) return `${SQL_TEMPLATES.NOT} ${clauses[0]}`
    const separator = ` ${SQL_TEMPLATES.AND} `
    return `${SQL_TEMPLATES.NOT} (${clauses.join(separator)})`
  }

  const separator = ` ${SQL_TEMPLATES.AND} `
  return buildNotComposite(
    expr,
    val,
    params,
    dialect,
    (e, subOp, subVal, p, d) =>
      buildScalarOperator(e, subOp, subVal, p, {
        mode: effectiveMode,
        fieldType,
        dialect: d,
        depth: depth + 1,
      }),
    separator,
  )
}

function buildDynamicLikePattern(op: string, placeholder: string): string {
  switch (op) {
    case Ops.CONTAINS:
      return `('%' || ${placeholder} || '%')`
    case Ops.STARTS_WITH:
      return `(${placeholder} || '%')`
    case Ops.ENDS_WITH:
      return `('%' || ${placeholder})`
    default:
      return placeholder
  }
}

function handleLikeOperator(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  mode: ModeType,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  if (isDynamicParameter(val)) {
    const placeholder = params.addAuto(val)
    const patternExpr = buildDynamicLikePattern(op, placeholder)

    if (mode === Modes.INSENSITIVE) {
      return caseInsensitiveLike(expr, patternExpr, dialect)
    }

    return `${expr} ${SQL_TEMPLATES.LIKE} ${patternExpr}`
  }

  const placeholder = params.add(Wildcards[op](String(val)))

  if (mode === Modes.INSENSITIVE) {
    return caseInsensitiveLike(expr, placeholder, dialect)
  }

  return `${expr} ${SQL_TEMPLATES.LIKE} ${placeholder}`
}

function handleInOperator(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  if (isDynamicParameter(val)) {
    const placeholder = params.addAuto(val)
    return op === Ops.IN
      ? inArray(expr, placeholder, dialect)
      : notInArray(expr, placeholder, dialect)
  }

  if (!Array.isArray(val)) {
    throw createError(`IN operators require array value`, {
      operator: op,
      value: val,
    })
  }

  if (val.length === 0) {
    return op === Ops.IN ? '0=1' : '1=1'
  }

  const paramValue = prepareArrayParam(val as unknown[], dialect)
  const placeholder = params.add(paramValue)

  return op === Ops.IN
    ? inArray(expr, placeholder, dialect)
    : notInArray(expr, placeholder, dialect)
}

function handleComparisonOperator(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
): string {
  if (val === undefined) return ''

  const COMPARISON_OPS: Record<string, string> = {
    [Ops.EQUALS]: '=',
    [Ops.GT]: '>',
    [Ops.GTE]: '>=',
    [Ops.LT]: '<',
    [Ops.LTE]: '<=',
  }

  const sqlOp = COMPARISON_OPS[op]
  if (!sqlOp) {
    throw createError(`Unsupported scalar operator: ${op}`, { operator: op })
  }

  const placeholder = params.addAuto(val)
  return `${expr} ${sqlOp} ${placeholder}`
}
