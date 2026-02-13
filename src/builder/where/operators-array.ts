import { isDynamicParameter } from '@dee-wan/schema-parser'
import {
  arrayContains,
  arrayOverlaps,
  arrayContainsAll,
  arrayIsEmpty,
  arrayIsNotEmpty,
  arrayEquals,
  getArrayType,
  prepareArrayParam,
  SqlDialect,
} from '../../sql-builder-dialect'
import {
  LIMITS,
  Ops,
  SQL_TEMPLATES,
  SQL_TEMPLATES as T,
} from '../shared/constants'
import { createError } from '../shared/errors'
import { ParamStore } from '../shared/param-store'
import { isEmptyArray, isPlainObject } from '../shared/validators/type-guards'
import { tryBuildNullComparison } from '../shared/null-comparison'

function buildArrayParam(
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
): string {
  if (isDynamicParameter(val)) {
    return params.addAuto(val)
  }

  if (!Array.isArray(val)) {
    throw createError(`Array operation requires array value`, { value: val })
  }

  if (val.length > LIMITS.MAX_ARRAY_SIZE) {
    throw createError(
      `Array too large (${val.length} elements, max ${LIMITS.MAX_ARRAY_SIZE})`,
      { value: `[${val.length} items]` },
    )
  }

  const paramValue = prepareArrayParam(val as unknown[], dialect)
  return params.add(paramValue)
}

export function buildArrayOperator(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  fieldType: string,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  const nullCheck = tryBuildNullComparison(expr, op, val, 'array operators')
  if (nullCheck) return nullCheck

  const cast = getArrayType(fieldType, dialect)

  if (op === Ops.EQUALS) {
    return handleArrayEquals(expr, val, params, cast, dialect)
  }

  if (op === Ops.NOT) {
    return handleArrayNot(expr, val, params, cast, dialect)
  }

  switch (op) {
    case Ops.HAS:
      return handleArrayHas(expr, val, params, cast, dialect)
    case Ops.HAS_SOME:
      return handleArrayHasSome(expr, val, params, cast, dialect)
    case Ops.HAS_EVERY:
      return handleArrayHasEvery(expr, val, params, cast, dialect)
    case Ops.IS_EMPTY:
      return handleArrayIsEmpty(expr, val, dialect)
    default:
      throw createError(`Unknown array operator: ${op}`, { operator: op })
  }
}

function handleArrayEquals(
  expr: string,
  val: unknown,
  params: ParamStore,
  cast: string,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  if (isEmptyArray(val)) {
    return arrayIsEmpty(expr, dialect)
  }

  const placeholder = buildArrayParam(val, params, dialect)
  return arrayEquals(expr, placeholder, cast, dialect)
}

function handleArrayNot(
  expr: string,
  val: unknown,
  params: ParamStore,
  cast: string,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  let target: unknown = val

  if (isPlainObject(val)) {
    const entries = Object.entries(val).filter(([, v]) => v !== undefined)
    if (entries.length === 1 && entries[0][0] === Ops.EQUALS) {
      target = entries[0][1]
    } else {
      throw createError(`Array NOT only supports { equals: ... } shape`, {
        operator: Ops.NOT,
        value: val,
      })
    }
  }

  if (target === null) {
    return `${expr} ${SQL_TEMPLATES.IS_NOT_NULL}`
  }

  if (isEmptyArray(target)) {
    return arrayIsNotEmpty(expr, dialect)
  }

  const placeholder = buildArrayParam(target, params, dialect)
  return `${T.NOT} (${arrayEquals(expr, placeholder, cast, dialect)})`
}

function handleArrayHas(
  expr: string,
  val: unknown,
  params: ParamStore,
  cast: string,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  if (val === null) {
    throw createError(`has requires scalar value`, {
      operator: Ops.HAS,
      value: val,
    })
  }

  if (!isDynamicParameter(val) && Array.isArray(val)) {
    throw createError(`has requires scalar value (single element), not array`, {
      operator: Ops.HAS,
      value: val,
    })
  }

  if (isPlainObject(val)) {
    throw createError(`has requires scalar value`, {
      operator: Ops.HAS,
      value: val,
    })
  }

  const placeholder = params.addAuto(val)
  return arrayContains(expr, placeholder, cast, dialect)
}

function handleArrayHasSome(
  expr: string,
  val: unknown,
  params: ParamStore,
  cast: string,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  if (isDynamicParameter(val)) {
    const placeholder = params.addAuto(val)
    return arrayOverlaps(expr, placeholder, cast, dialect)
  }

  if (!Array.isArray(val)) {
    throw createError(`hasSome requires array value`, {
      operator: Ops.HAS_SOME,
      value: val,
    })
  }

  if (val.length > LIMITS.MAX_ARRAY_SIZE) {
    throw createError(
      `Array too large (${val.length} elements, max ${LIMITS.MAX_ARRAY_SIZE})`,
      { operator: Ops.HAS_SOME, value: `[${val.length} items]` },
    )
  }

  if (val.length === 0) return '0=1'

  const paramValue = prepareArrayParam(val as unknown[], dialect)
  const placeholder = params.add(paramValue)
  return arrayOverlaps(expr, placeholder, cast, dialect)
}

function handleArrayHasEvery(
  expr: string,
  val: unknown,
  params: ParamStore,
  cast: string,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  const placeholder = buildArrayParam(val, params, dialect)
  return arrayContainsAll(expr, placeholder, cast, dialect)
}

function handleArrayIsEmpty(
  expr: string,
  val: unknown,
  dialect: SqlDialect,
): string {
  if (typeof val !== 'boolean') {
    throw createError(`isEmpty requires boolean value`, {
      operator: Ops.IS_EMPTY,
      value: val,
    })
  }

  return val === true
    ? arrayIsEmpty(expr, dialect)
    : arrayIsNotEmpty(expr, dialect)
}
