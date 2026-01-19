import {
  jsonExtractText,
  jsonExtractNumeric,
  jsonToText,
  caseInsensitiveLike,
  SqlDialect,
} from '../../sql-builder-dialect'
import { LIMITS, Ops, SQL_TEMPLATES } from '../shared/constants'
import { createError } from '../shared/errors'
import { ParamStore } from '../shared/param-store'
import { isNotNullish, isPlainObject } from '../shared/validators/type-guards'

const SAFE_JSON_PATH_SEGMENT = /^[a-zA-Z_]\w*$/

function validateJsonPathSegments(segments: string[]): void {
  for (const segment of segments) {
    if (typeof segment !== 'string') {
      throw createError('JSON path segments must be strings', {
        operator: Ops.PATH,
        value: segment,
      })
    }

    if (!SAFE_JSON_PATH_SEGMENT.test(segment)) {
      throw createError(
        `Invalid JSON path segment: '${segment}'. Must be alphanumeric with underscores, starting with letter or underscore.`,
        { operator: Ops.PATH, value: segment },
      )
    }
  }
}

export function buildJsonOperator(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
): string {
  if (val === undefined) return ''

  if (op === Ops.PATH && isPlainObject(val) && 'path' in val) {
    return handleJsonPath(expr, val, params, dialect)
  }

  const jsonWildcards: Record<string, (v: string) => string> = {
    [Ops.STRING_CONTAINS]: (v) => `%${v}%`,
    [Ops.STRING_STARTS_WITH]: (v) => `${v}%`,
    [Ops.STRING_ENDS_WITH]: (v) => `%${v}`,
  }

  if (op in jsonWildcards) {
    return handleJsonWildcard(expr, op, val, params, jsonWildcards, dialect)
  }

  throw createError(`Unsupported JSON operator: ${op}`, { operator: op })
}

function handleJsonPath(
  expr: string,
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
): string {
  const v = val as {
    path: string[]
    equals?: unknown
    gt?: unknown
    gte?: unknown
    lt?: unknown
    lte?: unknown
  }

  if (!Array.isArray(v.path)) {
    throw createError('JSON path must be an array', { operator: Ops.PATH })
  }

  if (v.path.length === 0) {
    throw createError('JSON path cannot be empty', { operator: Ops.PATH })
  }

  validateJsonPathSegments(v.path)

  const pathExpr =
    dialect === 'sqlite'
      ? params.add(`$.${v.path.join('.')}`)
      : params.add(v.path)

  const rawOps: [string, unknown][] = [
    ['=', v.equals],
    ['>', v.gt],
    ['>=', v.gte],
    ['<', v.lt],
    ['<=', v.lte],
  ]

  const ops: [string, unknown][] = rawOps.filter(
    ([, value]) => value !== undefined,
  )

  if (ops.length === 0) {
    throw createError('JSON path query missing comparison operator', {
      operator: Ops.PATH,
    })
  }

  const parts: string[] = []

  for (const [sqlOp, value] of ops) {
    if (value === null) {
      const base = jsonExtractText(expr, pathExpr, dialect)
      parts.push(`${base} ${SQL_TEMPLATES.IS_NULL}`)
      continue
    }

    const valPh = params.add(value)
    const base =
      typeof value === 'number'
        ? jsonExtractNumeric(expr, pathExpr, dialect)
        : jsonExtractText(expr, pathExpr, dialect)
    parts.push(`${base} ${sqlOp} ${valPh}`)
  }

  return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
}

function handleJsonWildcard(
  expr: string,
  op: string,
  val: unknown,
  params: ParamStore,
  wildcards: Record<string, (v: string) => string>,
  dialect: SqlDialect,
): string {
  if (!isNotNullish(val)) {
    throw createError(`JSON string operator requires non-null value`, {
      operator: op,
      value: val,
    })
  }

  if (isPlainObject(val) || Array.isArray(val)) {
    throw createError(`JSON string operator requires scalar value`, {
      operator: op,
      value: val,
    })
  }

  const strVal = String(val)
  if (strVal.length > LIMITS.MAX_STRING_LENGTH) {
    throw createError(
      `String too long (${strVal.length} chars, max ${LIMITS.MAX_STRING_LENGTH})`,
      { operator: op },
    )
  }

  const placeholder = params.add(wildcards[op](strVal))
  const jsonText = jsonToText(expr, dialect)
  return caseInsensitiveLike(jsonText, placeholder, dialect)
}
