import { isDynamicParameter } from '@dee-wan/schema-parser'
import {
  inArray,
  notInArray,
  prepareArrayParam,
  SqlDialect,
} from '../../sql-builder-dialect'
import { createError } from './errors'
import { ParamStore } from './param-store'

export function buildInCondition(
  expr: string,
  op: 'in' | 'notIn',
  val: unknown,
  params: ParamStore,
  dialect: SqlDialect,
  context: string,
): string {
  if (isDynamicParameter(val)) {
    const ph = params.addAuto(val)
    return op === 'in'
      ? inArray(expr, ph, dialect)
      : notInArray(expr, ph, dialect)
  }

  if (!Array.isArray(val)) {
    throw createError(`${context} requires array value`, {
      operator: op,
      value: val,
    })
  }

  if (val.length === 0) {
    return op === 'in' ? '0=1' : '1=1'
  }

  if (dialect === 'sqlite' && val.length <= 30) {
    const phs = val.map((item) => params.add(item))
    const list = phs.join(', ')
    return op === 'in' ? `${expr} IN (${list})` : `${expr} NOT IN (${list})`
  }

  const paramValue = prepareArrayParam(val, dialect)
  const ph = params.add(paramValue)
  return op === 'in'
    ? inArray(expr, ph, dialect)
    : notInArray(expr, ph, dialect)
}
