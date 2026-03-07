import type { Model, PrismaMethod } from '../types'
import type { BatchQuery } from './batch-builder'

import { transformQueryResults } from '../result-transformers'

const COUNT_RESULT_KEY_PREFIX = 'count_'
const DATETIME_FIELD_TYPE = 'DateTime'
const COUNT_PROPERTY = 'count'
const UNDERSCORE_COUNT_PROPERTY = '_count'
const COUNT_PROPERTY_SUFFIX = '_count'

function looksLikeJsonString(s: string): boolean {
  const t = s.trim()
  if (t.length === 0) return false
  const c0 = t.charCodeAt(0)
  const cN = t.charCodeAt(t.length - 1)
  const OPEN_BRACE = 123
  const CLOSE_BRACE = 125
  const OPEN_BRACKET = 91
  const CLOSE_BRACKET = 93
  if (c0 === OPEN_BRACE && cN === CLOSE_BRACE) return true
  if (c0 === OPEN_BRACKET && cN === CLOSE_BRACKET) return true
  if (t === 'null' || t === 'true' || t === 'false') return true
  return false
}

function parseJsonValue(value: unknown): unknown {
  if (typeof value !== 'string') return value
  if (!looksLikeJsonString(value)) return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

function findCountKey(obj: Record<string, unknown>): string | undefined {
  if (Object.prototype.hasOwnProperty.call(obj, COUNT_PROPERTY)) {
    return COUNT_PROPERTY
  }
  if (Object.prototype.hasOwnProperty.call(obj, UNDERSCORE_COUNT_PROPERTY)) {
    return UNDERSCORE_COUNT_PROPERTY
  }
  return Object.keys(obj).find((k) => k.endsWith(COUNT_PROPERTY_SUFFIX))
}

function extractNumericValue(value: unknown): number {
  if (typeof value === 'number') return value

  if (typeof value === 'bigint') {
    const n = Number(value)
    return Number.isSafeInteger(n) ? n : 0
  }

  if (typeof value === 'string') {
    const n = Number.parseInt(value, 10)
    return Number.isFinite(n) ? n : 0
  }

  return 0
}

function parseCountValue(value: unknown): number {
  if (value === null || value === undefined) return 0
  if (typeof value === 'number') return value
  if (typeof value === 'bigint' || typeof value === 'string') {
    return extractNumericValue(value)
  }

  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>
    const countKey = findCountKey(obj)
    if (countKey !== undefined) {
      return extractNumericValue(obj[countKey])
    }
  }

  return 0
}

export function parseBatchCountResults(
  row: Record<string, unknown>,
  queryCount: number,
): number[] {
  const results: number[] = []

  for (let i = 0; i < queryCount; i++) {
    const key = `${COUNT_RESULT_KEY_PREFIX}${i}`
    const value = row[key]

    if (value === null || value === undefined) {
      results.push(0)
      continue
    }

    if (typeof value === 'number') {
      results.push(value)
      continue
    }

    if (typeof value === 'bigint') {
      results.push(Number(value))
      continue
    }

    if (typeof value === 'string') {
      const parsed = parseInt(value, 10)
      results.push(isNaN(parsed) ? 0 : parsed)
      continue
    }

    results.push(0)
  }

  return results
}

function isDateTimeFieldType(type: string): boolean {
  const base = type.replace(/\[\]|\?/g, '')
  return base === DATETIME_FIELD_TYPE
}

function coerceDateTime(value: unknown): unknown {
  if (typeof value !== 'string') return value
  const d = new Date(value)
  if (Number.isFinite(d.getTime())) return d
  return value
}

function coerceFieldValue(
  value: unknown,
  field: {
    name: string
    type: string
    isRelation?: boolean
    relatedModel?: string
  },
  modelMap: Map<string, Model>,
): { value: unknown; changed: boolean } {
  if (field.isRelation && field.relatedModel) {
    const relModel = modelMap.get(field.relatedModel)
    if (relModel) {
      const coerced = coerceBatchRowTypes(value, relModel, modelMap)
      return { value: coerced, changed: coerced !== value }
    }
    return { value, changed: false }
  }

  if (isDateTimeFieldType(field.type)) {
    const coerced = coerceDateTime(value)
    return { value: coerced, changed: coerced !== value }
  }

  return { value, changed: false }
}

function coerceBatchRowTypes(
  obj: unknown,
  model: Model | undefined,
  modelMap: Map<string, Model>,
): unknown {
  if (!model || obj === null || obj === undefined) return obj

  if (Array.isArray(obj)) {
    return obj.map((item) => coerceBatchRowTypes(item, model, modelMap))
  }

  if (typeof obj !== 'object') return obj

  const record = obj as Record<string, unknown>
  const result: Record<string, unknown> = {}
  let changed = false

  for (const key in record) {
    if (!Object.prototype.hasOwnProperty.call(record, key)) continue

    const val = record[key]
    const field = model.fields.find((f) => f.name === key)

    if (!field) {
      result[key] = val
      continue
    }

    const coerced = coerceFieldValue(val, field, modelMap)
    result[key] = coerced.value
    if (coerced.changed) changed = true
  }

  return changed ? result : obj
}

function coerceBatchValue(
  value: unknown,
  method: PrismaMethod,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  if (!modelMap || method === 'count') return value

  const model = modelMap.get(modelName)
  if (!model) return value

  if (Array.isArray(value)) {
    return value.map((item) => coerceBatchRowTypes(item, model, modelMap))
  }

  if (value !== null && typeof value === 'object') {
    return coerceBatchRowTypes(value, model, modelMap)
  }

  return value
}

function coerceAggSubFields(
  inner: Record<string, unknown>,
  model: Model,
): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  let changed = false

  for (const fieldName in inner) {
    if (!Object.prototype.hasOwnProperty.call(inner, fieldName)) continue
    const fieldVal = inner[fieldName]
    const field = model.fields.find((f) => f.name === fieldName)

    if (field && isDateTimeFieldType(field.type)) {
      const coerced = coerceDateTime(fieldVal)
      result[fieldName] = coerced
      if (coerced !== fieldVal) changed = true
    } else {
      result[fieldName] = fieldVal
    }
  }

  return changed ? result : inner
}

const AGG_MIN_KEY = '_min'
const AGG_MAX_KEY = '_max'

function coerceAggregateResult(
  obj: unknown,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  if (!modelMap || obj === null || obj === undefined || typeof obj !== 'object')
    return obj

  const model = modelMap.get(modelName)
  if (!model) return obj

  const record = obj as Record<string, unknown>
  const result: Record<string, unknown> = {}
  let changed = false

  for (const key in record) {
    if (!Object.prototype.hasOwnProperty.call(record, key)) continue
    const val = record[key]

    if (
      (key === AGG_MIN_KEY || key === AGG_MAX_KEY) &&
      val !== null &&
      val !== undefined &&
      typeof val === 'object'
    ) {
      const coerced = coerceAggSubFields(val as Record<string, unknown>, model)
      result[key] = coerced
      if (coerced !== val) changed = true
    } else {
      result[key] = val
    }
  }

  return changed ? result : obj
}

function coerceGroupByResults(
  arr: unknown,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  if (!modelMap || !Array.isArray(arr)) return arr

  const model = modelMap.get(modelName)
  if (!model) return arr

  return arr.map((item) => {
    if (item === null || item === undefined || typeof item !== 'object')
      return item

    const record = item as Record<string, unknown>
    const result: Record<string, unknown> = {}
    let changed = false

    for (const key in record) {
      if (!Object.prototype.hasOwnProperty.call(record, key)) continue
      const val = record[key]

      if (
        (key === AGG_MIN_KEY || key === AGG_MAX_KEY) &&
        val !== null &&
        val !== undefined &&
        typeof val === 'object'
      ) {
        const coerced = coerceAggSubFields(
          val as Record<string, unknown>,
          model,
        )
        result[key] = coerced
        if (coerced !== val) changed = true
      } else {
        const field = model.fields.find((f) => f.name === key)
        if (field && isDateTimeFieldType(field.type)) {
          const coerced = coerceDateTime(val)
          result[key] = coerced
          if (coerced !== val) changed = true
        } else {
          result[key] = val
        }
      }
    }

    return changed ? result : item
  })
}

function parseBatchValue(
  rawValue: unknown,
  method: PrismaMethod,
  modelName: string,
  modelMap?: Map<string, Model>,
): unknown {
  switch (method) {
    case 'findMany': {
      const parsed = parseJsonValue(rawValue)
      const arr = Array.isArray(parsed) ? parsed : []
      return coerceBatchValue(arr, 'findMany', modelName, modelMap)
    }
    case 'findFirst':
    case 'findUnique': {
      const parsed = parseJsonValue(rawValue)
      const val = parsed ?? null
      return val === null
        ? null
        : coerceBatchValue(val, method, modelName, modelMap)
    }
    case 'count': {
      return parseCountValue(rawValue)
    }
    case 'aggregate': {
      const parsed = parseJsonValue(rawValue)
      const obj = (parsed ?? {}) as Record<string, unknown>
      const transformed = transformQueryResults('aggregate', [obj])
      return coerceAggregateResult(transformed, modelName, modelMap)
    }
    case 'groupBy': {
      const parsed = parseJsonValue(rawValue)
      const arr = Array.isArray(parsed) ? parsed : []
      const transformed = transformQueryResults('groupBy', arr)
      return coerceGroupByResults(transformed, modelName, modelMap)
    }
    default:
      return rawValue
  }
}

export function parseBatchResults(
  row: Record<string, unknown>,
  keys: string[],
  queries: Record<string, BatchQuery>,
  aliases?: string[],
  modelMap?: Map<string, Model>,
): Record<string, unknown> {
  const results: Record<string, unknown> = {}

  for (let i = 0; i < keys.length; i++) {
    const key = keys[i]
    const columnKey = aliases?.[i] ?? key
    const rawValue = row[columnKey]
    const query = queries[key]

    results[key] = parseBatchValue(
      rawValue,
      query.method,
      query.model,
      modelMap,
    )
  }

  return results
}
