import type { Model } from '../../types'
import { AGGREGATE_PREFIXES } from '../shared/constants'

type DecimalLike = {
  toNumber(): number
  toFixed(dp?: number): string
  toString(): string
  valueOf(): string
  toJSON(): string
}

class FallbackDecimal implements DecimalLike {
  private readonly raw: string

  constructor(value: string | number | bigint) {
    this.raw = typeof value === 'bigint' ? value.toString() : String(value)
  }

  toNumber(): number {
    return Number(this.raw)
  }

  toFixed(dp?: number): string {
    if (dp === undefined) return this.raw
    const n = Number(this.raw)
    return Number.isFinite(n) ? n.toFixed(dp) : this.raw
  }

  toString(): string {
    return this.raw
  }

  valueOf(): string {
    return this.raw
  }

  toJSON(): string {
    return this.raw
  }
}

let cachedDecimalCtor:
  | (new (value: string | number) => DecimalLike)
  | null
  | undefined = undefined

function getPrismaDecimalCtorSync():
  | (new (value: string | number) => DecimalLike)
  | null {
  if (cachedDecimalCtor !== undefined) return cachedDecimalCtor
  try {
    const { Prisma } = require('@prisma/client')
    const candidate = (Prisma as unknown as { Decimal?: unknown }).Decimal
    cachedDecimalCtor =
      typeof candidate === 'function'
        ? (candidate as new (value: string | number) => DecimalLike)
        : null
  } catch {
    cachedDecimalCtor = null
  }
  return cachedDecimalCtor
}

function isIntegerString(value: string): boolean {
  return /^-?\d+$/.test(value)
}

function isNumericString(value: string): boolean {
  return /^-?\d+(\.\d+)?$/.test(value)
}

function isDecimalInput(value: unknown): value is string | number | bigint {
  return (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'bigint'
  )
}

function isDecimalLike(value: unknown): value is DecimalLike {
  if (!value || typeof value !== 'object') return false
  const v = value as Record<string, unknown>
  return (
    typeof v.toString === 'function' &&
    typeof v.toJSON === 'function' &&
    typeof v.toFixed === 'function'
  )
}

function toDecimalLikeSync(value: string | number | bigint): DecimalLike {
  const normalized = typeof value === 'bigint' ? value.toString() : value
  const DecimalCtor = getPrismaDecimalCtorSync()
  if (DecimalCtor) {
    try {
      return new DecimalCtor(normalized)
    } catch {}
  }
  return new FallbackDecimal(normalized)
}

function getFieldType(
  model: Model | undefined,
  fieldName: string,
): string | null {
  if (!model) return null
  const field = model.fields.find((f) => f.name === fieldName && !f.isRelation)
  if (!field?.type) return null
  return String(field.type).replace(/\?$/, '')
}

function parseCountValue(value: unknown): unknown {
  if (typeof value === 'string' && isIntegerString(value)) {
    const n = Number(value)
    if (Number.isSafeInteger(n)) return n
    try {
      return BigInt(value)
    } catch {
      return value
    }
  }
  return value
}

function convertScalarAggregateValue(
  prefix: string,
  fieldName: string,
  value: unknown,
  model?: Model,
): unknown {
  if (value == null) return value

  if (prefix === '_count') {
    return parseCountValue(value)
  }

  const fieldType = getFieldType(model, fieldName)
  if (!fieldType) {
    return value
  }

  if (fieldType === 'Decimal') {
    if (isDecimalLike(value)) return value
    if (isDecimalInput(value)) return toDecimalLikeSync(value)
    return value
  }

  if (fieldType === 'BigInt') {
    if (typeof value === 'bigint') return value
    if (typeof value === 'string' && isIntegerString(value)) {
      try {
        return BigInt(value)
      } catch {
        return value
      }
    }
    return value
  }

  if (fieldType === 'Int' || fieldType === 'Float') {
    if (typeof value === 'number') return value
    if (typeof value === 'string' && isNumericString(value)) {
      return Number(value)
    }
    return value
  }

  if (fieldType === 'DateTime') {
    if (value instanceof Date) return value
    if (typeof value === 'string') {
      const dt = new Date(value)
      return Number.isNaN(dt.getTime()) ? value : dt
    }
    return value
  }

  return value
}

export function transformAggregateRow(row: any, model?: Model): any {
  if (!row || typeof row !== 'object') return row

  const result: any = {}

  for (const key in row) {
    if (!Object.prototype.hasOwnProperty.call(row, key)) continue

    const value = row[key]
    const dotIndex = key.indexOf('.')

    if (dotIndex === -1) {
      result[key] = value
      continue
    }

    const prefix = key.slice(0, dotIndex)
    const suffix = key.slice(dotIndex + 1)

    if (AGGREGATE_PREFIXES.has(prefix)) {
      if (!result[prefix]) {
        result[prefix] = {}
      }
      result[prefix][suffix] = convertScalarAggregateValue(
        prefix,
        suffix,
        value,
        model,
      )
    } else {
      result[key] = value
    }
  }

  return result
}

export function extractCountValue(row: any): number | bigint {
  if (!row || typeof row !== 'object') return 0

  if ('_count._all' in row) {
    const value = row['_count._all']
    if (typeof value === 'string') {
      const n = Number(value)
      if (Number.isSafeInteger(n)) return n
      return BigInt(value)
    }
    return value as number | bigint
  }

  if ('_count' in row && row['_count'] && typeof row['_count'] === 'object') {
    const countObj = row['_count'] as Record<string, unknown>
    if ('_all' in countObj) {
      const value = countObj['_all']
      if (typeof value === 'string') {
        const n = Number(value)
        if (Number.isSafeInteger(n)) return n
        return BigInt(value)
      }
      return value as number | bigint
    }
  }

  const keys = Object.keys(row)
  for (const key of keys) {
    if (key.includes('count') || key.includes('COUNT')) {
      const value = row[key]
      if (typeof value === 'number' || typeof value === 'bigint') {
        return value
      }
      if (typeof value === 'string') {
        const n = Number(value)
        if (Number.isSafeInteger(n)) return n
        return BigInt(value)
      }
    }
  }

  return 0
}

export function getRowTransformer(
  method: string,
  model?: Model,
): ((row: any) => any) | null {
  if (method === 'count') {
    return extractCountValue
  }

  if (method === 'groupBy' || method === 'aggregate') {
    return (row: any) => transformAggregateRow(row, model)
  }

  return null
}
