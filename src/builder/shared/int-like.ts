import { isDynamicParameter } from '@dee-wan/schema-parser'
import { isNotNullish } from './validators/type-guards'

type NormalizeIntLikeOptions = {
  min?: number
  max?: number
  allowZero?: boolean
}

export function normalizeIntLike(
  name: string,
  v: unknown,
  opts: NormalizeIntLikeOptions = {},
): number | string | undefined {
  if (!isNotNullish(v)) return undefined
  if (isDynamicParameter(v)) return v as string

  if (typeof v !== 'number' || !Number.isFinite(v) || !Number.isInteger(v)) {
    throw new Error(`${name} must be an integer`)
  }

  const min = opts.min ?? 0
  const allowZero = opts.allowZero ?? true

  if (!allowZero && v === 0) {
    throw new Error(`${name} must be > 0`)
  }
  if (v < min) {
    throw new Error(`${name} must be >= ${min}`)
  }

  if (typeof opts.max === 'number' && v > opts.max) {
    throw new Error(`${name} must be <= ${opts.max}`)
  }

  return v
}
