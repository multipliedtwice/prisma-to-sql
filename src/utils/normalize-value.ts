const MAX_DEPTH = 20

export function normalizeValue(
  value: unknown,
  seen = new WeakSet<object>(),
  depth = 0,
): unknown {
  if (depth > MAX_DEPTH) {
    throw new Error(`Max normalization depth exceeded (${MAX_DEPTH} levels)`)
  }
  if (value instanceof Date) {
    return normalizeDateValue(value)
  }
  if (typeof value === 'bigint') {
    return value.toString()
  }
  if (Array.isArray(value)) {
    return normalizeArrayValue(value, seen, depth)
  }
  if (value && typeof value === 'object') {
    return normalizeObjectValue(value, seen, depth)
  }
  return value
}

function normalizeDateValue(date: Date): string {
  const t = date.getTime()
  if (!Number.isFinite(t)) {
    throw new Error('Invalid Date value in SQL params')
  }
  return date.toISOString()
}

function normalizeArrayValue(
  value: unknown[],
  seen: WeakSet<object>,
  depth: number,
): unknown[] {
  const arrRef = value as unknown as object
  if (seen.has(arrRef)) {
    throw new Error('Circular reference in SQL params')
  }
  seen.add(arrRef)
  const out = value.map((v) => normalizeValue(v, seen, depth + 1))
  seen.delete(arrRef)
  return out
}

function normalizeObjectValue(
  value: object,
  seen: WeakSet<object>,
  depth: number,
): unknown {
  if (value instanceof Uint8Array) return value
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) return value

  const proto = Object.getPrototypeOf(value)
  const isPlain = proto === Object.prototype || proto === null
  if (!isPlain) return value

  const obj = value as Record<string, unknown>
  if (seen.has(obj)) {
    throw new Error('Circular reference in SQL params')
  }
  seen.add(obj)
  const out: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(obj)) {
    out[k] = normalizeValue(v, seen, depth + 1)
  }
  seen.delete(obj)
  return out
}
