/**
 * Normalize values for SQL params.
 *
 * ⚠️ IMPORTANT: Keep in sync with generated code in src/code-emitter.ts
 * Changes here must be manually copied to the generateCode() template.
 */
export function normalizeValue(
  value: unknown,
  seen = new WeakSet<object>(),
): unknown {
  // 1. Date → ISO string (with validation)
  if (value instanceof Date) {
    const t = value.getTime()
    if (!Number.isFinite(t)) {
      throw new Error('Invalid Date value in SQL params')
    }
    return value.toISOString()
  }

  // 2. BigInt → string (postgres/sqlite don't support BigInt directly)
  if (typeof value === 'bigint') {
    return value.toString()
  }

  // 3. Arrays → recurse with circular detection
  if (Array.isArray(value)) {
    const arrRef = value as unknown as object
    if (seen.has(arrRef)) {
      throw new Error('Circular reference in SQL params')
    }
    seen.add(arrRef)
    const out = value.map((v) => normalizeValue(v, seen))
    seen.delete(arrRef)
    return out
  }

  // 4. Objects → recurse (skip Buffer/Uint8Array, detect circular refs)
  if (value && typeof value === 'object') {
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
      out[k] = normalizeValue(v, seen)
    }
    seen.delete(obj)
    return out
  }

  // 5. Primitives → pass through
  return value
}
