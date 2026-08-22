export interface VerifyDiff {
  path: string
  prisma: string
  extended: string
}

function isDecimalLike(value: object): boolean {
  const ctor = (value as { constructor?: { name?: string } }).constructor
  if (ctor?.name === 'Decimal') return true
  const v = value as Record<string, unknown>
  return (
    typeof v.toString === 'function' &&
    typeof v.toFixed === 'function' &&
    ('d' in v || 's' in v) &&
    !Array.isArray(value)
  )
}

export const DECIMAL_MARKER = '__decimal__'

export function canonicalize(value: unknown): unknown {
  if (typeof value === 'bigint') return value.toString()
  if (value === null || typeof value !== 'object') return value
  if (value instanceof Date) return value.toISOString()
  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString('base64')
  }
  if (isDecimalLike(value)) return [DECIMAL_MARKER, String(value)]
  if (Array.isArray(value)) return value.map(canonicalize)

  const out: Record<string, unknown> = {}
  for (const k of Object.keys(value as Record<string, unknown>).sort()) {
    const v = (value as Record<string, unknown>)[k]
    if (v === undefined) continue
    out[k] = canonicalize(v)
  }
  return out
}

function isDecimalMarker(v: unknown): v is [string, string] {
  return Array.isArray(v) && v[0] === DECIMAL_MARKER
}

function decimalToNumber(v: unknown): number | undefined {
  if (isDecimalMarker(v)) {
    const n = Number(v[1])
    return Number.isFinite(n) ? n : undefined
  }
  return undefined
}

function fmt(value: unknown): string {
  if (isDecimalMarker(value)) return `Decimal(${JSON.stringify(value[1])})`
  if (value === undefined) return 'undefined'
  try {
    return JSON.stringify(value) ?? String(value)
  } catch {
    return String(value)
  }
}

function matchesDecimalMarker(
  a: unknown,
  b: unknown,
  path: string,
  out: VerifyDiff[],
): boolean {
  if (!isDecimalMarker(a) && !isDecimalMarker(b)) return false
  const na = isDecimalMarker(a) ? decimalToNumber(a) : (a as number)
  const nb = isDecimalMarker(b) ? decimalToNumber(b) : (b as number)
  const ok =
    typeof na === 'number' &&
    typeof nb === 'number' &&
    !Number.isNaN(na) &&
    !Number.isNaN(nb) &&
    Object.is(na, nb)
  if (!ok) {
    out.push({ path, prisma: fmt(a), extended: fmt(b) })
  }
  return true
}

export function findDifferences(
  a: unknown,
  b: unknown,
  path = '$',
  out: VerifyDiff[] = [],
  maxDiffs = 10,
): VerifyDiff[] {
  if (out.length >= maxDiffs) return out

  if (a === b) return out

  if (matchesDecimalMarker(a, b, path, out)) return out

  const aNull = a === null || a === undefined
  const bNull = b === null || b === undefined
  if (aNull || bNull) {
    if ((a === undefined && bNull && b !== null) || (b === undefined && aNull && a !== null)) {
      return out
    }
    out.push({ path, prisma: fmt(a), extended: fmt(b) })
    return out
  }

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) {
      out.push({
        path,
        prisma: `array(length=${a.length})`,
        extended: `array(length=${b.length})`,
      })
      return out
    }
    for (let i = 0; i < a.length; i++) {
      findDifferences(a[i], b[i], `${path}[${i}]`, out, maxDiffs)
      if (out.length >= maxDiffs) return out
    }
    return out
  }

  const ta = typeof a
  const tb = typeof b
  if (ta !== tb) {
    out.push({ path, prisma: `${fmt(a)} (${ta})`, extended: `${fmt(b)} (${tb})` })
    return out
  }

  if (ta === 'object') {
    const oa = a as Record<string, unknown>
    const ob = b as Record<string, unknown>
    const keys = new Set([...Object.keys(oa), ...Object.keys(ob)])
    for (const k of keys) {
      findDifferences(oa[k], ob[k], `${path}.${k}`, out, maxDiffs)
      if (out.length >= maxDiffs) return out
    }
    return out
  }

  if (!Object.is(a, b)) {
    out.push({ path, prisma: fmt(a), extended: fmt(b) })
  }
  return out
}

export function compareResults(
  prismaResult: unknown,
  extendedResult: unknown,
): VerifyDiff[] {
  return findDifferences(
    canonicalize(prismaResult),
    canonicalize(extendedResult),
  )
}
