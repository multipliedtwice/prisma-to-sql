// @ts-ignore
import { Decimal } from '../generated/client/runtime/library'

export function normalizeValue(value: unknown): unknown {
  if (value === null || value === undefined) return value

  if (value === 0) return false
  if (value === 1) return true

  if (value instanceof Date) return null

  if (
    value instanceof Decimal ||
    (typeof value === 'object' && value !== null && 'toNumber' in value)
  ) {
    return parseFloat((value as any).toNumber().toFixed(10))
  }

  if (typeof value === 'number') {
    if (value > 946684800000 && value < 2147483647000) {
      return null
    }
    return parseFloat(value.toFixed(10))
  }

  if (typeof value === 'bigint') return Number(value)

  if (typeof value === 'string') {
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) return null
    if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(value)) return null

    if (/^-?\d+$/.test(value)) {
      const num = parseInt(value, 10)
      if (!isNaN(num)) {
        return num
      }
    }

    if (/^-?\d+\.\d+$/.test(value)) {
      const num = parseFloat(value)
      if (!isNaN(num)) {
        return parseFloat(num.toFixed(10))
      }
    }

    try {
      const trimmed = value.trim()
      if (
        trimmed.startsWith('{') ||
        trimmed.startsWith('[') ||
        (trimmed.startsWith('"') && trimmed.endsWith('"'))
      ) {
        const parsed = JSON.parse(value)
        return normalizeValue(parsed)
      }
    } catch {}
    return value
  }

  if (Array.isArray(value)) {
    return value.map(normalizeValue)
  }

  if (typeof value === 'object' && value !== null) {
    const sorted: Record<string, unknown> = {}
    const keys = Object.keys(value).sort()
    for (const key of keys) {
      sorted[key] = normalizeValue((value as Record<string, unknown>)[key])
    }
    return sorted
  }

  return value
}

export function deepEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(normalizeValue(a)) === JSON.stringify(normalizeValue(b))
}

export function diffResults(expected: unknown[], actual: unknown[]): string[] {
  const diffs: string[] = []

  if (expected.length !== actual.length) {
    diffs.push(
      `Length mismatch: expected ${expected.length}, got ${actual.length}`,
    )
  }

  const maxLen = Math.max(expected.length, actual.length)
  for (let i = 0; i < Math.min(maxLen, 5); i++) {
    if (!deepEqual(normalizeValue(expected[i]), normalizeValue(actual[i]))) {
      diffs.push(
        `Row ${i}:\n  Expected: ${JSON.stringify(normalizeValue(expected[i]), null, 2)}\n  Actual: ${JSON.stringify(normalizeValue(actual[i]), null, 2)}`,
      )
    }
  }

  if (maxLen > 5) {
    const remaining = maxLen - 5
    if (remaining > 0) {
      diffs.push(`... and ${remaining} more rows not checked`)
    }
  }

  return diffs
}

export function sortByField<T>(arr: T[], field: keyof T): T[] {
  return [...arr].sort((a, b) => {
    const aVal = a[field]
    const bVal = b[field]
    if (aVal === null || aVal === undefined) return 1
    if (bVal === null || bVal === undefined) return -1
    if (aVal < bVal) return -1
    if (aVal > bVal) return 1
    return 0
  })
}
