// packages/generator/src/optimizer/builder/shared/validators/type-guards.ts

/**
 * Type Guards and Checks
 * Pure type checking with no business logic
 */

// ═══════════════════════════════════════════════════════════════
// Basic Type Guards
// ═══════════════════════════════════════════════════════════════

export function isNotNullish<T>(
  value: T | null | undefined,
): value is NonNullable<T> {
  return value !== null && value !== undefined
}

export function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0
}

export function isEmptyString(value: unknown): boolean {
  return typeof value === 'string' && value.trim().length === 0
}

export function isNonEmptyArray<T>(value: unknown): value is T[] {
  return Array.isArray(value) && value.length > 0
}

export function isEmptyArray(value: unknown): boolean {
  return Array.isArray(value) && value.length === 0
}

export function isPlainObject(val: unknown): val is Record<string, unknown> {
  if (!isNotNullish(val)) return false
  if (Array.isArray(val)) return false
  if (typeof val !== 'object') return false
  return Object.prototype.toString.call(val) === '[object Object]'
}

export function hasProperty<K extends string>(
  obj: unknown,
  key: K,
): obj is Record<K, unknown> {
  return isPlainObject(obj) && key in obj
}

// ═══════════════════════════════════════════════════════════════
// Prisma Type Checks
// ═══════════════════════════════════════════════════════════════

export function isArrayType(t: string | undefined): boolean {
  if (!isNotNullish(t)) return false
  const normalized = t.replace(/\?$/, '')
  return normalized.endsWith('[]')
}

export function isJsonType(t: string | undefined): boolean {
  return isNotNullish(t) && (t === 'Json' || t === 'Json?')
}

// ═══════════════════════════════════════════════════════════════
// Content Validation
// ═══════════════════════════════════════════════════════════════

export function hasValidContent(sql: string): boolean {
  return isNotNullish(sql) && sql.trim().length > 0
}

export function hasRequiredKeywords(sql: string): boolean {
  const upper = sql.toUpperCase()
  const hasSelect = upper.includes('SELECT')
  const hasFrom = upper.includes('FROM')
  return hasSelect && hasFrom && upper.indexOf('SELECT') < upper.indexOf('FROM')
}
