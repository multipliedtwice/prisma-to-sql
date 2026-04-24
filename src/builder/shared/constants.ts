export const IS_PRODUCTION = process.env.NODE_ENV === 'production'

export const SQL_SEPARATORS = Object.freeze({
  FIELD_LIST: ', ',
  CONDITION_AND: ' AND ',
  CONDITION_OR: ' OR ',
  ORDER_BY: ', ',
} as const)

export const ALIAS_FORBIDDEN_KEYWORDS = new Set([
  'select',
  'from',
  'where',
  'having',
  'order',
  'group',
  'limit',
  'offset',
  'join',
  'inner',
  'left',
  'right',
  'outer',
  'cross',
  'full',
  'and',
  'or',
  'not',
  'by',
  'as',
  'on',
  'union',
  'intersect',
  'except',
  'case',
  'when',
  'then',
  'else',
  'end',
])

export const SQL_KEYWORDS = new Set([
  ...ALIAS_FORBIDDEN_KEYWORDS,
  'user',
  'users',
  'table',
  'column',
  'index',
  'values',
  'in',
  'like',
  'between',
  'is',
  'exists',
  'null',
  'true',
  'false',
  'all',
  'any',
  'some',
  'update',
  'insert',
  'delete',
  'create',
  'drop',
  'alter',
  'truncate',
  'grant',
  'revoke',
  'exec',
  'execute',
])

export const SQL_RESERVED_WORDS = SQL_KEYWORDS

export const DEFAULT_WHERE_CLAUSE = '1=1' as const

export const SPECIAL_FIELDS = Object.freeze({
  ID: 'id',
} as const)

export const SQL_TEMPLATES = Object.freeze({
  PUBLIC_SCHEMA: 'public',
  WHERE: 'WHERE',
  SELECT: 'SELECT',
  FROM: 'FROM',
  ORDER_BY: 'ORDER BY',
  GROUP_BY: 'GROUP BY',
  HAVING: 'HAVING',
  LIMIT: 'LIMIT',
  OFFSET: 'OFFSET',
  COUNT_ALL: 'COUNT(*)',
  AS: 'AS',
  DISTINCT_ON: 'DISTINCT ON',
  IS_NULL: 'IS NULL',
  IS_NOT_NULL: 'IS NOT NULL',
  LIKE: 'LIKE',
  AND: 'AND',
  OR: 'OR',
  NOT: 'NOT',
} as const)

export const SCHEMA_PREFIXES = Object.freeze({
  INTERNAL: '@',
  COMMENT: '//',
} as const)

export const Ops = Object.freeze({
  EQUALS: 'equals',
  NOT: 'not',
  GT: 'gt',
  GTE: 'gte',
  LT: 'lt',
  LTE: 'lte',
  IN: 'in',
  NOT_IN: 'notIn',
  CONTAINS: 'contains',
  STARTS_WITH: 'startsWith',
  ENDS_WITH: 'endsWith',
  HAS: 'has',
  HAS_SOME: 'hasSome',
  HAS_EVERY: 'hasEvery',
  IS_EMPTY: 'isEmpty',
  PATH: 'path',
  STRING_CONTAINS: 'string_contains',
  STRING_STARTS_WITH: 'string_starts_with',
  STRING_ENDS_WITH: 'string_ends_with',
} as const)

export const LogicalOps = Object.freeze({
  AND: 'AND',
  OR: 'OR',
  NOT: 'NOT',
} as const)

export const RelationFilters = Object.freeze({
  SOME: 'some',
  EVERY: 'every',
  NONE: 'none',
} as const)

export const Modes = Object.freeze({
  INSENSITIVE: 'insensitive',
  DEFAULT: 'default',
} as const)

export const Wildcards: Readonly<Record<string, (v: string) => string>> =
  Object.freeze({
    [Ops.CONTAINS]: (v: string) => `%${v}%`,
    [Ops.STARTS_WITH]: (v: string) => `${v}%`,
    [Ops.ENDS_WITH]: (v: string) => `%${v}`,
  })

export const REGEX_CACHE = {
  PARAM_PLACEHOLDER: /\$(\d+)/g,
  VALID_IDENTIFIER: /^[a-z_][a-z0-9_]*$/,
} as const

export interface LimitsConfig {
  /** Max nesting depth for WHERE clauses (AND/OR/NOT). Default: 50 */
  MAX_QUERY_DEPTH: number
  /** Max elements in array params (in, hasSome, etc). Default: 10000 */
  MAX_ARRAY_SIZE: number
  /** Max string length for LIKE/JSON string operators. Default: 10000 */
  MAX_STRING_LENGTH: number
  /** Max nesting depth for HAVING clause. Default: 50 */
  MAX_HAVING_DEPTH: number
  /** Max depth of nested include/select relations. Default: 5 */
  MAX_INCLUDE_DEPTH: number
  /** Max number of relations included at a single level. Default: 10 */
  MAX_INCLUDES_PER_LEVEL: number
  /** Max total correlated subqueries across the entire query. Default: 100 */
  MAX_TOTAL_SUBQUERIES: number
  /** Max times a model can appear in its own include chain. Default: 2 */
  MAX_SELF_REFERENTIAL_DEPTH: number
  /** Max nesting depth for NOT operator composition. Default: 50 */
  MAX_NOT_DEPTH: number
  /** Postgres max integer for LIMIT/OFFSET. Default: 2147483647 */
  MAX_LIMIT_OFFSET: number
  /** Minimum allowed negative take value. Default: -10000 */
  MIN_NEGATIVE_TAKE: number
  /** Max depth for flat-join and lateral-join traversal. Default: 10 */
  MAX_NESTED_JOIN_DEPTH: number
  /** Safety threshold before alias counter overflow. Default: 1000 */
  MAX_ALIAS_COUNTER_THRESHOLD: number
  /** Max depth for relation-based orderBy resolution. Default: 10 */
  MAX_RELATION_ORDER_BY_DEPTH: number
  /** Max depth for join-based include strategy (0 = top-level only). Default: 0 */
  JOIN_INCLUDE_MAX_DEPTH: number
  /** Max recursion depth for where-in segment resolution. Default: 10 */
  MAX_WHERE_IN_RECURSIVE_DEPTH: number
  /** Max entries in the SQL query cache. Default: 1000 */
  QUERY_CACHE_SIZE: number
  /** Max entries in the SQLite prepared statement cache. Default: 1000 */
  STMT_CACHE_SIZE: number
}

const LIMITS_DEFAULTS: LimitsConfig = {
  MAX_QUERY_DEPTH: 50,
  MAX_ARRAY_SIZE: 10000,
  MAX_STRING_LENGTH: 10000,
  MAX_HAVING_DEPTH: 50,
  MAX_INCLUDE_DEPTH: 5,
  MAX_INCLUDES_PER_LEVEL: 10,
  MAX_TOTAL_SUBQUERIES: 100,
  MAX_SELF_REFERENTIAL_DEPTH: 2,
  MAX_NOT_DEPTH: 50,
  MAX_LIMIT_OFFSET: 2147483647,
  MIN_NEGATIVE_TAKE: -10000,
  MAX_NESTED_JOIN_DEPTH: 10,
  MAX_ALIAS_COUNTER_THRESHOLD: 1000,
  MAX_RELATION_ORDER_BY_DEPTH: 10,
  JOIN_INCLUDE_MAX_DEPTH: 0,
  MAX_WHERE_IN_RECURSIVE_DEPTH: 10,
  QUERY_CACHE_SIZE: 1000,
  STMT_CACHE_SIZE: 1000,
}

const limitsStore: LimitsConfig = { ...LIMITS_DEFAULTS }

/**
 * @returns Current limits configuration (mutable reference).
 */
export const LIMITS: LimitsConfig = limitsStore

/**
 * Override one or more query builder limits.
 * Only provided keys are updated; others keep their current values.
 */
export function setLimits(overrides: Partial<LimitsConfig>): void {
  for (const key of Object.keys(overrides) as Array<keyof LimitsConfig>) {
    const val = overrides[key]
    if (typeof val === 'number' && Number.isFinite(val)) {
      limitsStore[key] = val
    }
  }
}

/**
 * Returns a frozen snapshot of the current limits.
 */
export function getLimits(): Readonly<LimitsConfig> {
  return Object.freeze({ ...limitsStore })
}

/**
 * Reset all limits to their built-in defaults.
 */
export function resetLimits(): void {
  Object.assign(limitsStore, LIMITS_DEFAULTS)
}

export const AGGREGATE_PREFIXES = new Set([
  '_count',
  '_sum',
  '_avg',
  '_min',
  '_max',
]) as ReadonlySet<string>

export const DEBUG_PARAMS =
  typeof process !== 'undefined' && process.env?.DEBUG_PARAMS === '1'
