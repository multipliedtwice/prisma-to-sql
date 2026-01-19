export const SQL_SEPARATORS = Object.freeze({
  FIELD_LIST: ', ',
  CONDITION_AND: ' AND ',
  CONDITION_OR: ' OR ',
  ORDER_BY: ', ',
} as const)

export const SQL_RESERVED_WORDS = new Set([
  'select',
  'from',
  'where',
  'and',
  'or',
  'not',
  'in',
  'like',
  'between',
  'order',
  'by',
  'group',
  'having',
  'limit',
  'offset',
  'join',
  'inner',
  'left',
  'right',
  'outer',
  'on',
  'as',
  'table',
  'column',
  'index',
  'user',
  'users',
  'values',
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
  'union',
  'intersect',
  'except',
  'case',
  'when',
  'then',
  'else',
  'end',
  'null',
  'true',
  'false',
  'is',
  'exists',
  'all',
  'any',
  'some',
])

export const SQL_KEYWORDS = SQL_RESERVED_WORDS

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

export const LIMITS = Object.freeze({
  MAX_QUERY_DEPTH: 50,
  MAX_ARRAY_SIZE: 10000,
  MAX_STRING_LENGTH: 10000,
} as const)
