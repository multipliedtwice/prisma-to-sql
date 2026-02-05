import { isDynamicParameter } from '@dee-wan/schema-parser'
import { SQL_TEMPLATES, SQL_SEPARATORS } from '../shared/constants'
import { col, quote } from '../shared/sql-utils'
import { SelectQuerySpec, SqlResult } from '../shared/types'
import {
  validateSelectQuery,
  validateParamConsistencyByDialect,
} from '../shared/validators/sql-validators'
import {
  isNotNullish,
  isNonEmptyArray,
  isNonEmptyString,
  isPlainObject,
} from '../shared/validators/type-guards'
import { addAutoScoped } from '../shared/dynamic-params'
import { jsonBuildObject } from '../../sql-builder-dialect'
import { buildRelationCountSql } from './includes'
import { joinNonEmpty } from '../shared/string-builder'

const ALIAS_CAPTURE = '([A-Za-z_][A-Za-z0-9_]*)'
const COLUMN_PART = '(?:"([^"]+)"|([a-z_][a-z0-9_]*))'
const AS_PART = `(?:\\s+AS\\s+${COLUMN_PART})?`
const SIMPLE_COLUMN_PATTERN = `^${ALIAS_CAPTURE}\\.${COLUMN_PART}${AS_PART}$`
const SIMPLE_COLUMN_RE = new RegExp(SIMPLE_COLUMN_PATTERN, 'i')

function buildWhereSql(conditions: readonly string[]): string {
  if (!isNonEmptyArray(conditions)) return ''
  return (
    ' ' +
    SQL_TEMPLATES.WHERE +
    ' ' +
    conditions.join(SQL_SEPARATORS.CONDITION_AND)
  )
}

function buildJoinsSql(
  ...joinGroups: Array<readonly string[] | undefined>
): string {
  const all: string[] = []
  for (const g of joinGroups) {
    if (isNonEmptyArray(g)) {
      for (const j of g) all.push(j)
    }
  }
  return all.length > 0 ? ' ' + all.join(' ') : ''
}

function buildSelectList(baseSelect: string, extraCols: string): string {
  const base = baseSelect.trim()
  const extra = extraCols.trim()
  if (!base) return extra
  if (!extra) return base
  return base + SQL_SEPARATORS.FIELD_LIST + extra
}

function finalizeSql(
  sql: string,
  params: SelectQuerySpec['params'],
  dialect: SelectQuerySpec['dialect'],
): SqlResult {
  const snapshot = params.snapshot()
  validateSelectQuery(sql)
  validateParamConsistencyByDialect(
    sql,
    snapshot.params,
    dialect === 'sqlite' ? 'postgres' : dialect,
  )
  return {
    sql,
    params: snapshot.params,
    paramMappings: snapshot.mappings,
  }
}

function parseSimpleScalarSelect(select: string, fromAlias: string): string[] {
  const raw = select.trim()
  if (raw.length === 0) return []

  const fromLower = fromAlias.toLowerCase()
  const parts = raw.split(SQL_SEPARATORS.FIELD_LIST)
  const names: string[] = []

  const isIdent = (s: string): boolean => /^[A-Za-z_][A-Za-z0-9_]*$/.test(s)

  const readIdentOrQuoted = (
    s: string,
    start: number,
  ): { text: string; next: number; quoted: boolean } => {
    const n = s.length
    if (start >= n) return { text: '', next: start, quoted: false }

    if (s.charCodeAt(start) === 34) {
      let i = start + 1
      let out = ''
      let saw = false
      while (i < n) {
        const c = s.charCodeAt(i)
        if (c === 34) {
          const next = i + 1
          if (next < n && s.charCodeAt(next) === 34) {
            out += '"'
            saw = true
            i += 2
            continue
          }
          if (!saw)
            throw new Error(
              `sqlite distinct emulation: empty quoted identifier in: ${s}`,
            )
          return { text: out, next: i + 1, quoted: true }
        }
        out += s[i]
        saw = true
        i++
      }
      throw new Error(
        `sqlite distinct emulation: unterminated quoted identifier in: ${s}`,
      )
    }

    let i = start
    while (i < n) {
      const c = s.charCodeAt(i)
      if (c === 32 || c === 9) break
      if (c === 46) break
      i++
    }
    return { text: s.slice(start, i), next: i, quoted: false }
  }

  const skipSpaces = (s: string, i: number): number => {
    while (i < s.length) {
      const c = s.charCodeAt(i)
      if (c !== 32 && c !== 9) break
      i++
    }
    return i
  }

  for (let idx = 0; idx < parts.length; idx++) {
    const p = parts[idx].trim()
    if (p.length === 0) continue

    let i = 0
    i = skipSpaces(p, i)

    const a = readIdentOrQuoted(p, i)
    const actualAlias = a.text.toLowerCase()
    if (!isIdent(a.text)) {
      throw new Error(
        `sqlite distinct emulation requires scalar select fields to be simple columns (alias.column). Got: ${p}`,
      )
    }
    if (actualAlias !== fromLower) {
      throw new Error(`Expected alias '${fromAlias}', got '${a.text}' in: ${p}`)
    }
    i = a.next

    if (i >= p.length || p.charCodeAt(i) !== 46) {
      throw new Error(
        `sqlite distinct emulation requires scalar select fields to be simple columns (alias.column). Got: ${p}`,
      )
    }
    i++
    i = skipSpaces(p, i)

    const colPart = readIdentOrQuoted(p, i)
    const columnName = colPart.text.trim()
    if (columnName.length === 0) {
      throw new Error(`Failed to parse selected column name from: ${p}`)
    }
    i = colPart.next

    i = skipSpaces(p, i)

    let outAlias = ''
    if (i < p.length) {
      const rest = p.slice(i).trim()
      if (rest.length > 0) {
        const m = rest.match(/^AS\s+/i)
        if (!m) {
          throw new Error(
            `sqlite distinct emulation requires scalar select fields to be simple columns (optionally with AS). Got: ${p}`,
          )
        }
        let j = i
        j = skipSpaces(p, j)
        if (!/^AS\b/i.test(p.slice(j))) {
          throw new Error(`Failed to parse AS in: ${p}`)
        }
        j += 2
        j = skipSpaces(p, j)
        const out = readIdentOrQuoted(p, j)
        outAlias = out.text.trim()
        if (outAlias.length === 0) {
          throw new Error(`Failed to parse output alias from: ${p}`)
        }
        j = skipSpaces(p, out.next)
        if (j !== p.length) {
          throw new Error(
            `sqlite distinct emulation requires scalar select fields to be simple columns (optionally with AS). Got: ${p}`,
          )
        }
      }
    }

    const name = outAlias.length > 0 ? outAlias : columnName
    names.push(name)
  }

  return names
}

function replaceOrderByAlias(
  orderBy: string,
  fromAlias: string,
  outerAlias: string,
): string {
  const src = String(fromAlias)
  if (src.length === 0) return orderBy
  const escaped = src.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  const re = new RegExp(`\\b${escaped}\\.`, 'gi')
  return orderBy.replace(re, outerAlias + '.')
}

function buildDistinctColumns(
  distinct: readonly string[],
  fromAlias: string,
  model?: any,
): string {
  return distinct
    .map((f) => col(fromAlias, f, model))
    .join(SQL_SEPARATORS.FIELD_LIST)
}

function buildOutputColumns(
  scalarNames: string[],
  includeNames: string[],
  hasCount: boolean,
): string {
  const outputCols = hasCount
    ? [...scalarNames, ...includeNames, '_count']
    : [...scalarNames, ...includeNames]

  const formatted = outputCols
    .map((n) => quote(n))
    .join(SQL_SEPARATORS.FIELD_LIST)
  if (!isNonEmptyString(formatted)) {
    throw new Error('distinct emulation requires at least one output column')
  }
  return formatted
}

function buildWindowOrder(args: {
  baseOrder: string
  idField: any
  fromAlias: string
  model?: any
}): string {
  const { baseOrder, idField, fromAlias, model } = args
  const fromLower = String(fromAlias).toLowerCase()

  const orderFields = baseOrder
    .split(SQL_SEPARATORS.ORDER_BY)
    .map((s) => s.trim().toLowerCase())

  const hasIdInOrder = orderFields.some(
    (f) =>
      f.startsWith(fromLower + '.id ') || f.startsWith(fromLower + '."id" '),
  )

  if (hasIdInOrder) return baseOrder

  const idTiebreaker = idField
    ? ', ' + col(fromAlias, 'id', model) + ' ASC'
    : ''
  return baseOrder + idTiebreaker
}

function buildSqliteDistinctQuery(
  spec: SelectQuerySpec,
  selectWithIncludes: string,
  countJoins?: string[],
): string {
  const { includes, from, whereClause, whereJoins, orderBy, distinct, model } =
    spec
  if (!isNotNullish(distinct) || !isNonEmptyArray(distinct)) {
    throw new Error('buildSqliteDistinctQuery requires distinct fields')
  }

  const scalarNames = parseSimpleScalarSelect(spec.select, from.alias)
  const includeNames = includes.map((i) => i.name)
  const hasCount = Boolean(spec.args?.select?._count)

  const outerSelectCols = buildOutputColumns(
    scalarNames,
    includeNames,
    hasCount,
  )
  const distinctCols = buildDistinctColumns([...distinct], from.alias, model)

  const fallbackOrder = [...distinct]
    .map((f) => col(from.alias, f, model) + ' ASC')
    .join(SQL_SEPARATORS.FIELD_LIST)

  const idField = model.fields.find(
    (f: any) => f.name === 'id' && !f.isRelation,
  )
  const baseOrder = isNonEmptyString(orderBy) ? orderBy : fallbackOrder

  const windowOrder = buildWindowOrder({
    baseOrder,
    idField,
    fromAlias: from.alias,
    model,
  })

  const outerOrder = isNonEmptyString(orderBy)
    ? replaceOrderByAlias(orderBy, from.alias, '"__tp_distinct"')
    : replaceOrderByAlias(fallbackOrder, from.alias, '"__tp_distinct"')

  const joins = buildJoinsSql(whereJoins, countJoins)

  const conditions: string[] = []
  if (whereClause && whereClause !== '1=1') conditions.push(whereClause)
  const whereSql = buildWhereSql(conditions)

  const innerSelectList = selectWithIncludes.trim()
  const innerComma = innerSelectList.length > 0 ? SQL_SEPARATORS.FIELD_LIST : ''

  const innerParts: string[] = [
    SQL_TEMPLATES.SELECT,
    innerSelectList + innerComma,
    'ROW_NUMBER() OVER (PARTITION BY ' +
      distinctCols +
      ' ORDER BY ' +
      windowOrder +
      ')',
    SQL_TEMPLATES.AS,
    '"__tp_rn"',
    SQL_TEMPLATES.FROM,
    from.table,
    from.alias,
  ]
  if (joins) innerParts.push(joins)
  if (whereSql) innerParts.push(whereSql)
  const inner = innerParts.join(' ')

  const outerParts: string[] = [
    SQL_TEMPLATES.SELECT,
    outerSelectCols,
    SQL_TEMPLATES.FROM,
    '(' + inner + ')',
    SQL_TEMPLATES.AS,
    '"__tp_distinct"',
    SQL_TEMPLATES.WHERE,
    '"__tp_rn" = 1',
  ]
  if (isNonEmptyString(outerOrder)) {
    outerParts.push(SQL_TEMPLATES.ORDER_BY, outerOrder)
  }
  return outerParts.join(' ')
}

function buildIncludeColumns(spec: SelectQuerySpec): {
  includeCols: string
  selectWithIncludes: string
  countJoins: string[]
} {
  const { select, includes, dialect, model, schemas, from, params } = spec
  const baseSelect = (select ?? '').trim()

  let countCols = ''
  let countJoins: string[] = []

  const countSelect = spec.args?.select?._count
  if (countSelect) {
    if (isPlainObject(countSelect) && 'select' in countSelect) {
      const countBuild = buildRelationCountSql(
        (countSelect as any).select,
        model,
        schemas,
        from.alias,
        params,
        dialect,
      )
      if (countBuild.jsonPairs) {
        countCols =
          jsonBuildObject(countBuild.jsonPairs, dialect) +
          ' ' +
          SQL_TEMPLATES.AS +
          ' ' +
          quote('_count')
      }
      countJoins = countBuild.joins
    }
  }

  const hasIncludes = isNonEmptyArray(includes)
  const hasCountCols = isNonEmptyString(countCols)

  if (!hasIncludes && !hasCountCols) {
    return { includeCols: '', selectWithIncludes: baseSelect, countJoins: [] }
  }

  const emptyJson = dialect === 'postgres' ? `'[]'::json` : `json('[]')`

  const includeCols = hasIncludes
    ? includes
        .map((inc) => {
          const expr = inc.isOneToOne
            ? '(' + inc.sql + ')'
            : 'COALESCE((' + inc.sql + '), ' + emptyJson + ')'
          return expr + ' ' + SQL_TEMPLATES.AS + ' ' + quote(inc.name)
        })
        .join(SQL_SEPARATORS.FIELD_LIST)
    : ''

  const allCols = joinNonEmpty(
    [includeCols, countCols],
    SQL_SEPARATORS.FIELD_LIST,
  )
  const selectWithIncludes = buildSelectList(baseSelect, allCols)
  return { includeCols: allCols, selectWithIncludes, countJoins }
}

function appendPagination(sql: string, spec: SelectQuerySpec): string {
  const { method, pagination, params } = spec
  const isFindUniqueOrFirst = method === 'findUnique' || method === 'findFirst'

  if (isFindUniqueOrFirst) {
    const parts: string[] = [sql, SQL_TEMPLATES.LIMIT, '1']
    const hasSkip =
      isNotNullish(pagination.skip) &&
      (isDynamicParameter(pagination.skip) ||
        (typeof pagination.skip === 'number' && pagination.skip > 0)) &&
      method === 'findFirst'

    if (hasSkip) {
      const placeholder = addAutoScoped(
        params,
        pagination.skip,
        'root.pagination.skip',
      )
      parts.push(SQL_TEMPLATES.OFFSET, placeholder)
    }
    return parts.join(' ')
  }

  const parts: string[] = [sql]

  if (isNotNullish(pagination.take)) {
    const placeholder = addAutoScoped(
      params,
      pagination.take,
      'root.pagination.take',
    )
    parts.push(SQL_TEMPLATES.LIMIT, placeholder)
  }

  if (isNotNullish(pagination.skip)) {
    const placeholder = addAutoScoped(
      params,
      pagination.skip,
      'root.pagination.skip',
    )
    parts.push(SQL_TEMPLATES.OFFSET, placeholder)
  }

  return parts.join(' ')
}

function hasWindowDistinct(spec: SelectQuerySpec): boolean {
  const d = spec.distinct
  return isNotNullish(d) && isNonEmptyArray(d)
}

function assertDistinctAllowed(
  method: SelectQuerySpec['method'],
  enabled: boolean,
): void {
  if (enabled && method !== 'findMany') {
    throw new Error(
      'distinct is only supported for findMany in this SQL builder',
    )
  }
}

function assertHasSelectFields(baseSelect: string, includeCols: string): void {
  if (!isNonEmptyString(baseSelect) && !isNonEmptyString(includeCols)) {
    throw new Error('SELECT requires at least one selected field or include')
  }
}

function withCountJoins(
  spec: SelectQuerySpec,
  countJoins: string[],
  whereJoins?: readonly string[],
): SelectQuerySpec {
  return {
    ...spec,
    whereJoins: [...(whereJoins || []), ...(countJoins || [])],
  }
}

function buildPostgresDistinctOnClause(
  fromAlias: string,
  distinct?: readonly string[],
  model?: any,
): string | null {
  if (!isNonEmptyArray(distinct)) return null
  const distinctCols = buildDistinctColumns([...distinct], fromAlias, model)
  return SQL_TEMPLATES.DISTINCT_ON + ' (' + distinctCols + ')'
}

function pushJoinGroups(
  parts: string[],
  ...groups: Array<readonly string[] | undefined>
): void {
  for (const g of groups) {
    if (isNonEmptyArray(g)) parts.push(g.join(' '))
  }
}

function buildConditions(
  whereClause?: string,
  cursorClause?: string,
): string[] {
  const conditions: string[] = []
  if (whereClause && whereClause !== '1=1') conditions.push(whereClause)
  if (isNotNullish(cursorClause) && isNonEmptyString(cursorClause))
    conditions.push(cursorClause)
  return conditions
}

function pushWhere(parts: string[], conditions: string[]): void {
  if (!isNonEmptyArray(conditions)) return
  parts.push(SQL_TEMPLATES.WHERE, conditions.join(SQL_SEPARATORS.CONDITION_AND))
}

export function constructFinalSql(spec: SelectQuerySpec): SqlResult {
  const {
    select,
    from,
    whereClause,
    whereJoins,
    orderBy,
    distinct,
    method,
    cursorCte,
    cursorClause,
    params,
    dialect,
    model,
  } = spec

  const useWindowDistinct = hasWindowDistinct(spec)
  assertDistinctAllowed(method, useWindowDistinct)

  const { includeCols, selectWithIncludes, countJoins } =
    buildIncludeColumns(spec)

  if (useWindowDistinct) {
    const baseSelect = (select ?? '').trim()
    assertHasSelectFields(baseSelect, includeCols)

    const spec2 = withCountJoins(spec, countJoins, whereJoins)
    let sql = buildSqliteDistinctQuery(spec2, selectWithIncludes).trim()
    sql = appendPagination(sql, spec)
    return finalizeSql(sql, params, dialect)
  }

  const parts: string[] = []
  if (cursorCte) parts.push('WITH ' + cursorCte)

  parts.push(SQL_TEMPLATES.SELECT)

  const distinctOn =
    dialect === 'postgres'
      ? buildPostgresDistinctOnClause(from.alias, distinct, model)
      : null
  if (distinctOn) parts.push(distinctOn)

  const baseSelect = (select ?? '').trim()
  const fullSelectList = buildSelectList(baseSelect, includeCols)
  if (!isNonEmptyString(fullSelectList)) {
    throw new Error('SELECT requires at least one selected field or include')
  }

  parts.push(fullSelectList)
  parts.push(SQL_TEMPLATES.FROM, from.table, from.alias)

  if (cursorCte) {
    const cteName = cursorCte.split(' AS ')[0].trim()
    parts.push('CROSS JOIN', cteName)
  }

  pushJoinGroups(parts, whereJoins, countJoins)

  const conditions = buildConditions(whereClause, cursorClause)
  pushWhere(parts, conditions)

  if (isNonEmptyString(orderBy)) parts.push(SQL_TEMPLATES.ORDER_BY, orderBy)

  let sql = parts.join(' ').trim()
  sql = appendPagination(sql, spec)
  return finalizeSql(sql, params, dialect)
}
