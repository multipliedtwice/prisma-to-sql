import type { Model } from '../../types'
import type { WhereInSegment } from '../select/segment-planner'
import { getPrimaryKeyField } from './primary-key-utils'

const POSTGRES_MAX_PARAMS = 65535
const SQLITE_MAX_PARAMS = 999
const PARAM_SAFETY_MARGIN = 10

export function paramLimitForDialect(dialect: 'postgres' | 'sqlite'): number {
  return dialect === 'postgres' ? POSTGRES_MAX_PARAMS : SQLITE_MAX_PARAMS
}

export function maxTuplesPerBatch(
  dialect: 'postgres' | 'sqlite',
  keyColumnCount: number,
  baseChildParams: number,
): number {
  const limit = paramLimitForDialect(dialect)
  const cols = Math.max(1, keyColumnCount)
  const available = limit - baseChildParams - PARAM_SAFETY_MARGIN

  if (available < cols) {
    throw new Error(
      `Cannot build where-in batch: child query already uses ${baseChildParams} params, ` +
        `leaving fewer than ${cols} params for one key tuple under ${dialect} limit ${limit}.`,
    )
  }

  return Math.floor(available / cols)
}

export function compositeKey(values: readonly unknown[]): string {
  return JSON.stringify(values.map(toKeyPart))
}

export function toKeyPart(value: unknown): unknown {
  if (typeof value === 'bigint') return { __bigint: value.toString() }
  if (value instanceof Date) return { __date: value.getTime() }
  return value
}

export function extractTupleSafe(
  row: any,
  fieldNames: readonly string[],
): unknown[] | null {
  const values: unknown[] = []
  for (const f of fieldNames) {
    const v = row[f]
    if (v == null) return null
    values.push(v)
  }
  return values
}

export function extractTuples(
  rows: any[],
  fieldNames: readonly string[],
): unknown[][] {
  const tuples: unknown[][] = []
  for (const row of rows) {
    const t = extractTupleSafe(row, fieldNames)
    if (t !== null) tuples.push(t)
  }
  return tuples
}

export function dedupeTuples(tuples: unknown[][]): unknown[][] {
  const seen = new Set<string>()
  const unique: unknown[][] = []
  for (const t of tuples) {
    const k = compositeKey(t)
    if (seen.has(k)) continue
    seen.add(k)
    unique.push(t)
  }
  return unique
}

export function buildParentKeyIndex(
  parentRows: any[],
  parentKeyFieldNames: readonly string[],
): Map<string, any[]> {
  const index = new Map<string, any[]>()
  for (const parent of parentRows) {
    const t = extractTupleSafe(parent, parentKeyFieldNames)
    if (t === null) continue
    const key = compositeKey(t)
    let arr = index.get(key)
    if (!arr) {
      arr = []
      index.set(key, arr)
    }
    arr.push(parent)
  }
  return index
}

export function needsPerParentPagination(segment: WhereInSegment): boolean {
  return (
    segment.isList &&
    ((segment.perParentSkip != null && segment.perParentSkip > 0) ||
      segment.perParentTake != null)
  )
}

export function applyPerParentPagination(
  groupedChildren: any[],
  perParentSkip: number,
  perParentTake: number | undefined,
): any[] {
  if (perParentTake !== undefined && perParentTake < 0) {
    const absTake = -perParentTake
    const endIdx = groupedChildren.length - perParentSkip
    if (endIdx <= 0) return []
    const startIdx = Math.max(0, endIdx - absTake)
    return groupedChildren.slice(startIdx, endIdx)
  }
  const start = perParentSkip
  const end = perParentTake !== undefined ? start + perParentTake : undefined
  return groupedChildren.slice(start, end)
}

export function stitchChildrenToParents(
  children: any[],
  segment: WhereInSegment,
  parentKeyIndex: Map<string, any[]>,
): void {
  const grouped = new Map<string, any[]>()
  for (const child of children) {
    const t = extractTupleSafe(child, segment.fkFieldNames)
    if (t === null) continue
    const key = compositeKey(t)
    let arr = grouped.get(key)
    if (!arr) {
      arr = []
      grouped.set(key, arr)
    }
    arr.push(child)
  }

  const perParentPaginated = needsPerParentPagination(segment)

  for (const [fkKey, groupedChildren] of grouped) {
    const matchingParents = parentKeyIndex.get(fkKey)
    if (!matchingParents) continue

    const sliced = perParentPaginated
      ? applyPerParentPagination(
          groupedChildren,
          segment.perParentSkip || 0,
          segment.perParentTake,
        )
      : groupedChildren

    for (const parent of matchingParents) {
      if (segment.isList) {
        if (!Array.isArray(parent[segment.relationName])) {
          parent[segment.relationName] = []
        }
        for (const child of sliced) {
          parent[segment.relationName].push(child)
        }
      } else {
        parent[segment.relationName] = sliced[0] ?? null
      }
    }
  }
}

export function copyChildArgsBase(source: any, stripPagination: boolean): any {
  const base: any = {}
  if (source.select !== undefined) base.select = source.select
  if (source.include !== undefined) base.include = source.include
  if (source.orderBy !== undefined) base.orderBy = source.orderBy
  if (!stripPagination) {
    if (source.take !== undefined) base.take = source.take
    if (source.skip !== undefined) base.skip = source.skip
  }
  if (source.cursor !== undefined) base.cursor = source.cursor
  if (source.distinct !== undefined) base.distinct = source.distinct
  return base
}

export function applyWhereCondition(
  base: any,
  source: any,
  inCondition: Record<string, unknown>,
): void {
  base.where =
    source && source.where ? { AND: [source.where, inCondition] } : inCondition
}

export function isObjectArgs(relArgs: unknown): boolean {
  return relArgs !== true && typeof relArgs === 'object' && relArgs !== null
}

export function buildChildArgs(
  relArgs: unknown,
  fkFieldNames: readonly string[],
  uniqueParentTuples: unknown[][],
  stripPagination: boolean,
): any {
  const source = isObjectArgs(relArgs) ? (relArgs as any) : null
  const base = source ? copyChildArgsBase(source, stripPagination) : {}

  if (fkFieldNames.length === 1) {
    const values = uniqueParentTuples.map((t) => t[0])
    const inCondition = { [fkFieldNames[0]]: { in: values } }
    applyWhereCondition(base, source, inCondition)
    return base
  }

  if (source?.where !== undefined) base.where = source.where
  return base
}

export function buildPrerenderChildArgs(
  relArgs: unknown,
  fkFieldName: string,
  dynamicParam: string,
  stripPagination: boolean,
): any {
  const source = isObjectArgs(relArgs) ? (relArgs as any) : null
  const base = source ? copyChildArgsBase(source, stripPagination) : {}

  const inCondition = { [fkFieldName]: { in: dynamicParam as any } }
  applyWhereCondition(base, source, inCondition)
  return base
}

export function ensureFkInSelect(
  childArgs: any,
  fkFieldNames: readonly string[],
): string[] {
  if (!childArgs.select) return []
  const added: string[] = []
  const newSelect = { ...childArgs.select }
  for (const fk of fkFieldNames) {
    if (!newSelect[fk]) {
      newSelect[fk] = true
      added.push(fk)
    }
  }
  if (added.length === 0) return []
  childArgs.select = newSelect
  return added
}

export function ensureOrderByPk(childArgs: any, childModel: Model): void {
  if (childArgs.orderBy) return
  const pkField = getPrimaryKeyField(childModel)
  childArgs.orderBy = { [pkField]: 'asc' }
}

export function initRelationPlaceholders(
  row: any,
  segments: WhereInSegment[],
): void {
  for (const seg of segments) {
    row[seg.relationName] = seg.isList ? [] : null
  }
}

export const SQL_TERMINATOR_KEYWORDS = [
  'ORDER BY',
  'GROUP BY',
  'HAVING',
  'LIMIT',
  'OFFSET',
  'UNION',
  'INTERSECT',
  'EXCEPT',
  'FETCH',
] as const

export function scanSqlForKeywords(sql: string): {
  whereIdx: number
  terminatorIdx: number
} {
  let depth = 0
  let inDouble = false
  let inSingle = false
  let whereIdx = -1
  let terminatorIdx = -1

  for (let i = 0; i < sql.length; i++) {
    const c = sql[i]

    if (inSingle) {
      if (c === "'") {
        if (sql[i + 1] === "'") {
          i++
          continue
        }
        inSingle = false
      }
      continue
    }
    if (inDouble) {
      if (c === '"') {
        if (sql[i + 1] === '"') {
          i++
          continue
        }
        inDouble = false
      }
      continue
    }
    if (c === "'") {
      inSingle = true
      continue
    }
    if (c === '"') {
      inDouble = true
      continue
    }
    if (c === '(') {
      depth++
      continue
    }
    if (c === ')') {
      depth--
      continue
    }

    if (depth !== 0) continue

    const prevC = i > 0 ? sql[i - 1] : ' '
    if (!/\s/.test(prevC)) continue

    if (whereIdx === -1 && sql.startsWith('WHERE', i)) {
      const nextC = sql[i + 5]
      if (nextC === undefined || /\s/.test(nextC)) {
        whereIdx = i
        continue
      }
    }

    if (terminatorIdx === -1) {
      for (const kw of SQL_TERMINATOR_KEYWORDS) {
        if (!sql.startsWith(kw, i)) continue
        const nextC = sql[i + kw.length]
        if (nextC === undefined || /\s/.test(nextC)) {
          terminatorIdx = i
          break
        }
      }
    }
  }

  return { whereIdx, terminatorIdx }
}

export function extractMainTableAlias(sql: string): string | null {
  const match = sql.match(
    /\bFROM\s+(?:"[^"]+"\.)?"[^"]+"\s+(?:("[^"]+")|([A-Za-z_][A-Za-z0-9_]*))/i,
  )
  if (!match) return null
  return match[1] ?? match[2] ?? null
}

export function buildTupleInClause(args: {
  childModel: Model
  fkFieldNames: readonly string[]
  tuples: readonly unknown[][]
  tableAlias: string | null
  paramStartIdx: number
  dialect: 'postgres' | 'sqlite'
}): { sql: string; params: unknown[] } {
  const {
    childModel,
    fkFieldNames,
    tuples,
    tableAlias,
    paramStartIdx,
    dialect,
  } = args
  const prefix = tableAlias ? `${tableAlias}.` : ''
  const cols = fkFieldNames
    .map((name) => {
      const field = childModel.fields.find((f) => f.name === name)
      const dbName = field?.dbName || name
      return `${prefix}"${dbName}"`
    })
    .join(', ')

  let paramIdx = paramStartIdx
  const params: unknown[] = []
  const tupleParts: string[] = []
  for (const tuple of tuples) {
    const phs: string[] = []
    for (const v of tuple) {
      params.push(v)
      phs.push(dialect === 'postgres' ? `$${paramIdx++}` : '?')
    }
    tupleParts.push(`(${phs.join(', ')})`)
  }

  const sql = `(${cols}) IN (${tupleParts.join(', ')})`
  return { sql, params }
}

export function injectAndWhere(sql: string, additionalClause: string): string {
  const { whereIdx, terminatorIdx } = scanSqlForKeywords(sql)

  if (whereIdx !== -1) {
    const whereEnd = terminatorIdx !== -1 ? terminatorIdx : sql.length
    const whereContent = sql.slice(whereIdx + 6, whereEnd).trim()
    const rest = whereEnd < sql.length ? ' ' + sql.slice(whereEnd) : ''
    return (
      sql.slice(0, whereIdx) +
      `WHERE (${whereContent}) AND ${additionalClause}` +
      rest
    )
  }

  if (terminatorIdx !== -1) {
    return (
      sql.slice(0, terminatorIdx) +
      `WHERE ${additionalClause} ` +
      sql.slice(terminatorIdx)
    )
  }

  return `${sql} WHERE ${additionalClause}`
}

export function ensurePkInSelect(
  childArgs: any,
  childModel: Model,
): string | null {
  if (!childArgs.select) return null
  const pkField = getPrimaryKeyField(childModel)
  if (childArgs.select[pkField]) return null
  childArgs.select = { ...childArgs.select, [pkField]: true }
  return pkField
}
