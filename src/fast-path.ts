import type { Model } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import { buildTableReference, quote } from './builder/shared/sql-utils'
import { SQL_TEMPLATES } from './builder/shared/constants'
import { isPlainObject } from './builder/shared/validators/type-guards'
import { normalizeValue } from './utils/normalize-value'
import { makeAlias } from './query-cache'

interface SqlResult {
  sql: string
  params: unknown[]
}

function getIdField(model: Model): { name: string; dbName: string } | null {
  const idField = model.fields.find((f) => f.name === 'id' && !f.isRelation)
  if (!idField) return null
  return {
    name: 'id',
    dbName: idField.dbName || 'id',
  }
}

function buildColumnList(model: Model, alias: string): string {
  const cols: string[] = []
  for (const f of model.fields) {
    if (f.isRelation) continue
    if (f.name.startsWith('@') || f.name.startsWith('//')) continue
    const dbName = f.dbName || f.name
    if (dbName !== f.name) {
      cols.push(`${alias}.${quote(dbName)} AS ${quote(f.name)}`)
    } else {
      cols.push(`${alias}.${quote(dbName)}`)
    }
  }
  return cols.join(', ')
}

function getTableAndAlias(
  model: Model,
  dialect: SqlDialect,
): { tableName: string; alias: string } {
  const tableName = buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    model.tableName,
    dialect,
  )
  const alias = makeAlias(model.tableName)
  return { tableName, alias }
}

function buildSimpleQuery(
  model: Model,
  dialect: SqlDialect,
  where: string,
  params: unknown[],
  suffix: string = '',
): SqlResult {
  const { tableName, alias } = getTableAndAlias(model, dialect)
  const columns = buildColumnList(model, alias)
  const sql = `SELECT ${columns} FROM ${tableName} ${alias} ${where}${suffix}`
  return { sql, params }
}

function norm(value: unknown): unknown {
  return normalizeValue(value)
}

function isScalar(value: unknown): boolean {
  return value !== null && typeof value !== 'object'
}

function tryFindUniqueById(
  model: Model,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where
  if (
    !isPlainObject(where) ||
    Object.keys(where).length !== 1 ||
    !('id' in where) ||
    !isScalar(where.id) ||
    args.select ||
    args.include
  ) {
    return null
  }

  const idField = getIdField(model)
  if (!idField) return null

  const { alias } = getTableAndAlias(model, dialect)

  const whereClause =
    dialect === 'sqlite'
      ? `WHERE ${alias}.${quote(idField.dbName)} = ?`
      : `WHERE ${alias}.${quote(idField.dbName)} = $1`

  return buildSimpleQuery(
    model,
    dialect,
    whereClause,
    [norm(where.id)],
    ' LIMIT 1',
  )
}

function tryFindManyById(
  model: Model,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where
  if (
    !isPlainObject(where) ||
    Object.keys(where).length !== 1 ||
    !('id' in where) ||
    !isScalar(where.id) ||
    args.select ||
    args.include ||
    args.orderBy ||
    args.take ||
    args.skip ||
    args.distinct ||
    args.cursor
  ) {
    return null
  }

  const idField = getIdField(model)
  if (!idField) return null

  const { alias } = getTableAndAlias(model, dialect)

  const whereClause =
    dialect === 'sqlite'
      ? `WHERE ${alias}.${quote(idField.dbName)} = ?`
      : `WHERE ${alias}.${quote(idField.dbName)} = $1`

  return buildSimpleQuery(model, dialect, whereClause, [norm(where.id)])
}

function tryCountAll(
  model: Model,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where
  if ((where && Object.keys(where).length > 0) || args.select || args.skip) {
    return null
  }

  const { tableName, alias } = getTableAndAlias(model, dialect)
  const sql = `SELECT COUNT(*) AS ${quote('_count._all')} FROM ${tableName} ${alias}`
  return { sql, params: [] }
}

function tryFindManyWithLimit(
  model: Model,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where
  if (
    (where && Object.keys(where).length > 0) ||
    args.select ||
    args.include ||
    args.orderBy ||
    args.skip ||
    args.distinct ||
    args.cursor ||
    typeof args.take !== 'number' ||
    !Number.isInteger(args.take) ||
    args.take <= 0
  ) {
    return null
  }

  const { tableName, alias } = getTableAndAlias(model, dialect)
  const columns = buildColumnList(model, alias)
  const sql =
    dialect === 'sqlite'
      ? `SELECT ${columns} FROM ${tableName} ${alias} LIMIT ?`
      : `SELECT ${columns} FROM ${tableName} ${alias} LIMIT $1`

  return { sql, params: [norm(args.take)] }
}

function tryFindFirstBySingleField(
  model: Model,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where
  if (
    !isPlainObject(where) ||
    Object.keys(where).length !== 1 ||
    args.select ||
    args.include ||
    args.orderBy ||
    args.skip
  ) {
    return null
  }

  const field = Object.keys(where)[0]
  const value = where[field]
  if (value === null || typeof value === 'object') {
    return null
  }

  const fieldDef = model.fields.find((f) => f.name === field)
  if (!fieldDef || fieldDef.isRelation) {
    return null
  }

  const { alias } = getTableAndAlias(model, dialect)
  const columnName = fieldDef.dbName || field

  const whereClause =
    dialect === 'sqlite'
      ? `WHERE ${alias}.${quote(columnName)} = ?`
      : `WHERE ${alias}.${quote(columnName)} = $1`

  return buildSimpleQuery(
    model,
    dialect,
    whereClause,
    [norm(value)],
    ' LIMIT 1',
  )
}

function tryFindManyBySingleField(
  model: Model,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where
  if (
    !isPlainObject(where) ||
    Object.keys(where).length !== 1 ||
    args.select ||
    args.include ||
    args.orderBy ||
    args.take ||
    args.skip ||
    args.distinct ||
    args.cursor
  ) {
    return null
  }

  const field = Object.keys(where)[0]
  const value = where[field]
  if (value === null || typeof value === 'object') {
    return null
  }

  const fieldDef = model.fields.find((f) => f.name === field)
  if (!fieldDef || fieldDef.isRelation) {
    return null
  }

  const { alias } = getTableAndAlias(model, dialect)
  const columnName = fieldDef.dbName || field

  const whereClause =
    dialect === 'sqlite'
      ? `WHERE ${alias}.${quote(columnName)} = ?`
      : `WHERE ${alias}.${quote(columnName)} = $1`

  return buildSimpleQuery(model, dialect, whereClause, [norm(value)])
}

function tryFindManyAll(
  model: Model,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where
  if (
    (where && Object.keys(where).length > 0) ||
    args.select ||
    args.include ||
    args.orderBy ||
    args.take !== undefined ||
    args.skip !== undefined ||
    args.distinct ||
    args.cursor
  ) {
    return null
  }

  const { tableName, alias } = getTableAndAlias(model, dialect)
  const columns = buildColumnList(model, alias)
  const sql = `SELECT ${columns} FROM ${tableName} ${alias}`
  return { sql, params: [] }
}

export function tryFastPath(
  model: Model,
  method: string,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  if (method === 'findUnique') {
    return tryFindUniqueById(model, args, dialect)
  }

  if (method === 'count') {
    return tryCountAll(model, args, dialect)
  }

  if (method === 'findFirst') {
    return tryFindFirstBySingleField(model, args, dialect)
  }

  if (method === 'findMany') {
    return (
      tryFindManyById(model, args, dialect) ||
      tryFindManyWithLimit(model, args, dialect) ||
      tryFindManyBySingleField(model, args, dialect) ||
      tryFindManyAll(model, args, dialect)
    )
  }

  return null
}
