import type { Model } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import { buildTableReference, quote } from './builder/shared/sql-utils'
import { SQL_TEMPLATES } from './builder/shared/constants'
import { isPlainObject } from './builder/shared/validators/type-guards'
import { normalizeValue } from './utils/normalize-value'

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

function buildColumnList(model: Model): string {
  const cols: string[] = []
  for (const f of model.fields) {
    if (f.isRelation) continue
    if (f.name.startsWith('@') || f.name.startsWith('//')) continue
    const dbName = f.dbName || f.name
    if (dbName !== f.name) {
      cols.push(`${quote(dbName)} AS ${quote(f.name)}`)
    } else {
      cols.push(quote(dbName))
    }
  }
  return cols.join(', ')
}

function buildSimpleQuery(
  model: Model,
  dialect: SqlDialect,
  where: string,
  params: unknown[],
  suffix: string = '',
): SqlResult {
  const tableName = buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    model.tableName,
    dialect,
  )
  const columns = buildColumnList(model)
  const sql = `SELECT ${columns} FROM ${tableName} ${where}${suffix}`
  return { sql, params }
}

function norm(value: unknown): unknown {
  return normalizeValue(value)
}

function isScalar(value: unknown): boolean {
  return value !== null && typeof value !== 'object'
}

export function tryFastPath(
  model: Model,
  method: string,
  args: Record<string, unknown>,
  dialect: SqlDialect,
): SqlResult | null {
  const where = args.where

  if (
    method === 'findUnique' &&
    isPlainObject(where) &&
    Object.keys(where).length === 1 &&
    'id' in where &&
    isScalar(where.id) &&
    !args.select &&
    !args.include
  ) {
    const idField = getIdField(model)
    if (!idField) return null
    const whereClause =
      dialect === 'sqlite'
        ? `WHERE ${quote(idField.dbName)} = ?`
        : `WHERE ${quote(idField.dbName)} = $1`
    return buildSimpleQuery(
      model,
      dialect,
      whereClause,
      [norm(where.id)],
      ' LIMIT 1',
    )
  }

  if (
    method === 'findMany' &&
    isPlainObject(where) &&
    Object.keys(where).length === 1 &&
    'id' in where &&
    isScalar(where.id) &&
    !args.select &&
    !args.include &&
    !args.orderBy &&
    !args.take &&
    !args.skip &&
    !args.distinct &&
    !args.cursor
  ) {
    const idField = getIdField(model)
    if (!idField) return null
    const whereClause =
      dialect === 'sqlite'
        ? `WHERE ${quote(idField.dbName)} = ?`
        : `WHERE ${quote(idField.dbName)} = $1`
    return buildSimpleQuery(model, dialect, whereClause, [norm(where.id)])
  }

  if (
    method === 'count' &&
    (!where || Object.keys(where).length === 0) &&
    !args.select &&
    !args.skip
  ) {
    const tableName = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      model.tableName,
      dialect,
    )
    const sql = `SELECT COUNT(*) AS ${quote('_count._all')} FROM ${tableName}`
    return { sql, params: [] }
  }

  if (
    method === 'findMany' &&
    (!where || Object.keys(where).length === 0) &&
    !args.select &&
    !args.include &&
    !args.orderBy &&
    !args.skip &&
    !args.distinct &&
    !args.cursor &&
    typeof args.take === 'number' &&
    args.take > 0
  ) {
    const tableName = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      model.tableName,
      dialect,
    )
    const columns = buildColumnList(model)
    const sql =
      dialect === 'sqlite'
        ? `SELECT ${columns} FROM ${tableName} LIMIT ?`
        : `SELECT ${columns} FROM ${tableName} LIMIT $1`
    return { sql, params: [norm(args.take)] }
  }

  if (
    method === 'findFirst' &&
    isPlainObject(where) &&
    Object.keys(where).length === 1 &&
    !args.select &&
    !args.include &&
    !args.orderBy &&
    !args.skip
  ) {
    const field = Object.keys(where)[0]
    const value = where[field]
    if (value !== null && typeof value !== 'object') {
      const fieldDef = model.fields.find((f) => f.name === field)
      if (fieldDef && !fieldDef.isRelation) {
        const columnName = fieldDef.dbName || field
        const whereClause =
          dialect === 'sqlite'
            ? `WHERE ${quote(columnName)} = ?`
            : `WHERE ${quote(columnName)} = $1`
        return buildSimpleQuery(
          model,
          dialect,
          whereClause,
          [norm(value)],
          ' LIMIT 1',
        )
      }
    }
  }

  if (
    method === 'findMany' &&
    isPlainObject(where) &&
    Object.keys(where).length === 1 &&
    !args.select &&
    !args.include &&
    !args.orderBy &&
    !args.take &&
    !args.skip &&
    !args.distinct &&
    !args.cursor
  ) {
    const field = Object.keys(where)[0]
    const value = where[field]
    if (value !== null && typeof value !== 'object') {
      const fieldDef = model.fields.find((f) => f.name === field)
      if (fieldDef && !fieldDef.isRelation) {
        const columnName = fieldDef.dbName || field
        const whereClause =
          dialect === 'sqlite'
            ? `WHERE ${quote(columnName)} = ?`
            : `WHERE ${quote(columnName)} = $1`
        return buildSimpleQuery(model, dialect, whereClause, [norm(value)])
      }
    }
  }

  if (
    method === 'findMany' &&
    (!where || Object.keys(where).length === 0) &&
    !args.select &&
    !args.include &&
    !args.orderBy &&
    !args.take &&
    !args.skip &&
    !args.distinct &&
    !args.cursor
  ) {
    const tableName = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      model.tableName,
      dialect,
    )
    const columns = buildColumnList(model)
    const sql = `SELECT ${columns} FROM ${tableName}`
    return { sql, params: [] }
  }

  return null
}
