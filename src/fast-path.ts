import type { Model } from './types'
import type { SqlDialect } from './sql-builder-dialect'
import { buildTableReference, quote } from './builder/shared/sql-utils'
import { SQL_TEMPLATES } from './builder/shared/constants'

interface SqlResult {
  sql: string
  params: unknown[]
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
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
    !args.select &&
    !args.include
  ) {
    const tableName = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      model.tableName,
      dialect,
    )
    
    const idField = model.fields.find(f => f.name === 'id')
    const columnName = idField?.dbName || 'id'
    
    const sql = dialect === 'sqlite'
      ? `SELECT * FROM ${tableName} WHERE ${quote(columnName)} = ? LIMIT 1`
      : `SELECT * FROM ${tableName} WHERE ${quote(columnName)} = $1 LIMIT 1`
    
    return {
      sql,
      params: [where.id]
    }
  }
  
  if (
    method === 'findMany' &&
    isPlainObject(where) &&
    Object.keys(where).length === 1 &&
    'id' in where &&
    !args.select &&
    !args.include &&
    !args.orderBy &&
    !args.take &&
    !args.skip
  ) {
    const tableName = buildTableReference(
      SQL_TEMPLATES.PUBLIC_SCHEMA,
      model.tableName,
      dialect,
    )
    
    const idField = model.fields.find(f => f.name === 'id')
    const columnName = idField?.dbName || 'id'
    
    const sql = dialect === 'sqlite'
      ? `SELECT * FROM ${tableName} WHERE ${quote(columnName)} = ?`
      : `SELECT * FROM ${tableName} WHERE ${quote(columnName)} = $1`
    
    return {
      sql,
      params: [where.id]
    }
  }
  
  return null
}