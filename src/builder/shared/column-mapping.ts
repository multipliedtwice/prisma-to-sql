import { Model } from '../../types'
import { col as colSql, quoteColumn as quoteColumnSql } from './sql-utils'

export function resolveColumnName(model: Model, fieldName: string): string {
  const q = quoteColumnSql(model, fieldName)
  if (q.startsWith('"') && q.endsWith('"') && q.length >= 2) {
    return q.slice(1, -1).replace(/""/g, '"')
  }
  return q
}

export function quoteColumnOf(model: Model, fieldName: string): string {
  return quoteColumnSql(model, fieldName)
}

export function colOf(model: Model, alias: string, fieldName: string): string {
  return colSql(alias, fieldName, model)
}
