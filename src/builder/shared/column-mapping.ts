import { Model } from '../../types'
import { quote } from './sql-utils'

type AnyField = Record<string, unknown>

function pickFirstString(obj: AnyField, keys: string[]): string | undefined {
  for (const k of keys) {
    const v = obj[k]
    if (typeof v === 'string' && v.trim().length > 0) return v.trim()
  }
  return undefined
}

function fieldDbName(field: AnyField): string | undefined {
  return pickFirstString(field, [
    'dbName',
    'columnName',
    'mappedName',
    'databaseName',
    'db_name',
    'db_name_raw',
  ])
}

export function resolveColumnName(model: Model, fieldName: string): string {
  const f = model.fields.find((x: any) => x?.name === fieldName) as
    | AnyField
    | undefined
  if (!f) return fieldName
  return fieldDbName(f) ?? fieldName
}

export function quoteColumnOf(model: Model, fieldName: string): string {
  return quote(resolveColumnName(model, fieldName))
}

export function colOf(model: Model, alias: string, fieldName: string): string {
  return `${alias}.${quoteColumnOf(model, fieldName)}`
}
