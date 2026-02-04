import { Model } from '../../types'
import { quote } from './sql-utils'
import { getColumnMap } from './model-field-cache'

export function resolveColumnName(model: Model, fieldName: string): string {
  const map = getColumnMap(model)
  return map.get(fieldName) || fieldName
}

export function quoteColumnOf(model: Model, fieldName: string): string {
  return quote(resolveColumnName(model, fieldName))
}

export function colOf(model: Model, alias: string, fieldName: string): string {
  return `${alias}.${quoteColumnOf(model, fieldName)}`
}
