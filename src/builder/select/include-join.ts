import { Model } from '../../types'
import { SqlDialect } from '../../sql-builder-dialect'
import { SQL_TEMPLATES } from '../shared/constants'
import { buildTableReference } from '../shared/sql-utils'

const INCLUDE_SCOPE_ROOT = 'include'
const INCLUDE_SCOPE_SEGMENT = '.include'
const PG_EMPTY_JSON_ARRAY = "'[]'::json"
const SQLITE_EMPTY_JSON_ARRAY = "json('[]')"

export function emptyJsonArray(dialect: SqlDialect): string {
  return dialect === 'postgres' ? PG_EMPTY_JSON_ARRAY : SQLITE_EMPTY_JSON_ARRAY
}

export function buildIncludeScope(includePath: readonly string[]): string {
  if (includePath.length === 0) return INCLUDE_SCOPE_ROOT
  let scope = INCLUDE_SCOPE_ROOT
  for (let i = 0; i < includePath.length; i++) {
    scope += `.${includePath[i]}`
    if (i < includePath.length - 1) {
      scope += INCLUDE_SCOPE_SEGMENT
    }
  }
  return scope
}

export function getRelationTableReference(
  relModel: Model,
  dialect: SqlDialect,
): string {
  return buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    relModel.tableName,
    dialect,
  )
}
