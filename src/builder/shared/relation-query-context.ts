import { Model } from '../../types'
import { SQL_SEPARATORS } from './constants'
import { quoteColumn } from './sql-utils'
import { isPlainObject, hasProperty } from './validators/type-guards'

export function extractWhereInput(relArgs: unknown): Record<string, unknown> {
  if (!isPlainObject(relArgs)) return {}
  if (!hasProperty(relArgs, 'where')) return {}
  const w = (relArgs as Record<string, unknown>).where
  return isPlainObject(w) ? (w as Record<string, unknown>) : {}
}

export function buildScalarColumnSelect(model: Model, alias: string): string {
  const cols: string[] = []
  for (const f of model.fields) {
    if (f.isRelation) continue
    cols.push(`${alias}.${quoteColumn(model, f.name)}`)
  }
  return cols.length > 0 ? cols.join(SQL_SEPARATORS.FIELD_LIST) : '*'
}
