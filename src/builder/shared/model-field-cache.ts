import { Model, Field } from '../../types'
import { quote } from './sql-utils'

const SCALAR_FIELD_CACHE = new WeakMap<Model, Set<string>>()
const RELATION_FIELD_CACHE = new WeakMap<Model, Set<string>>()
const COLUMN_MAP_CACHE = new WeakMap<Model, Map<string, string>>()
const QUOTED_COLUMN_CACHE = new WeakMap<Model, Map<string, string>>()
const FIELD_BY_NAME_CACHE = new WeakMap<Model, Map<string, Field>>()

export function getScalarFieldSet(model: Model): ReadonlySet<string> {
  let cached = SCALAR_FIELD_CACHE.get(model)
  if (cached) return cached

  const set = new Set<string>()
  for (const f of model.fields) {
    if (!f.isRelation) set.add(f.name)
  }

  SCALAR_FIELD_CACHE.set(model, set)
  return set
}

export function getRelationFieldSet(model: Model): ReadonlySet<string> {
  let cached = RELATION_FIELD_CACHE.get(model)
  if (cached) return cached

  const set = new Set<string>()
  for (const f of model.fields) {
    if (f.isRelation) set.add(f.name)
  }

  RELATION_FIELD_CACHE.set(model, set)
  return set
}

export function getColumnMap(model: Model): ReadonlyMap<string, string> {
  let cached = COLUMN_MAP_CACHE.get(model)
  if (cached) return cached

  const map = new Map<string, string>()
  for (const f of model.fields) {
    if (f.dbName && f.dbName !== f.name) {
      map.set(f.name, f.dbName)
    }
  }

  COLUMN_MAP_CACHE.set(model, map)
  return map
}

export function getQuotedColumn(
  model: Model,
  fieldName: string,
): string | undefined {
  let cache = QUOTED_COLUMN_CACHE.get(model)
  if (!cache) {
    cache = new Map()
    QUOTED_COLUMN_CACHE.set(model, cache)
  }

  const cached = cache.get(fieldName)
  if (cached !== undefined) return cached

  const columnMap = getColumnMap(model)
  const columnName = columnMap.get(fieldName) || fieldName
  const quoted = quote(columnName)

  cache.set(fieldName, quoted)
  return quoted
}

export function getFieldByName(
  model: Model,
  fieldName: string,
): Field | undefined {
  let cache = FIELD_BY_NAME_CACHE.get(model)
  if (!cache) {
    cache = new Map()
    for (const field of model.fields) {
      cache.set(field.name, field)
    }
    FIELD_BY_NAME_CACHE.set(model, cache)
  }

  return cache.get(fieldName)
}
