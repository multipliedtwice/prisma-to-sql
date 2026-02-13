import { Model, Field } from '../../types'
import { Field as ParsedField } from '@dee-wan/schema-parser'
import { quote } from './sql-utils'

interface FieldIndices {
  scalarFields: ReadonlyMap<string, Field>
  relationFields: ReadonlyMap<string, Field>
  scalarNames: readonly string[]
  relationNames: readonly string[]
  jsonFields: ReadonlySet<string>
  pkFields: readonly string[]
  columnMap: ReadonlyMap<string, string>
  quotedColumns: ReadonlyMap<string, string>
}

const FIELD_INDICES_CACHE = new WeakMap<Model, FieldIndices>()

function normalizeField(field: ParsedField): Field {
  return field as unknown as Field
}

export function getFieldIndices(model: Model): FieldIndices {
  let cached = FIELD_INDICES_CACHE.get(model)
  if (cached) return cached

  const scalarFields = new Map<string, Field>()
  const relationFields = new Map<string, Field>()
  const scalarNames: string[] = []
  const relationNames: string[] = []
  const jsonFields = new Set<string>()
  const pkFields: string[] = []
  const columnMap = new Map<string, string>()
  const quotedColumns = new Map<string, string>()

  for (const rawField of model.fields) {
    const field = normalizeField(rawField)

    if (field.isRelation) {
      relationFields.set(field.name, field)
      relationNames.push(field.name)
    } else {
      scalarFields.set(field.name, field)
      scalarNames.push(field.name)

      const fieldType = String((field as any).type ?? '').toLowerCase()
      if (fieldType === 'json') {
        jsonFields.add(field.name)
      }

      if (
        (field as any).isId ||
        (field as any).isPrimaryKey ||
        (field as any).primaryKey
      ) {
        pkFields.push(field.name)
      }

      if (field.dbName && field.dbName !== field.name) {
        columnMap.set(field.name, field.dbName)
      }

      const columnName = field.dbName || field.name
      quotedColumns.set(field.name, quote(columnName))
    }
  }

  cached = Object.freeze({
    scalarFields,
    relationFields,
    scalarNames,
    relationNames,
    jsonFields,
    pkFields,
    columnMap,
    quotedColumns,
  })

  FIELD_INDICES_CACHE.set(model, cached)
  return cached
}

export function getRelationFieldSet(model: Model): ReadonlySet<string> {
  return new Set(getFieldIndices(model).relationNames)
}

export function getScalarFieldSet(model: Model): ReadonlySet<string> {
  return new Set(getFieldIndices(model).scalarNames)
}

export function getColumnMap(model: Model): ReadonlyMap<string, string> {
  return getFieldIndices(model).columnMap
}

export function getScalarFieldNames(model: Model): string[] {
  return [...getFieldIndices(model).scalarNames]
}

export function getQuotedColumn(
  model: Model,
  fieldName: string,
): string | undefined {
  return getFieldIndices(model).quotedColumns.get(fieldName)
}

export function getJsonFieldSet(model: Model): ReadonlySet<string> {
  return getFieldIndices(model).jsonFields
}

export function parseJsonIfNeeded(isJson: boolean, value: any): any {
  if (!isJson) return value
  if (value == null) return value
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

export function maybeParseJson(
  value: any,
  jsonSet: ReadonlySet<string>,
  fieldName: string,
): any {
  if (!jsonSet.has(fieldName)) return value
  if (value == null) return value
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}
