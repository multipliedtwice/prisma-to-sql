import { Model, Field } from '../../types'

interface FieldInfo {
  name: string
  dbName: string
  type: string
  isRelation: boolean
  isRequired: boolean
}

interface CachedModelInfo {
  fieldInfo: Map<string, FieldInfo>
  scalarFields: Set<string>
  relationFields: Set<string>
  columnMap: Map<string, string>
  fieldByName: Map<string, Field>
  quotedColumns: Map<string, string>
}

const MODEL_CACHE = new WeakMap<Model, CachedModelInfo>()

function quote(id: string): string {
  const needsQuoting =
    !/^[a-z_][a-z0-9_]*$/.test(id) ||
    /^(select|from|where|having|order|group|limit|offset|join|inner|left|right|outer|cross|full|and|or|not|by|as|on|union|intersect|except|case|when|then|else|end|user|users|table|column|index|values|in|like|between|is|exists|null|true|false|all|any|some|update|insert|delete|create|drop|alter|truncate|grant|revoke|exec|execute)$/i.test(
      id,
    )

  if (needsQuoting) {
    return `"${id.replace(/"/g, '""')}"`
  }
  return id
}

function ensureFullCache(model: Model): CachedModelInfo {
  let cache = MODEL_CACHE.get(model)

  if (!cache) {
    const fieldInfo = new Map<string, FieldInfo>()
    const scalarFields = new Set<string>()
    const relationFields = new Set<string>()
    const columnMap = new Map<string, string>()
    const fieldByName = new Map<string, Field>()
    const quotedColumns = new Map<string, string>()

    for (const f of model.fields) {
      const info: FieldInfo = {
        name: f.name,
        dbName: f.dbName || f.name,
        type: f.type,
        isRelation: !!f.isRelation,
        isRequired: !!f.isRequired,
      }
      fieldInfo.set(f.name, info)
      fieldByName.set(f.name, f)

      if (info.isRelation) {
        relationFields.add(f.name)
      } else {
        scalarFields.add(f.name)
        const dbName = info.dbName
        columnMap.set(f.name, dbName)
        quotedColumns.set(f.name, quote(dbName))
      }
    }

    cache = {
      fieldInfo,
      scalarFields,
      relationFields,
      columnMap,
      fieldByName,
      quotedColumns,
    }
    MODEL_CACHE.set(model, cache)
  }

  return cache
}

export function getFieldInfo(
  model: Model,
  fieldName: string,
): FieldInfo | undefined {
  return ensureFullCache(model).fieldInfo.get(fieldName)
}

export function getScalarFieldSet(model: Model): Set<string> {
  return ensureFullCache(model).scalarFields
}

export function getRelationFieldSet(model: Model): Set<string> {
  return ensureFullCache(model).relationFields
}

export function getColumnMap(model: Model): Map<string, string> {
  return ensureFullCache(model).columnMap
}

export function getFieldByName(
  model: Model,
  fieldName: string,
): Field | undefined {
  return ensureFullCache(model).fieldByName.get(fieldName)
}

export function getQuotedColumn(
  model: Model,
  fieldName: string,
): string | undefined {
  return ensureFullCache(model).quotedColumns.get(fieldName)
}
