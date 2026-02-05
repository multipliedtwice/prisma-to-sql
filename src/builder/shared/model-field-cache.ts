import { Model } from '../../types'

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
}

const MODEL_CACHE = new WeakMap<Model, CachedModelInfo>()

function ensureFullCache(model: Model): CachedModelInfo {
  let cache = MODEL_CACHE.get(model)

  if (!cache) {
    const fieldInfo = new Map<string, FieldInfo>()
    const scalarFields = new Set<string>()
    const relationFields = new Set<string>()
    const columnMap = new Map<string, string>()

    for (const f of model.fields) {
      const info: FieldInfo = {
        name: f.name,
        dbName: f.dbName || f.name,
        type: f.type,
        isRelation: !!f.isRelation,
        isRequired: !!f.isRequired,
      }
      fieldInfo.set(f.name, info)

      if (info.isRelation) {
        relationFields.add(f.name)
      } else {
        scalarFields.add(f.name)
        columnMap.set(f.name, info.dbName)
      }
    }

    cache = { fieldInfo, scalarFields, relationFields, columnMap }
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
