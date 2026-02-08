import { Model, Field } from '../../types'
import { needsQuoting } from './validators/sql-validators'

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

function quoteIdent(id: string): string {
  if (typeof id !== 'string' || id.trim().length === 0) {
    throw new Error('quoteIdent: identifier is required and cannot be empty')
  }
  for (let i = 0; i < id.length; i++) {
    const code = id.charCodeAt(i)
    if ((code >= 0x00 && code <= 0x1f) || code === 0x7f) {
      throw new Error(
        `quoteIdent: identifier contains invalid characters: ${JSON.stringify(id)}`,
      )
    }
  }
  if (needsQuoting(id)) {
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
        quotedColumns.set(f.name, quoteIdent(dbName))
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

export function getQuotedColumn(
  model: Model,
  fieldName: string,
): string | undefined {
  return ensureFullCache(model).quotedColumns.get(fieldName)
}
