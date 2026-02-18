import { Model } from '../../types'
import {
  getScalarFieldNames,
  getJsonFieldSet,
  maybeParseJson,
} from '../shared/model-field-cache'
import type { LateralRelationMeta } from './lateral-join'

interface LateralReducerConfig {
  parentScalarFields: string[]
  parentJsonSet: ReadonlySet<string>
  relations: LateralRelationMeta[]
}

export function buildLateralReducerConfig(
  parentModel: Model,
  lateralMeta: LateralRelationMeta[],
): LateralReducerConfig {
  return {
    parentScalarFields: getScalarFieldNames(parentModel),
    parentJsonSet: getJsonFieldSet(parentModel),
    relations: lateralMeta,
  }
}

function fixChildTypes(
  obj: any,
  fieldTypes: LateralRelationMeta['fieldTypes'],
  nestedRelations: LateralRelationMeta[],
): void {
  for (const { fieldName, type } of fieldTypes) {
    if (!(fieldName in obj) || obj[fieldName] == null) continue
    if (type === 'datetime' && typeof obj[fieldName] === 'string') {
      obj[fieldName] = new Date(obj[fieldName])
    }
  }

  for (const rel of nestedRelations) {
    const data = obj[rel.name]
    if (Array.isArray(data)) {
      for (let i = 0; i < data.length; i++) {
        fixChildTypes(data[i], rel.fieldTypes, rel.nestedRelations)
      }
    } else if (data != null && typeof data === 'object') {
      fixChildTypes(data, rel.fieldTypes, rel.nestedRelations)
    }
  }
}

export function reduceLateralRows(
  rows: any[],
  config: LateralReducerConfig,
): any[] {
  if (rows.length === 0) return []

  const { parentScalarFields, parentJsonSet, relations } = config
  const results = new Array(rows.length)

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]
    const record: any = {}

    for (const fieldName of parentScalarFields) {
      if (!(fieldName in row)) continue
      record[fieldName] = maybeParseJson(
        row[fieldName],
        parentJsonSet,
        fieldName,
      )
    }

    for (const rel of relations) {
      const data = row[rel.name]

      if (rel.isList) {
        const arr = Array.isArray(data) ? data : []
        for (let j = 0; j < arr.length; j++) {
          fixChildTypes(arr[j], rel.fieldTypes, rel.nestedRelations)
        }
        record[rel.name] = arr
      } else {
        if (data != null && typeof data === 'object') {
          fixChildTypes(data, rel.fieldTypes, rel.nestedRelations)
        }
        record[rel.name] = data ?? null
      }
    }

    results[i] = record
  }

  return results
}
