import { Model } from '../../types'
import { getPrimaryKeyFields } from '../shared/primary-key-utils'
import {
  getJsonFieldSet,
  getScalarFieldNames,
  getFieldIndices,
  maybeParseJson,
  parseJsonIfNeeded,
} from '../shared/model-field-cache'
import {
  extractScalarSelection,
  extractNestedIncludeSpec,
} from '../shared/relation-utils'
import { isPlainObject } from '../shared/validators/type-guards'

export interface ArrayAggRelationMeta {
  name: string
  isList: boolean
  scalarFields: Array<{
    fieldName: string
    colName: string
    isJson: boolean
  }>
  pkFieldName: string
}

export interface ArrayAggReducerConfig {
  parentModel: Model
  parentScalarFields: string[]
  parentJsonSet: ReadonlySet<string>
  relations: ArrayAggRelationMeta[]
}

export function buildArrayAggReducerConfig(
  parentModel: Model,
  includeSpec: Record<string, any>,
  allModels: readonly Model[],
): ArrayAggReducerConfig {
  const modelMap = new Map(allModels.map((m) => [m.name, m]))
  const parentJsonSet = getJsonFieldSet(parentModel)
  const parentScalarFields = getScalarFieldNames(parentModel)

  const relations: ArrayAggRelationMeta[] = []

  for (const [relName, relValue] of Object.entries(includeSpec)) {
    if (relValue === false) continue

    const field = parentModel.fields.find((f) => f.name === relName)
    if (!field || !field.isRelation || !field.relatedModel) continue

    const relModel = modelMap.get(field.relatedModel)
    if (!relModel) continue

    const isList = typeof field.type === 'string' && field.type.endsWith('[]')
    const indices = getFieldIndices(relModel)
    const scalarSel = extractScalarSelection(relValue, relModel)
    const pkFields = getPrimaryKeyFields(relModel)
    const relJsonSet = getJsonFieldSet(relModel)

    const selectedFields = scalarSel.includeAllScalars
      ? Array.from(indices.scalarFields.keys())
      : [...new Set([...pkFields, ...scalarSel.selectedScalarFields])]

    const scalarCols = selectedFields
      .map((fieldName) => {
        const f = indices.scalarFields.get(fieldName)
        if (!f) return null
        return {
          fieldName: f.name,
          colName: `${relName}.${f.name}`,
          isJson: relJsonSet.has(f.name),
        }
      })
      .filter(Boolean) as ArrayAggRelationMeta['scalarFields']

    relations.push({
      name: relName,
      isList,
      scalarFields: scalarCols,
      pkFieldName: pkFields[0],
    })
  }

  return {
    parentModel,
    parentScalarFields,
    parentJsonSet,
    relations,
  }
}

export function reduceArrayAggRows(
  rows: any[],
  config: ArrayAggReducerConfig,
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
      const firstCol = rel.scalarFields[0]
      if (!firstCol) {
        record[rel.name] = rel.isList ? [] : null
        continue
      }

      const arr = row[firstCol.colName]

      if (!Array.isArray(arr) || arr.length === 0) {
        record[rel.name] = rel.isList ? [] : null
        continue
      }

      const len = arr.length
      const children = new Array(len)

      for (let j = 0; j < len; j++) {
        const child: any = {}
        for (const col of rel.scalarFields) {
          const values = row[col.colName]
          child[col.fieldName] = parseJsonIfNeeded(
            col.isJson,
            Array.isArray(values) ? values[j] : null,
          )
        }
        children[j] = child
      }

      record[rel.name] = rel.isList ? children : children[0] ?? null
    }

    results[i] = record
  }

  return results
}
