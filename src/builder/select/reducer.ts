import { Model } from '../../types'
import {
  getJsonFieldSet,
  getRelationFieldSet,
  maybeParseJson,
  parseJsonIfNeeded,
} from '../shared/model-field-cache'
import { getPrimaryKeyFields } from '../shared/primary-key-utils'
import { extractRelationEntries } from '../shared/relation-extraction-utils'
import { isPlainObject } from '../shared/validators/type-guards'
import { buildCompositeKey } from '../shared/key-utils'

import { getScalarFieldNames } from '../shared/model-field-cache'
import {
  extractScalarSelection,
  extractNestedIncludeSpec,
} from '../shared/relation-utils'

export interface ReducerConfig {
  parentModel: Model
  includedRelations: RelationMetadata[]
  allModels: readonly Model[]
}

interface ScalarColSpec {
  fieldName: string
  colName: string
  isJson: boolean
}

export interface RelationMetadata {
  name: string
  cardinality: 'one' | 'many'
  relatedModel: Model
  primaryKeyFields: string[]
  includeAllScalars: boolean
  selectedScalarFields: string[]
  nestedIncludes?: ReducerConfig | null

  path: string
  keyCols: string[]
  scalarCols: ScalarColSpec[]
}

function buildRelationScalarCols(
  relModel: Model,
  relPath: string,
  includeAllScalars: boolean,
  selectedScalarFields: string[],
): ScalarColSpec[] {
  const jsonSet = getJsonFieldSet(relModel)
  const scalarFields = includeAllScalars
    ? getScalarFieldNames(relModel)
    : selectedScalarFields

  const out: ScalarColSpec[] = []
  for (const fieldName of scalarFields) {
    out.push({
      fieldName,
      colName: `${relPath}.${fieldName}`,
      isJson: jsonSet.has(fieldName),
    })
  }
  return out
}

export function buildReducerConfig(
  parentModel: Model,
  includeSpec: Record<string, any>,
  allModels: readonly Model[],
  prefix: string = '',
  depth: number = 0,
): ReducerConfig {
  if (depth > 10) {
    throw new Error(
      `Reducer config exceeded maximum depth of 10 at path '${prefix}'`,
    )
  }

  const includedRelations: RelationMetadata[] = []
  const modelMap = new Map(allModels.map((m) => [m.name, m]))

  for (const [incName, incValue] of Object.entries(includeSpec)) {
    if (incValue === false) continue

    const field = parentModel.fields.find((f) => f.name === incName)
    if (!field || !field.isRelation) {
      throw new Error(
        `Field '${incName}' is not a relation on model ${parentModel.name}`,
      )
    }

    const relatedModel = modelMap.get(field.relatedModel!)
    if (!relatedModel) {
      throw new Error(
        `Related model '${field.relatedModel}' not found for relation '${incName}'`,
      )
    }

    const isList = typeof field.type === 'string' && field.type.endsWith('[]')
    const primaryKeyFields = getPrimaryKeyFields(relatedModel)
    const scalarSel = extractScalarSelection(incValue, relatedModel)

    const relPath = prefix ? `${prefix}.${incName}` : incName

    let nestedIncludes: ReducerConfig | null = null
    const nestedSpec = extractNestedIncludeSpec(incValue, relatedModel)
    if (Object.keys(nestedSpec).length > 0) {
      nestedIncludes = buildReducerConfig(
        relatedModel,
        nestedSpec,
        allModels,
        relPath,
        depth + 1,
      )
    }

    const keyCols = primaryKeyFields.map((f) => `${relPath}.${f}`)

    const scalarCols = buildRelationScalarCols(
      relatedModel,
      relPath,
      scalarSel.includeAllScalars,
      scalarSel.selectedScalarFields,
    )

    includedRelations.push({
      name: incName,
      cardinality: isList ? 'many' : 'one',
      relatedModel,
      primaryKeyFields,
      includeAllScalars: scalarSel.includeAllScalars,
      selectedScalarFields: scalarSel.selectedScalarFields,
      nestedIncludes,

      path: relPath,
      keyCols,
      scalarCols,
    })
  }

  return {
    parentModel,
    includedRelations,
    allModels,
  }
}

type ManyIndex = Map<string, any>
type IndexByPath = Map<string, ManyIndex>

function getIndexForParent(
  store: WeakMap<object, IndexByPath>,
  parentObj: object,
  path: string,
): ManyIndex {
  let byPath = store.get(parentObj)
  if (!byPath) {
    byPath = new Map()
    store.set(parentObj, byPath)
  }

  let idx = byPath.get(path)
  if (!idx) {
    idx = new Map()
    byPath.set(path, idx)
  }

  return idx
}

function initNestedPlaceholders(
  obj: any,
  nested: ReducerConfig | null | undefined,
): void {
  if (!nested) return
  for (const r of nested.includedRelations) {
    obj[r.name] = r.cardinality === 'many' ? [] : null
  }
}

function materializeRelationObject(
  row: any,
  rel: RelationMetadata,
): any | null {
  const relKey = buildCompositeKey(row, rel.keyCols)
  if (relKey == null) return null

  const obj: any = {}
  for (const c of rel.scalarCols) {
    obj[c.fieldName] = parseJsonIfNeeded(c.isJson, row[c.colName])
  }

  initNestedPlaceholders(obj, rel.nestedIncludes)
  return obj
}

function processNestedRelations(
  obj: any,
  rel: RelationMetadata,
  row: any,
  manyStore: WeakMap<object, IndexByPath>,
): void {
  if (!rel.nestedIncludes) return
  for (const nestedRel of rel.nestedIncludes.includedRelations) {
    processRelation(obj, nestedRel, row, manyStore)
  }
}

function processRelation(
  parentObj: any,
  rel: RelationMetadata,
  row: any,
  manyStore: WeakMap<object, IndexByPath>,
): void {
  const relKey = buildCompositeKey(row, rel.keyCols)
  if (relKey == null) return

  if (rel.cardinality === 'one') {
    let current = parentObj[rel.name]
    if (current == null) {
      const created = materializeRelationObject(row, rel)
      if (!created) return
      parentObj[rel.name] = created
      current = created
    }

    processNestedRelations(current, rel, row, manyStore)
    return
  }

  const arr = parentObj[rel.name] as any[]
  const idx = getIndexForParent(manyStore, parentObj, rel.path)

  const existing = idx.get(relKey)
  if (existing) {
    processNestedRelations(existing, rel, row, manyStore)
    return
  }

  const created = materializeRelationObject(row, rel)
  if (!created) return

  arr.push(created)
  idx.set(relKey, created)

  processNestedRelations(created, rel, row, manyStore)
}

function pickParentScalarFieldsFromRows(
  parentModel: Model,
  rows: any[],
): string[] {
  const all = getScalarFieldNames(parentModel)
  if (rows.length === 0) return all

  const row0 = rows[0]
  const picked: string[] = []
  for (const f of all) {
    if (Object.prototype.hasOwnProperty.call(row0, f)) picked.push(f)
  }
  return picked.length > 0 ? picked : all
}

export function reduceFlatRows(rows: any[], config: ReducerConfig): any[] {
  if (rows.length === 0) return []

  const { parentModel, includedRelations } = config
  const parentPkFields = getPrimaryKeyFields(parentModel)

  const parentKeyCols = parentPkFields
  const parentScalarFields = pickParentScalarFieldsFromRows(parentModel, rows)
  const parentJsonSet = getJsonFieldSet(parentModel)

  const resultMap = new Map<string, any>()
  const manyStore = new WeakMap<object, IndexByPath>()

  for (let rowIdx = 0; rowIdx < rows.length; rowIdx++) {
    const row = rows[rowIdx]
    const parentKey = buildCompositeKey(row, parentKeyCols)

    if (parentKey == null) continue

    let record = resultMap.get(parentKey)
    if (!record) {
      record = {}
      for (const fieldName of parentScalarFields) {
        record[fieldName] = maybeParseJson(
          row[fieldName],
          parentJsonSet,
          fieldName,
        )
      }
      for (const rel of includedRelations) {
        record[rel.name] = rel.cardinality === 'many' ? [] : null
      }
      resultMap.set(parentKey, record)
    }

    for (const rel of includedRelations) {
      processRelation(record, rel, row, manyStore)
    }
  }

  return Array.from(resultMap.values())
}
