import type { Model } from '../../types'
import type { RelationMetadata } from './reducer'
import { getPrimaryKeyFields } from '../shared/primary-key-utils'
import { buildCompositeKey } from '../shared/key-utils'
import {
  maybeParseJson,
  parseJsonIfNeeded,
  getJsonFieldSet,
} from '../shared/model-field-cache'

export interface CoreReducerConfig {
  parentModel: Model
  includedRelations: RelationMetadata[]
}

export interface CoreReducer {
  processRow(row: any): string | null
  getParent(key: string): any | null
  getAllParents(): any[]
  getParentMap(): Map<string, any>
}

type IndexByPath = Map<string, Map<string, any>>

const getOrCreateRelationMap = (
  relationMaps: WeakMap<object, IndexByPath>,
  parent: object,
): IndexByPath => {
  let relMap = relationMaps.get(parent)
  if (!relMap) {
    relMap = new Map()
    relationMaps.set(parent, relMap)
  }
  return relMap
}

const getOrCreateChildMap = (
  relMap: IndexByPath,
  path: string,
): Map<string, any> => {
  let childMap = relMap.get(path)
  if (!childMap) {
    childMap = new Map()
    relMap.set(path, childMap)
  }
  return childMap
}

const createParentObject = (
  row: any,
  scalarFields: any[],
  jsonSet: ReadonlySet<string>,
  includedRelations: readonly RelationMetadata[],
): any => {
  const parent: any = {}

  for (const field of scalarFields) {
    if (!(field.name in row)) continue
    parent[field.name] = maybeParseJson(row[field.name], jsonSet, field.name)
  }

  for (const rel of includedRelations) {
    parent[rel.name] = rel.cardinality === 'many' ? [] : null
  }

  return parent
}

const createChildObject = (row: any, rel: RelationMetadata): any => {
  const child: any = {}

  for (const spec of rel.scalarCols) {
    if (!(spec.colName in row)) continue
    child[spec.fieldName] = parseJsonIfNeeded(spec.isJson, row[spec.colName])
  }

  if (rel.nestedIncludes) {
    for (const nested of rel.nestedIncludes.includedRelations) {
      child[nested.name] = nested.cardinality === 'many' ? [] : null
    }
  }

  return child
}

const extractChildKey = (row: any, rel: RelationMetadata): string | null => {
  const cols = rel.primaryKeyFields.map((f) => `${rel.path}.${f}`)
  return buildCompositeKey(row, cols)
}

const attachChildToParent = (
  parent: any,
  child: any,
  rel: RelationMetadata,
): void => {
  if (rel.cardinality === 'many') {
    parent[rel.name].push(child)
  } else {
    parent[rel.name] = child
  }
}

const createRelationProcessor = (
  relationMaps: WeakMap<object, IndexByPath>,
) => {
  const processRelation = (
    parent: any,
    rel: RelationMetadata,
    row: any,
  ): void => {
    const childKey = extractChildKey(row, rel)
    if (!childKey) return

    const relMap = getOrCreateRelationMap(relationMaps, parent)
    const childMap = getOrCreateChildMap(relMap, rel.path)

    if (childMap.has(childKey)) {
      const existing = childMap.get(childKey)!

      if (rel.nestedIncludes) {
        for (const nested of rel.nestedIncludes.includedRelations) {
          processRelation(existing, nested, row)
        }
      }
      return
    }

    const child = createChildObject(row, rel)
    childMap.set(childKey, child)

    attachChildToParent(parent, child, rel)

    if (rel.nestedIncludes) {
      for (const nested of rel.nestedIncludes.includedRelations) {
        processRelation(child, nested, row)
      }
    }
  }

  return processRelation
}

export const createCoreReducer = (config: CoreReducerConfig): CoreReducer => {
  const parentMap = new Map<string, any>()
  const relationMaps = new WeakMap<object, IndexByPath>()

  const scalarFields = config.parentModel.fields.filter((f) => !f.isRelation)
  const jsonSet = getJsonFieldSet(config.parentModel)
  const parentPkFields = getPrimaryKeyFields(config.parentModel)
  const includedRelations = config.includedRelations

  const extractParentKey = (row: any): string | null =>
    buildCompositeKey(row, parentPkFields)

  const processRelation = createRelationProcessor(relationMaps)

  const processRow = (row: any): string | null => {
    const parentKey = extractParentKey(row)
    if (!parentKey) return null

    const parent = parentMap.has(parentKey)
      ? parentMap.get(parentKey)!
      : (() => {
          const newParent = createParentObject(
            row,
            scalarFields,
            jsonSet,
            includedRelations,
          )
          parentMap.set(parentKey, newParent)
          return newParent
        })()

    for (const rel of includedRelations) {
      processRelation(parent, rel, row)
    }

    return parentKey
  }

  return {
    processRow,
    getParent: (key: string) => parentMap.get(key) ?? null,
    getAllParents: () => Array.from(parentMap.values()),
    getParentMap: () => parentMap,
  }
}
