import type { Model } from '../../types'
import type { RelationMetadata } from './reducer'
import { getPrimaryKeyFields } from '../shared/primary-key-utils'
import { buildKey } from '../shared/key-utils'
import {
  maybeParseJson,
  parseJsonIfNeeded,
  getJsonFieldSet,
} from '../shared/model-field-cache'

interface CoreReducerConfig {
  parentModel: Model
  includedRelations: RelationMetadata[]
}

interface CoreReducer {
  processRow(row: any): unknown
  getParent(key: unknown): any | null
  getAllParents(): any[]
  getParentMap(): Map<unknown, any>
}

type IndexByPath = Map<string, Map<unknown, any>>

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
): Map<unknown, any> => {
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

interface PreparedRelation {
  rel: RelationMetadata
  prefixedPkFields: string[]
  nested: PreparedRelation[] | null
}

function prepareRelations(
  relations: readonly RelationMetadata[],
): PreparedRelation[] {
  return relations.map((rel) => ({
    rel,
    prefixedPkFields: rel.primaryKeyFields.map((f) => `${rel.path}.${f}`),
    nested: rel.nestedIncludes
      ? prepareRelations(rel.nestedIncludes.includedRelations)
      : null,
  }))
}

const createRelationProcessor = (
  relationMaps: WeakMap<object, IndexByPath>,
) => {
  const processRelation = (
    parent: any,
    prepared: PreparedRelation,
    row: any,
  ): void => {
    const { rel, prefixedPkFields, nested } = prepared

    const childKey = buildKey(row, prefixedPkFields)
    if (childKey == null) return

    const relMap = getOrCreateRelationMap(relationMaps, parent)
    const childMap = getOrCreateChildMap(relMap, rel.path)

    if (childMap.has(childKey)) {
      if (nested) {
        const existing = childMap.get(childKey)!
        for (const nestedPrepared of nested) {
          processRelation(existing, nestedPrepared, row)
        }
      }
      return
    }

    const child = createChildObject(row, rel)
    childMap.set(childKey, child)

    attachChildToParent(parent, child, rel)

    if (nested) {
      for (const nestedPrepared of nested) {
        processRelation(child, nestedPrepared, row)
      }
    }
  }

  return processRelation
}

export const createCoreReducer = (config: CoreReducerConfig): CoreReducer => {
  const parentMap = new Map<unknown, any>()
  const relationMaps = new WeakMap<object, IndexByPath>()

  const scalarFields = config.parentModel.fields.filter((f) => !f.isRelation)
  const jsonSet = getJsonFieldSet(config.parentModel)
  const parentPkFields = getPrimaryKeyFields(config.parentModel)
  const includedRelations = config.includedRelations

  const preparedRelations = prepareRelations(includedRelations)

  const processRelation = createRelationProcessor(relationMaps)

  const processRow = (row: any): unknown => {
    const parentKey = buildKey(row, parentPkFields)
    if (parentKey == null) return null

    let parent: any
    if (parentMap.has(parentKey)) {
      parent = parentMap.get(parentKey)!
    } else {
      parent = createParentObject(row, scalarFields, jsonSet, includedRelations)
      parentMap.set(parentKey, parent)
    }

    for (const prepared of preparedRelations) {
      processRelation(parent, prepared, row)
    }

    return parentKey
  }

  return {
    processRow,
    getParent: (key: unknown) => parentMap.get(key) ?? null,
    getAllParents: () => Array.from(parentMap.values()),
    getParentMap: () => parentMap,
  }
}
