import { Model } from '../../types'
import { getRelationFieldSet } from '../shared/model-field-cache'
import { isPlainObject } from '../shared/validators/type-guards'

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

interface RelationMetadata {
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

const PK_FIELDS_CACHE = new WeakMap<Model, string[]>()
const SCALAR_FIELDS_CACHE = new WeakMap<Model, string[]>()
const JSON_FIELD_SET_CACHE = new WeakMap<Model, ReadonlySet<string>>()

function findPrimaryKeyFieldsCached(model: Model): string[] {
  const cached = PK_FIELDS_CACHE.get(model)
  if (cached) return cached

  const pkFields = model.fields.filter((f) => f.isId && !f.isRelation)
  if (pkFields.length > 0) {
    const out = pkFields.map((f) => f.name)
    PK_FIELDS_CACHE.set(model, out)
    return out
  }

  const defaultId = model.fields.find((f) => f.name === 'id' && !f.isRelation)
  if (defaultId) {
    const out = ['id']
    PK_FIELDS_CACHE.set(model, out)
    return out
  }

  throw new Error(
    `Model ${model.name} has no primary key field. Models must have either fields with isId=true or a field named 'id'.`,
  )
}

function scalarFieldNamesCached(model: Model): string[] {
  const cached = SCALAR_FIELDS_CACHE.get(model)
  if (cached) return cached
  const out = model.fields.filter((f) => !f.isRelation).map((f) => f.name)
  SCALAR_FIELDS_CACHE.set(model, out)
  return out
}

function jsonFieldSetCached(model: Model): ReadonlySet<string> {
  const cached = JSON_FIELD_SET_CACHE.get(model)
  if (cached) return cached

  const s = new Set<string>()
  for (const f of model.fields) {
    if (f.isRelation) continue
    const t = String((f as any).type ?? '').toLowerCase()
    if (t === 'json') s.add(f.name)
  }

  JSON_FIELD_SET_CACHE.set(model, s)
  return s
}

function maybeParseJsonScalarFast(isJson: boolean, value: any): any {
  if (!isJson) return value
  if (value == null) return value
  if (typeof value !== 'string') return value
  try {
    return JSON.parse(value)
  } catch {
    return value
  }
}

function extractIncludeSpecFromRelArgs(
  relArgs: unknown,
  relModel: Model,
): Record<string, any> {
  const relationSet = getRelationFieldSet(relModel)
  const out: Record<string, any> = {}

  if (!isPlainObject(relArgs)) return out
  const obj = relArgs as Record<string, unknown>

  if (isPlainObject(obj.include)) {
    for (const [k, v] of Object.entries(
      obj.include as Record<string, unknown>,
    )) {
      if (!relationSet.has(k)) continue
      if (v === false) continue
      out[k] = v
    }
  }

  if (isPlainObject(obj.select)) {
    for (const [k, v] of Object.entries(
      obj.select as Record<string, unknown>,
    )) {
      if (!relationSet.has(k)) continue
      if (v === false) continue
      if (v === true) {
        out[k] = true
        continue
      }
      if (isPlainObject(v)) {
        const vv = v as Record<string, unknown>
        if (isPlainObject(vv.include) || isPlainObject(vv.select)) {
          out[k] = v
        }
      }
    }
  }

  return out
}

function extractScalarSelection(
  relArgs: unknown,
  relModel: Model,
): { includeAllScalars: boolean; selectedScalarFields: string[] } {
  const scalars = scalarFieldNamesCached(relModel)
  const scalarSet = new Set(scalars)

  if (relArgs === true || !isPlainObject(relArgs)) {
    return { includeAllScalars: true, selectedScalarFields: scalars }
  }

  const obj = relArgs as Record<string, unknown>
  if (!isPlainObject(obj.select)) {
    return { includeAllScalars: true, selectedScalarFields: scalars }
  }

  const sel = obj.select as Record<string, unknown>
  const selected: string[] = []
  for (const [k, v] of Object.entries(sel)) {
    if (!scalarSet.has(k)) continue
    if (v === true) selected.push(k)
  }

  return { includeAllScalars: false, selectedScalarFields: selected }
}

function buildRelationScalarCols(
  relModel: Model,
  relPath: string,
  includeAllScalars: boolean,
  selectedScalarFields: string[],
): ScalarColSpec[] {
  const jsonSet = jsonFieldSetCached(relModel)
  const scalarFields = includeAllScalars
    ? scalarFieldNamesCached(relModel)
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
    const primaryKeyFields = findPrimaryKeyFieldsCached(relatedModel)
    const scalarSel = extractScalarSelection(incValue, relatedModel)

    const relPath = prefix ? `${prefix}.${incName}` : incName

    let nestedIncludes: ReducerConfig | null = null
    const nestedSpec = extractIncludeSpecFromRelArgs(incValue, relatedModel)
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

function typedKeyPart(v: any): string {
  const t = typeof v
  if (t === 'string') return `s:${v}`
  if (t === 'number') return `n:${v}`
  if (t === 'boolean') return `b:${v ? 1 : 0}`
  return `o:${String(v)}`
}

function keyFromRowByCols(row: any, cols: string[]): string | null {
  if (cols.length === 0) return null

  if (cols.length === 1) {
    const v = row[cols[0]]
    if (v == null) return null
    return typedKeyPart(v)
  }

  let out = ''
  for (let i = 0; i < cols.length; i++) {
    const v = row[cols[i]]
    if (v == null) return null
    if (i > 0) out += '\u001f'
    out += typedKeyPart(v)
  }
  return out
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
  const relKey = keyFromRowByCols(row, rel.keyCols)
  if (relKey == null) return null

  const obj: any = {}
  for (const c of rel.scalarCols) {
    obj[c.fieldName] = maybeParseJsonScalarFast(c.isJson, row[c.colName])
  }

  initNestedPlaceholders(obj, rel.nestedIncludes)
  return obj
}

function processRelation(
  parentObj: any,
  rel: RelationMetadata,
  row: any,
  manyStore: WeakMap<object, IndexByPath>,
): void {
  const relKey = keyFromRowByCols(row, rel.keyCols)
  if (relKey == null) return

  if (rel.cardinality === 'one') {
    let current = parentObj[rel.name]
    if (current == null) {
      const created = materializeRelationObject(row, rel)
      if (!created) return
      parentObj[rel.name] = created
      current = created
    }

    if (rel.nestedIncludes) {
      for (const nestedRel of rel.nestedIncludes.includedRelations) {
        processRelation(current, nestedRel, row, manyStore)
      }
    }
    return
  }

  const arr = parentObj[rel.name] as any[]
  const idx = getIndexForParent(manyStore, parentObj, rel.path)

  const existing = idx.get(relKey)
  if (existing) {
    if (rel.nestedIncludes) {
      for (const nestedRel of rel.nestedIncludes.includedRelations) {
        processRelation(existing, nestedRel, row, manyStore)
      }
    }
    return
  }

  const created = materializeRelationObject(row, rel)
  if (!created) return

  arr.push(created)
  idx.set(relKey, created)

  if (rel.nestedIncludes) {
    for (const nestedRel of rel.nestedIncludes.includedRelations) {
      processRelation(created, nestedRel, row, manyStore)
    }
  }
}

function pickParentScalarFieldsFromRows(
  parentModel: Model,
  rows: any[],
): string[] {
  const all = scalarFieldNamesCached(parentModel)
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
  const parentPkFields = findPrimaryKeyFieldsCached(parentModel)

  const parentKeyCols = parentPkFields
  const parentScalarFields = pickParentScalarFieldsFromRows(parentModel, rows)
  const parentJsonSet = jsonFieldSetCached(parentModel)

  const resultMap = new Map<string, any>()
  const manyStore = new WeakMap<object, IndexByPath>()

  for (const row of rows) {
    const parentKey = keyFromRowByCols(row, parentKeyCols)
    if (parentKey == null) continue

    let record = resultMap.get(parentKey)
    if (!record) {
      record = {}
      for (const fieldName of parentScalarFields) {
        record[fieldName] = maybeParseJsonScalarFast(
          parentJsonSet.has(fieldName),
          row[fieldName],
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
