import { isPlainObject } from '../shared/validators/type-guards'
import { extractRelationEntries } from '../shared/relation-extraction-utils'
import { resolveRelationKeys } from '../shared/relation-key-utils'
import { Field, Model } from '../../types'
import { canUseFlatJoinForAll } from './flat-join'
import {
  pickIncludeStrategy,
  hasChildPaginationAnywhere,
} from './strategy-estimator'
import { canUseLateralJoin } from './lateral-join'
import { getOrCreateModelMap } from '../shared/include-tree-walker'

export interface WhereInSegment {
  relationName: string
  relArgs: unknown
  childModelName: string
  fkFieldName: string
  parentKeyFieldName: string
  isList: boolean
  perParentTake?: number
  perParentSkip?: number
}

interface QueryPlan {
  filteredArgs: any
  originalArgs: any
  whereInSegments: WhereInSegment[]
  injectedParentKeys: string[]
}

function isListField(field: Field): boolean {
  return typeof field.type === 'string' && field.type.endsWith('[]')
}

function resolveRelation(
  model: Model,
  relName: string,
  allModels: readonly Model[],
  modelMap?: Map<string, Model>,
): { field: Field; relModel: Model } | null {
  const field = model.fields.find((f) => f.name === relName)
  if (!field || !field.isRelation || !field.relatedModel) return null

  const relModel = modelMap
    ? modelMap.get(field.relatedModel)
    : allModels.find((m) => m.name === field.relatedModel)
  if (!relModel) return null

  return { field: field as Field, relModel }
}

function extractPagination(relArgs: unknown): {
  take?: number
  skip?: number
} {
  if (!isPlainObject(relArgs)) return {}
  const obj = relArgs as Record<string, unknown>
  const result: { take?: number; skip?: number } = {}

  if ('take' in obj && typeof obj.take === 'number') {
    result.take = obj.take
  }
  if ('skip' in obj && typeof obj.skip === 'number' && obj.skip > 0) {
    result.skip = obj.skip
  }

  return result
}

function buildWhereInSegment(
  name: string,
  relArgs: unknown,
  field: Field,
  relModel: Model,
): WhereInSegment | null {
  const keys = resolveRelationKeys(field, 'whereIn')
  if (keys.childKeys.length !== 1) return null

  const isList = isListField(field)
  const pagination = isList ? extractPagination(relArgs) : {}

  return {
    relationName: name,
    relArgs,
    childModelName: relModel.name,
    fkFieldName: keys.childKeys[0],
    parentKeyFieldName: keys.parentKeys[0],
    isList,
    perParentTake: pagination.take,
    perParentSkip: pagination.skip,
  }
}

function removeRelationsFromArgs(args: any, names: Set<string>): any {
  if (!args) return args

  const filtered = { ...args }

  if (filtered.include && isPlainObject(filtered.include)) {
    filtered.include = { ...filtered.include }
    for (const name of names) {
      delete filtered.include[name]
    }
    if (Object.keys(filtered.include).length === 0) {
      delete filtered.include
    }
  }

  if (filtered.select && isPlainObject(filtered.select)) {
    filtered.select = { ...filtered.select }
    for (const name of names) {
      delete filtered.select[name]
    }
  }

  return filtered
}

function ensureParentKeysInSelect(
  args: any,
  segments: WhereInSegment[],
): { args: any; injectedKeys: string[] } {
  if (!args?.select) return { args, injectedKeys: [] }

  const injected: string[] = []
  const newSelect = { ...args.select }

  for (const seg of segments) {
    if (!newSelect[seg.parentKeyFieldName]) {
      newSelect[seg.parentKeyFieldName] = true
      injected.push(seg.parentKeyFieldName)
    }
  }

  if (injected.length === 0) return { args, injectedKeys: [] }

  return { args: { ...args, select: newSelect }, injectedKeys: injected }
}

function extractIncludeSpec(args: any, model: Model): Record<string, any> {
  const spec: Record<string, any> = {}
  const entries = extractRelationEntries(args, model)
  for (const e of entries) {
    if (e.value !== false) {
      spec[e.name] = e.value
    }
  }
  return spec
}

export function planQueryStrategy(params: {
  model: Model
  method: string
  args: any
  allModels: readonly Model[]
  dialect: 'postgres' | 'sqlite'
  debug?: boolean
}): QueryPlan {
  const { model, args, allModels, dialect, debug } = params
  const emptyPlan: QueryPlan = {
    filteredArgs: args,
    originalArgs: args,
    whereInSegments: [],
    injectedParentKeys: [],
  }

  const entries = extractRelationEntries(args, model)
  if (entries.length === 0) {
    return emptyPlan
  }

  if (dialect === 'postgres') {
    const includeSpec = extractIncludeSpec(args, model)

    if (Object.keys(includeSpec).length > 0) {
      const modelMap = getOrCreateModelMap(allModels)

      const hasPagination =
        isPlainObject(args) && 'take' in args && args.take != null
      const takeValue =
        isPlainObject(args) && typeof args.take === 'number' ? args.take : null

      const canFlatJoin = canUseFlatJoinForAll(
        includeSpec,
        model,
        allModels,
        false,
        modelMap,
      )
      const canLateral = canUseLateralJoin(
        includeSpec,
        model,
        allModels,
        modelMap,
      )
      const hasChildPag = hasChildPaginationAnywhere(
        includeSpec,
        model,
        allModels,
        0,
        modelMap,
      )

      const strategy = pickIncludeStrategy({
        includeSpec,
        model,
        schemas: allModels,
        method: params.method,
        args,
        takeValue,
        hasPagination,
        canFlatJoin,
        canLateral,
        hasChildPagination: hasChildPag,
        debug,
        modelMap,
      })

      if (debug) {
        console.log(`  [planner] ${model.name}: strategy=${strategy}`)
      }

      if (strategy !== 'where-in') {
        return emptyPlan
      }
    }
  }

  const modelMap = getOrCreateModelMap(allModels)
  const whereInSegments: WhereInSegment[] = []
  const toRemove = new Set<string>()

  for (const entry of entries) {
    const resolved = resolveRelation(model, entry.name, allModels, modelMap)
    if (!resolved) continue

    const segment = buildWhereInSegment(
      entry.name,
      entry.value,
      resolved.field,
      resolved.relModel,
    )
    if (segment) {
      whereInSegments.push(segment)
      toRemove.add(entry.name)
    }
  }

  if (toRemove.size === 0) {
    return emptyPlan
  }

  const filteredArgs = removeRelationsFromArgs(args, toRemove)
  const { args: finalArgs, injectedKeys } = ensureParentKeysInSelect(
    filteredArgs,
    whereInSegments,
  )

  return {
    filteredArgs: finalArgs,
    originalArgs: args,
    whereInSegments,
    injectedParentKeys: injectedKeys,
  }
}
