import type { Model } from '../../types'
import { isPlainObject } from '../shared/validators/type-guards'
import { resolveIncludeRelations } from '../shared/include-tree-walker'
import { LIMITS } from '../shared/constants'
import { isDynamicParameter } from '@dee-wan/schema-parser'
import { getFieldIndices } from '../shared/model-field-cache'

type RelStats = {
  avg: number
  p95: number
  p99: number
  max: number
  coverage: number
}

type RelationStatsMap = Record<string, Record<string, RelStats>>

let globalRelationStats: RelationStatsMap | undefined

export interface StrategyConfig {
  /** Cost (in row-equivalents) of one additional database roundtrip. Default: 73 */
  roundtripRowEquivalent: number
  /** Multiplier for JSON aggregation overhead per row. Default: 1.5 */
  jsonRowFactor: number
  /** Correlated subquery cost factor when child has LIMIT. Default: 0.5 */
  correlatedBoundedFactor: number
  /** Correlated subquery cost factor when child is unbounded. Default: 3.0 */
  correlatedUnboundedFactor: number
  /** Extra cost multiplier when child relation has a WHERE clause. Default: 3.0 */
  correlatedWherePenalty: number
  /** Assumed fan-out when no relation stats are available. Default: 10 */
  defaultFanOut: number
  /** Assumed parent row count when take is not specified. Default: 50 */
  defaultParentCount: number
  /** Max include depth that allows flat-join for single-parent queries. Default: 2 */
  singleParentMaxFlatJoinDepth: number
  /** Minimum stats coverage to trust relation cardinality data. Default: 0.1 */
  minStatsCoverage: number
  /** Assumed take value for dynamic (runtime) parameters. Default: 10 */
  dynamicTakeEstimate: number
}

const strategyStore: StrategyConfig = {
  roundtripRowEquivalent: 73,
  jsonRowFactor: 1.5,
  correlatedBoundedFactor: 0.5,
  correlatedUnboundedFactor: 3.0,
  correlatedWherePenalty: 3.0,
  defaultFanOut: 10,
  defaultParentCount: 50,
  singleParentMaxFlatJoinDepth: 2,
  minStatsCoverage: 0.1,
  dynamicTakeEstimate: 10,
}

/**
 * Override one or more strategy cost-model parameters.
 * Only provided keys are updated; others keep their current values.
 */
export function setStrategyConfig(overrides: Partial<StrategyConfig>): void {
  for (const key of Object.keys(overrides) as Array<keyof StrategyConfig>) {
    const val = overrides[key]
    if (typeof val === 'number' && Number.isFinite(val)) {
      strategyStore[key] = val
    }
  }
}

/**
 * Returns a frozen snapshot of the current strategy config.
 */
export function getStrategyConfig(): Readonly<StrategyConfig> {
  return Object.freeze({ ...strategyStore })
}

export function setRoundtripRowEquivalent(value: number): void {
  strategyStore.roundtripRowEquivalent = value
}

export function setJsonRowFactor(value: number): void {
  strategyStore.jsonRowFactor = value
}

export function setRelationStats(stats: RelationStatsMap): void {
  globalRelationStats = stats
}

export function getRelationStats(): RelationStatsMap | undefined {
  return globalRelationStats
}

type IncludeStrategy = 'flat-join' | 'where-in' | 'fallback'

interface RelationCostNode {
  name: string
  fan: number
  take: number
  eff: number
  isList: boolean
  hasChildWhere: boolean
  children: RelationCostNode[]
}

function getFanOut(modelName: string, relName: string): number {
  if (!globalRelationStats) return strategyStore.defaultFanOut
  const modelStats = globalRelationStats[modelName]
  if (!modelStats) return strategyStore.defaultFanOut
  const relStat = modelStats[relName]
  if (!relStat || relStat.coverage < strategyStore.minStatsCoverage)
    return strategyStore.defaultFanOut
  return relStat.avg
}

function readTake(relArgs: unknown): number {
  if (!isPlainObject(relArgs)) return Infinity
  const obj = relArgs as Record<string, unknown>
  if ('take' in obj && typeof obj.take === 'number' && obj.take > 0)
    return obj.take
  if ('take' in obj && obj.take != null && isDynamicParameter(obj.take))
    return strategyStore.dynamicTakeEstimate
  return Infinity
}

function hasWhereClause(relArgs: unknown): boolean {
  if (!isPlainObject(relArgs)) return false
  const obj = relArgs as Record<string, unknown>
  return 'where' in obj && obj.where != null && isPlainObject(obj.where)
}

function isListField(field: { type?: unknown }): boolean {
  return typeof field.type === 'string' && field.type.endsWith('[]')
}

function hasPaginationArgs(value: unknown): boolean {
  if (!isPlainObject(value)) return false
  const obj = value as Record<string, unknown>
  return (
    ('take' in obj && obj.take != null) ||
    ('skip' in obj &&
      obj.skip != null &&
      ((typeof obj.skip === 'number' && obj.skip > 0) ||
        isDynamicParameter(obj.skip)))
  )
}

function hasFlatJoinBlockingRootArgs(args: unknown): boolean {
  if (!isPlainObject(args)) return false
  const obj = args as Record<string, unknown>
  if ('cursor' in obj && obj.cursor != null) return true
  if (Array.isArray(obj.distinct) && (obj.distinct as unknown[]).length > 0)
    return true
  if (
    isPlainObject(obj.include) &&
    (obj.include as Record<string, unknown>)['_count']
  )
    return true
  if (
    isPlainObject(obj.select) &&
    (obj.select as Record<string, unknown>)['_count']
  )
    return true
  return false
}

function buildCostTree(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
  depth: number = 0,
  modelMap?: Map<string, Model>,
): RelationCostNode[] {
  if (depth > LIMITS.MAX_INCLUDE_DEPTH) return []

  const relations = resolveIncludeRelations(
    includeSpec,
    model,
    schemas,
    modelMap,
  )
  const nodes: RelationCostNode[] = []

  for (const rel of relations) {
    const fan = rel.isList ? getFanOut(model.name, rel.relName) : 1
    const take = rel.isList ? readTake(rel.value) : 1
    const eff = Math.min(fan, take)

    const children =
      Object.keys(rel.nestedSpec).length > 0
        ? buildCostTree(
            rel.nestedSpec,
            rel.relModel,
            schemas,
            depth + 1,
            modelMap,
          )
        : []

    nodes.push({
      name: rel.relName,
      fan,
      take,
      eff,
      isList: rel.isList,
      hasChildWhere: hasWhereClause(rel.value),
      children,
    })
  }

  return nodes
}

function maxDepthFromTree(nodes: RelationCostNode[]): number {
  if (nodes.length === 0) return 0
  let max = 0
  for (const n of nodes) {
    const d = 1 + maxDepthFromTree(n.children)
    if (d > max) max = d
  }
  return max
}

function anyChildHasWhere(nodes: RelationCostNode[]): boolean {
  for (const n of nodes) {
    if (n.hasChildWhere) return true
  }
  return false
}

function computeWhereInCost(
  nodes: RelationCostNode[],
  parentCount: number,
): number {
  const R = strategyStore.roundtripRowEquivalent
  let totalRows = 0
  let roundtrips = 0

  function walk(ns: RelationCostNode[], P: number): void {
    if (ns.length === 0) return
    roundtrips++
    for (const n of ns) {
      const rows = P * n.eff
      totalRows += rows
      if (n.children.length > 0) {
        walk(n.children, rows)
      }
    }
  }

  walk(nodes, parentCount)
  return (1 + roundtrips) * R + totalRows
}

function computeCorrelatedCost(
  nodes: RelationCostNode[],
  parentCount: number,
): number {
  const R = strategyStore.roundtripRowEquivalent

  function subqueryCost(ns: RelationCostNode[]): number {
    let total = 0
    for (const n of ns) {
      const sBase =
        n.take !== Infinity
          ? strategyStore.correlatedBoundedFactor
          : strategyStore.correlatedUnboundedFactor
      const s = n.hasChildWhere
        ? sBase * strategyStore.correlatedWherePenalty
        : sBase
      let nodeCost = n.eff * s
      if (n.children.length > 0) {
        nodeCost += n.eff * subqueryCost(n.children)
      }
      total += nodeCost
    }
    return total
  }

  return R + parentCount * subqueryCost(nodes)
}

function hasOnlyToOneRelations(
  includeSpec: Record<string, any>,
  model: Model,
): boolean {
  const indices = getFieldIndices(model)
  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue
    const field = indices.allFieldsByName.get(relName)
    if (!field?.isRelation) continue
    if (isListField(field)) return false
  }
  return true
}

export function countIncludeDepth(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
  depth: number = 0,
  modelMap?: Map<string, Model>,
): number {
  if (depth > LIMITS.MAX_INCLUDE_DEPTH) return 0

  const relations = resolveIncludeRelations(
    includeSpec,
    model,
    schemas,
    modelMap,
  )
  let maxDepth = 0

  for (const rel of relations) {
    let childDepth = 1
    if (Object.keys(rel.nestedSpec).length > 0) {
      childDepth += countIncludeDepth(
        rel.nestedSpec,
        rel.relModel,
        schemas,
        depth + 1,
        modelMap,
      )
    }
    if (childDepth > maxDepth) maxDepth = childDepth
  }

  return maxDepth
}

export function hasChildPaginationAnywhere(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
  depth: number = 0,
  modelMap?: Map<string, Model>,
): boolean {
  if (depth > LIMITS.MAX_INCLUDE_DEPTH) return false

  for (const [, value] of Object.entries(includeSpec)) {
    if (value === false) continue
    if (hasPaginationArgs(value)) return true
  }

  const relations = resolveIncludeRelations(
    includeSpec,
    model,
    schemas,
    modelMap,
  )

  for (const rel of relations) {
    if (Object.keys(rel.nestedSpec).length > 0) {
      if (
        hasChildPaginationAnywhere(
          rel.nestedSpec,
          rel.relModel,
          schemas,
          depth + 1,
          modelMap,
        )
      ) {
        return true
      }
    }
  }

  return false
}

export function pickIncludeStrategy(params: {
  includeSpec: Record<string, any>
  model: Model
  schemas: readonly Model[]
  method: string
  args?: any
  takeValue: number | null
  hasPagination: boolean
  canFlatJoin: boolean
  hasChildPagination: boolean
  debug?: boolean
  modelMap?: Map<string, Model>
}): IncludeStrategy {
  const {
    includeSpec,
    model,
    schemas,
    method,
    args,
    takeValue,
    canFlatJoin,
    hasChildPagination,
    debug,
    modelMap,
  } = params

  if (Object.keys(includeSpec).length === 0) return 'where-in'

  const blocked = hasFlatJoinBlockingRootArgs(args)

  if (canFlatJoin && !blocked && hasOnlyToOneRelations(includeSpec, model)) {
    if (debug)
      console.log(`  [strategy] ${model.name}: all one-to-one → flat-join`)
    return 'flat-join'
  }

  const isSingleParent = method === 'findFirst' || method === 'findUnique'
  if (isSingleParent && canFlatJoin && !blocked) {
    const depth = countIncludeDepth(includeSpec, model, schemas, 0, modelMap)
    if (depth <= strategyStore.singleParentMaxFlatJoinDepth) {
      if (debug)
        console.log(
          `  [strategy] ${model.name}: single parent depth≤${strategyStore.singleParentMaxFlatJoinDepth} → flat-join`,
        )
      return 'flat-join'
    }
  }

  const costTree = buildCostTree(includeSpec, model, schemas, 0, modelMap)
  const treeDepth = maxDepthFromTree(costTree)

  if (hasChildPagination && treeDepth >= 2) {
    if (debug)
      console.log(
        `  [strategy] ${model.name}: childPagination + depth=${treeDepth} ≥ 2 → fallback`,
      )
    return 'fallback'
  }

  if (hasChildPagination && treeDepth === 1) {
    if (anyChildHasWhere(costTree)) {
      if (debug)
        console.log(
          `  [strategy] ${model.name}: childPagination + depth=1 + childWhere → where-in`,
        )
      return 'where-in'
    }

    const hasSelectNarrowing =
      isPlainObject(args) &&
      isPlainObject((args as Record<string, unknown>).select)

    if (hasSelectNarrowing) {
      if (debug)
        console.log(
          `  [strategy] ${model.name}: childPagination + depth=1 + selectNarrowing → fallback`,
        )
      return 'fallback'
    }

    if (debug)
      console.log(
        `  [strategy] ${model.name}: childPagination + depth=1 → where-in`,
      )
    return 'where-in'
  }

  if (treeDepth === 1 && anyChildHasWhere(costTree)) {
    if (debug)
      console.log(`  [strategy] ${model.name}: depth-1 + childWhere → where-in`)
    return 'where-in'
  }

  const P = isSingleParent ? 1 : takeValue ?? strategyStore.defaultParentCount

  const costW = computeWhereInCost(costTree, P)
  const costC = computeCorrelatedCost(costTree, P)

  if (debug) {
    console.log(
      `  [strategy] ${model.name}: P=${P} D=${treeDepth}` +
        ` costW=${costW.toFixed(0)} costC=${costC.toFixed(0)}`,
    )
  }

  if (costC < costW) {
    if (debug) console.log(`  [strategy] ${model.name}: costC wins → fallback`)
    return 'fallback'
  }

  if (debug) console.log(`  [strategy] ${model.name}: costW wins → where-in`)
  return 'where-in'
}
