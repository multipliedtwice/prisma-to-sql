import type { Model } from '../../types'
import { isPlainObject } from '../shared/validators/type-guards'
import { resolveIncludeRelations } from '../shared/include-tree-walker'

type RelStats = {
  avg: number
  p95: number
  p99: number
  max: number
  coverage: number
}

type RelationStatsMap = Record<string, Record<string, RelStats>>

let globalRelationStats: RelationStatsMap | undefined
let globalRoundtripRowEquivalent = 73
let globalJsonRowFactor = 1.5

const CORRELATED_S_BOUNDED = 0.5
const CORRELATED_S_UNBOUNDED = 3.0
const CORRELATED_WHERE_PENALTY = 3.0
const DEFAULT_FAN = 10
const DEFAULT_PARENT_COUNT = 50
const MIN_STATS_COVERAGE = 0.1
const SINGLE_PARENT_MAX_FLAT_JOIN_DEPTH = 1

export function setRoundtripRowEquivalent(value: number): void {
  globalRoundtripRowEquivalent = value
}

export function setJsonRowFactor(value: number): void {
  globalJsonRowFactor = value
}

export function setRelationStats(stats: RelationStatsMap): void {
  globalRelationStats = stats
}

export function getRelationStats(): RelationStatsMap | undefined {
  return globalRelationStats
}

type IncludeStrategy = 'flat-join' | 'lateral' | 'where-in' | 'fallback'

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
  if (!globalRelationStats) return DEFAULT_FAN
  const modelStats = globalRelationStats[modelName]
  if (!modelStats) return DEFAULT_FAN
  const relStat = modelStats[relName]
  if (!relStat || relStat.coverage < MIN_STATS_COVERAGE) return DEFAULT_FAN
  return relStat.avg
}

function readTake(relArgs: unknown): number {
  if (!isPlainObject(relArgs)) return Infinity
  const obj = relArgs as Record<string, unknown>
  if ('take' in obj && typeof obj.take === 'number' && obj.take > 0)
    return obj.take
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
    ('skip' in obj && typeof obj.skip === 'number' && obj.skip > 0)
  )
}

function buildCostTree(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): RelationCostNode[] {
  const relations = resolveIncludeRelations(includeSpec, model, schemas)
  const nodes: RelationCostNode[] = []

  for (const rel of relations) {
    const fan = rel.isList ? getFanOut(model.name, rel.relName) : 1
    const take = rel.isList ? readTake(rel.value) : 1
    const eff = Math.min(fan, take)

    const children =
      Object.keys(rel.nestedSpec).length > 0
        ? buildCostTree(rel.nestedSpec, rel.relModel, schemas)
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

function computeWhereInCost(
  nodes: RelationCostNode[],
  parentCount: number,
): number {
  const R = globalRoundtripRowEquivalent
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
  const R = globalRoundtripRowEquivalent

  function subqueryCost(ns: RelationCostNode[]): number {
    let total = 0
    for (const n of ns) {
      const sBase =
        n.take !== Infinity ? CORRELATED_S_BOUNDED : CORRELATED_S_UNBOUNDED
      const s = n.hasChildWhere ? sBase * CORRELATED_WHERE_PENALTY : sBase
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
  for (const [relName, value] of Object.entries(includeSpec)) {
    if (value === false) continue
    const field = model.fields.find((f) => f.name === relName)
    if (!field?.isRelation) continue
    if (isListField(field)) return false
  }
  return true
}

export function countIncludeDepth(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): number {
  const relations = resolveIncludeRelations(includeSpec, model, schemas)
  let maxDepth = 0

  for (const rel of relations) {
    let childDepth = 1
    if (Object.keys(rel.nestedSpec).length > 0) {
      childDepth += countIncludeDepth(rel.nestedSpec, rel.relModel, schemas)
    }
    if (childDepth > maxDepth) maxDepth = childDepth
  }

  return maxDepth
}

export function hasChildPaginationAnywhere(
  includeSpec: Record<string, any>,
  model: Model,
  schemas: readonly Model[],
): boolean {
  for (const [, value] of Object.entries(includeSpec)) {
    if (value === false) continue
    if (hasPaginationArgs(value)) return true
  }

  const relations = resolveIncludeRelations(includeSpec, model, schemas)

  for (const rel of relations) {
    if (Object.keys(rel.nestedSpec).length > 0) {
      if (hasChildPaginationAnywhere(rel.nestedSpec, rel.relModel, schemas)) {
        return true
      }
    }
  }

  return false
}

/**
 * Cost-model strategy picker.
 *
 * Structural rules (deterministic, skip cost computation):
 *   1. allOneToOne + canFlatJoin → flat-join
 *      No row multiplication. JOIN adds columns only.
 *   2. singleParent + canFlatJoin + depth<=1 → flat-join
 *      P=1 eliminates dedup cost. Depth>1 excluded because
 *      nested product grows faster than roundtrip savings.
 *
 * Cost formulas (row-equivalent units):
 *   R  = roundtripRowEquivalent (default 73, calibrate per-db)
 *   S  = correlated scan factor (bounded=0.5, unbounded=3.0)
 *        ×3.0 when child has WHERE (per-parent filter overhead)
 *
 *   costW = (1 + roundtrips) × R + Σ(P_level × eff)
 *   costC = R + P × Σ(eff × S + eff × subCost(children))
 *
 * Validated against forced-strategy benchmarks across
 * depth-1..4, fan-1..100, paginated/unbounded topologies.
 */
export function pickIncludeStrategy(params: {
  includeSpec: Record<string, any>
  model: Model
  schemas: readonly Model[]
  method: string
  args?: any
  takeValue: number | null
  hasPagination: boolean
  canFlatJoin: boolean
  canLateral: boolean
  hasChildPagination: boolean
  debug?: boolean
}): IncludeStrategy {
  const { includeSpec, model, schemas, method, takeValue, canFlatJoin, debug } =
    params

  if (Object.keys(includeSpec).length === 0) return 'where-in'

  if (canFlatJoin && hasOnlyToOneRelations(includeSpec, model)) {
    if (debug)
      console.log(`  [strategy] ${model.name}: all one-to-one → flat-join`)
    return 'flat-join'
  }

  const isSingleParent = method === 'findFirst' || method === 'findUnique'
  if (isSingleParent && canFlatJoin) {
    const depth = countIncludeDepth(includeSpec, model, schemas)
    if (depth <= SINGLE_PARENT_MAX_FLAT_JOIN_DEPTH) {
      if (debug)
        console.log(
          `  [strategy] ${model.name}: single parent depth≤${SINGLE_PARENT_MAX_FLAT_JOIN_DEPTH} → flat-join`,
        )
      return 'flat-join'
    }
  }

  const costTree = buildCostTree(includeSpec, model, schemas)
  const P = isSingleParent ? 1 : takeValue ?? DEFAULT_PARENT_COUNT

  const costW = computeWhereInCost(costTree, P)
  const costC = computeCorrelatedCost(costTree, P)

  if (debug) {
    const depth = maxDepthFromTree(costTree)
    console.log(
      `  [strategy] ${model.name}: P=${P} D=${depth}` +
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
