import type { Model } from '../../types'
import { buildSQL } from '../..'
import { buildReducerConfig, reduceFlatRows } from '../select/reducer'
import type { WhereInSegment } from '../select/segment-planner'
import { isPlainObject } from './validators/type-guards'

export interface ExecuteWhereInParams {
  segments: WhereInSegment[]
  parentRows: any[]
  parentModel: Model
  allModels: readonly Model[]
  modelMap: Map<string, Model>
  dialect: 'postgres' | 'sqlite'
  execute: (sql: string, params: unknown[]) => Promise<any[]>
}

function getParamLimit(dialect: 'postgres' | 'sqlite'): number {
  return dialect === 'postgres' ? 32000 : 900
}

function chunkArray<T>(arr: T[], size: number): T[][] {
  if (arr.length <= size) return [arr]
  const chunks: T[][] = []
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size))
  }
  return chunks
}

export function buildChildArgs(
  relArgs: unknown,
  fkFieldName: string,
  parentIds: unknown[],
): any {
  const base: any =
    relArgs === true || !isPlainObject(relArgs) ? {} : { ...(relArgs as any) }

  const existingWhere = base.where

  const inCondition = { [fkFieldName]: { in: parentIds } }

  base.where = existingWhere
    ? { AND: [existingWhere, inCondition] }
    : inCondition

  return base
}

function ensureFkInSelect(childArgs: any, fkFieldName: string): boolean {
  if (!childArgs.select) return false
  if (childArgs.select[fkFieldName]) return false

  childArgs.select = { ...childArgs.select, [fkFieldName]: true }
  return true
}

export function stitchResults(
  parentRows: any[],
  segment: WhereInSegment,
  childRows: any[],
  stripFk: boolean,
): void {
  const grouped = new Map<unknown, any[]>()

  for (const child of childRows) {
    const fk = child[segment.fkFieldName]
    if (fk == null) continue

    let arr = grouped.get(fk)
    if (!arr) {
      arr = []
      grouped.set(fk, arr)
    }

    if (stripFk) {
      const cleaned = { ...child }
      delete cleaned[segment.fkFieldName]
      arr.push(cleaned)
    } else {
      arr.push(child)
    }
  }

  for (const parent of parentRows) {
    const pk = parent[segment.parentKeyFieldName]
    const children = grouped.get(pk) || []

    if (segment.isList) {
      parent[segment.relationName] = children
    } else {
      parent[segment.relationName] = children[0] || null
    }
  }
}

export async function executeSegmentBase(
  segment: WhereInSegment,
  parentRows: any[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
  execute: (sql: string, params: unknown[]) => Promise<any[]>,
): Promise<void> {
  const parentIds = parentRows
    .map((r) => r[segment.parentKeyFieldName])
    .filter((v) => v != null)

  if (parentIds.length === 0) {
    for (const parent of parentRows) {
      parent[segment.relationName] = segment.isList ? [] : null
    }
    return
  }

  const uniqueIds = [...new Set(parentIds)]
  const paramLimit = getParamLimit(dialect)
  const chunks = chunkArray(uniqueIds, paramLimit)

  const childModel = modelMap.get(segment.childModelName)
  if (!childModel) {
    for (const parent of parentRows) {
      parent[segment.relationName] = segment.isList ? [] : null
    }
    return
  }

  const allChildRows: any[] = []
  let needsStripFk = false

  for (const chunk of chunks) {
    const childArgs = buildChildArgs(
      segment.relArgs,
      segment.fkFieldName,
      chunk,
    )
    const stripFk = ensureFkInSelect(childArgs, segment.fkFieldName)
    if (stripFk) needsStripFk = true

    const result = buildSQL(
      childModel,
      allModels as Model[],
      'findMany',
      childArgs,
      dialect,
    )

    let rows = await execute(result.sql, result.params as unknown[])

    if (result.requiresReduction && result.includeSpec) {
      const config = buildReducerConfig(
        childModel,
        result.includeSpec as Record<string, any>,
        allModels,
      )
      rows = reduceFlatRows(rows, config)
    }

    for (const row of rows) {
      allChildRows.push(row)
    }
  }

  stitchResults(parentRows, segment, allChildRows, needsStripFk)
}
