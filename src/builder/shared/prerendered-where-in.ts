import type { Model } from '../../types'
import type {
  WhereInSegment,
  PrerenderedWhereIn,
} from '../select/segment-planner'
import { planQueryStrategy } from '../select/segment-planner'
import { buildSQL } from '../..'
import { buildReducerConfig } from '../select/reducer'
import { prepareArrayParam, SqlDialect } from '../../sql-builder-dialect'
import {
  needsPerParentPagination,
  buildChildArgs,
  ensureFkInSelect,
  ensureOrderByPk,
} from './where-in-utils'
import { withValidationSuppressed } from './validators/sql-validators'

const DYNAMIC_IN_PREFIX = 'whereIn'

function makeDynamicParam(name: string): string {
  return `$$dp_${name}$$`
}

export function resolvePrerenderedParams(
  paramMappings: PrerenderedWhereIn['paramMappings'],
  dynamicInName: string,
  uniqueIds: unknown[],
  dialect: SqlDialect,
): unknown[] | null {
  const params = new Array(paramMappings.length)

  for (let i = 0; i < paramMappings.length; i++) {
    const m = paramMappings[i]
    if (m.dynamicName === dynamicInName) {
      params[i] = prepareArrayParam(uniqueIds as any[], dialect)
    } else if (m.dynamicName) {
      return null
    } else {
      params[i] = m.value
    }
  }

  return params
}

export function prerenderSegment(
  segment: WhereInSegment,
  segmentIndex: number,
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
): PrerenderedWhereIn | null {
  const childModel = modelMap.get(segment.childModelName)
  if (!childModel) return null

  const stripPagination = needsPerParentPagination(segment)
  const dynamicInName = `${DYNAMIC_IN_PREFIX}_${segmentIndex}`
  const dynamicParam = makeDynamicParam(dynamicInName)

  const childArgs = buildChildArgs(
    segment.relArgs,
    segment.fkFieldName,
    dynamicParam as any,
    stripPagination,
  )

  ensureOrderByPk(childArgs, childModel)
  const needsStripFk = ensureFkInSelect(childArgs, segment.fkFieldName)

  const childPlan = planQueryStrategy({
    model: childModel,
    method: 'findMany',
    args: childArgs,
    allModels,
    dialect,
  })

  try {
    const result = withValidationSuppressed(() =>
      buildSQL(
        childModel,
        allModels as Model[],
        'findMany',
        childPlan.filteredArgs,
        dialect,
      ),
    ) as any

    if (!result.paramMappings) return null

    const requiresReduction = Boolean(
      result.requiresReduction && result.includeSpec,
    )
    const includeSpec = requiresReduction ? result.includeSpec : null

    let reducerConfig: any = null
    if (requiresReduction && includeSpec) {
      reducerConfig = buildReducerConfig(childModel, includeSpec, allModels)
    }

    const nestedSegments = prerenderSegments(
      childPlan.whereInSegments,
      allModels,
      modelMap,
      dialect,
    )

    return {
      sql: result.sql,
      paramMappings: result.paramMappings,
      dynamicInName,
      needsStripFk,
      requiresReduction,
      includeSpec,
      reducerConfig,
      nestedSegments,
      injectedParentKeys: childPlan.injectedParentKeys,
    }
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err)
    console.warn(
      `  ⚠ Prerender failed for ${segment.childModelName}.${segment.relationName}: ${errMsg}`,
    )
    return null
  }
}

export function prerenderSegments(
  segments: WhereInSegment[],
  allModels: readonly Model[],
  modelMap: Map<string, Model>,
  dialect: 'postgres' | 'sqlite',
): WhereInSegment[] {
  if (segments.length === 0) return segments

  return segments.map((seg, i) => {
    const prerendered = prerenderSegment(seg, i, allModels, modelMap, dialect)
    if (prerendered) {
      return { ...seg, prerendered }
    }
    return seg
  })
}
