import { Field, Model } from '../../types'
import { SqlDialect } from '../../sql-builder-dialect'
import { buildTableReference, col, quote } from './sql-utils'
import { joinCondition } from '../joins'
import {
  getRelationFieldSet,
  getScalarFieldSet,
  getFieldIndices,
} from './model-field-cache'
import { SQL_SEPARATORS } from './constants'
import { isPlainObject, isNotNullish } from './validators/type-guards'
import {
  expandOrderByInput,
  normalizeAndValidateOrderBy,
} from './order-by-utils'
import { parseOrderByValue, buildOrderByFragment } from '../pagination'

const MAX_RELATION_ORDER_BY_DEPTH = 10

interface OrderByWithRelationsResult {
  sql: string
  joins: string[]
}

interface RelationOrderByContext {
  schemas: Model[]
  dialect: SqlDialect
  joins: string[]
  usedAliases: Set<string>
  aliasCounter: { value: number }
  modelMap: Map<string, Model>
}

function resolveTableRef(model: Model, dialect: SqlDialect): string {
  const tableName =
    (model as any).tableName || (model as any).dbName || model.name
  if (dialect === 'sqlite') {
    return quote(tableName)
  }
  const schema = (model as any).schema || (model as any).schemaName || 'public'
  return buildTableReference(schema, tableName, dialect)
}

function findRelationField(model: Model, fieldName: string): Field | undefined {
  const field = getFieldIndices(model).allFieldsByName.get(fieldName)
  if (!field || !field.isRelation) return undefined
  return field as Field
}

function nextJoinAlias(ctx: RelationOrderByContext): string {
  let alias: string
  do {
    alias = `ob_${ctx.aliasCounter.value++}`
  } while (ctx.usedAliases.has(alias))
  ctx.usedAliases.add(alias)
  return alias
}

function resolveRelationOrderByChain(
  relationFieldName: string,
  value: Record<string, unknown>,
  currentModel: Model,
  currentAlias: string,
  ctx: RelationOrderByContext,
  depth: number,
): string[] {
  if (depth > MAX_RELATION_ORDER_BY_DEPTH) {
    throw new Error(
      `Relation orderBy nesting too deep (max ${MAX_RELATION_ORDER_BY_DEPTH} levels)`,
    )
  }

  if ('_count' in value && value._count !== undefined) {
    throw new Error(
      `Relation orderBy with _count on '${relationFieldName}' is not yet supported by prisma-sql`,
    )
  }

  const field = findRelationField(currentModel, relationFieldName)
  if (!field) {
    throw new Error(
      `Relation field '${relationFieldName}' not found on model ${currentModel.name}`,
    )
  }

  const relatedModel = ctx.modelMap.get(field.relatedModel!)
  if (!relatedModel) {
    throw new Error(
      `Related model '${field.relatedModel}' not found for relation '${relationFieldName}'`,
    )
  }

  const joinAlias = nextJoinAlias(ctx)
  const tableRef = resolveTableRef(relatedModel, ctx.dialect)
  const cond = joinCondition(
    field as unknown as Field,
    currentModel,
    relatedModel,
    currentAlias,
    joinAlias,
  )
  ctx.joins.push(`LEFT JOIN ${tableRef} ${joinAlias} ON ${cond}`)

  const relScalarSet = getScalarFieldSet(relatedModel)
  const relRelationSet = getRelationFieldSet(relatedModel)
  const nestedEntries = Object.entries(value).filter(([, v]) => v !== undefined)
  const orderFragments: string[] = []

  for (const [nestedField, nestedValue] of nestedEntries) {
    if (relScalarSet.has(nestedField)) {
      const entries = normalizeAndValidateOrderBy(
        [{ [nestedField]: nestedValue }],
        relatedModel,
        parseOrderByValue,
      )
      const sql = buildOrderByFragment(
        entries,
        joinAlias,
        ctx.dialect,
        relatedModel,
      )
      if (sql) orderFragments.push(sql)
      continue
    }

    if (relRelationSet.has(nestedField)) {
      if (!isPlainObject(nestedValue)) {
        throw new Error(
          `Relation orderBy for '${nestedField}' must be an object`,
        )
      }
      const nested = resolveRelationOrderByChain(
        nestedField,
        nestedValue,
        relatedModel,
        joinAlias,
        ctx,
        depth + 1,
      )
      orderFragments.push(...nested)
      continue
    }

    throw new Error(
      `orderBy field '${nestedField}' does not exist on related model '${relatedModel.name}'`,
    )
  }

  return orderFragments
}

export function buildOrderByWithRelations(
  orderBy: unknown,
  alias: string,
  dialect: SqlDialect,
  model: Model,
  schemas: Model[],
): OrderByWithRelationsResult {
  if (!isNotNullish(orderBy)) return { sql: '', joins: [] }

  const expanded = expandOrderByInput(orderBy)
  if (expanded.length === 0) return { sql: '', joins: [] }

  const relationSet = getRelationFieldSet(model)
  const scalarSet = getScalarFieldSet(model)
  const orderFragments: string[] = []

  const modelMap = new Map<string, Model>()
  for (const m of schemas) modelMap.set(m.name, m)

  const ctx: RelationOrderByContext = {
    schemas,
    dialect,
    joins: [],
    usedAliases: new Set<string>(),
    aliasCounter: { value: 0 },
    modelMap,
  }

  for (const [fieldName, value] of expanded) {
    if (scalarSet.has(fieldName)) {
      const entries = normalizeAndValidateOrderBy(
        [{ [fieldName]: value }],
        model,
        parseOrderByValue,
      )
      const sql = buildOrderByFragment(entries, alias, dialect, model)
      if (sql) orderFragments.push(sql)
      continue
    }

    if (relationSet.has(fieldName)) {
      if (!isPlainObject(value)) {
        throw new Error(`Relation orderBy for '${fieldName}' must be an object`)
      }

      const fragments = resolveRelationOrderByChain(
        fieldName,
        value,
        model,
        alias,
        ctx,
        0,
      )
      orderFragments.push(...fragments)
      continue
    }

    throw new Error(
      `orderBy field '${fieldName}' does not exist on model ${model.name}`,
    )
  }

  return {
    sql: orderFragments.join(SQL_SEPARATORS.ORDER_BY),
    joins: ctx.joins,
  }
}
