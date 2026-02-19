import { Field, Model } from '../../types'
import { SqlDialect } from '../../sql-builder-dialect'
import { buildTableReference, col, quote } from './sql-utils'
import { joinCondition, getModelByName } from '../joins'
import { getRelationFieldSet, getScalarFieldSet } from './model-field-cache'
import { SQL_SEPARATORS } from './constants'
import { isPlainObject, isNotNullish } from './validators/type-guards'
import {
  expandOrderByInput,
  normalizeAndValidateOrderBy,
} from './order-by-utils'
import { parseOrderByValue, buildOrderByFragment } from '../pagination'

interface OrderByWithRelationsResult {
  sql: string
  joins: string[]
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

function findRelationField(model: Model, fieldName: string) {
  return model.fields.find((f) => f.name === fieldName && f.isRelation)
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
  const joins: string[] = []
  const usedAliases = new Set<string>()
  let relAliasCounter = 0

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

      const nestedEntries = Object.entries(value)
      if (nestedEntries.length === 0) continue

      if ('_count' in value) {
        throw new Error(
          `Relation orderBy with _count on '${fieldName}' is not yet supported by prisma-sql`,
        )
      }

      const field = findRelationField(model, fieldName)
      if (!field) {
        throw new Error(
          `Relation field '${fieldName}' not found on model ${model.name}`,
        )
      }

      const relatedModel = getModelByName(schemas, field.relatedModel!)
      if (!relatedModel) {
        throw new Error(
          `Related model '${field.relatedModel}' not found for relation '${fieldName}'`,
        )
      }

      const relScalarSet = getScalarFieldSet(relatedModel)
      const relRelationSet = getRelationFieldSet(relatedModel)

      for (const [nestedField] of nestedEntries) {
        if (relRelationSet.has(nestedField)) {
          throw new Error(
            `Nested relation orderBy (${fieldName}.${nestedField}) is not yet supported by prisma-sql`,
          )
        }
        if (!relScalarSet.has(nestedField)) {
          throw new Error(
            `orderBy field '${nestedField}' does not exist on related model '${relatedModel.name}'`,
          )
        }
      }

      let joinAlias: string
      do {
        joinAlias = `ob_${relAliasCounter++}`
      } while (usedAliases.has(joinAlias))
      usedAliases.add(joinAlias)

      const tableRef = resolveTableRef(relatedModel, dialect)
      const cond = joinCondition(
        field as unknown as Field,
        model,
        relatedModel,
        alias,
        joinAlias,
      )
      joins.push(`LEFT JOIN ${tableRef} ${joinAlias} ON ${cond}`)

      const entries = normalizeAndValidateOrderBy(
        [value],
        relatedModel,
        parseOrderByValue,
      )
      const sql = buildOrderByFragment(
        entries,
        joinAlias,
        dialect,
        relatedModel,
      )
      if (sql) orderFragments.push(sql)
      continue
    }

    throw new Error(
      `orderBy field '${fieldName}' does not exist on model ${model.name}`,
    )
  }

  return {
    sql: orderFragments.join(SQL_SEPARATORS.ORDER_BY),
    joins,
  }
}
