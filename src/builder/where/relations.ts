import { isValidRelationField, joinCondition } from '../joins'
import {
  SQL_TEMPLATES,
  RelationFilters,
  SQL_SEPARATORS,
  DEFAULT_WHERE_CLAUSE,
} from '../shared/constants'
import { createError } from '../shared/errors'
import {
  buildTableReference,
  normalizeKeyList,
  quoteColumn,
} from '../shared/sql-utils'
import { BuildContext, QueryResult } from '../shared/types'
import { isNotNullish, isPlainObject } from '../shared/validators/type-guards'
import { Field, Model } from '../../types'
import { getFieldByName } from '../shared/primary-key-utils'

export interface IWhereBuilder {
  build(where: Record<string, unknown>, ctx: BuildContext): QueryResult
}

type ToOneFilterKey = 'is' | 'isNot'

type RelationFilterArgs = {
  fieldName: string
  value: Record<string, unknown>
  ctx: BuildContext
  whereBuilder: IWhereBuilder
  field: Field
  relModel: Model
  relTable: string
  relAlias: string
  join: string
}

const NO_JOINS: readonly string[] = []

function isListRelation(fieldType: unknown): boolean {
  return typeof fieldType === 'string' && fieldType.endsWith('[]')
}

function buildToOneNullCheck(
  field: Field,
  parentModel: Model,
  parentAlias: string,
  relTable: string,
  relAlias: string,
  join: string,
  wantNull: boolean,
): string {
  const isLocal = field.isForeignKeyLocal === true
  const fkFields = normalizeKeyList(field.foreignKey)

  if (isLocal) {
    if (fkFields.length === 0) {
      throw createError(`Relation '${field.name}' is missing foreignKey`, {
        field: field.name,
      })
    }

    const parts = fkFields.map((fk) => {
      const expr = `${parentAlias}.${quoteColumn(parentModel, fk)}`
      return wantNull
        ? `${expr} ${SQL_TEMPLATES.IS_NULL}`
        : `${expr} ${SQL_TEMPLATES.IS_NOT_NULL}`
    })

    if (parts.length === 1) return parts[0]
    return wantNull ? `(${parts.join(' OR ')})` : `(${parts.join(' AND ')})`
  }

  const existsSql = `EXISTS (${SQL_TEMPLATES.SELECT} 1 ${SQL_TEMPLATES.FROM} ${relTable} ${relAlias} ${SQL_TEMPLATES.WHERE} ${join})`
  return wantNull ? `${SQL_TEMPLATES.NOT} ${existsSql}` : existsSql
}

function buildToOneExistsMatch(
  relTable: string,
  relAlias: string,
  join: string,
  sub: QueryResult,
): string {
  const joins = sub.joins.length > 0 ? ` ${sub.joins.join(' ')}` : ''
  return `EXISTS (${SQL_TEMPLATES.SELECT} 1 ${SQL_TEMPLATES.FROM} ${relTable} ${relAlias}${joins} ${SQL_TEMPLATES.WHERE} ${join} ${SQL_TEMPLATES.AND} ${sub.clause})`
}

function buildToOneNotExistsMatch(
  relTable: string,
  relAlias: string,
  join: string,
  sub: QueryResult,
): string {
  const joins = sub.joins.length > 0 ? ` ${sub.joins.join(' ')}` : ''
  return `${SQL_TEMPLATES.NOT} EXISTS (${SQL_TEMPLATES.SELECT} 1 ${SQL_TEMPLATES.FROM} ${relTable} ${relAlias}${joins} ${SQL_TEMPLATES.WHERE} ${join} ${SQL_TEMPLATES.AND} ${sub.clause})`
}

function tryOptimizeNoneFilter(
  noneValue: unknown,
  ctx: BuildContext,
  relModel: Model,
  relTable: string,
  relAlias: string,
  join: string,
  sub: QueryResult,
): QueryResult | null {
  const isEmptyFilter =
    isPlainObject(noneValue) && Object.keys(noneValue).length === 0

  const canOptimize =
    !ctx.isSubquery &&
    isEmptyFilter &&
    sub.clause === DEFAULT_WHERE_CLAUSE &&
    sub.joins.length === 0

  if (!canOptimize) return null

  const checkField =
    relModel.fields.find(
      (f) => !f.isRelation && f.isRequired && f.name !== 'id',
    ) || relModel.fields.find((f) => !f.isRelation && f.name === 'id')

  if (!checkField) return null

  const leftJoinSql = `LEFT JOIN ${relTable} ${relAlias} ON ${join}`
  const whereClause = `${relAlias}.${quoteColumn(relModel, checkField.name)} IS NULL`

  return {
    clause: whereClause,
    joins: [leftJoinSql],
  }
}

function processRelationFilter(
  key:
    | typeof RelationFilters.SOME
    | typeof RelationFilters.EVERY
    | typeof RelationFilters.NONE,
  wrap: (c: string, j: string) => string,
  args: RelationFilterArgs,
): string | null {
  const { value, fieldName, ctx, relAlias, relModel, whereBuilder } = args
  const raw = value[key]
  if (raw === undefined || raw === null) return null

  const sub = whereBuilder.build(raw as Record<string, unknown>, {
    ...ctx,
    alias: relAlias,
    model: relModel,
    path: [...ctx.path, fieldName, key],
    isSubquery: true,
    depth: ctx.depth + 1,
  })

  const j = sub.joins.length > 0 ? ` ${sub.joins.join(' ')}` : ''
  return wrap(sub.clause, j)
}

function buildListRelationFilters(args: RelationFilterArgs): QueryResult {
  const {
    fieldName,
    value,
    ctx,
    whereBuilder,
    relModel,
    relTable,
    relAlias,
    join,
  } = args

  const noneValue = value[RelationFilters.NONE]

  if (noneValue !== undefined && noneValue !== null) {
    const sub = whereBuilder.build(noneValue as Record<string, unknown>, {
      ...ctx,
      alias: relAlias,
      model: relModel,
      path: [...ctx.path, fieldName, RelationFilters.NONE],
      isSubquery: true,
      depth: ctx.depth + 1,
    })

    const optimized = tryOptimizeNoneFilter(
      noneValue,
      ctx,
      relModel,
      relTable,
      relAlias,
      join,
      sub,
    )

    if (optimized) return optimized
  }

  const filters: Array<{
    key:
      | typeof RelationFilters.SOME
      | typeof RelationFilters.EVERY
      | typeof RelationFilters.NONE
    wrap: (c: string, j: string) => string
  }> = [
    {
      key: RelationFilters.SOME,
      wrap: (c, j) =>
        `EXISTS (${SQL_TEMPLATES.SELECT} 1 ${SQL_TEMPLATES.FROM} ${relTable} ${relAlias}${j} ${SQL_TEMPLATES.WHERE} ${join} ${SQL_TEMPLATES.AND} ${c})`,
    },
    {
      key: RelationFilters.EVERY,
      wrap: (c, j) =>
        `${SQL_TEMPLATES.NOT} EXISTS (${SQL_TEMPLATES.SELECT} 1 ${SQL_TEMPLATES.FROM} ${relTable} ${relAlias}${j} ${SQL_TEMPLATES.WHERE} ${join} ${SQL_TEMPLATES.AND} ${SQL_TEMPLATES.NOT} (${c}))`,
    },
    {
      key: RelationFilters.NONE,
      wrap: (c, j) => {
        const condition =
          c === DEFAULT_WHERE_CLAUSE ? '' : ` ${SQL_TEMPLATES.AND} ${c}`
        return `${SQL_TEMPLATES.NOT} EXISTS (${SQL_TEMPLATES.SELECT} 1 ${SQL_TEMPLATES.FROM} ${relTable} ${relAlias}${j} ${SQL_TEMPLATES.WHERE} ${join}${condition})`
      },
    },
  ]

  const clauses: string[] = []

  for (const { key, wrap } of filters) {
    const clause = processRelationFilter(key, wrap, args)
    if (clause) clauses.push(clause)
  }

  if (clauses.length === 0) {
    throw createError(
      `List relation '${fieldName}' requires one of { some, every, none }`,
      { field: fieldName, path: ctx.path, modelName: ctx.model.name },
    )
  }

  return {
    clause: clauses.join(SQL_SEPARATORS.CONDITION_AND),
    joins: NO_JOINS,
  }
}

function buildToOneRelationFilters(args: RelationFilterArgs): QueryResult {
  const {
    fieldName,
    value,
    ctx,
    whereBuilder,
    field,
    relModel,
    relTable,
    relAlias,
    join,
  } = args

  const hasSomeEveryNone =
    isNotNullish(value[RelationFilters.SOME]) ||
    isNotNullish(value[RelationFilters.EVERY]) ||
    isNotNullish(value[RelationFilters.NONE])

  if (hasSomeEveryNone) {
    throw createError(
      `To-one relation '${fieldName}' does not support { some, every, none }; use { is, isNot }`,
      { field: fieldName, path: ctx.path, modelName: ctx.model.name },
    )
  }

  const hasIs = Object.prototype.hasOwnProperty.call(value, 'is')
  const hasIsNot = Object.prototype.hasOwnProperty.call(value, 'isNot')

  let filterKey: ToOneFilterKey
  let filterVal: unknown

  if (hasIs) {
    filterKey = 'is'
    filterVal = value.is
  } else if (hasIsNot) {
    filterKey = 'isNot'
    filterVal = value.isNot
  } else {
    filterKey = 'is'
    filterVal = value
  }

  if (filterVal === undefined) {
    return {
      clause: DEFAULT_WHERE_CLAUSE,
      joins: NO_JOINS,
    }
  }

  if (filterVal === null) {
    const wantNull = filterKey === 'is'
    const clause = buildToOneNullCheck(
      field,
      ctx.model,
      ctx.alias,
      relTable,
      relAlias,
      join,
      wantNull,
    )
    return {
      clause,
      joins: NO_JOINS,
    }
  }

  if (!isPlainObject(filterVal)) {
    throw createError(
      `Relation '${fieldName}' filter must be an object or null`,
      {
        field: fieldName,
        path: ctx.path,
        modelName: ctx.model.name,
        value: filterVal,
      },
    )
  }

  const sub = whereBuilder.build(filterVal, {
    ...ctx,
    alias: relAlias,
    model: relModel,
    path: [...ctx.path, fieldName, filterKey],
    isSubquery: true,
    depth: ctx.depth + 1,
  })

  const clause =
    filterKey === 'is'
      ? buildToOneExistsMatch(relTable, relAlias, join, sub)
      : buildToOneNotExistsMatch(relTable, relAlias, join, sub)

  return {
    clause,
    joins: NO_JOINS,
  }
}

function ensureRelationFilterObject(
  fieldName: string,
  value: Record<string, unknown>,
  ctx: BuildContext,
): void {
  if (!isPlainObject(value)) {
    throw createError(`Relation filter '${fieldName}' must be an object`, {
      path: [...ctx.path, fieldName],
      field: fieldName,
      modelName: ctx.model.name,
      value,
    })
  }
}

function buildRelation(
  fieldName: string,
  value: Record<string, unknown>,
  ctx: BuildContext,
  whereBuilder: IWhereBuilder,
): QueryResult {
  const field = getFieldByName(ctx.model, fieldName)

  if (!isValidRelationField(field)) {
    throw createError(`Invalid relation '${fieldName}'`, {
      field: fieldName,
      path: ctx.path,
      modelName: ctx.model.name,
    })
  }

  const relModel = ctx.schemaModels.find((m) => m.name === field.relatedModel)

  if (!isNotNullish(relModel)) {
    throw createError(
      `Related model '${field.relatedModel}' not found in schema. ` +
        `Available models: ${ctx.schemaModels.map((m) => m.name).join(', ')}`,
      {
        field: fieldName,
        path: ctx.path,
        modelName: ctx.model.name,
      },
    )
  }

  const relTable = buildTableReference(
    SQL_TEMPLATES.PUBLIC_SCHEMA,
    relModel.tableName,
    ctx.dialect,
  )

  const relAlias = ctx.aliasGen.next(fieldName)

  const join = joinCondition(field, ctx.model, relModel, ctx.alias, relAlias)

  const args: RelationFilterArgs = {
    fieldName,
    value,
    ctx,
    whereBuilder,
    field,
    relModel,
    relTable,
    relAlias,
    join,
  }

  if (isListRelation(field.type)) return buildListRelationFilters(args)
  return buildToOneRelationFilters(args)
}

export function buildTopLevelRelation(
  fieldName: string,
  value: Record<string, unknown>,
  ctx: BuildContext,
  whereBuilder: IWhereBuilder,
): QueryResult {
  ensureRelationFilterObject(fieldName, value, ctx)
  return buildRelation(fieldName, value, ctx, whereBuilder)
}

export function buildNestedRelation(
  fieldName: string,
  value: Record<string, unknown>,
  ctx: BuildContext,
  whereBuilder: IWhereBuilder,
): QueryResult {
  return buildTopLevelRelation(fieldName, value, ctx, whereBuilder)
}
