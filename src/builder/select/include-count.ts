import { Model, Field } from '../../types'
import { SqlDialect } from '../../sql-builder-dialect'
import { SQL_SEPARATORS } from '../shared/constants'
import { sqlStringLiteral } from '../shared/sql-utils'
import { ParamStore } from '../shared/param-store'
import { createAliasGenerator } from '../shared/alias-generator'
import { isValidRelationField } from '../joins'
import { isNotNullish } from '../shared/validators/type-guards'
import { getRelationFieldSet } from '../shared/model-field-cache'
import { resolveRelationKeys } from '../shared/relation-key-utils'
import { getRelationTableReference } from './include-join'
import {
  fkColumnName,
  buildFkSelectList,
  buildFkPartitionBy,
  buildFkJoinCondition,
} from '../shared/fk-join-utils'

const COUNT_COLUMN = '__cnt'
const COUNT_SUBQUERY_PREFIX = '__tp_cnt_'
const COUNT_JOIN_PREFIX = '__tp_cnt_j_'

interface RelationCountBuild {
  joins: string[]
  jsonPairs: string
}

function resolveCountRelationOrThrow(
  relName: string,
  model: Model,
  schemaByName: Map<string, Model>,
): { field: Field; relModel: Model } {
  const relationSet = getRelationFieldSet(model)
  if (!relationSet.has(relName)) {
    throw new Error(
      `_count.${relName} references unknown relation on model ${model.name}`,
    )
  }

  const field = model.fields.find((f) => f.name === relName) as
    | Field
    | undefined
  if (!field) {
    throw new Error(
      `_count.${relName} references unknown relation on model ${model.name}`,
    )
  }

  if (!isValidRelationField(field)) {
    throw new Error(
      `_count.${relName} has invalid relation metadata on model ${model.name}`,
    )
  }

  const relatedModelName = field.relatedModel
  if (
    !isNotNullish(relatedModelName) ||
    String(relatedModelName).trim().length === 0
  ) {
    throw new Error(
      `_count.${relName} is missing relatedModel metadata on model ${model.name}`,
    )
  }

  const relModel = schemaByName.get(relatedModelName)
  if (!relModel) {
    throw new Error(
      `Related model '${relatedModelName}' not found for _count.${relName}`,
    )
  }

  return { field, relModel }
}

function subqueryForCount(args: {
  dialect: SqlDialect
  relTable: string
  countAlias: string
  relModel: Model
  relKeyFields: string[]
}): string {
  const selectKeys = buildFkSelectList(
    args.countAlias,
    args.relModel,
    args.relKeyFields,
  )

  const groupByKeys = buildFkPartitionBy(
    args.countAlias,
    args.relModel,
    args.relKeyFields,
  )

  const cntExpr =
    args.dialect === 'postgres'
      ? `COUNT(*)::int AS ${COUNT_COLUMN}`
      : `COUNT(*) AS ${COUNT_COLUMN}`

  return `(SELECT ${selectKeys}${SQL_SEPARATORS.FIELD_LIST}${cntExpr} FROM ${args.relTable} ${args.countAlias} GROUP BY ${groupByKeys})`
}

function nextAliasAvoiding(
  aliasGen: ReturnType<typeof createAliasGenerator>,
  base: string,
  forbidden: Set<string>,
): string {
  let a = aliasGen.next(base)
  while (forbidden.has(a)) a = aliasGen.next(base)
  return a
}

function buildCountJoinAndPair(args: {
  relName: string
  field: Field
  relModel: Model
  parentModel: Model
  parentAlias: string
  dialect: SqlDialect
  aliasGen: ReturnType<typeof createAliasGenerator>
}): { joinSql: string; pairSql: string } {
  const relTable = getRelationTableReference(args.relModel, args.dialect)
  const { childKeys: relKeyFields, parentKeys: parentKeyFields } =
    resolveRelationKeys(args.field, 'count')

  const forbidden = new Set<string>([args.parentAlias])

  const countAlias = nextAliasAvoiding(
    args.aliasGen,
    `${COUNT_SUBQUERY_PREFIX}${args.relName}`,
    forbidden,
  )
  forbidden.add(countAlias)

  const subquery = subqueryForCount({
    dialect: args.dialect,
    relTable,
    countAlias,
    relModel: args.relModel,
    relKeyFields,
  })

  const joinAlias = nextAliasAvoiding(
    args.aliasGen,
    `${COUNT_JOIN_PREFIX}${args.relName}`,
    forbidden,
  )

  const leftJoinOn = buildFkJoinCondition(
    joinAlias,
    args.parentAlias,
    args.parentModel,
    parentKeyFields,
  )

  return {
    joinSql: `LEFT JOIN ${subquery} ${joinAlias} ON ${leftJoinOn}`,
    pairSql: `${sqlStringLiteral(args.relName)}, COALESCE(${joinAlias}.${COUNT_COLUMN}, 0)`,
  }
}

export function buildRelationCountSql(
  countSelect: Record<string, boolean>,
  model: Model,
  schemas: readonly Model[],
  parentAlias: string,
  _params: ParamStore,
  dialect: SqlDialect,
): RelationCountBuild {
  const joins: string[] = []
  const pairs: string[] = []
  const aliasGen = createAliasGenerator()

  const schemaByName = new Map<string, Model>()
  for (const m of schemas) schemaByName.set(m.name, m)

  for (const [relName, shouldCount] of Object.entries(countSelect)) {
    if (!shouldCount) continue

    const resolved = resolveCountRelationOrThrow(relName, model, schemaByName)
    const built = buildCountJoinAndPair({
      relName,
      field: resolved.field,
      relModel: resolved.relModel,
      parentModel: model,
      parentAlias,
      dialect,
      aliasGen,
    })

    joins.push(built.joinSql)
    pairs.push(built.pairSql)
  }

  return { joins, jsonPairs: pairs.join(SQL_SEPARATORS.FIELD_LIST) }
}
