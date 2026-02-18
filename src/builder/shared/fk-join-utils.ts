import { SQL_SEPARATORS } from './constants'
import { quoteColumn } from './sql-utils'
import { Model } from '../../types'

const FK_COLUMN_PREFIX = '__fk'

export function fkColumnName(index: number): string {
  return `"${FK_COLUMN_PREFIX}${index}"`
}

export function buildFkSelectList(
  sourceAlias: string,
  sourceModel: Model,
  keyFields: string[],
): string {
  return keyFields
    .map(
      (f, i) =>
        `${sourceAlias}.${quoteColumn(sourceModel, f)} AS ${fkColumnName(i)}`,
    )
    .join(SQL_SEPARATORS.FIELD_LIST)
}

export function buildFkGroupBy(keyFields: string[]): string {
  return keyFields
    .map((_, i) => fkColumnName(i))
    .join(SQL_SEPARATORS.FIELD_LIST)
}

export function buildFkPartitionBy(
  sourceAlias: string,
  sourceModel: Model,
  keyFields: string[],
): string {
  return keyFields
    .map((f) => `${sourceAlias}.${quoteColumn(sourceModel, f)}`)
    .join(SQL_SEPARATORS.FIELD_LIST)
}

export function buildFkJoinCondition(
  joinAlias: string,
  parentAlias: string,
  parentModel: Model,
  parentKeyFields: string[],
): string {
  const parts = parentKeyFields.map(
    (f, i) =>
      `${joinAlias}.${fkColumnName(i)} = ${parentAlias}.${quoteColumn(parentModel, f)}`,
  )
  return parts.length === 1 ? parts[0] : `(${parts.join(' AND ')})`
}
