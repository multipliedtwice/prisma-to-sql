import { DMMF } from '@prisma/generator-helper'
import { OrderByType } from './builder/shared/order-by-utils'
import { SqlDialect } from './sql-builder-dialect'
import { BatchQuery } from './batch'

export interface Field {
  name: string
  dbName: string
  type: string
  isRequired: boolean
  isRelation: boolean
  isId?: boolean
  relatedModel?: string
  relationName?: string
  foreignKey?: string | string[]
  references?: string | string[]
  isForeignKeyLocal?: boolean
}

export interface Model {
  name: string
  tableName: string
  fields: Field[]
}

export interface PrismaQueryArgs {
  where?: Record<string, unknown>
  select?: Record<string, unknown>
  include?: Record<string, unknown>
  orderBy?: OrderByType
  cursor?: Record<string, unknown>
  take?: number | string
  skip?: number | string
  distinct?: readonly string[]

  by?: readonly string[]
  having?: Record<string, unknown>

  _count?: unknown
  _sum?: unknown
  _avg?: unknown
  _min?: unknown
  _max?: unknown

  method?: string
}

export type PrismaMethod =
  | 'findMany'
  | 'findFirst'
  | 'findUnique'
  | 'count'
  | 'aggregate'
  | 'groupBy'

export interface PrismaSQLConfig<TClient> {
  client: TClient
  models?: Model[]
  dmmf?: DMMF.Document
  dialect: SqlDialect
  execute: (
    client: TClient,
    sql: string,
    params: unknown[],
  ) => Promise<unknown[]>
}

export interface SqlResult {
  sql: string
  params: unknown[]
}

export interface PrismaSQLResult<TClient> {
  toSQL: (
    model: string,
    method: PrismaMethod,
    args?: Record<string, unknown>,
  ) => SqlResult
  query: <T = unknown[]>(
    model: string,
    method: PrismaMethod,
    args?: Record<string, unknown>,
  ) => Promise<T>
  batchSql: (queries: Record<string, BatchQuery>) => SqlResult
  client: TClient
}
