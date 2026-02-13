import { Model as DMMMModel } from '@dee-wan/schema-parser'

export type Model = DMMMModel

export interface ParamMapping {
  index: number
  dynamicName?: string
  value?: unknown
}

export interface SqlResult {
  sql: string
  params: unknown[]
  paramMappings?: ParamMapping[]
  requiresReduction?: boolean
  includeSpec?: Record<string, any>
  supportsStreaming?: boolean
}

export interface PrismaQueryArgs {
  where?: Record<string, unknown>
  select?: Record<string, unknown>
  include?: Record<string, unknown>
  orderBy?: unknown
  take?: number | string
  skip?: number | string
  cursor?: Record<string, unknown>
  distinct?: string[]
  _count?: unknown
  _sum?: unknown
  _avg?: unknown
  _min?: unknown
  _max?: unknown
  by?: string[]
  having?: Record<string, unknown>
  [key: string]: unknown
}

export type PrismaMethod =
  | 'findUnique'
  | 'findFirst'
  | 'findMany'
  | 'create'
  | 'createMany'
  | 'update'
  | 'updateMany'
  | 'upsert'
  | 'delete'
  | 'deleteMany'
  | 'count'
  | 'aggregate'
  | 'groupBy'

export interface PrismaSQLConfig<TClient> {
  client: TClient
  models?: Model[]
  dmmf?: any
  dialect: 'postgres' | 'sqlite'
  execute: (
    client: TClient,
    sql: string,
    params: unknown[],
  ) => Promise<unknown[]>
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
  batchSql: (queries: Record<string, any>) => SqlResult
  client: TClient
}

export interface Field {
  name: string
  type: string
  isId?: boolean
  isUnique?: boolean
  isRequired?: boolean
  isList?: boolean
  isRelation?: boolean
  relationName?: string
  relatedModel?: string
  dbName?: string | null
  foreignKey?: string | string[]
  references?: string | string[]
  isForeignKeyLocal?: boolean
  [key: string]: unknown
}
