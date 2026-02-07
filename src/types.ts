import { OrderByType } from './builder/shared/order-by-utils'

export interface Field {
  name: string
  dbName: string
  type: string
  isRequired: boolean
  isRelation: boolean
  relatedModel?: string
  relationName?: string
  foreignKey?: string
  references?: string
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
