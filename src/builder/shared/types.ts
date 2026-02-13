import type {
  Model,
  PrismaQueryArgs,
  ParamMapping,
  SqlResult,
} from '../../types'
import type { SqlDialect } from '../../sql-builder-dialect'
import type { ParamStore } from './param-store'

export type { SqlResult, ParamMapping }

export interface BuildContext {
  readonly alias: string
  readonly schemaModels: readonly Model[]
  readonly model: Model
  readonly path: readonly string[]
  readonly isSubquery: boolean
  readonly aliasGen: AliasGenerator
  readonly dialect: SqlDialect
  readonly params: ParamStore
  readonly depth: number
  readonly seenObjects: WeakSet<object>
}

export interface AliasGenerator {
  next(baseName: string): string
}

export interface QueryResult {
  readonly clause: string
  readonly joins: readonly string[]
}

export interface SelectQuerySpec {
  readonly select: string
  readonly includes: readonly IncludeSpec[]
  readonly from: { table: string; alias: string }
  readonly whereClause: string
  readonly whereJoins: readonly string[]
  readonly orderBy: string
  readonly pagination: { take?: number | string; skip?: number | string }
  readonly distinct?: readonly string[]
  readonly method: string
  readonly cursorCte?: string
  readonly cursorClause?: string
  readonly params: ParamStore
  readonly dialect: SqlDialect
  readonly model: Model
  readonly schemas: readonly Model[]
  readonly args: PrismaQueryArgs
}

export interface IncludeSpec {
  readonly name: string
  readonly sql: string
  readonly isOneToOne: boolean
  readonly joinSql?: string
  readonly selectExpr?: string
}

export interface WhereClauseResult {
  readonly clause: string
  readonly joins: readonly string[]
  readonly params: readonly unknown[]
  readonly paramMappings: readonly ParamMapping[]
  readonly nextParamIndex: number
}

export interface ErrorContext {
  field?: string
  operator?: string
  value?: unknown
  path?: readonly string[]
  modelName?: string
  availableFields?: readonly string[]
}

export interface SqlBatchQuery {
  readonly sql: string
  readonly params: readonly unknown[]
  readonly paramMappings: readonly ParamMapping[]
  readonly role: 'parent' | 'children'
  readonly relationName?: string
  readonly parentKeyField?: string
  readonly childKeyField?: string
  readonly isOneToOne?: boolean
}

export interface BatchRelationMeta {
  readonly name: string
  readonly foreignKey: string
  readonly referenceKey: string
  readonly isOneToOne: boolean
}

export type MergeStrategy =
  | { readonly type: 'single' }
  | {
      readonly type: 'batch'
      readonly parentIdField: string
      readonly relations: readonly BatchRelationMeta[]
    }

export interface SqlBatchResult {
  readonly queries: readonly SqlBatchQuery[]
  readonly mergeStrategy: MergeStrategy
}
