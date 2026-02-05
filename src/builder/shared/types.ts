import { Model, PrismaQueryArgs } from '../../types'
import { SqlDialect } from '../../sql-builder-dialect'
import { ParamStore } from './param-store'
import { ParamMap } from '@dee-wan/schema-parser'

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
}

export interface WhereClauseResult {
  readonly clause: string
  readonly joins: readonly string[]
  readonly params: readonly unknown[]
  readonly paramMappings: readonly ParamMap[]
  readonly nextParamIndex: number
}

export interface SqlResult {
  readonly sql: string
  readonly params: readonly unknown[]
  readonly paramMappings: readonly ParamMap[]
}

export interface ErrorContext {
  field?: string
  operator?: string
  value?: unknown
  path?: readonly string[]
  modelName?: string
  availableFields?: readonly string[]
}
