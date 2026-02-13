import {
  convertDMMFToModels,
  processAllDirectives,
} from '@dee-wan/schema-parser'
import { DMMF } from '@prisma/generator-helper'
import { mkdir, writeFile } from 'fs/promises'
import { join, resolve } from 'path'
import { generateSQL } from '.'
import { setGlobalDialect } from './sql-builder-dialect'
import {
  emitPlannerGeneratedModule,
  type GeneratePlannerArtifacts,
  collectRelationCardinalities,
  createDatabaseExecutor,
} from './cardinality-planner'
import { createQueryKey, countTotalQueries } from './utils/pure-utils'

interface GenerateConfig {
  dialect: 'postgres' | 'sqlite'
  skipInvalid: boolean
}

interface GenerateClientOptions {
  datamodel: DMMF.Datamodel
  outputDir: string
  config: GenerateConfig
  runtimeImportPath?: string
  plannerArtifacts?: GeneratePlannerArtifacts
  executor?: {
    query: (
      sql: string,
      params?: unknown[],
    ) => Promise<Array<Record<string, unknown>>>
  }
  datasourceUrl?: string
}

interface EnumInfo {
  mappings: Record<string, Record<string, string>>
  fieldTypes: Record<string, Record<string, string>>
}

function extractEnumMappings(datamodel: DMMF.Datamodel): EnumInfo {
  const mappings: Record<string, Record<string, string>> = {}
  const fieldTypes: Record<string, Record<string, string>> = {}

  for (const enumDef of datamodel.enums) {
    const enumMapping: Record<string, string> = {}
    for (const value of enumDef.values) {
      enumMapping[value.name] = value.dbName || value.name
    }
    if (Object.keys(enumMapping).length > 0) {
      mappings[enumDef.name] = enumMapping
    }
  }

  for (const model of datamodel.models) {
    fieldTypes[model.name] = {}
    for (const field of model.fields) {
      const baseType = field.type.replace(/\[\]|\?/g, '')
      if (mappings[baseType]) {
        fieldTypes[model.name][field.name] = baseType
      }
    }
  }

  return { mappings, fieldTypes }
}

interface ProcessDirectiveResult {
  queries: Map<string, Map<string, Map<string, any>>>
  skippedCount: number
}

function processModelDirectives(
  modelName: string,
  result: any,
  config: GenerateConfig,
): { modelQueries: Map<string, Map<string, any>>; skipped: number } {
  const modelQueries = new Map<string, Map<string, any>>()
  let skipped = 0

  for (const directive of result.directives) {
    try {
      const method = directive.method
      const sqlDirective = generateSQL(directive)

      if (!modelQueries.has(method)) {
        modelQueries.set(method, new Map())
      }

      const methodQueriesMap = modelQueries.get(method)!
      const queryKey = createQueryKey(directive.query.processed)

      methodQueriesMap.set(queryKey, {
        sql: sqlDirective.sql,
        params: sqlDirective.staticParams,
        dynamicKeys: sqlDirective.dynamicKeys,
        paramMappings: sqlDirective.paramMappings,
        requiresReduction: sqlDirective.requiresReduction || false,
        includeSpec: sqlDirective.includeSpec || {},
      })
    } catch (error) {
      if (!config.skipInvalid) throw error
      skipped++
      const errMsg = error instanceof Error ? error.message : String(error)
      console.warn(`  ⚠ Skipped ${modelName}.${directive.method}: ${errMsg}`)
    }
  }

  return { modelQueries, skipped }
}

function processAllModelDirectives(
  directiveResults: Map<string, any>,
  config: GenerateConfig,
): ProcessDirectiveResult {
  const queries = new Map<string, Map<string, Map<string, any>>>()
  let skippedCount = 0

  for (const [modelName, result] of directiveResults) {
    if (result.directives.length === 0) continue

    const { modelQueries, skipped } = processModelDirectives(
      modelName,
      result,
      config,
    )

    queries.set(modelName, modelQueries)
    skippedCount += skipped
  }

  return { queries, skippedCount }
}

export async function generateClient(options: GenerateClientOptions) {
  const { datamodel, outputDir, config, datasourceUrl } = options
  const runtimeImportPath = options.runtimeImportPath ?? 'prisma-sql'

  setGlobalDialect(config.dialect)

  const models = convertDMMFToModels(datamodel)

  const directiveResults = processAllDirectives(
    datamodel.models as unknown as DMMF.Model[],
    datamodel,
    {
      skipInvalid: config.skipInvalid,
    },
  )

  const { queries, skippedCount } = processAllModelDirectives(
    directiveResults,
    config,
  )

  const absoluteOutputDir = resolve(process.cwd(), outputDir)
  await mkdir(absoluteOutputDir, { recursive: true })

  let plannerArtifacts = options.plannerArtifacts
  let relationStats = plannerArtifacts?.relationStats

  if (!relationStats && !options.executor && datasourceUrl) {
    let cleanup: (() => Promise<void>) | undefined
    try {
      const dbConn = await createDatabaseExecutor({
        databaseUrl: datasourceUrl,
        dialect: config.dialect,
      })
      cleanup = dbConn.cleanup

      console.log('📊 Collecting relation cardinalities...')
      relationStats = await collectRelationCardinalities({
        executor: dbConn.executor,
        datamodel,
        dialect: config.dialect,
      })
    } catch (error) {
      console.warn(
        '⚠ Failed to collect stats:',
        error instanceof Error ? error.message : error,
      )
    } finally {
      if (cleanup) {
        await cleanup()
      }
    }
  } else if (!relationStats && options.executor) {
    console.log('📊 Collecting relation cardinalities...')
    relationStats = await collectRelationCardinalities({
      executor: options.executor,
      datamodel,
      dialect: config.dialect,
    })
  }

  if (!relationStats) {
    relationStats = {}
  }

  if (!plannerArtifacts) {
    plannerArtifacts = { relationStats }
  }

  const plannerCode = emitPlannerGeneratedModule(plannerArtifacts)
  const plannerPath = join(absoluteOutputDir, 'planner.generated.ts')
  await writeFile(plannerPath, plannerCode)

  const code = generateCode(
    models,
    queries,
    config.dialect,
    datamodel,
    runtimeImportPath,
  )
  const outputPath = join(absoluteOutputDir, 'index.ts')
  await writeFile(outputPath, code)

  const totalQueries = countTotalQueries(queries)

  console.log(
    `✓ Generated ${queries.size} models, ${totalQueries} prebaked queries`,
  )
  if (skippedCount > 0) {
    console.log(`⚠ Skipped ${skippedCount} directive(s) due to errors`)
  }
  console.log(`✓ Output: ${outputPath}`)
}

function generateImports(runtimeImportPath: string): string {
  return `import { 
  transformAggregateRow, 
  extractCountValue, 
  buildSQL, 
  buildBatchSql, 
  parseBatchResults, 
  buildBatchCountSql, 
  parseBatchCountResults, 
  createTransactionExecutor, 
  transformQueryResults, 
  normalizeValue, 
  planQueryStrategy, 
  executeWhereInSegments,
  buildReducerConfig,
  reduceFlatRows,
  type PrismaMethod, 
  type Model, 
  type BatchQuery, 
  type BatchCountQuery, 
  type TransactionQuery, 
  type TransactionOptions,
  getOrPrepareStatement,
  shouldSqliteUseGet,
  normalizeParams,
  executePostgresQuery,
  executeSqliteQuery,
  executeRaw,
} from ${JSON.stringify(runtimeImportPath)}
import { RELATION_STATS } from './planner.generated'`
}

function generateCoreTypes(): string {
  return `class DeferredQuery {
  constructor(
    public readonly model: string,
    public readonly method: PrismaMethod,
    public readonly args: any,
  ) {}

  then(onfulfilled?: any, onrejected?: any): any {
    throw new Error(
      'Cannot await a batch query. Batch queries must not be awaited inside the $batch callback.',
    )
  }
}

interface BatchProxy {
  [modelName: string]: {
    findMany: (args?: any) => DeferredQuery
    findFirst: (args?: any) => DeferredQuery
    findUnique: (args?: any) => DeferredQuery
    count: (args?: any) => DeferredQuery
    aggregate: (args?: any) => DeferredQuery
    groupBy: (args?: any) => DeferredQuery
  }
}

const ACCELERATED_METHODS = new Set<PrismaMethod>([
  'findMany',
  'findFirst',
  'findUnique',
  'count',
  'aggregate',
  'groupBy',
])`
}

function generateHelpers(): string {
  return `function createBatchProxy(): BatchProxy {
  return new Proxy(
    {},
    {
      get(_target, modelName: string): any {
        if (typeof modelName === 'symbol') return undefined
        const model = MODEL_MAP.get(modelName)
        if (!model) {
          throw new Error(
            \`Model '\${modelName}' not found. Available: \${[...MODEL_MAP.keys()].join(', ')}\`,
          )
        }
        return new Proxy(
          {},
          {
            get(_target, method: string): (args?: any) => DeferredQuery {
              if (!ACCELERATED_METHODS.has(method as PrismaMethod)) {
                throw new Error(
                  \`Method '\${method}' not supported in batch. Supported: \${[...ACCELERATED_METHODS].join(', ')}\`,
                )
              }
              return (args?: any): DeferredQuery => {
                return new DeferredQuery(
                  modelName,
                  method as PrismaMethod,
                  args,
                )
              }
            },
          },
        )
      },
    },
  ) as BatchProxy
}

function getByPath(obj: any, path: string): unknown {
  if (!obj || !path) return undefined
  const keys = path.split('.')
  let result = obj
  for (const key of keys) {
    if (result == null) return undefined
    result = result[key]
  }
  return result
}

function resolveParamsFromMappings(args: any, paramMappings: any[]): unknown[] {
  const params: unknown[] = []
  for (let i = 0; i < paramMappings.length; i++) {
    const m = paramMappings[i]
    if (m.value !== undefined) {
      params.push(m.value)
      continue
    }
    if (m.dynamicName === undefined) {
      throw new Error(\`CRITICAL: ParamMap \${m.index} has neither dynamicName nor value\`)
    }
    const colonIdx = m.dynamicName.indexOf(':')
    const scopePath = colonIdx !== -1 ? m.dynamicName.slice(0, colonIdx) : null
    const name = colonIdx !== -1 ? m.dynamicName.slice(colonIdx + 1) : m.dynamicName
    let value: unknown
    if (!scopePath || scopePath.startsWith('root.')) {
      value = name.includes('.')
        ? getByPath(args, name)
        : args?.[name]
    } else {
      value = getByPath(args, scopePath)
    }
    if (value === undefined) {
      throw new Error(\`Missing required parameter: \${m.dynamicName}\`)
    }
    params.push(normalizeValue(value))
  }
  return params
}`
}

function generateDataConstants(
  cleanModels: any[],
  mappings: Record<string, Record<string, string>>,
  fieldTypes: Record<string, Record<string, string>>,
  queries: Map<string, Map<string, Map<string, any>>>,
  dialect: string,
): string {
  return `export const MODELS: Model[] = ${JSON.stringify(cleanModels, null, 2)}

const ENUM_MAPPINGS: Record<string, Record<string, string>> = ${JSON.stringify(mappings, null, 2)}

const ENUM_FIELDS: Record<string, Record<string, string>> = ${JSON.stringify(fieldTypes, null, 2)}

const QUERIES: Record<string, Record<string, Record<string, {
  sql: string
  params: unknown[]
  dynamicKeys: string[]
  paramMappings: any[]
  requiresReduction: boolean
  includeSpec: Record<string, any>
}>>> = ${formatQueries(queries)}

const DIALECT = ${JSON.stringify(dialect)}

const MODEL_MAP = new Map(MODELS.map(m => [m.name, m]))`
}

function generateTransformLogic(): string {
  return `function isDynamicKeyForQueryKey(path: string[], key: string): boolean {
  if (key !== 'skip' && key !== 'take' && key !== 'cursor') return false
  const parent = path.length > 0 ? path[path.length - 1] : null
  if (!parent) return true
  if (parent === 'include' || parent === 'select') return false
  if (path.includes('where')) return false
  if (path.includes('data')) return false
  if (path.includes('orderBy')) return false
  if (path.includes('having')) return false
  if (path.includes('by')) return false
  return true
}

function shouldSkipEnumTransform(path: string[], key: string): boolean {
  if (path.length === 0) return false
  
  const parent = path[path.length - 1]
  
  if (parent === 'orderBy') return true
  if (path.includes('orderBy')) return true
  
  if (parent === 'cursor') return true
  if (path.includes('cursor')) return true
  
  return false
}

function transformEnumInValue(value: unknown, enumType: string | undefined): unknown {
  if (!enumType || value === null || value === undefined) {
    return value
  }

  const mapping = ENUM_MAPPINGS[enumType]
  if (!mapping) {
    return value
  }

  if (Array.isArray(value)) {
    return value.map(v => {
      if (typeof v === 'string') {
        if (mapping[v] !== undefined) {
          return mapping[v]
        }
        throw new Error(
          \`Invalid enum value '\${v}' for type \${enumType}. Valid values: \${Object.keys(mapping).join(', ')}\`
        )
      }
      return v
    })
  }

  if (typeof value === 'string') {
    if (mapping[value] !== undefined) {
      return mapping[value]
    }
    throw new Error(
      \`Invalid enum value '\${value}' for type \${enumType}. Valid values: \${Object.keys(mapping).join(', ')}\`
    )
  }

  if (typeof value === 'object' && !(value instanceof Date)) {
    const result: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
      result[k] = transformEnumInValue(v, enumType)
    }
    return result
  }

  return value
}

function getRelatedModelName(currentModelName: string, relationFieldName: string): string | null {
  const m = MODEL_MAP.get(currentModelName)
  if (!m) return null
  const f: any = (m as any).fields?.find((x: any) => x?.name === relationFieldName)
  if (!f || !f.isRelation) return null
  return f.relatedModel || null
}

function transformEnumValuesByModel(modelName: string, obj: any, path: string[] = []): any {
  if (obj === null || obj === undefined) {
    return obj
  }

  if (Array.isArray(obj)) {
    return obj.map(item => transformEnumValuesByModel(modelName, item, path))
  }

  if (obj instanceof Date) {
    return obj
  }

  if (typeof obj === 'object') {
    const transformed: any = {}
    const modelFields = (ENUM_FIELDS as any)[modelName] || {}
    for (const [key, value] of Object.entries(obj)) {
      const nextPath = [...path, key]

      if (shouldSkipEnumTransform(path, key)) {
        transformed[key] = value
        continue
      }

      const relModel = getRelatedModelName(modelName, key)
      if (relModel && value && typeof value === 'object') {
        transformed[key] = transformEnumValuesByModel(relModel, value, nextPath)
        continue
      }

      const enumType = modelFields[key]
      if (enumType) {
        if (value && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
          const transformedOperators: any = {}
          for (const [op, opValue] of Object.entries(value)) {
            transformedOperators[op] = transformEnumInValue(opValue, enumType)
          }
          transformed[key] = transformedOperators
        } else {
          transformed[key] = transformEnumInValue(value, enumType)
        }
      } else if (value instanceof Date) {
        transformed[key] = value
      } else if (typeof value === 'object' && value !== null) {
        transformed[key] = transformEnumValuesByModel(modelName, value, nextPath)
      } else {
        transformed[key] = value
      }
    }
    return transformed
  }

  return obj
}

const BIGINT_MARKER = '\x00BIGINT\x00'

function bigIntSafeReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') return BIGINT_MARKER + value.toString()
  return value
}

function bigIntReviver(_key: string, value: unknown): unknown {
  if (typeof value === 'string' && value.startsWith(BIGINT_MARKER)) {
    return BigInt(value.slice(BIGINT_MARKER.length))
  }
  return value
}

function normalizeQuery(args: any): string {
  if (!args) return '{}'
  const jsonStr = JSON.stringify(args, bigIntSafeReplacer)
  const normalized = JSON.parse(jsonStr, bigIntReviver)

  function replaceDynamicParams(obj: any, path: string[] = []): any {
    if (!obj || typeof obj !== 'object') return obj
    if (Array.isArray(obj)) {
      return obj.map((v) => replaceDynamicParams(v, path))
    }
    const result: any = {}
    for (const [key, value] of Object.entries(obj)) {
      if (isDynamicKeyForQueryKey(path, key)) {
        result[key] = \`__DYNAMIC_\${key}__\`
      } else {
        result[key] = replaceDynamicParams(value, [...path, key])
      }
    }
    return result
  }

  const withMarkers = replaceDynamicParams(normalized)

  return JSON.stringify(withMarkers, (key, value) => {
    if (typeof value === 'bigint') return '__bigint__' + value.toString()
    if (value && typeof value === 'object' && !Array.isArray(value)) {
      const sorted: Record<string, unknown> = {}
      for (const k of Object.keys(value).sort()) {
        sorted[k] = (value as any)[k]
      }
      return sorted
    }
    return value
  })
}`
}

function generateExtension(runtimeImportPath: string): string {
  return `export function speedExtension(config: {
  postgres?: any
  sqlite?: any
  debug?: boolean
  onQuery?: (info: {
    model: string
    method: string
    sql: string
    params: unknown[]
    duration: number
    prebaked: boolean
  }) => void
}) {
  const { postgres, sqlite, debug, onQuery } = config
  if (!postgres && !sqlite) {
    throw new Error('speedExtension requires postgres or sqlite client')
  }
  const client = postgres || sqlite
  const actualDialect = postgres ? 'postgres' : 'sqlite'
  if (actualDialect !== DIALECT) {
    throw new Error(\`Generated code is for \${DIALECT}, but you provided \${actualDialect}\`)
  }

  async function executeQuery(
    sql: string,
    params: unknown[],
    method: string,
    requiresReduction: boolean,
    includeSpec: Record<string, any> | undefined,
    model: any | undefined,
  ): Promise<unknown[]> {
    if (DIALECT === 'postgres') {
      return executePostgresQuery(client, sql, params, method, requiresReduction, includeSpec, model, MODELS)
    }
    
    return executeSqliteQuery(client, sql, params, method, requiresReduction, includeSpec, model, MODELS)
  }

  async function executeWhereInQuery(sql: string, params: unknown[]): Promise<unknown[]> {
    const normalizedParams = normalizeParams(params)
    
    if (DIALECT === 'postgres') {
      const results: any[] = []
      await client.unsafe(sql, normalizedParams).forEach((row: any) => {
        results.push(row)
      })
      return results
    }
    
    const stmt = getOrPrepareStatement(client, sql)
    return stmt.all(...normalizedParams)
  }

  return (prisma: any) => {
    const txExecutor = createTransactionExecutor({
      modelMap: MODEL_MAP,
      allModels: MODELS,
      dialect: DIALECT,
      executeRaw: (sql: string, params?: unknown[]) => executeRaw(client, sql, params, DIALECT),
      postgresClient: postgres,
    })

    interface ModelContext {
      name?: string
      $name?: string
      $parent?: any
    }

    async function handleMethod(
      this: ModelContext,
      method: PrismaMethod,
      args: unknown
    ): Promise<unknown> {
      const modelName = this?.name || this?.$name
      
      if (!modelName || typeof modelName !== 'string') {
        throw new Error('Cannot determine model name from context')
      }
      
      const startTime = Date.now()

      try {
        if (args !== undefined && args !== null && typeof args !== 'object') {
          throw new Error(
            \`Invalid args type for \${modelName}.\${method}: expected object, got \${typeof args}\`
          )
        }

        const transformedArgs = transformEnumValuesByModel(modelName, args || {})

        const model = MODEL_MAP.get(modelName)
        if (!model) {
          if (!this.$parent?.[modelName]?.[method]) {
            throw new Error(\`Model '\${modelName}' not found and no Prisma fallback available\`)
          }
          return this.$parent[modelName][method](args)
        }

        const plan = planQueryStrategy({
          model,
          method,
          args: transformedArgs,
          allModels: MODELS,
          relationStats: RELATION_STATS as any,
          dialect: DIALECT,
        })

        const queryKey = normalizeQuery(plan.filteredArgs)
        const prebakedQuery = QUERIES[modelName]?.[method]?.[queryKey]
        let sql: string
        let params: unknown[]
        let prebaked = false
        let requiresReduction = false
        let includeSpec: Record<string, any> | undefined

        if (prebakedQuery) {
          sql = prebakedQuery.sql
          params = resolveParamsFromMappings(plan.filteredArgs, prebakedQuery.paramMappings)
          prebaked = true
          requiresReduction = prebakedQuery.requiresReduction || false
          includeSpec = prebakedQuery.includeSpec
        } else {
          const result = buildSQL(model, MODELS, method, plan.filteredArgs, DIALECT)
          sql = result.sql
          params = result.params as unknown[]
          requiresReduction = result.requiresReduction || false
          includeSpec = result.includeSpec
        }

        if (debug) {
          const strategy = DIALECT === 'postgres'
            ? (requiresReduction ? 'STREAMING REDUCTION' : 'STREAMING')
            : (requiresReduction ? 'BUFFERED REDUCTION' : 'DIRECT')
          
          const whereInMode = plan.whereInSegments.length > 0
            ? (DIALECT === 'postgres' ? 'STREAMING PARALLEL' : 'SEQUENTIAL')
            : 'NONE'
          
          console.log(\`[\${DIALECT}] \${modelName}.\${method} - \${strategy} + WHERE IN: \${whereInMode}\`)
          console.log(\`  Prebaked: \${prebaked}\`)
          console.log('  SQL:', sql)
          console.log('  Params:', params)
        }

        let results = await executeQuery(sql, params, method, requiresReduction, includeSpec, model)
        
        const duration = Date.now() - startTime

        onQuery?.({
          model: modelName,
          method,
          sql,
          params,
          duration,
          prebaked,
        })

        let transformed = transformQueryResults(method, results)

        if (plan.whereInSegments.length > 0) {
          const resultArray = Array.isArray(transformed) ? transformed : [transformed]

          if (resultArray.length > 0 && resultArray[0] != null) {
            if (DIALECT === 'postgres') {
              const { executeWhereInSegmentsStreaming } = await import(${JSON.stringify(runtimeImportPath)})
              
              const streamResults = await executeWhereInSegmentsStreaming({
                segments: plan.whereInSegments,
                parentSql: sql,
                parentParams: params,
                parentModel: model,
                allModels: MODELS,
                modelMap: MODEL_MAP,
                dialect: DIALECT,
                execute: async (sql: string, params: unknown[]) => {
                  const results: any[] = []
                  await client.unsafe(sql, normalizeParams(params)).forEach((row: any) => {
                    results.push(row)
                  })
                  return results
                },
                batchSize: 100,
                maxConcurrency: 10
              })
              
              transformed = Array.isArray(transformed) ? streamResults : streamResults[0]
            } else {
              await executeWhereInSegments({
                segments: plan.whereInSegments,
                parentRows: resultArray,
                parentModel: model,
                allModels: MODELS,
                modelMap: MODEL_MAP,
                dialect: DIALECT,
                execute: executeWhereInQuery,
              })
            }
          }
        }

        return transformed
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error)
        console.warn(\`[prisma-sql] \${modelName}.\${method} acceleration failed: \${msg}\`)
        if (debug && error instanceof Error && error.stack) {
          console.warn(error.stack)
        }
        
        if (!this.$parent?.[modelName]?.[method]) {
          throw error
        }
        
        return this.$parent[modelName][method](args)
      }
    }

    async function* findManyStream(
      this: ModelContext,
      args?: unknown
    ): AsyncIterableIterator<any> {
      const modelName = this?.name || this?.$name
      
      if (!modelName || typeof modelName !== 'string') {
        throw new Error('Cannot determine model name from context')
      }
      
      if (DIALECT !== 'postgres') {
        throw new Error('Streaming requires postgres.js client')
      }
      
      const transformedArgs = transformEnumValuesByModel(modelName, args || {})
      const model = MODEL_MAP.get(modelName)
      if (!model) {
        throw new Error(\`Model '\${modelName}' not found\`)
      }
      
      const plan = planQueryStrategy({
        model,
        method: 'findMany',
        args: transformedArgs,
        allModels: MODELS,
        relationStats: RELATION_STATS as any,
        dialect: DIALECT,
      })
      
      const queryKey = normalizeQuery(plan.filteredArgs)
      const prebakedQuery = QUERIES[modelName]?.['findMany']?.[queryKey]
      
      let sql: string
      let params: unknown[]
      let requiresReduction = false
      let includeSpec: Record<string, any> | undefined
      
      if (prebakedQuery) {
        sql = prebakedQuery.sql
        params = resolveParamsFromMappings(plan.filteredArgs, prebakedQuery.paramMappings)
        requiresReduction = prebakedQuery.requiresReduction
        includeSpec = prebakedQuery.includeSpec
      } else {
        const result = buildSQL(model, MODELS, 'findMany', plan.filteredArgs, DIALECT)
        sql = result.sql
        params = result.params as unknown[]
        requiresReduction = result.requiresReduction || false
        includeSpec = result.includeSpec
      }
      
      const normalizedParams = normalizeParams(params)
      
      if (requiresReduction && includeSpec) {
        const { createProgressiveReducer } = await import(${JSON.stringify(runtimeImportPath)})
        const config = buildReducerConfig(model, includeSpec, MODELS)
        const reducer = createProgressiveReducer(config)
        
        const completed: any[] = []
        let lastParentKey: string | null = null
        
        await client.unsafe(sql, normalizedParams).forEach((row: any) => {
          reducer.processRow(row)
          const currentKey = reducer.getCurrentParentKey(row)
          
          if (currentKey !== lastParentKey && lastParentKey !== null) {
            const parent = reducer.getCompletedParent(lastParentKey)
            if (parent) {
              completed.push(parent)
            }
          }
          
          lastParentKey = currentKey
        })
        
        const remaining = reducer.getRemainingParents()
        for (const parent of remaining) {
          completed.push(parent)
        }
        
        for (const item of completed) {
          yield item
        }
      } else {
        const rows: any[] = []
        await client.unsafe(sql, normalizedParams).forEach((row: any) => {
          rows.push(row)
        })
        
        for (const row of rows) {
          yield row
        }
      }
    }

    async function batch<T extends Record<string, DeferredQuery>>(
      callback: (batch: BatchProxy) => T | Promise<T>,
    ): Promise<{ [K in keyof T]: any }> {
      const batchProxy = createBatchProxy()
      const queries = await callback(batchProxy)
      const batchQueries: Record<string, BatchQuery> = {}
      for (const [key, deferred] of Object.entries(queries)) {
        if (!(deferred instanceof DeferredQuery)) {
          throw new Error(
            \`Batch query '\${key}' must be a deferred query. Did you await it?\`,
          )
        }
        batchQueries[key] = {
          model: deferred.model,
          method: deferred.method,
          args: transformEnumValuesByModel(deferred.model, deferred.args || {}),
        }
      }

      const startTime = Date.now()
      const { sql, params, keys, aliases } = buildBatchSql(
        batchQueries,
        MODEL_MAP,
        MODELS,
        DIALECT,
      )

      if (debug) {
        console.log(\`[\${DIALECT}] $batch (\${keys.length} queries)\`)
        console.log('SQL:', sql)
        console.log('Params:', params)
      }

      const normalizedParams = normalizeParams(params)
      
      let row: Record<string, unknown>
      if (DIALECT === 'postgres') {
        const rows: any[] = []
        await client.unsafe(sql, normalizedParams as any[]).forEach((r: any) => {
          rows.push(r)
        })
        row = rows[0] as Record<string, unknown>
      } else {
        const stmt = getOrPrepareStatement(client, sql)
        row = stmt.get(...normalizedParams) as Record<string, unknown>
      }
      
      const results = parseBatchResults(row, keys, batchQueries, aliases, MODEL_MAP)

      const duration = Date.now() - startTime
      onQuery?.({
        model: '_batch',
        method: 'batch',
        sql,
        params: normalizedParams,
        duration,
        prebaked: false,
      })

      return results as { [K in keyof T]: any }
    }

    async function batchCount(queries: BatchCountQuery[]): Promise<number[]> {
      if (queries.length === 0) return []
      const startTime = Date.now()
      const { sql, params } = buildBatchCountSql(
        queries,
        MODEL_MAP,
        MODELS,
        DIALECT,
      )

      if (debug) {
        console.log(\`[\${DIALECT}] $batchCount (\${queries.length} queries)\`)
        console.log('SQL:', sql)
        console.log('Params:', params)
      }

      const normalizedParams = normalizeParams(params)
      
      let row: Record<string, unknown>
      if (DIALECT === 'postgres') {
        const rows: any[] = []
        await client.unsafe(sql, normalizedParams as any[]).forEach((r: any) => {
          rows.push(r)
        })
        row = rows[0] as Record<string, unknown>
      } else {
        const stmt = getOrPrepareStatement(client, sql)
        row = stmt.get(...normalizedParams) as Record<string, unknown>
      }
      
      const results = parseBatchCountResults(row, queries.length)

      const duration = Date.now() - startTime
      onQuery?.({
        model: '_batch',
        method: 'count',
        sql,
        params: normalizedParams,
        duration,
        prebaked: false,
      })

      return results
    }

    async function transaction(
      queries: TransactionQuery[],
      options?: TransactionOptions,
    ): Promise<unknown[]> {
      const startTime = Date.now()
      if (debug) {
        console.log(\`[\${DIALECT}] $transaction (\${queries.length} queries)\`)
        if (options?.isolationLevel) {
          console.log(\`  Isolation level: \${options.isolationLevel}\`)
        }
      }
      const transformedQueries = queries.map(q => ({
        ...q,
        args: transformEnumValuesByModel(q.model, q.args || {}),
      }))
      const results = await txExecutor.execute(transformedQueries, options)
      const duration = Date.now() - startTime
      onQuery?.({
        model: '_transaction',
        method: 'transaction',
        sql: \`TRANSACTION(\${queries.length})\`,
        params: [],
        duration,
        prebaked: false,
      })
      return results
    }

    return prisma.$extends({
      name: 'prisma-sql-generated',
      client: {
        $original: prisma,
        $batch: batch as <T extends Record<string, DeferredQuery>>(
          callback: (batch: BatchProxy) => T | Promise<T>,
        ) => Promise<{ [K in keyof T]: any }>,
        $batchCount: batchCount as (...args: any[]) => Promise<number[]>,
        $transaction: transaction as (...args: any[]) => Promise<unknown[]>,
      },
      model: {
        $allModels: {
          async findMany(args: any) {
            return handleMethod.call(this, 'findMany', args)
          },
          async findFirst(args: any) {
            return handleMethod.call(this, 'findFirst', args)
          },
          async findUnique(args: any) {
            return handleMethod.call(this, 'findUnique', args)
          },
          async count(args: any) {
            return handleMethod.call(this, 'count', args)
          },
          async aggregate(args: any) {
            return handleMethod.call(this, 'aggregate', args)
          },
          async groupBy(args: any) {
            return handleMethod.call(this, 'groupBy', args)
          },
          findManyStream(args?: any): AsyncIterableIterator<any> {
            return findManyStream.call(this, args)
          },
        },
      },
    })
  }
}`
}

function generateTypeExports(): string {
  return `type SpeedExtensionReturn = ReturnType<ReturnType<typeof speedExtension>>

export type SpeedClient<T> = T & {
  $batch<T extends Record<string, DeferredQuery>>(
    callback: (batch: BatchProxy) => T | Promise<T>,
  ): Promise<{ [K in keyof T]: any }>
  $batchCount(queries: BatchCountQuery[]): Promise<number[]>
  $transaction(queries: TransactionQuery[], options?: TransactionOptions): Promise<unknown[]>
}

export type WithStreaming<T> = T & {
  findManyStream(args?: any): AsyncIterableIterator<any>
}

export type { BatchCountQuery, TransactionQuery, TransactionOptions }`
}

function generateCode(
  models: any[],
  queries: Map<string, Map<string, Map<string, any>>>,
  dialect: 'postgres' | 'sqlite',
  datamodel: DMMF.Datamodel,
  runtimeImportPath: string,
): string {
  const cleanModels = models.map((model) => ({
    ...model,
    fields: model.fields.filter((f: any) => f !== undefined && f !== null),
  }))

  const { mappings, fieldTypes } = extractEnumMappings(datamodel)

  return [
    generateImports(runtimeImportPath),
    generateCoreTypes(),
    generateHelpers(),
    generateDataConstants(cleanModels, mappings, fieldTypes, queries, dialect),
    generateTransformLogic(),
    generateExtension(runtimeImportPath),
    generateTypeExports(),
    `export * from './planner.generated'`,
  ].join('\n\n')
}

function formatQueries(
  queries: Map<string, Map<string, Map<string, any>>>,
): string {
  if (queries.size === 0) {
    return '{}'
  }

  const modelEntries: string[] = []

  for (const [modelName, methodMap] of queries) {
    const methodEntries: string[] = []

    for (const [method, queryMap] of methodMap) {
      const queryEntries: string[] = []

      for (const [queryKey, query] of queryMap) {
        queryEntries.push(`    ${JSON.stringify(queryKey)}: {
      sql: ${JSON.stringify(query.sql)},
      params: ${JSON.stringify(query.params)},
      dynamicKeys: ${JSON.stringify(query.dynamicKeys)},
      paramMappings: ${JSON.stringify(query.paramMappings)},
      requiresReduction: ${query.requiresReduction || false},
      includeSpec: ${JSON.stringify(query.includeSpec || {})},
    }`)
      }

      methodEntries.push(`    ${JSON.stringify(method)}: {
${queryEntries.join(',\n')}
    }`)
    }

    modelEntries.push(`  ${JSON.stringify(modelName)}: {
${methodEntries.join(',\n')}
  }`)
  }

  return `{
${modelEntries.join(',\n')}
}`
}
