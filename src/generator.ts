#!/usr/bin/env node
import { generatorHandler, GeneratorOptions } from '@prisma/generator-helper'
import { generateClient } from './code-emitter'
import { dirname, join, resolve } from 'path'
const { version } = require('../package.json')

function getDialectFromProvider(provider: string): 'postgres' | 'sqlite' {
  const normalized = provider.toLowerCase()
  if (normalized === 'sqlite') return 'sqlite'
  if (normalized === 'postgresql' || normalized === 'postgres')
    return 'postgres'
  throw new Error(
    `Unsupported database provider: ${provider}. ` +
      `Supported: postgresql, postgres, sqlite`,
  )
}

function getOutputDir(options: GeneratorOptions): string {
  const schemaDir = dirname(options.schemaPath)
  if (options.generator.output?.value) {
    return resolve(schemaDir, options.generator.output.value)
  }
  const clientGenerator = options.otherGenerators.find(
    (g) => g.provider.value === 'prisma-client-js',
  )
  if (clientGenerator?.output?.value) {
    const clientOutput = resolve(schemaDir, clientGenerator.output.value)
    return join(resolve(dirname(clientOutput), '..'), 'sql')
  }
  return resolve(schemaDir, './generated/sql')
}

function getDatasourceUrl(options: GeneratorOptions): string | undefined {
  const configUrl = options.generator.config.databaseUrl
  if (typeof configUrl === 'string' && configUrl) {
    return configUrl
  }

  const datasource = options.datasources?.[0]
  if (datasource?.url?.value) {
    return datasource.url.value
  }
  if (datasource?.url?.fromEnvVar) {
    const fromEnv = process.env[datasource.url.fromEnvVar]
    if (fromEnv) return fromEnv
  }

  return process.env.DATABASE_URL || undefined
}

generatorHandler({
  onManifest() {
    return {
      version,
      defaultOutput: './generated/sql',
      prettyName: 'prisma-sql-generator',
    }
  },
  async onGenerate(options: GeneratorOptions) {
    const { generator, dmmf, datasources } = options
    if (!datasources || datasources.length === 0) {
      throw new Error('No datasource found in schema')
    }

    const autoDialect = getDialectFromProvider(datasources[0].provider)
    const configDialect = generator.config.dialect as
      | 'postgres'
      | 'sqlite'
      | undefined
    const dialect = configDialect || autoDialect

    if (configDialect && configDialect !== autoDialect) {
      console.warn(
        `Generator dialect (${configDialect}) differs from datasource provider (${datasources[0].provider}). ` +
          `Using generator config: ${configDialect}`,
      )
    }

    const config = {
      dialect,
      skipInvalid: generator.config.skipInvalid === 'true',
    }

    const outputDir = getOutputDir(options)
    const datasourceUrl = getDatasourceUrl(options)

    console.info(`Generating SQL client to ${outputDir}`)
    console.info(`Datasource: ${datasources[0].provider}`)
    console.info(`Dialect: ${config.dialect}`)
    console.info(`Skip invalid: ${config.skipInvalid}`)
    console.info(
      `Database URL: ${datasourceUrl ? '✓ available' : '✗ not available (stats collection will be skipped)'}`,
    )

    await generateClient({
      datamodel: dmmf.datamodel,
      outputDir,
      config,
      datasourceUrl,
    })

    console.info('✓ Generated SQL client successfully')
  },
})
