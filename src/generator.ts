#!/usr/bin/env node

import { generatorHandler, GeneratorOptions } from '@prisma/generator-helper'
import { generateClient } from './code-emitter'
import { logger } from '@prisma/internals'
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

generatorHandler({
  onManifest() {
    return {
      version,
      defaultOutput: './generated/sql',
      prettyName: 'prisma-sql-generator',
      requiresGenerators: ['prisma-client-js'],
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
      logger.warn(
        `Generator dialect (${configDialect}) differs from datasource provider (${datasources[0].provider}). ` +
          `Using generator config: ${configDialect}`,
      )
    }

    const config = {
      dialect,
      skipInvalid: generator.config.skipInvalid === 'true',
    }

    const outputDir = getOutputDir(options)

    logger.info(`Generating SQL client to ${outputDir}`)
    logger.info(`Datasource: ${datasources[0].provider}`)
    logger.info(`Dialect: ${config.dialect}`)
    logger.info(`Skip invalid: ${config.skipInvalid}`)

    await generateClient({
      datamodel: dmmf.datamodel,
      outputDir,
      config,
    })

    logger.info('✓ Generated SQL client successfully')
  },
})
