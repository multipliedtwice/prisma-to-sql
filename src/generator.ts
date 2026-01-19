#!/usr/bin/env node

import { generatorHandler, GeneratorOptions } from '@prisma/generator-helper'
import { generateClient } from './code-emitter'
import { logger } from '@prisma/internals'

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

generatorHandler({
  onManifest() {
    return {
      version,
      defaultOutput: '../node_modules/.prisma/client/sql',
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

    const outputDir =
      generator.output?.value || '../node_modules/.prisma/client/sql'

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
