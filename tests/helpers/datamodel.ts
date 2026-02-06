// tests/helpers/datamodel.ts

import { getDMMF } from '@prisma/internals'
import { readFileSync, existsSync } from 'node:fs'
import { resolve, join } from 'node:path'
import type { DMMF } from '@prisma/generator-helper'

function detectPrismaVersion(): number {
  if (process.env.PRISMA_VERSION) {
    return parseInt(process.env.PRISMA_VERSION, 10)
  }
  try {
    const pkg = require(
      resolve(process.cwd(), 'node_modules', 'prisma', 'package.json'),
    )
    return parseInt(pkg.version.split('.')[0], 10) >= 7 ? 7 : 6
  } catch {
    return 6
  }
}

const PRISMA_VERSION = detectPrismaVersion()

function preprocessSchemaForV7(schema: string): string {
  return schema.replace(/^\s*url\s*=\s*.*$/gm, '')
}

async function loadDmmfForDialect(
  dialect: 'postgres' | 'sqlite',
): Promise<DMMF.Datamodel> {
  const prismaDir = resolve(process.cwd(), 'tests/prisma')

  const versionedSchemaFile = `schema-${dialect}-v${PRISMA_VERSION}.prisma`
  const baseSchemaFile = `schema-${dialect}.prisma`

  const versionedPath = resolve(prismaDir, versionedSchemaFile)
  const basePath = resolve(prismaDir, baseSchemaFile)

  let schemaPath: string
  let datamodel: string

  if (existsSync(versionedPath)) {
    schemaPath = versionedPath
    datamodel = readFileSync(schemaPath, 'utf8')
  } else if (existsSync(basePath)) {
    schemaPath = basePath
    datamodel = readFileSync(schemaPath, 'utf8')
  } else {
    const headerPath = resolve(prismaDir, `${dialect}.prisma`)
    const baseModelPath = resolve(prismaDir, 'base.prisma')

    if (!existsSync(headerPath) || !existsSync(baseModelPath)) {
      throw new Error(
        `Cannot find schema files for ${dialect}. Looked for: ${versionedPath}, ${basePath}, ${headerPath}+${baseModelPath}`,
      )
    }

    const header = readFileSync(headerPath, 'utf8')
    const base = readFileSync(baseModelPath, 'utf8')
    datamodel = `${header}\n\n${base}`
  }

  if (PRISMA_VERSION >= 7) {
    datamodel = preprocessSchemaForV7(datamodel)
  }

  const dmmf = await getDMMF({ datamodel })
  return dmmf.datamodel as DMMF.Datamodel
}

let postgresDatamodel: DMMF.Datamodel | null = null
let sqliteDatamodel: DMMF.Datamodel | null = null
let cachedVersion: number | null = null

export async function getDatamodel(
  dialect: 'postgres' | 'sqlite',
): Promise<DMMF.Datamodel> {
  if (cachedVersion !== PRISMA_VERSION) {
    postgresDatamodel = null
    sqliteDatamodel = null
    cachedVersion = PRISMA_VERSION
  }

  if (dialect === 'postgres') {
    if (!postgresDatamodel) {
      postgresDatamodel = await loadDmmfForDialect('postgres')
    }
    return postgresDatamodel
  } else {
    if (!sqliteDatamodel) {
      sqliteDatamodel = await loadDmmfForDialect('sqlite')
    }
    return sqliteDatamodel
  }
}

export function clearDatamodelCache(): void {
  postgresDatamodel = null
  sqliteDatamodel = null
  cachedVersion = null
}
