import { getDMMF } from '@prisma/internals'
import { readFileSync, existsSync } from 'node:fs'
import { resolve } from 'node:path'
import type { DMMF } from '@prisma/generator-helper'

const PRISMA_VERSION = parseInt(process.env.PRISMA_VERSION || '6', 10)

function preprocessSchemaForV7(schema: string): string {
  return schema.replace(/^\s*url\s*=\s*["'][^"']*["']\s*$/gm, '')
}

async function loadDmmfForDialect(
  dialect: 'postgres' | 'sqlite',
): Promise<DMMF.Datamodel> {
  const prismaDir = resolve(process.cwd(), 'tests/prisma')

  const versionedSchemaFile = `schema-${dialect}-v${PRISMA_VERSION}.prisma`
  const baseSchemaFile = `schema-${dialect}.prisma`

  const versionedPath = resolve(prismaDir, versionedSchemaFile)
  const basePath = resolve(prismaDir, baseSchemaFile)

  const schemaPath = existsSync(versionedPath) ? versionedPath : basePath
  let datamodel = readFileSync(schemaPath, 'utf8')

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
