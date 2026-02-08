import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { readFileSync, writeFileSync, unlinkSync } from 'fs'
import { join } from 'path'
import { createToSQL } from '../../src'
import { convertDMMFToModels } from '@dee-wan/schema-parser'
import { setGlobalDialect } from '../../src/sql-builder-dialect'
import { getDatamodel } from '../helpers/datamodel'

const PRISMA_DIR = join(process.cwd(), 'tests', 'prisma')
const SCHEMA_PATH = join(PRISMA_DIR, 'schema-postgres.prisma')

function mergeSchema(): void {
  const base = readFileSync(join(PRISMA_DIR, 'base.prisma'), 'utf-8')

  const header = `generator client {
  provider = "prisma-client-js"
  output   = "../generated/postgres"
}

datasource db {
  provider = "postgresql"
}`

  writeFileSync(SCHEMA_PATH, `${header}\n\n${base}`)
}

function cleanupSchema(): void {
  try {
    unlinkSync(SCHEMA_PATH)
  } catch {}
}

describe('Column Name Mapping', () => {
  let toSQL: ReturnType<typeof createToSQL>

  beforeAll(async () => {
    mergeSchema()
    const datamodel = await getDatamodel('postgres')
    const models = convertDMMFToModels(datamodel)
    setGlobalDialect('postgres')
    toSQL = createToSQL(models, 'postgres')
  })

  afterAll(() => {
    cleanupSchema()
  })

  it('should use dbName mapping for snake_case columns in WHERE', () => {
    const { sql } = toSQL('User', 'findMany', {
      where: { isDeleted: false },
    })

    const whereClause = sql.split('WHERE')[1]
    expect(whereClause).toContain('is_deleted')
    expect(whereClause).not.toContain('isDeleted')
  })

  it('should quote camelCase columns without dbName in WHERE', () => {
    const { sql } = toSQL('User', 'findMany', {
      where: { lastLoginAt: null },
    })

    const whereClause = sql.split('WHERE')[1]
    expect(whereClause).toContain('"lastLoginAt"')
  })

  it('should use dbName in WHERE clause for mapped columns', () => {
    const { sql } = toSQL('User', 'findMany', {
      where: { avatarUrl: { not: null } },
    })

    const whereClause = sql.split('WHERE')[1]
    expect(whereClause).toContain('avatar_url')
  })

  it('should select with proper column aliases', () => {
    const { sql } = toSQL('User', 'findMany', {})

    expect(sql).toContain('avatar_url AS "avatarUrl"')
    expect(sql).toContain('is_deleted AS "isDeleted"')
  })

  it('should handle orderBy with mapped columns', () => {
    const { sql } = toSQL('User', 'findMany', {
      orderBy: { avatarUrl: 'asc' },
    })

    const orderClause = sql.split('ORDER BY')[1]
    expect(orderClause).toContain('avatar_url')
  })

  it('should handle count with mapped columns', () => {
    const { sql } = toSQL('User', 'count', {
      where: { isDeleted: false },
    })

    const whereClause = sql.split('WHERE')[1]
    expect(whereClause).toContain('is_deleted')
  })
})
