import { drizzle as drizzlePg } from 'drizzle-orm/postgres-js'
import { drizzle as drizzleSqlite } from 'drizzle-orm/better-sqlite3'
import type { Logger } from 'drizzle-orm/logger'
import {
  eq,
  and,
  or,
  not,
  inArray,
  gt,
  gte,
  lt,
  lte,
  sql,
  isNull,
  isNotNull,
  like,
  desc,
  asc,
} from 'drizzle-orm'
import postgres from 'postgres'
import Database from 'better-sqlite3'
import * as schemaAll from '../drizzle/schema'
import { CaptureDrizzleLogger } from './query-capture'

export type PostgresDrizzleDB = {
  db: ReturnType<typeof drizzlePg>
  client: postgres.Sql
  dialect: 'postgres'
}

export type SqliteDrizzleDB = {
  db: ReturnType<typeof drizzleSqlite>
  client: Database.Database
  dialect: 'sqlite'
}

export type DrizzleDB = PostgresDrizzleDB | SqliteDrizzleDB

function pickSchema(prefix: 'pg' | 'sqlite') {
  return Object.fromEntries(
    Object.entries(schemaAll).filter(([k]) => k.startsWith(prefix)),
  ) as Record<string, unknown>
}

const pgSchema = pickSchema('pg')
const sqliteSchema = pickSchema('sqlite')

const logger: Logger = new CaptureDrizzleLogger()

export function createDrizzleDB(
  dialect: 'postgres',
  connectionString?: string,
): PostgresDrizzleDB
export function createDrizzleDB(
  dialect: 'sqlite',
  connectionString?: string,
): SqliteDrizzleDB
export function createDrizzleDB(
  dialect: 'postgres' | 'sqlite',
  connectionString?: string,
): DrizzleDB {
  if (dialect === 'postgres') {
    const client = postgres(
      connectionString ||
        'postgresql://postgres:postgres@localhost:5433/prisma_test',
    )
    const db = drizzlePg(client, { schema: pgSchema as any, logger })
    return { db, client, dialect: 'postgres' }
  }

  const dbPath = connectionString || './tests/prisma/db.sqlite'
  const client = new Database(dbPath)
  const db = drizzleSqlite(client, { schema: sqliteSchema as any, logger })
  return { db, client, dialect: 'sqlite' }
}

export {
  eq,
  and,
  or,
  not,
  inArray,
  gt,
  gte,
  lt,
  lte,
  sql,
  isNull,
  isNotNull,
  like,
  desc,
  asc,
}
