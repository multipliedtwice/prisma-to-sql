import { drizzle as drizzlePg } from 'drizzle-orm/postgres-js'
import { drizzle as drizzleSqlite } from 'drizzle-orm/better-sqlite3'
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
import * as pgSchema from '../drizzle/schema'
import * as sqliteSchema from '../drizzle/schema'

export type PostgresDrizzleDB = {
  db: ReturnType<typeof drizzlePg<typeof pgSchema>>
  client: postgres.Sql
  dialect: 'postgres'
  schema: typeof pgSchema
}

export type SqliteDrizzleDB = {
  db: ReturnType<typeof drizzleSqlite<typeof sqliteSchema>>
  client: Database.Database
  dialect: 'sqlite'
  schema: typeof sqliteSchema
}

export type DrizzleDB = PostgresDrizzleDB | SqliteDrizzleDB

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
    const db = drizzlePg(client, { schema: pgSchema })
    return { db, client, dialect: 'postgres', schema: pgSchema }
  } else {
    const dbPath = connectionString || './tests/prisma/db.sqlite'
    const client = new Database(dbPath)
    const db = drizzleSqlite(client, { schema: sqliteSchema })
    return { db, client, dialect: 'sqlite', schema: sqliteSchema }
  }
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
