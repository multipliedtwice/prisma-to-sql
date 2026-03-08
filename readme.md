# prisma-sql

<img width="250" height="170" alt="image" src="https://github.com/user-attachments/assets/3f9233f2-5d5c-41e3-b1cd-ced7ce0b54c2" />

Prerender Prisma queries to SQL and execute them directly via `postgres.js` or `better-sqlite3`.

**Same Prisma API. Same Prisma types. Lower read overhead.**

```ts
import { PrismaClient } from '@prisma/client'
import { speedExtension, type SpeedClient } from './generated/sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL!)
const basePrisma = new PrismaClient()

export const prisma = basePrisma.$extends(
  speedExtension({ postgres: sql }),
) as SpeedClient<typeof basePrisma>

const users = await prisma.user.findMany({
  where: { status: 'ACTIVE' },
  include: { posts: true },
})

const dashboard = await prisma.$batch((batch) => ({
  activeUsers: batch.user.count({ where: { status: 'ACTIVE' } }),
  recentPosts: batch.post.findMany({
    take: 10,
    orderBy: { createdAt: 'desc' },
  }),
  taskStats: batch.task.aggregate({
    _count: true,
    _avg: { estimatedHours: true },
  }),
}))
```

## What it does

`prisma-sql` accelerates Prisma **read** queries by skipping Prisma's read execution path and running generated SQL directly through a database-native client.

It keeps the Prisma client for:

- schema and migrations
- generated types
- writes
- fallback for unsupported cases

It accelerates:

- `findMany`
- `findFirst`
- `findUnique`
- `count`
- `aggregate`
- `groupBy`
- PostgreSQL `$batch`

## Why use it

Prisma's DX is excellent, but read queries still pay runtime overhead for query-engine planning, validation, transformation, and result mapping.

`prisma-sql` moves that work out of the hot path:

- builds SQL from Prisma-style query args
- can prebake hot queries at generate time
- executes via `postgres.js` or `better-sqlite3`
- maps results back to Prisma-like shapes

The goal is simple:

- keep Prisma's developer experience
- cut read-path overhead
- stay compatible with existing Prisma code

## Installation

### PostgreSQL

```bash
npm install prisma-sql postgres
```

### SQLite

```bash
npm install prisma-sql better-sqlite3
```

## Quick start

### 1) Add the generator

```prisma
generator client {
  provider = "prisma-client"
}

generator sql {
  provider = "prisma-sql-generator"
}

model User {
  id     Int    @id @default(autoincrement())
  email  String @unique
  status String
  posts  Post[]
}

model Post {
  id        Int    @id @default(autoincrement())
  title     String
  authorId  Int
  author    User   @relation(fields: [authorId], references: [id])
}
```

### 2) Generate

```bash
npx prisma generate
```

This generates `./generated/sql/index.ts`.

### 3) Extend Prisma

### PostgreSQL

```ts
import { PrismaClient } from '@prisma/client'
import { speedExtension, type SpeedClient } from './generated/sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL!)
const basePrisma = new PrismaClient()

export const prisma = basePrisma.$extends(
  speedExtension({ postgres: sql }),
) as SpeedClient<typeof basePrisma>
```

### SQLite

```ts
import { PrismaClient } from '@prisma/client'
import { speedExtension, type SpeedClient } from './generated/sql'
import Database from 'better-sqlite3'

const db = new Database('./data.db')
const basePrisma = new PrismaClient()

export const prisma = basePrisma.$extends(
  speedExtension({ sqlite: db }),
) as SpeedClient<typeof basePrisma>
```

### With existing Prisma extensions

Apply `speedExtension` last so it sees the final query surface.

```ts
import { PrismaClient } from '@prisma/client'
import { speedExtension, type SpeedClient } from './generated/sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL!)
const basePrisma = new PrismaClient()

const extendedPrisma = basePrisma
  .$extends(myCustomExtension)
  .$extends(anotherExtension)

export const prisma = extendedPrisma.$extends(
  speedExtension({ postgres: sql }),
) as SpeedClient<typeof extendedPrisma>
```

## Supported queries

### Accelerated

- `findMany`
- `findFirst`
- `findUnique`
- `count`
- `aggregate`
- `groupBy`
- `$batch` for PostgreSQL

### Not accelerated

These continue to run through Prisma:

- `create`
- `update`
- `delete`
- `upsert`
- `createMany`
- `updateMany`
- `deleteMany`

### Fallback behavior

If a query shape is unsupported or cannot be accelerated safely, the extension falls back to Prisma instead of returning incorrect results.

Enable `debug: true` to see generated SQL and fallback behavior.

## Features

### 1) Runtime SQL generation

Any supported read query can be converted from Prisma args into SQL at runtime.

```ts
const users = await prisma.user.findMany({
  where: {
    status: 'ACTIVE',
    email: { contains: '@example.com' },
  },
  orderBy: { createdAt: 'desc' },
  take: 20,
})
```

### 2) Prebaked hot queries with `@optimize`

For the hottest query shapes, you can prebake SQL at generate time.

```prisma
/// @optimize {
///   "method": "findMany",
///   "query": {
///     "where": { "status": "ACTIVE" },
///     "orderBy": { "createdAt": "desc" },
///     "skip": "$skip",
///     "take": "$take"
///   }
/// }
model User {
  id        Int      @id @default(autoincrement())
  email     String   @unique
  status    String
  createdAt DateTime @default(now())
}
```

At runtime:

- matching query shape → prebaked SQL
- non-matching query shape → runtime SQL generation

### 3) PostgreSQL batch queries

`$batch` combines multiple independent read queries into one round trip.

```ts
const results = await prisma.$batch((batch) => ({
  users: batch.user.findMany({ where: { status: 'ACTIVE' } }),
  posts: batch.post.count(),
  stats: batch.task.aggregate({ _count: true }),
}))
```

### 4) Include and relation reduction

For supported include trees, `prisma-sql` can execute flat SQL and reduce rows back into Prisma-like nested results.

### 5) Aggregate result type handling

Aggregates are mapped back to Prisma-style value types instead of flattening everything into strings or plain numbers.

That includes preserving types like:

- `Decimal`
- `BigInt`
- `DateTime`
- `_count`

## Query examples

### Filters

```ts
{ age: { gt: 18, lte: 65 } }
{ status: { in: ['ACTIVE', 'PENDING'] } }
{ status: { notIn: ['DELETED'] } }

{ email: { contains: '@example.com' } }
{ email: { startsWith: 'user' } }
{ email: { endsWith: '.com' } }
{ email: { contains: 'EXAMPLE', mode: 'insensitive' } }

{ AND: [{ status: 'ACTIVE' }, { verified: true }] }
{ OR: [{ role: 'ADMIN' }, { role: 'MODERATOR' }] }
{ NOT: { status: 'DELETED' } }

{ deletedAt: null }
{ deletedAt: { not: null } }
```

### Relations

```ts
{
  include: {
    posts: true,
    profile: true,
  }
}
```

```ts
{
  include: {
    posts: {
      where: { published: true },
      orderBy: { createdAt: 'desc' },
      take: 5,
      include: {
        comments: true,
      },
    },
  },
}
```

```ts
{
  where: {
    posts: { some: { published: true } },
  },
}
```

```ts
{
  where: {
    posts: { every: { published: true } },
  },
}
```

```ts
{
  where: {
    posts: { none: { published: false } },
  },
}
```

### Pagination and ordering

```ts
{
  take: 10,
  skip: 20,
  orderBy: { createdAt: 'desc' },
}
```

```ts
{
  cursor: { id: 100 },
  skip: 1,
  take: 10,
  orderBy: { id: 'asc' },
}
```

```ts
{
  orderBy: [
    { status: 'asc' },
    { priority: 'desc' },
    { createdAt: 'desc' },
  ],
}
```

### Composite cursor pagination

For composite cursors, use an `orderBy` that starts with the cursor fields in the same order.

```ts
{
  cursor: { tenantId: 10, id: 500 },
  skip: 1,
  take: 20,
  orderBy: [
    { tenantId: 'asc' },
    { id: 'asc' },
  ],
}
```

This matches keyset pagination expectations and avoids unstable page boundaries.

### Aggregates

```ts
await prisma.user.count({
  where: { status: 'ACTIVE' },
})
```

```ts
await prisma.task.aggregate({
  where: { status: 'DONE' },
  _count: { _all: true },
  _sum: { estimatedHours: true },
  _avg: { estimatedHours: true },
  _min: { startedAt: true },
  _max: { completedAt: true },
})
```

```ts
await prisma.task.groupBy({
  by: ['status', 'priority'],
  _count: { _all: true },
  _avg: { estimatedHours: true },
  having: {
    status: {
      _count: { gte: 5 },
    },
  },
})
```

## Cardinality planner

The cardinality planner is the piece that decides how relation-heavy reads should be executed for best performance.

In practice, it helps choose between strategies such as:

- direct joins
- lateral/subquery-style fetches
- flat row expansion + reducer
- segmented follow-up loading for high fan-out relations

This matters because the fastest strategy depends on **cardinality**, not just query shape.

A `profile` include behaves very differently from a `posts.comments.likes` include.

### Why it matters

A naive join strategy can explode row counts:

- `User -> Profile` is usually low fan-out
- `User -> Posts -> Comments` can multiply rows aggressively
- `Organization -> Users -> Sessions -> Events` can become huge very quickly

The planner tries to keep read amplification under control.

### Best setup for the planner

To get the best results, prepare your schema and indexes so the planner can make good choices.

#### 1) Model real cardinality accurately

Use correct relation fields and uniqueness constraints.

Good examples:

```prisma
model User {
  id      Int      @id @default(autoincrement())
  profile Profile?
  posts   Post[]
}

model Profile {
  id      Int  @id @default(autoincrement())
  userId  Int  @unique
  user    User @relation(fields: [userId], references: [id])
}

model Post {
  id       Int  @id @default(autoincrement())
  authorId Int
  author   User @relation(fields: [authorId], references: [id])

  @@index([authorId])
}
```

Why this helps:

- `@unique` on one-to-one foreign keys tells the planner the relation is bounded
- indexes on one-to-many foreign keys make follow-up or segmented loading cheap

#### 2) Index every foreign key used in includes and relation filters

At minimum, index:

- all `@relation(fields: [...])` foreign keys on the child side
- fields used in nested `where`
- fields used in nested `orderBy`
- fields used in cursor pagination

Example:

```prisma
model Comment {
  id        Int      @id @default(autoincrement())
  postId    Int
  createdAt DateTime @default(now())
  published Boolean  @default(false)

  post Post @relation(fields: [postId], references: [id])

  @@index([postId])
  @@index([postId, createdAt])
  @@index([postId, published])
}
```

#### 3) Prefer deterministic nested ordering

When including collections, always provide a stable order when practical.

```ts
const users = await prisma.user.findMany({
  include: {
    posts: {
      orderBy: { createdAt: 'desc' },
      take: 5,
    },
  },
})
```

That helps both the planner and the reducer keep result shapes predictable.

### What to configure

Use the cardinality planner wherever your generator/runtime exposes it.

Because config names can differ between versions, the safe rule is:

- enable the planner in generator/runtime config if your build exposes that switch
- keep it on for relation-heavy workloads
- tune any thresholds only after measuring with real production-shaped queries

If your project has planner thresholds, start conservatively:

- prefer bounded strategies for one-to-one and unique includes
- prefer segmented or reduced strategies for one-to-many and many-to-many
- lower thresholds for deep includes with large child tables
- raise thresholds only after verifying lower fan-out in production data

### How to verify the planner is helping

Use `debug` and `onQuery`.

Look for:

- large latency spikes on include-heavy queries
- unusually large result sets for a small parent page
- repeated slow nested includes on high-fanout relations

```ts
const prisma = basePrisma.$extends(
  speedExtension({
    postgres: sql,
    debug: true,
    onQuery: (info) => {
      console.log(`${info.model}.${info.method} ${info.duration}ms`)
      console.log(info.sql)
    },
  }),
) as SpeedClient<typeof basePrisma>
```

What good results look like:

- small parent page stays small in latency
- bounded child includes remain predictable
- high-fanout includes stop exploding row counts
- moving a heavy include into `$batch` or splitting it improves latency materially

## Deployment without database access at build time

The cardinality planner collects relation statistics and roundtrip cost measurements directly from the database during `prisma generate`. In CI/CD pipelines or containerized builds, the database is often unreachable.

### Skip planner during generation

Set `PRISMA_SQL_SKIP_PLANNER=true` to skip stats collection at generate time. The generator will emit default planner values instead.

```bash
PRISMA_SQL_SKIP_PLANNER=true npx prisma generate
```

### Collect stats before server start

Run `prisma-sql-collect-stats` as a pre-start step, after deployment, when the database is reachable.

```bash
prisma-sql-collect-stats \
  --output dist/prisma/generated/sql/planner.generated.js \
  --prisma-client dist/prisma/generated/client/index.js
```

| Flag              | Default                                            | Description                                                    |
| ----------------- | -------------------------------------------------- | -------------------------------------------------------------- |
| `--output`        | `./dist/prisma/generated/sql/planner.generated.js` | Path to the generated planner module                           |
| `--prisma-client` | `@prisma/client`                                   | Path to the compiled Prisma client (must expose `Prisma.dmmf`) |

The script reads `DATABASE_URL` from the environment (supports `.env` via `dotenv`). If the connection fails or times out, it exits silently without blocking startup.

### Example scripts

```json
{
  "prisma:generate": "PRISMA_SQL_SKIP_PLANNER=true prisma generate",
  "collect-planner-stats": "prisma-sql-collect-stats --output dist/prisma/generated/sql/planner.generated.js --prisma-client dist/prisma/generated/client/index.js",
  "start:production": "yarn collect-planner-stats; node dist/src/index.js"
}
```

The semicolon (`;`) after `collect-planner-stats` ensures the server starts even if stats collection fails. Use `&&` instead if you want startup to abort on failure.

### What happens with default planner values

When stats are not collected, the planner uses conservative defaults:

- `roundtripRowEquivalent`: 73
- `jsonRowFactor`: 1.5
- `relationStats`: empty (all relations treated as unknown cardinality)

This means the planner cannot make informed decisions about join strategies. Queries still work correctly — the planner falls back to safe general-purpose strategies — but relation-heavy reads may not use the optimal execution plan.

### Timeout control

Stats collection has a default timeout of 15 seconds. Override with:

```bash
PRISMA_SQL_PLANNER_TIMEOUT_MS=5000 yarn collect-planner-stats
```

### Practical recommendations

For best results with the planner:

1. index all relation keys
2. encode one-to-one relations with `@unique`
3. use stable `orderBy`
4. cap nested collections with `take`
5. page parents before including deep trees
6. split unrelated heavy branches into `$batch`
7. benchmark with real data distributions, not toy fixtures

## Batch queries

`$batch` runs multiple independent read queries in one PostgreSQL round trip.

```ts
const dashboard = await prisma.$batch((batch) => ({
  totalUsers: batch.user.count(),
  activeUsers: batch.user.count({
    where: { status: 'ACTIVE' },
  }),
  recentProjects: batch.project.findMany({
    take: 5,
    orderBy: { createdAt: 'desc' },
    include: { organization: true },
  }),
  taskStats: batch.task.aggregate({
    _count: true,
    _avg: { estimatedHours: true },
    where: { status: 'IN_PROGRESS' },
  }),
}))
```

### Rules

Do not `await` inside the callback.

Incorrect:

```ts
await prisma.$batch(async (batch) => ({
  users: await batch.user.findMany(),
}))
```

Correct:

```ts
await prisma.$batch((batch) => ({
  users: batch.user.findMany(),
}))
```

### Best use cases

- dashboards
- analytics summaries
- counts + page data
- multiple independent aggregates
- splitting unrelated heavy reads instead of building one massive include tree

### Limitations

- PostgreSQL only
- queries are independent
- not transactional
- use `$transaction` when you need transactional guarantees

## Configuration

### Debug logging

```ts
const prisma = basePrisma.$extends(
  speedExtension({
    postgres: sql,
    debug: true,
  }),
) as SpeedClient<typeof basePrisma>
```

### Performance hook

```ts
const prisma = basePrisma.$extends(
  speedExtension({
    postgres: sql,
    onQuery: (info) => {
      console.log(`${info.model}.${info.method}: ${info.duration}ms`)
      console.log(`prebaked=${info.prebaked}`)
    },
  }),
) as SpeedClient<typeof basePrisma>
```

The callback receives:

```ts
interface QueryInfo {
  model: string
  method: string
  sql: string
  params: unknown[]
  duration: number
  prebaked: boolean
}
```

## Generator configuration

```prisma
generator sql {
  provider = "prisma-sql-generator"

  // optional
  // dialect = "postgres"

  // optional
  // output = "./generated/sql"

  // optional
  // skipInvalid = "true"
}
```

## `@optimize` examples

### Basic prebaked query

```prisma
/// @optimize {
///   "method": "findMany",
///   "query": {
///     "where": { "status": "ACTIVE" }
///   }
/// }
model User {
  id     Int    @id
  status String
}
```

### Dynamic parameters

```prisma
/// @optimize {
///   "method": "findMany",
///   "query": {
///     "where": { "status": "$status" },
///     "skip": "$skip",
///     "take": "$take"
///   }
/// }
model User {
  id     Int    @id
  status String
}
```

### Nested include

```prisma
/// @optimize {
///   "method": "findMany",
///   "query": {
///     "include": {
///       "posts": {
///         "where": { "published": true },
///         "orderBy": { "createdAt": "desc" },
///         "take": 5
///       }
///     }
///   }
/// }
model User {
  id    Int    @id
  posts Post[]
}
```

## Edge usage

### Vercel Edge

```ts
import { PrismaClient } from '@prisma/client'
import { speedExtension, type SpeedClient } from './generated/sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL!)
const prisma = new PrismaClient().$extends(
  speedExtension({ postgres: sql }),
) as SpeedClient<typeof PrismaClient>

export const config = { runtime: 'edge' }

export default async function handler() {
  const users = await prisma.user.findMany()
  return Response.json(users)
}
```

### Cloudflare Workers

Use the standalone SQL generation API.

```ts
import { createToSQL } from 'prisma-sql'
import { MODELS } from './generated/sql'

const toSQL = createToSQL(MODELS, 'sqlite')

export default {
  async fetch(request: Request, env: Env) {
    const { sql, params } = toSQL('User', 'findMany', {
      where: { status: 'ACTIVE' },
    })

    const result = await env.DB.prepare(sql)
      .bind(...params)
      .all()

    return Response.json(result.results)
  },
}
```

## Performance

Performance depends on:

- database type
- query shape
- indexing
- relation fan-out
- whether the query is prebaked
- whether the cardinality planner can choose a bounded strategy

Typical gains are strongest when:

- Prisma overhead dominates total time
- includes are moderate but structured well
- query shapes repeat
- indexes exist on relation and filter columns

Run your own benchmarks on production-shaped data.

## Troubleshooting

### `speedExtension requires postgres or sqlite client`

Pass a database-native client to the generated extension.

```ts
const prisma = new PrismaClient().$extends(speedExtension({ postgres: sql }))
```

### Generated dialect mismatch

If generated code targets PostgreSQL, do not pass SQLite, and vice versa.

Override dialect in the generator if needed.

```prisma
generator sql {
  provider = "prisma-sql-generator"
  dialect  = "postgres"
}
```

### Results differ from Prisma

Turn on debug logging and compare generated SQL with Prisma query logs.

```ts
const prisma = new PrismaClient().$extends(
  speedExtension({
    postgres: sql,
    debug: true,
  }),
)
```

If behavior differs, open an issue with:

- schema excerpt
- Prisma query
- generated SQL
- expected result
- actual result

### Performance is worse on a relation-heavy query

Check these first:

- missing foreign-key indexes
- deep unbounded includes
- no nested `take`
- unstable or missing `orderBy`
- high-fanout relation trees that should be split into `$batch`

### Connection pool exhaustion

Increase `postgres.js` pool size if needed.

```ts
const sql = postgres(process.env.DATABASE_URL!, {
  max: 50,
})
```

## Limitations

### Partially supported

- basic array operators
- basic JSON path filtering

### Not yet supported

These should fall back to Prisma:

- full-text `search`
- composite/document-style embedded types
- vendor-specific extensions not yet modeled by the SQL builder
- some advanced `groupBy` edge cases

## FAQ

**Do I still need Prisma?**  
Yes. Prisma remains the source of truth for schema, migrations, types, writes, and fallback behavior.

**Does this replace Prisma Client?**  
No. It extends Prisma Client.

**What gets accelerated?**  
Supported read queries only.

**What about writes?**  
Writes continue through Prisma.

**Do I need `@optimize`?**  
No. It is optional. It only reduces the overhead of repeated hot query shapes.

**Does `$batch` work with SQLite?**  
Not currently.

**Is it safe to use in production?**  
Use it the same way you would adopt any query-path optimization layer: benchmark it on real data, compare against Prisma for parity, and keep Prisma fallback enabled for unsupported cases.

## Migration

### Before

```ts
const prisma = new PrismaClient()
const users = await prisma.user.findMany()
```

### After

```ts
import { PrismaClient } from '@prisma/client'
import { speedExtension, type SpeedClient } from './generated/sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL!)
const basePrisma = new PrismaClient()

export const prisma = basePrisma.$extends(
  speedExtension({ postgres: sql }),
) as SpeedClient<typeof basePrisma>

const users = await prisma.user.findMany()
```

## Examples

- `examples/generator-mode`
- `tests/e2e/postgres.test.ts`
- `tests/e2e/sqlite.e2e.test.ts`
- `tests/sql-injection/batch-transaction.test.ts`

## Development

```bash
git clone https://github.com/multipliedtwice/prisma-to-sql
cd prisma-sql
npm install
npm run build
npm test
```

## License

MIT

## Links

- [NPM Package](https://www.npmjs.com/package/prisma-sql)
- [GitHub Repository](https://github.com/multipliedtwice/prisma-to-sql)
- [Issue Tracker](https://github.com/multipliedtwice/prisma-to-sql/issues)
