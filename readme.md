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

It keeps Prisma Client for:

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
  id       Int    @id @default(autoincrement())
  title    String
  authorId Int
  author   User   @relation(fields: [authorId], references: [id])
}
```

### 2) Generate

```bash
npx prisma generate
```

This generates:

```txt
./generated/sql/index.ts
./generated/sql/planner.generated.ts
```

`planner.generated.ts` contains relation and model statistics used by the strategy planner.

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

- matching query shape: prebaked SQL
- non-matching query shape: runtime SQL generation

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

For supported include trees, `prisma-sql` can execute optimized SQL and reduce rows back into Prisma-like nested results.

It supports strategy switching between:

- flat joins
- where-in segmented loading
- correlated subqueries for small/bounded cases

### 5) Large to-many include protection

`prisma-sql` uses generated `MODEL_STATS` to avoid expensive correlated per-parent scans over large child tables.

When a query includes a large to-many relation and the parent result is estimated to be small, the planner switches to batched where-in loading:

```ts
const companies = await prisma.company.findMany({
  include: {
    jobAds: true,
  },
})
```

Conceptually, this becomes:

```sql
SELECT * FROM company;
SELECT * FROM jobAd WHERE companyId IN (...);
```

Results are stitched in memory to preserve Prisma-style include semantics. This applies to both bounded and unbounded includes.

Composite foreign keys are supported through row-value tuple `IN`:

```sql
WHERE (tenantId, companyId) IN (($1, $2), ($3, $4))
```

SQLite requires version 3.15 or newer for row-value tuple `IN`.

### 6) Aggregate result type handling

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
  },
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

## Configuration

`prisma-sql` exposes internal limit and strategy constants so you can tune the query builder for your schema without forking the library.

### Extension config

Pass `limits` and/or `strategy` directly to `speedExtension`:

```ts
import { speedExtension } from './generated/sql'

const db = prisma.$extends(
  speedExtension({
    postgres: sql,
    limits: {
      MAX_INCLUDES_PER_LEVEL: 20,
      MAX_INCLUDE_DEPTH: 8,
    },
    strategy: {
      defaultFanOut: 5,
      roundtripRowEquivalent: 40,
    },
  }),
)
```

Both fields accept partial objects. Only the keys you provide are overwritten.

### Programmatic API

If you use `prisma-sql` as a library without the generated extension, the same knobs are available as standalone functions:

```ts
import {
  setLimits,
  getLimits,
  resetLimits,
  setStrategyConfig,
  getStrategyConfig,
  rebuildQueryCache,
} from 'prisma-sql'

setLimits({ MAX_INCLUDES_PER_LEVEL: 25 })
rebuildQueryCache()

setStrategyConfig({ correlatedBoundedFactor: 0.3 })

console.log(getLimits())
console.log(getStrategyConfig())

resetLimits()
```

`rebuildQueryCache()` must be called after changing `QUERY_CACHE_SIZE`. All other limit changes take effect on the next query.

### Query builder limits

These control safety boundaries and complexity caps for generated SQL.

| Key                            |      Default | Description                                                                                      |
| ------------------------------ | -----------: | ------------------------------------------------------------------------------------------------ |
| `MAX_INCLUDE_DEPTH`            |          `5` | Max depth of nested `include` / `select` relations.                                              |
| `MAX_INCLUDES_PER_LEVEL`       |         `10` | Max number of relations included at a single nesting level.                                      |
| `MAX_TOTAL_SUBQUERIES`         |        `100` | Total correlated subqueries allowed across the query tree.                                       |
| `MAX_SELF_REFERENTIAL_DEPTH`   |          `2` | Max times a model can appear in its own include chain.                                           |
| `MAX_QUERY_DEPTH`              |         `50` | Max nesting depth for `WHERE` clauses.                                                           |
| `MAX_NOT_DEPTH`                |         `50` | Max nesting depth for `NOT` operator composition.                                                |
| `MAX_HAVING_DEPTH`             |         `50` | Max nesting depth for `HAVING` clauses.                                                          |
| `MAX_NESTED_JOIN_DEPTH`        |         `10` | Max depth for join-based relation traversal.                                                     |
| `MAX_RELATION_ORDER_BY_DEPTH`  |         `10` | Max depth for relation-based `orderBy` resolution.                                               |
| `JOIN_INCLUDE_MAX_DEPTH`       |          `0` | Deprecated no-op retained for compatibility. Strategy selection no longer depends on this value. |
| `MAX_WHERE_IN_RECURSIVE_DEPTH` |         `10` | Max recursion depth for where-in segment resolution.                                             |
| `MAX_ARRAY_SIZE`               |      `10000` | Max elements in array params such as `in` and `hasSome`.                                         |
| `MAX_STRING_LENGTH`            |      `10000` | Max string length for `LIKE` and JSON string operators.                                          |
| `MAX_LIMIT_OFFSET`             | `2147483647` | PostgreSQL engine limit for `LIMIT` / `OFFSET`.                                                  |
| `MIN_NEGATIVE_TAKE`            |     `-10000` | Minimum allowed negative `take` value.                                                           |
| `MAX_ALIAS_COUNTER_THRESHOLD`  |       `1000` | Safety threshold before alias counter overflow.                                                  |
| `QUERY_CACHE_SIZE`             |       `1000` | Max entries in the SQL query cache. Call `rebuildQueryCache()` after changing.                   |
| `STMT_CACHE_SIZE`              |       `1000` | Max entries in the SQLite prepared statement cache per client.                                   |

Example for a wide schema:

```ts
speedExtension({
  postgres: sql,
  limits: {
    MAX_INCLUDES_PER_LEVEL: 20,
    MAX_TOTAL_SUBQUERIES: 200,
  },
})
```

Example for a deeply nested tree structure:

```ts
speedExtension({
  postgres: sql,
  limits: {
    MAX_INCLUDE_DEPTH: 8,
    MAX_SELF_REFERENTIAL_DEPTH: 5,
  },
})
```

### Strategy cost-model parameters

These control how `prisma-sql` chooses between query strategies.

| Key                            |  Default | Description                                                                                                             |
| ------------------------------ | -------: | ----------------------------------------------------------------------------------------------------------------------- |
| `roundtripRowEquivalent`       |     `73` | Cost, in row-equivalents, of one extra database roundtrip. Lower values favor multi-roundtrip strategies like where-in. |
| `jsonRowFactor`                |    `1.5` | Multiplier for JSON aggregation overhead per row.                                                                       |
| `correlatedBoundedFactor`      |    `0.5` | Cost factor for correlated subqueries when the child has `LIMIT`.                                                       |
| `correlatedUnboundedFactor`    |    `3.0` | Cost factor for correlated subqueries when the child is unbounded.                                                      |
| `correlatedWherePenalty`       |    `3.0` | Extra cost multiplier when a child relation has `where` inside a correlated subquery.                                   |
| `defaultFanOut`                |     `10` | Assumed average children per parent when relation stats are unavailable.                                                |
| `defaultParentCount`           |     `50` | Assumed parent row count when `take` is not specified on the root query.                                                |
| `dynamicTakeEstimate`          |     `10` | Assumed `take` value when the actual value is a runtime dynamic parameter.                                              |
| `singleParentMaxFlatJoinDepth` |      `2` | Max include depth that allows flat-join strategy for `findFirst` / `findUnique`.                                        |
| `minStatsCoverage`             |    `0.1` | Minimum relation stats coverage to trust collected cardinality data.                                                    |
| `largeChildTableRows`          | `100000` | Child table row threshold for the large to-many include guard.                                                          |
| `smallParentCountThreshold`    |   `1000` | Parent result estimate threshold for the large to-many include guard.                                                   |

Example for a low-latency local database:

```ts
speedExtension({
  postgres: sql,
  strategy: {
    roundtripRowEquivalent: 20,
  },
})
```

Example for a remote database with high latency:

```ts
speedExtension({
  postgres: sql,
  strategy: {
    roundtripRowEquivalent: 200,
  },
})
```

Example for projects where large child tables are well indexed and correlated scans are still cheap:

```ts
speedExtension({
  postgres: sql,
  strategy: {
    largeChildTableRows: 500000,
  },
})
```

## Pagination and ordering

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

## Aggregates

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

The cardinality planner decides how relation-heavy reads should be executed.

It helps choose between:

- flat joins
- where-in segmented loading
- correlated subqueries for small/bounded cases

The fastest strategy depends on cardinality, not just query shape.

A `profile` include behaves very differently from a `posts.comments.likes` include.

### Planner stats

During generation, `prisma-sql` can collect planner artifacts into `planner.generated.ts`.

Those artifacts include:

- `RELATION_STATS`
- `MODEL_STATS`
- `ROUNDTRIP_ROW_EQUIVALENT`
- `JSON_ROW_FACTOR`

`MODEL_STATS` is used by the large to-many include guard. When a child table is large and the parent result is small, `prisma-sql` prefers where-in segmented loading instead of correlated per-parent subqueries.

If `MODEL_STATS` is missing or a model has `known: false`, the large-table guard is inactive for that model. Queries still work, but relation-heavy reads may not use the optimal strategy until stats are collected.

### Why it matters

A naive join strategy can explode row counts:

- `User -> Profile` is usually low fan-out
- `User -> Posts -> Comments` can multiply rows aggressively
- `Organization -> Users -> Sessions -> Events` can become huge quickly

The planner tries to keep read amplification under control while preserving Prisma-compatible result shapes.

### Strategy behavior

At a high level:

- low-depth includes may use flat joins
- small bounded child includes may use correlated subqueries
- large to-many includes use where-in segmented loading
- composite foreign keys use tuple-IN in the where-in path

This means unbounded nested includes remain valid Prisma-style queries. You do not need to cap a nested include for correctness. Capping with `take` can still be useful for product behavior and latency, but it is not required to make the query valid.

### Best setup for the planner

#### 1) Model real cardinality accurately

Use correct relation fields and uniqueness constraints.

```prisma
model User {
  id      Int      @id @default(autoincrement())
  profile Profile?
  posts   Post[]
}

model Profile {
  id     Int  @id @default(autoincrement())
  userId Int  @unique
  user   User @relation(fields: [userId], references: [id])
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
- indexes on one-to-many foreign keys make segmented loading cheap

#### 2) Index every foreign key used in includes and relation filters

At minimum, index:

- all `@relation(fields: [...])` foreign keys on the child side
- fields used in nested `where`
- fields used in nested `orderBy`
- fields used in cursor pagination

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

When including collections, provide a stable order when practical.

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

### How to verify the planner is helping

Use `debug` and `onQuery`.

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

Look for:

- large latency spikes on include-heavy queries
- unusually large result sets for a small parent page
- repeated slow nested includes on high-fanout relations

Good results usually look like:

- small parent page stays small in latency
- bounded child includes remain predictable
- high-fanout includes avoid correlated per-parent scans
- unrelated heavy branches improve when split into `$batch`

## Deployment without database access at build time

The cardinality planner can collect relation statistics and roundtrip cost measurements from the database during `prisma generate`.

In CI/CD pipelines or containerized builds, the database is often unreachable. In that case, you can skip planner collection during build and collect stats at runtime or in a deployment job.

### Skip planner during generation

Set `PRISMA_SQL_SKIP_PLANNER=true` to skip stats collection at generate time.

```bash
PRISMA_SQL_SKIP_PLANNER=true npx prisma generate
```

The generator will emit default planner values.

### Collect stats at runtime

Run `prisma-sql-collect-stats` as a pre-start step or background job after deployment, when the database is reachable.

```bash
prisma-sql-collect-stats \
  --output dist/prisma/generated/sql/planner.generated.js \
  --prisma-client dist/prisma/generated/client/index.js
```

| Flag              | Default                                            | Description                                                       |
| ----------------- | -------------------------------------------------- | ----------------------------------------------------------------- |
| `--output`        | `./dist/prisma/generated/sql/planner.generated.js` | Path to the generated planner module.                             |
| `--prisma-client` | `@prisma/client`                                   | Path to the compiled Prisma client. It must expose `Prisma.dmmf`. |

The script reads `DATABASE_URL` from the environment and supports `.env` via `dotenv`. If the connection fails or times out, it exits without blocking startup.

### Load stats at runtime

Use `loadExternalPlannerStats` to load planner stats from an external file at runtime.

```ts
import { loadExternalPlannerStats } from 'prisma-sql'

const loaded = loadExternalPlannerStats(
  '/data/planner-stats/planner.generated.js',
)

if (loaded) {
  console.log('Planner stats loaded from volume')
}
```

This applies `RELATION_STATS`, `MODEL_STATS`, `ROUNDTRIP_ROW_EQUIVALENT`, and `JSON_ROW_FACTOR` to the global strategy estimator, overriding whatever was baked into the generated code.

Returns `true` on success and `false` if the file does not exist or cannot be parsed.

### Incremental collection

The collector supports incremental mode. When an output file already exists, it reads previous results and can skip work that does not need repeating.

Freshness check:

- if the existing file was written less than `PRISMA_SQL_STATS_MAX_AGE_MS` ago, the collector exits immediately
- default freshness window is 24 hours

Fast mode:

- default mode
- uses PostgreSQL catalog statistics
- usually completes much faster than full relation scans
- falls back to precise mode if catalog stats appear stale

Precise mode:

- runs full relation cardinality queries
- can be slower on large schemas
- useful when catalog stats are stale or missing

Slow edge skip:

- each relation edge's collection time is recorded
- edges slower than `PRISMA_SQL_SLOW_EDGE_MS` can reuse previous stats
- skipped slow edges are re-measured after `PRISMA_SQL_STALE_EDGE_HOURS`

Per-edge timeout:

- individual relation edge queries are bounded by `PRISMA_SQL_EDGE_TIMEOUT_MS`
- on timeout, the collector falls back to previous stats or conservative defaults

### Running `ANALYZE`

By default, planner collection reads existing database statistics and does not run `ANALYZE`.

Set `PRISMA_SQL_ANALYZE=1` to run `ANALYZE` during planner collection.

```bash
PRISMA_SQL_ANALYZE=1 npx prisma generate
```

Use this when you want PostgreSQL catalog stats to be refreshed as part of collection. Avoid it in environments where generation should not mutate database planner statistics.

### Environment variables

| Variable                        | Default    | Description                                                                  |
| ------------------------------- | ---------- | ---------------------------------------------------------------------------- |
| `PRISMA_SQL_SKIP_PLANNER`       | `false`    | Skip stats collection during `prisma generate`.                              |
| `PRISMA_SQL_ANALYZE`            | unset      | Set to `1` to run `ANALYZE` during planner stats collection.                 |
| `PRISMA_SQL_STATS_MAX_AGE_MS`   | `86400000` | Skip collection if existing stats are younger than this age.                 |
| `PRISMA_SQL_STATS_MODE`         | `fast`     | `fast` uses catalog stats; `precise` uses full relation cardinality queries. |
| `PRISMA_SQL_SLOW_EDGE_MS`       | `10000`    | Reuse cached stats for edges slower than this threshold.                     |
| `PRISMA_SQL_EDGE_TIMEOUT_MS`    | `30000`    | Timeout for an individual relation edge query.                               |
| `PRISMA_SQL_STALE_EDGE_HOURS`   | `168`      | Re-measure slow edges after this age.                                        |
| `PRISMA_SQL_PLANNER_TIMEOUT_MS` | `15000`    | Total timeout for planner stats collection during `prisma generate`.         |

### Example: background collection with persistent storage

For containerized deployments where the generated SQL directory should remain immutable from the image, write planner stats to a separate persistent path and load them at startup.

```ts
import { PrismaClient } from '@prisma/client'
import {
  speedExtension,
  loadExternalPlannerStats,
  type SpeedClient,
} from './generated/sql'
import postgres from 'postgres'
import { execFile } from 'child_process'

const PLANNER_PATH = '/data/planner-stats/planner.generated.js'

loadExternalPlannerStats(PLANNER_PATH)

const sql = postgres(process.env.DATABASE_URL!)
const basePrisma = new PrismaClient()

export const prisma = basePrisma.$extends(
  speedExtension({ postgres: sql }),
) as SpeedClient<typeof basePrisma>

execFile(
  'node',
  [
    'node_modules/.bin/prisma-sql-collect-stats',
    '--output',
    PLANNER_PATH,
    '--prisma-client',
    'dist/prisma/generated/client/index.js',
  ],
  (err) => {
    if (err) {
      console.error('Stats collection failed:', err.message)
      return
    }

    loadExternalPlannerStats(PLANNER_PATH)
    console.log('Planner stats refreshed')
  },
)
```

With Docker Compose, mount only the stats directory:

```yaml
services:
  app:
    image: myapp:latest
    volumes:
      - planner-stats:/data/planner-stats

volumes:
  planner-stats:
```

Do not mount the generated SQL output directory. That directory contains the generated query client and should stay immutable from the image. Mounting it as a volume can cause new deploys to run stale generated code from the first deployment.

### Example scripts

```json
{
  "prisma:generate": "PRISMA_SQL_SKIP_PLANNER=true prisma generate",
  "collect-planner-stats": "prisma-sql-collect-stats --output dist/prisma/generated/sql/planner.generated.js --prisma-client dist/prisma/generated/client/index.js",
  "start:production": "yarn collect-planner-stats; node dist/src/index.js"
}
```

The semicolon after `collect-planner-stats` ensures the server starts even if stats collection fails. Use `&&` if startup should abort on stats collection failure.

### What happens with default planner values

When stats are not collected, the planner uses conservative defaults:

- `roundtripRowEquivalent`: `73`
- `jsonRowFactor`: `1.5`
- `relationStats`: empty
- `modelStats`: empty

Queries still work correctly. However, without `MODEL_STATS`, the large to-many include guard cannot detect large child tables, so relation-heavy reads may not use the optimal execution plan.

### Practical recommendations

For best results with the planner:

1. index all relation keys
2. encode one-to-one relations with `@unique`
3. use stable `orderBy`
4. benchmark with real data distributions
5. use `take` when the product behavior is naturally bounded
6. page parents before including deep trees
7. split unrelated heavy branches into `$batch`

Unbounded nested includes are supported. Use bounds because they match product behavior, not because they are required for correctness.

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

Use `$transaction` when you need transactional guarantees.

## Debugging and observability

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
  dialect  = "postgres"
  output   = "./generated/sql"
  skipInvalid = "true"
}
```

Supported generator fields:

| Field         | Default                | Description                                                        |
| ------------- | ---------------------- | ------------------------------------------------------------------ |
| `provider`    | required               | Use `prisma-sql-generator`.                                        |
| `dialect`     | inferred when possible | `postgres` or `sqlite`.                                            |
| `output`      | `./generated/sql`      | Generated SQL client output path.                                  |
| `skipInvalid` | `false`                | Skip invalid `@optimize` directives instead of failing generation. |

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
const basePrisma = new PrismaClient()

const prisma = basePrisma.$extends(
  speedExtension({ postgres: sql }),
) as SpeedClient<typeof basePrisma>

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
- whether planner stats are available
- whether the planner can choose a bounded or segmented strategy

Typical gains are strongest when:

- Prisma overhead dominates total time
- query shapes repeat
- indexes exist on relation and filter columns
- include trees are relation-heavy but indexed
- large to-many includes can use where-in segmented loading

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
- missing `MODEL_STATS`
- stale PostgreSQL catalog stats
- high-fanout relation trees
- unstable or missing `orderBy`
- unrelated heavy branches that should be split into `$batch`

You can refresh PostgreSQL planner stats during collection with:

```bash
PRISMA_SQL_ANALYZE=1 npx prisma generate
```

### Large include still uses the wrong strategy

Check:

- `planner.generated.ts` contains non-empty `MODEL_STATS`
- the child model's stats have `known: true`
- `largeChildTableRows` is not set too high
- `smallParentCountThreshold` is not set too low
- debug logs show where-in segmented loading for the relation

### SQLite tuple-IN error

Composite foreign-key where-in requires SQLite 3.15 or newer. Upgrade SQLite if row-value tuple `IN` fails.

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
- composite foreign-key includes through tuple-IN in the where-in path

### Not yet supported

These should fall back to Prisma:

- writes
- full-text `search`
- composite/document-style embedded types
- vendor-specific extensions not yet modeled by the SQL builder
- some advanced `groupBy` edge cases

### Streaming limitations

`findManyStream` cannot stream queries that require segmented where-in include loading. Use regular `findMany` for those queries, or select scalar columns only.

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

**Are unbounded nested includes supported?**
Yes. `prisma-sql` preserves Prisma-style include semantics. For large to-many branches, the planner can switch to where-in segmented loading and stitch results in memory.

**Do I need planner stats?**
Queries work without planner stats, but relation-heavy queries may not choose the best strategy. Collect planner stats for best performance on real production-shaped data.

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
