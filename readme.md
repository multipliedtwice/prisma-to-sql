# prisma-sql

Speed up Prisma reads **2-7x** by executing queries via postgres.js instead of Prisma's query engine.

```typescript
const sql = postgres(DATABASE_URL)
const prisma = new PrismaClient().$extends(speedExtension({ postgres: sql }))

// Same Prisma API, 2-7x faster reads
const users = await prisma.user.findMany({ where: { status: 'ACTIVE' } })
```

## Why?

Prisma's query engine adds overhead even in v7:

- Query translation and validation layer
- Type checking and transformation
- Query planning and optimization
- Result serialization and mapping

This extension bypasses the engine for read queries and executes raw SQL directly via postgres.js or better-sqlite3.

**Result:** Same API, same types, 2-7x faster reads.

## Installation

**PostgreSQL:**

```bash
npm install prisma-sql postgres
```

**SQLite:**

```bash
npm install prisma-sql better-sqlite3
```

## Quick Start

### PostgreSQL

```typescript
import { PrismaClient } from '@prisma/client'
import { speedExtension } from 'prisma-sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL)
const prisma = new PrismaClient().$extends(speedExtension({ postgres: sql }))

// All reads now execute via postgres.js
const users = await prisma.user.findMany({
  where: { status: 'ACTIVE' },
  include: { posts: true },
})
```

### SQLite

```typescript
import { PrismaClient } from '@prisma/client'
import { speedExtension } from 'prisma-sql'
import Database from 'better-sqlite3'

const db = new Database('./data.db')
const prisma = new PrismaClient().$extends(speedExtension({ sqlite: db }))

const users = await prisma.user.findMany({ where: { status: 'ACTIVE' } })
```

### Explicit DMMF (Edge Runtimes)

In some environments (Cloudflare Workers, Vercel Edge, bundlers), Prisma's DMMF may not be auto-detectable. Provide it explicitly:

```typescript
import { PrismaClient, Prisma } from '@prisma/client'
import { speedExtension } from 'prisma-sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL)
const prisma = new PrismaClient().$extends(
  speedExtension({
    postgres: sql,
    dmmf: Prisma.dmmf, // Required in edge runtimes
  }),
)
```

## What Gets Faster

**Accelerated (via raw SQL):**

- `findMany`, `findFirst`, `findUnique`
- `count`
- `aggregate` (\_count, \_sum, \_avg, \_min, \_max)
- `groupBy` with having clauses

**Unchanged (still uses Prisma):**

- `create`, `update`, `delete`, `upsert`
- `createMany`, `updateMany`, `deleteMany`
- Transactions (`$transaction`)
- Middleware

---

## Performance

Benchmarks from 137 E2E tests comparing identical queries against Prisma v6, Prisma v7, and Drizzle:

### BENCHMARK RESULTS - Prisma v6 vs v7 vs Generated SQL

## POSTGRES Results:

| Test                     | Prisma v6 | Prisma v7 | Generated | Drizzle | v6 Speedup | v7 Speedup |
| ------------------------ | --------- | --------- | --------- | ------- | ---------- | ---------- |
| findMany basic           | 0.45ms    | 0.35ms    | 0.18ms    | 0.29ms  | 2.53x      | 1.74x      |
| findMany where =         | 0.40ms    | 0.34ms    | 0.17ms    | 0.24ms  | 2.39x      | 1.55x      |
| findMany where >=        | 15.88ms   | 8.03ms    | 2.86ms    | 6.18ms  | 5.55x      | 2.92x      |
| findMany where IN        | 0.52ms    | 0.52ms    | 0.29ms    | 0.38ms  | 1.79x      | 1.38x      |
| findMany where null      | 0.23ms    | 0.29ms    | 0.10ms    | 0.16ms  | 2.20x      | 2.56x      |
| findMany ILIKE           | 0.24ms    | 0.22ms    | 0.18ms    | 0.17ms  | 1.29x      | 0.99x      |
| findMany AND             | 2.36ms    | 1.20ms    | 0.44ms    | 1.42ms  | 5.41x      | 2.73x      |
| findMany OR              | 13.10ms   | 6.90ms    | 2.37ms    | 5.58ms  | 5.53x      | 2.93x      |
| findMany NOT             | 0.51ms    | 1.14ms    | 0.30ms    | 0.33ms  | 1.73x      | 3.74x      |
| findMany orderBy         | 2.19ms    | 2.31ms    | 0.85ms    | 0.72ms  | 2.58x      | 2.05x      |
| findMany pagination      | 0.23ms    | 0.28ms    | 0.20ms    | 0.19ms  | 1.15x      | 1.39x      |
| findMany select          | 0.23ms    | 0.22ms    | 0.09ms    | 0.13ms  | 2.57x      | 2.21x      |
| findMany relation some   | 0.83ms    | 0.72ms    | 0.41ms    | N/A     | 2.04x      | 1.72x      |
| findMany relation every  | 0.70ms    | 0.79ms    | 0.47ms    | N/A     | 1.50x      | 1.70x      |
| findMany relation none   | 28.35ms   | 14.34ms   | 4.81ms    | N/A     | 5.90x      | 2.75x      |
| findMany nested relation | 0.70ms    | 0.72ms    | 0.71ms    | N/A     | 0.98x      | 1.36x      |
| findMany complex         | 1.18ms    | 1.19ms    | 0.48ms    | 0.64ms  | 2.45x      | 2.81x      |
| findFirst                | 0.22ms    | 0.25ms    | 0.15ms    | 0.20ms  | 1.45x      | 3.05x      |
| findFirst skip           | 0.26ms    | 0.32ms    | 0.15ms    | 0.23ms  | 1.75x      | 3.09x      |
| findUnique id            | 0.20ms    | 0.21ms    | 0.13ms    | 0.13ms  | 1.52x      | 2.53x      |
| findUnique email         | 0.18ms    | 0.19ms    | 0.09ms    | 0.12ms  | 2.03x      | 2.17x      |
| count                    | 0.11ms    | 0.12ms    | 0.04ms    | 0.07ms  | 2.95x      | 2.50x      |
| count where              | 0.43ms    | 0.47ms    | 0.26ms    | 0.27ms  | 1.62x      | 1.90x      |
| aggregate count          | 0.22ms    | 0.24ms    | 0.13ms    | N/A     | 1.63x      | 1.51x      |
| aggregate sum/avg        | 0.30ms    | 0.32ms    | 0.23ms    | N/A     | 1.32x      | 1.35x      |
| aggregate where          | 0.42ms    | 0.44ms    | 0.24ms    | N/A     | 1.75x      | 1.84x      |
| aggregate min/max        | 0.30ms    | 0.32ms    | 0.23ms    | N/A     | 1.29x      | 1.32x      |
| aggregate complete       | 0.36ms    | 0.41ms    | 0.27ms    | N/A     | 1.36x      | 1.39x      |
| groupBy                  | 0.38ms    | 0.41ms    | 0.29ms    | N/A     | 1.33x      | 1.36x      |
| groupBy count            | 0.44ms    | 0.42ms    | 0.32ms    | N/A     | 1.36x      | 1.37x      |
| groupBy multi            | 0.53ms    | 0.51ms    | 0.37ms    | N/A     | 1.43x      | 1.43x      |
| groupBy having           | 0.52ms    | 0.50ms    | 0.40ms    | N/A     | 1.29x      | 1.40x      |
| groupBy + where          | 0.52ms    | 0.49ms    | 0.31ms    | N/A     | 1.65x      | 1.88x      |
| groupBy aggregates       | 0.50ms    | 0.48ms    | 0.38ms    | N/A     | 1.31x      | 1.32x      |
| groupBy min/max          | 0.49ms    | 0.50ms    | 0.38ms    | N/A     | 1.29x      | 1.35x      |
| include posts            | 2.53ms    | 1.59ms    | 1.85ms    | N/A     | 1.37x      | 0.81x      |
| include profile          | 0.47ms    | 0.60ms    | 0.21ms    | N/A     | 2.26x      | 2.89x      |
| include 3 levels         | 1.56ms    | 1.64ms    | 1.15ms    | N/A     | 1.36x      | 1.33x      |
| include 4 levels         | 1.80ms    | 1.76ms    | 0.99ms    | N/A     | 1.81x      | 1.74x      |
| include + where          | 1.21ms    | 1.06ms    | 1.47ms    | N/A     | 0.82x      | 0.69x      |
| include + select nested  | 1.23ms    | 0.84ms    | 1.43ms    | N/A     | 0.86x      | 0.54x      |
| findMany startsWith      | 0.21ms    | 0.24ms    | 0.14ms    | 0.17ms  | 1.46x      | 1.73x      |
| findMany endsWith        | 0.48ms    | 0.38ms    | 0.19ms    | 0.28ms  | 2.51x      | 1.87x      |
| findMany NOT contains    | 0.47ms    | 0.40ms    | 0.18ms    | 0.25ms  | 2.62x      | 2.49x      |
| findMany LIKE            | 0.19ms    | 0.22ms    | 0.09ms    | 0.14ms  | 2.19x      | 2.61x      |
| findMany <               | 25.94ms   | 14.16ms   | 4.16ms    | 9.59ms  | 6.23x      | 3.12x      |
| findMany <=              | 26.79ms   | 13.87ms   | 4.90ms    | 9.73ms  | 5.46x      | 3.07x      |
| findMany >               | 15.22ms   | 7.55ms    | 2.69ms    | 5.74ms  | 5.66x      | 2.71x      |
| findMany NOT IN          | 0.54ms    | 0.42ms    | 0.26ms    | 0.36ms  | 2.07x      | 1.42x      |
| findMany isNot null      | 0.52ms    | 0.39ms    | 0.19ms    | 0.24ms  | 2.75x      | 2.21x      |
| orderBy multi-field      | 3.97ms    | 2.38ms    | 1.09ms    | 1.54ms  | 3.65x      | 3.71x      |
| distinct status          | 8.72ms    | 7.84ms    | 2.15ms    | N/A     | 4.06x      | 4.68x      |
| distinct multi           | 11.71ms   | 11.09ms   | 2.09ms    | N/A     | 5.61x      | 5.30x      |
| cursor pagination        | 0.29ms    | 0.34ms    | 0.23ms    | N/A     | 1.25x      | 1.62x      |
| select + include         | 0.89ms    | 0.70ms    | 0.17ms    | N/A     | 5.19x      | 3.46x      |
| \_count relation         | 0.71ms    | 0.66ms    | 0.55ms    | N/A     | 1.29x      | 1.20x      |
| \_count multi-relation   | 0.24ms    | 0.29ms    | 0.16ms    | N/A     | 1.52x      | 1.90x      |
| ILIKE special chars      | 0.22ms    | 0.26ms    | 0.14ms    | N/A     | 1.54x      | 1.76x      |
| LIKE case sensitive      | 0.19ms    | 0.22ms    | 0.12ms    | N/A     | 1.62x      | 1.85x      |

##### Summary:

- Generated SQL vs Prisma v6: **2.39x faster**
- Generated SQL vs Prisma v7: **2.10x faster**
- Generated SQL vs Drizzle: **1.53x faster**

---

#### SQLITE:

---

| Test                     | Prisma v6 | Prisma v7 | Generated | Drizzle | v6 Speedup | v7 Speedup |
| ------------------------ | --------- | --------- | --------- | ------- | ---------- | ---------- |
| findMany basic           | 0.44ms    | 0.27ms    | 0.04ms    | 0.17ms  | 9.59x      | 5.47x      |
| findMany where =         | 0.45ms    | 0.23ms    | 0.03ms    | 0.10ms  | 14.14x     | 6.25x      |
| findMany where >=        | 12.72ms   | 4.70ms    | 1.02ms    | 2.09ms  | 12.51x     | 4.16x      |
| findMany where IN        | 0.40ms    | 0.28ms    | 0.04ms    | 0.10ms  | 10.35x     | 6.55x      |
| findMany where null      | 0.15ms    | 0.19ms    | 0.01ms    | 0.06ms  | 10.97x     | 12.56x     |
| findMany LIKE            | 0.15ms    | 0.17ms    | 0.02ms    | 0.06ms  | 8.64x      | 9.41x      |
| findMany AND             | 1.49ms    | 0.95ms    | 0.26ms    | 0.43ms  | 5.75x      | 3.45x      |
| findMany OR              | 10.32ms   | 3.87ms    | 0.93ms    | 1.85ms  | 11.09x     | 3.64x      |
| findMany NOT             | 0.42ms    | 0.28ms    | 0.03ms    | 0.09ms  | 12.59x     | 7.05x      |
| findMany orderBy         | 2.24ms    | 1.92ms    | 1.76ms    | 1.81ms  | 1.27x      | 1.11x      |
| findMany pagination      | 0.13ms    | 0.15ms    | 0.02ms    | 0.06ms  | 5.69x      | 6.24x      |
| findMany select          | 0.15ms    | 0.11ms    | 0.02ms    | 0.04ms  | 9.50x      | 6.22x      |
| findMany relation some   | 4.50ms    | 0.56ms    | 0.40ms    | N/A     | 11.15x     | 1.32x      |
| findMany relation every  | 9.53ms    | 9.54ms    | 6.38ms    | N/A     | 1.49x      | 1.45x      |
| findMany relation none   | 166.62ms  | 128.44ms  | 2.40ms    | N/A     | 69.43x     | 49.51x     |
| findMany nested relation | 1.00ms    | 0.51ms    | 0.31ms    | N/A     | 3.28x      | 1.70x      |
| findMany complex         | 0.79ms    | 0.83ms    | 0.43ms    | 0.48ms  | 1.84x      | 1.74x      |
| findFirst                | 0.16ms    | 0.17ms    | 0.01ms    | 0.06ms  | 11.57x     | 12.00x     |
| findFirst skip           | 0.25ms    | 0.23ms    | 0.03ms    | 0.08ms  | 8.62x      | 13.31x     |
| findUnique id            | 0.12ms    | 0.15ms    | 0.01ms    | 0.05ms  | 9.92x      | 11.62x     |
| findUnique email         | 0.12ms    | 0.15ms    | 0.01ms    | 0.05ms  | 8.73x      | 11.43x     |
| count                    | 0.17ms    | 0.07ms    | 0.01ms    | 0.02ms  | 13.33x     | 10.73x     |
| count where              | 0.28ms    | 0.28ms    | 0.16ms    | 0.17ms  | 1.77x      | 1.85x      |
| aggregate count          | 0.15ms    | 0.11ms    | 0.01ms    | N/A     | 14.80x     | 9.69x      |
| aggregate sum/avg        | 0.27ms    | 0.25ms    | 0.15ms    | N/A     | 1.82x      | 1.62x      |
| aggregate where          | 0.25ms    | 0.26ms    | 0.15ms    | N/A     | 1.66x      | 1.73x      |
| aggregate min/max        | 0.28ms    | 0.25ms    | 0.16ms    | N/A     | 1.80x      | 1.52x      |
| aggregate complete       | 0.39ms    | 0.34ms    | 0.21ms    | N/A     | 1.81x      | 1.61x      |
| groupBy                  | 0.56ms    | 0.53ms    | 0.44ms    | N/A     | 1.28x      | 1.22x      |
| groupBy count            | 0.57ms    | 0.57ms    | 0.45ms    | N/A     | 1.28x      | 1.27x      |
| groupBy multi            | 1.14ms    | 1.08ms    | 0.95ms    | N/A     | 1.20x      | 1.17x      |
| groupBy having           | 0.64ms    | 0.64ms    | 0.47ms    | N/A     | 1.37x      | 1.32x      |
| groupBy + where          | 0.31ms    | 0.33ms    | 0.18ms    | N/A     | 1.70x      | 1.84x      |
| groupBy aggregates       | 0.71ms    | 0.66ms    | 0.54ms    | N/A     | 1.32x      | 1.23x      |
| groupBy min/max          | 0.72ms    | 0.70ms    | 0.56ms    | N/A     | 1.29x      | 1.25x      |
| include posts            | 1.88ms    | 1.13ms    | 0.90ms    | N/A     | 2.10x      | 1.12x      |
| include profile          | 0.32ms    | 0.41ms    | 0.05ms    | N/A     | 6.17x      | 6.48x      |
| include 3 levels         | 1.11ms    | 1.08ms    | 0.63ms    | N/A     | 1.77x      | 1.86x      |
| include 4 levels         | 1.15ms    | 1.10ms    | 0.42ms    | N/A     | 2.72x      | 2.70x      |
| include + where          | 0.77ms    | 0.72ms    | 0.11ms    | N/A     | 7.13x      | 6.99x      |
| include + select nested  | 0.73ms    | 0.53ms    | 0.83ms    | N/A     | 0.88x      | 0.64x      |
| findMany startsWith      | 0.15ms    | 0.16ms    | 0.02ms    | 0.06ms  | 6.73x      | 6.98x      |
| findMany endsWith        | 0.43ms    | 0.26ms    | 0.04ms    | 0.15ms  | 9.74x      | 5.22x      |
| findMany NOT contains    | 0.45ms    | 0.28ms    | 0.04ms    | 0.11ms  | 11.65x     | 6.57x      |
| findMany <               | 21.60ms   | 8.27ms    | 1.88ms    | 4.07ms  | 11.49x     | 4.24x      |
| findMany <=              | 22.34ms   | 8.50ms    | 1.97ms    | 4.40ms  | 11.36x     | 4.25x      |
| findMany >               | 11.54ms   | 4.33ms    | 0.94ms    | 2.13ms  | 12.22x     | 4.17x      |
| findMany NOT IN          | 0.42ms    | 0.28ms    | 0.04ms    | 0.12ms  | 10.40x     | 6.23x      |
| findMany isNot null      | 0.45ms    | 0.27ms    | 0.03ms    | 0.11ms  | 13.03x     | 6.91x      |
| orderBy multi-field      | 0.66ms    | 0.59ms    | 0.37ms    | 0.43ms  | 1.78x      | 1.67x      |
| distinct status          | 10.61ms   | 6.79ms    | 4.09ms    | N/A     | 2.59x      | 1.53x      |
| distinct multi           | 11.66ms   | 7.12ms    | 5.09ms    | N/A     | 2.29x      | 1.34x      |
| cursor pagination        | 0.21ms    | 0.26ms    | 0.04ms    | N/A     | 4.60x      | 5.52x      |
| select + include         | 0.51ms    | 0.43ms    | 0.04ms    | N/A     | 13.19x     | 11.31x     |
| \_count relation         | 0.62ms    | 0.46ms    | 0.32ms    | N/A     | 1.93x      | 1.44x      |
| \_count multi-relation   | 0.14ms    | 0.17ms    | 0.04ms    | N/A     | 3.22x      | 4.09x      |

##### Summary:

- Generated SQL vs Prisma v6: **7.51x faster**
- Generated SQL vs Prisma v7: **5.48x faster**
- Generated SQL vs Drizzle: **2.61x faster**

> **Note on Prisma v7:** Prisma v7 introduced significant performance improvements (39% faster than v6 on PostgreSQL, 24% faster on SQLite), but this extension still provides 2-7x additional speedup over v7.

> **Benchmarks:** These are representative results from our test suite running on a MacBook Pro M1 with PostgreSQL 15 and SQLite 3.43. Your mileage may vary based on:
>
> - Database configuration and indexes
> - Query complexity and data volume
> - Hardware and network latency
> - Concurrent load
>
> Run benchmarks with your own schema and data for accurate measurements. See [Benchmarking](#benchmarking) section below.

---

## Configuration

### Basic Configuration

```typescript
import { Prisma } from '@prisma/client'

speedExtension({
  // Database client (required - choose one)
  postgres: sql, // For PostgreSQL via postgres.js
  sqlite: db, // For SQLite via better-sqlite3

  // DMMF (optional - auto-detected in most cases)
  dmmf: Prisma.dmmf, // Required in edge runtimes/bundled apps

  // Debug mode (optional)
  debug: true, // Log all generated SQL

  // Selective models (optional)
  models: ['User', 'Post'], // Only accelerate these models

  // Performance monitoring (optional)
  onQuery: (info) => {
    console.log(`${info.model}.${info.method}: ${info.duration}ms`)
  },
})
```

### Debug Mode

See generated SQL for every query:

```typescript
speedExtension({
  postgres: sql,
  debug: true,
})

// Logs:
// [postgres] User.findMany
// SQL: SELECT ... FROM users WHERE status = $1
// Params: ['ACTIVE']
```

### Selective Models

Only accelerate specific models:

```typescript
speedExtension({
  postgres: sql,
  models: ['User', 'Post'], // Only User and Post get accelerated
})

// Order, Product, etc. still use Prisma
```

### Performance Monitoring

Track query performance:

```typescript
speedExtension({
  postgres: sql,
  onQuery: (info) => {
    console.log(`${info.model}.${info.method} completed in ${info.duration}ms`)

    if (info.duration > 100) {
      logger.warn('Slow query detected', {
        model: info.model,
        method: info.method,
        sql: info.sql,
      })
    }
  },
})
```

### When to Provide DMMF Explicitly

Provide `dmmf: Prisma.dmmf` if:

- Using Cloudflare Workers, Vercel Edge, or similar edge runtimes
- Bundling with webpack, esbuild, or Rollup
- In a monorepo with complex Prisma setup
- You see "Cannot access Prisma DMMF" error

```typescript
import { Prisma } from '@prisma/client'

speedExtension({
  postgres: sql,
  dmmf: Prisma.dmmf, // Explicit DMMF
})
```

## Advanced Usage

### Read Replicas

Send writes to primary, reads to replica:

```typescript
// Primary database for writes
const primary = new PrismaClient()

// Replica for fast reads
const replica = postgres(process.env.REPLICA_URL)
const fastPrisma = new PrismaClient().$extends(
  speedExtension({ postgres: replica })
)

// Use appropriately
await primary.user.create({ data: { ... } })      // → Primary
const users = await fastPrisma.user.findMany()    // → Replica
```

### Connection Pooling

Configure postgres.js connection pool:

```typescript
const sql = postgres(process.env.DATABASE_URL, {
  max: 20, // Pool size
  idle_timeout: 20, // Close idle connections after 20s
  connect_timeout: 10, // Connection timeout
  ssl: 'require', // Force SSL
})

const prisma = new PrismaClient().$extends(speedExtension({ postgres: sql }))
```

### Gradual Rollout

Feature-flag the extension for safe rollout:

```typescript
const USE_FAST_READS = process.env.FAST_READS === 'true'

const sql = postgres(DATABASE_URL)
const prisma = new PrismaClient()

const db = USE_FAST_READS
  ? prisma.$extends(speedExtension({ postgres: sql }))
  : prisma

// Disable in production if issues arise:
// FAST_READS=false pm2 restart app
```

### Access Original Prisma

```typescript
const prisma = new PrismaClient().$extends(speedExtension({ postgres: sql }))

// Use extension (fast)
const fast = await prisma.user.findMany()

// Bypass extension (original Prisma)
const slow = await prisma.$original.user.findMany()
```

## Edge Runtime

### Vercel Edge Functions

```typescript
import { PrismaClient, Prisma } from '@prisma/client'
import { speedExtension } from 'prisma-sql'
import postgres from 'postgres'

const sql = postgres(process.env.DATABASE_URL)
const prisma = new PrismaClient().$extends(
  speedExtension({
    postgres: sql,
    dmmf: Prisma.dmmf, // Explicit dmmf required in edge runtime
  }),
)

export const config = { runtime: 'edge' }

export default async function handler(req: Request) {
  const users = await prisma.user.findMany()
  return Response.json(users)
}
```

### Cloudflare Workers

For Cloudflare Workers, use the standalone SQL generation API instead of the extension:

```typescript
import { createToSQL } from 'prisma-sql'
import { Prisma } from '@prisma/client'

const toSQL = createToSQL(Prisma.dmmf, 'sqlite')

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

> **Note:** The Prisma Client extension is not recommended for Cloudflare Workers due to cold start overhead. Use the `createToSQL` API for edge deployments.

## Supported Queries

### Filters

```typescript
// Comparison operators
{ age: { gt: 18, lte: 65 } }
{ status: { in: ['ACTIVE', 'PENDING'] } }
{ status: { notIn: ['DELETED'] } }

// String operations
{ email: { contains: '@example.com' } }
{ email: { startsWith: 'user' } }
{ email: { endsWith: '.com' } }
{ email: { contains: 'EXAMPLE', mode: 'insensitive' } }

// Logical operators
{ AND: [{ status: 'ACTIVE' }, { verified: true }] }
{ OR: [{ role: 'ADMIN' }, { role: 'MODERATOR' }] }
{ NOT: { status: 'DELETED' } }

// Null checks
{ deletedAt: null }
{ deletedAt: { not: null } }
```

### Relations

```typescript
// Include relations
{
  include: {
    posts: true,
    profile: true
  }
}

// Nested includes
{
  include: {
    posts: {
      include: { comments: true },
      where: { published: true },
      orderBy: { createdAt: 'desc' },
      take: 5
    }
  }
}

// Relation filters
{
  where: {
    posts: {
      some: { published: true }
    }
  }
}

{
  where: {
    posts: {
      every: { published: true }
    }
  }
}

{
  where: {
    posts: {
      none: { published: false }
    }
  }
}
```

### Pagination & Ordering

```typescript
// Limit/offset
{
  take: 10,
  skip: 20,
  orderBy: { createdAt: 'desc' }
}

// Cursor-based pagination
{
  cursor: { id: 100 },
  take: 10,
  skip: 1,  // Skip cursor itself
  orderBy: { id: 'asc' }
}

// Multiple ordering
{
  orderBy: [
    { status: 'asc' },
    { priority: 'desc' },
    { createdAt: 'desc' }
  ]
}

// Null positioning (PostgreSQL)
{
  orderBy: {
    name: {
      sort: 'asc',
      nulls: 'last'
    }
  }
}
```

### Aggregations

```typescript
// Count
await prisma.user.count({ where: { status: 'ACTIVE' } })

// Multiple aggregations
await prisma.task.aggregate({
  where: { status: 'DONE' },
  _count: { _all: true },
  _sum: { estimatedHours: true },
  _avg: { estimatedHours: true },
  _min: { startedAt: true },
  _max: { completedAt: true },
})

// Group by
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

### Distinct

```typescript
// Single field (PostgreSQL uses DISTINCT ON)
{
  distinct: ['status'],
  orderBy: { status: 'asc' }
}

// Multiple fields (SQLite uses window functions)
{
  distinct: ['status', 'priority'],
  orderBy: [
    { status: 'asc' },
    { priority: 'asc' }
  ]
}
```

## Migration Guide

### From Prisma Client

**Before:**

```typescript
const prisma = new PrismaClient()
const users = await prisma.user.findMany()
```

**After:**

```typescript
import postgres from 'postgres'
import { speedExtension } from 'prisma-sql'

const sql = postgres(DATABASE_URL)
const prisma = new PrismaClient().$extends(speedExtension({ postgres: sql }))

const users = await prisma.user.findMany() // Same code, 2-7x faster
```

### From Drizzle

**Before:**

```typescript
import { drizzle } from 'drizzle-orm/postgres-js'
import postgres from 'postgres'

const sql = postgres(DATABASE_URL)
const db = drizzle(sql)

const users = await db
  .select()
  .from(usersTable)
  .where(eq(usersTable.status, 'ACTIVE'))
```

**After:**

```typescript
import { speedExtension } from 'prisma-sql'

const sql = postgres(DATABASE_URL)
const prisma = new PrismaClient().$extends(speedExtension({ postgres: sql }))

// Use Prisma's familiar API instead
const users = await prisma.user.findMany({
  where: { status: 'ACTIVE' },
})
```

## Limitations

### Partially Supported

These features work but have limitations:

- ⚠️ **Array operations**: Basic operations (`has`, `hasSome`, `hasEvery`, `isEmpty`) work. Advanced filtering like `array_contains(array_field, [1,2,3])` not yet supported.
- ⚠️ **JSON operations**: Path-based filtering works (`json.path(['field'], { equals: 'value' })`). Advanced JSON functions not yet supported.

### Not Yet Supported

These Prisma features are not supported and will fall back to Prisma Client:

- ❌ Full-text search (`search` operator)
- ❌ Composite types (MongoDB-style embedded documents)
- ❌ Raw database features (PostGIS, pg_trgm, etc.)
- ❌ Some advanced aggregations in `groupBy` (nested aggregations)

If you encounter unsupported queries, enable `debug: true` to see which queries are being converted and which fall back to Prisma.

### Database Support

- ✅ PostgreSQL 12+
- ✅ SQLite 3.35+
- ❌ MySQL (not yet implemented)
- ❌ MongoDB (not applicable - document database)
- ❌ SQL Server (not yet implemented)
- ❌ CockroachDB (not yet tested)

## Troubleshooting

### "Results don't match Prisma Client"

Enable debug mode to inspect generated SQL:

```typescript
speedExtension({
  postgres: sql,
  debug: true,
})
```

Compare with Prisma's query log:

```typescript
new PrismaClient({ log: ['query'] })
```

File an issue if results differ: https://github.com/dee-see/prisma-sql/issues

### "Connection pool exhausted"

Increase postgres.js pool size:

```typescript
const sql = postgres(DATABASE_URL, {
  max: 50, // Increase from default 10
})
```

### "Cannot access Prisma DMMF" Error

If you see this error:

```
Cannot access Prisma DMMF. Please provide dmmf in config
```

Explicitly provide the DMMF:

```typescript
import { Prisma } from '@prisma/client'

const prisma = new PrismaClient().$extends(
  speedExtension({
    postgres: sql,
    dmmf: Prisma.dmmf, // Add this
  }),
)
```

This is required in:

- Edge runtimes (Cloudflare Workers, Vercel Edge)
- Bundled applications (webpack, esbuild)
- Some monorepo setups
- When using Prisma Client programmatically

### "Type errors after extending"

Ensure `@prisma/client` is up to date:

```bash
npm update @prisma/client
npx prisma generate
```

### "Performance not improving"

Some queries won't see dramatic improvements:

- Very simple `findUnique` by ID (already fast)
- Queries with no WHERE clause on small tables
- Aggregations on unindexed fields

Use `onQuery` to measure actual speedup:

```typescript
speedExtension({
  postgres: sql,
  onQuery: (info) => {
    console.log(`${info.method} took ${info.duration}ms`)
  },
})
```

## FAQ

**Q: Do I need to keep using Prisma Client?**  
A: Yes. You need Prisma for schema management, migrations, types, and write operations. This extension only speeds up reads.

**Q: Does it work with my existing schema?**  
A: Yes. No schema changes required. It works with your existing Prisma schema and generated client.

**Q: What about writes (create, update, delete)?**  
A: Writes still use Prisma Client. This extension only accelerates reads. For write-heavy workloads, this provides less benefit.

**Q: Is it production ready?**  
A: Yes. 137 E2E tests verify exact parity with Prisma Client across both Prisma v6 and v7. Used in production.

**Q: Can I use it with PlanetScale, Neon, Supabase?**  
A: Yes. Works with any PostgreSQL-compatible database. Just pass the connection string to postgres.js.

**Q: Does it support Prisma middlewares?**  
A: The extension runs after middlewares. If you need middleware to see the actual SQL, use Prisma's query logging.

**Q: Can I still use `$queryRaw` and `$executeRaw`?**  
A: Yes. Those methods are unaffected. You also still have direct access to the postgres.js client.

**Q: Do I need to provide `dmmf` in the config?**  
A: Usually no - it's auto-detected from Prisma Client. However, in edge runtimes (Cloudflare Workers, Vercel Edge) or bundled applications, you must provide it explicitly:

```typescript
import { Prisma } from '@prisma/client'

speedExtension({
  postgres: sql,
  dmmf: Prisma.dmmf, // Required in edge runtimes
})
```

If you see "Cannot access Prisma DMMF" error, add this parameter.

**Q: What's the overhead of SQL generation?**  
A: ~0.03-0.04ms per query. Even with this overhead, total time is 2-7x faster than Prisma.

**Q: How do I benchmark my own queries?**  
A: Use the `onQuery` callback to measure each query, or see the [Benchmarking](#benchmarking) section below.

**Q: How does performance compare to Prisma v7?**  
A: Prisma v7 introduced significant improvements (~39% faster than v6 on PostgreSQL, ~24% on SQLite), but this extension still provides 2-7x additional speedup over v7 depending on query complexity.

## Examples

- [PostgreSQL E2E Tests](./tests/e2e/postgres.test.ts) - Comprehensive query examples
- [SQLite E2E Tests](./tests/e2e/sqlite.e2e.test.ts) - SQLite-specific queries
- [Runtime API Tests](./tests/e2e/runtime-api.test.ts) - All three APIs

To run examples locally:

```bash
git clone https://github.com/dee-see/prisma-sql
cd prisma-sql
npm install
npm test
```

## Benchmarking

Benchmark your own queries:

```typescript
import { speedExtension } from 'prisma-sql'

const queries: { name: string; duration: number }[] = []

const prisma = new PrismaClient().$extends(
  speedExtension({
    postgres: sql,
    onQuery: (info) => {
      queries.push({
        name: `${info.model}.${info.method}`,
        duration: info.duration,
      })
    },
  }),
)

// Run your queries
await prisma.user.findMany({ where: { status: 'ACTIVE' } })
await prisma.post.findMany({ include: { author: true } })

// Analyze
console.table(queries)
```

Or run the full test suite benchmarks:

```bash
git clone https://github.com/dee-see/prisma-sql
cd prisma-sql
npm install

# Setup test database
npx prisma db push

# Run PostgreSQL benchmarks
DATABASE_URL="postgresql://..." npm run test:e2e:postgres

# Run SQLite benchmarks
npm run test:e2e:sqlite
```

Results include timing for Prisma vs Extension vs Drizzle (where applicable).

## Contributing

PRs welcome! Priority areas:

- MySQL support implementation
- Additional PostgreSQL/SQLite operators
- Performance optimizations
- Edge runtime compatibility
- Documentation improvements

Setup:

```bash
git clone https://github.com/dee-see/prisma-sql
cd prisma-sql
npm install
npm run generate
npm test
```

Please ensure:

- All tests pass (`npm test`)
- New features have tests
- Types are properly exported
- README is updated

## How It Works

```
┌─────────────────────────────────────────────────────┐
│  prisma.user.findMany({ where: { status: 'ACTIVE' }})│
└────────────────────┬────────────────────────────────┘
                     │
         ┌───────────▼──────────┐
         │  Speed Extension     │
         │  Intercepts query    │
         └───────────┬──────────┘
                     │
         ┌───────────▼──────────┐
         │  Generate SQL        │
         │  Parser + Builder    │
         └───────────┬──────────┘
                     │
         ┌───────────▼──────────┐
         │  SELECT ... FROM users│
         │  WHERE status = $1   │
         └───────────┬──────────┘
                     │
         ┌───────────▼──────────┐
         │  Execute via         │
         │  postgres.js         │ ← Bypasses Prisma's query engine
         └───────────┬──────────┘
                     │
         ┌───────────▼──────────┐
         │  Return results      │
         │  (same format as     │
         │   Prisma)            │
         └──────────────────────┘
```

## License

MIT

## Links

- [NPM Package](https://www.npmjs.com/package/prisma-sql)
- [GitHub Repository](https://github.com/dee-see/prisma-sql)
- [Issue Tracker](https://github.com/dee-see/prisma-sql/issues)

---

**Made for developers who need Prisma's DX with raw SQL performance.**
