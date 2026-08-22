# Architecture

Map of `src/`. Read top to bottom: generation happens first, runtime second.

## Two sides

```
prisma generate                runtime
-------------                  -------
generator.ts                   your app
  |                              |
sql-generator.ts               generated/sql/index.ts   <- emitted by code-emitter.ts
code-emitter.ts                      |
cardinality-planner.ts         fast-path.ts / query-cache.ts
  |                            builder/** strategies
planner.generated.ts           result-transformers.ts
```

## Generator side (runs during `prisma generate`)

| File | Role |
| --- | --- |
| `generator.ts` | Generator entry point (`@prisma/generator-helper`). Reads config: dialect, output, skipInvalid. |
| `sql-generator.ts` | Converts DMMF to internal `Model[]` via `@dee-wan/schema-parser`, parses `@optimize` directives. |
| `code-emitter.ts` | Writes the generated extension package: `index.ts` + `planner.generated.ts`. Connect timeout 5s. |
| `cardinality-planner.ts` | Collects relation/model stats and roundtrip cost from the database during generation. Exports `loadExternalPlannerStats` for runtime loading. Emits `RELATION_STATS`, `MODEL_STATS`, `ROUNDTRIP_ROW_EQUIVALENT`, `JSON_ROW_FACTOR`. |
| `collect-planner-stats.ts` | Standalone CLI bin to re-collect stats after deployment without regenerating. |

## Runtime side

| File | Role |
| --- | --- |
| `index.ts` | Public API: `buildSQL`, `createToSQL`, limits/strategy setters, reducer and executor exports. |
| `types.ts` | Shared types: `Model`, `PrismaMethod`, `SqlResult`, configs. |
| `sql-builder-dialect.ts` | Global dialect state (`postgres` \| `sqlite`). Set once, read everywhere. |
| `fast-path.ts` | Direct SQL templates for simple scalar-only shapes. Skips the full builder. |
| `query-cache.ts` | Caches built SQL per query shape. `rebuildQueryCache()` clears it. |

### Strategy selection (`builder/select/`)

The core problem: same args, many valid execution plans. Chosen per include tree.

| File | Role |
| --- | --- |
| `segment-planner.ts` | Entry decision: `planQueryStrategy()` picks flat join vs correlated subquery vs where-in segments per relation edge, using cost model + stats. |
| `strategy-estimator.ts` | Cost model constants (`roundtripRowEquivalent`, fan-out defaults...) and stats holders (`setRelationStats`, `setModelStats`). All tuning knobs live here. |
| `strategies.ts` | Strategy definitions and applicability checks. |
| `flat-join.ts`, `include-join.ts`, `includes.ts` | Join-based plans for shallow or bounded trees. |
| `lateral-join.ts`, `lateral-reducer.ts` | LATERAL join plan plus row stitching for bounded children. |
| `where-in-executor.ts` (root), `streaming-where-in-executor.ts` | Segmented loading: parents first, then `WHERE fk IN (...)` batches (tuple-IN for composite FKs), stitched in memory. Streaming variant for `findManyStream`. |
| `assembly.ts`, `reducer.ts`, `core-reducer.ts`, `row-transformers.ts` | Turn raw joined rows into Prisma-shaped nested results. Type mapping for Decimal/BigInt/DateTime lives here. |
| `or-rewrite.ts` | Rewrites some `OR` filters into union-of-ids form when cheaper. |
| `streaming-reducer.ts`, `streaming-progressive-reducer.ts` | Reduce rows incrementally for streams. |
| `include-count.ts`, `distinct.ts`, `fields.ts` | `_count` includes, DISTINCT handling, select projection. |

### Where clause (`builder/where/`)

`builder.ts` dispatches to `operators-scalar`, `operators-array`, `operators-json`, `relations` (`some`/`every`/`none`). Depth caps enforced here.

### Shared primitives (`builder/shared/`)

Alias generation, param stores, FK join helpers, primary key utils, negative-take handling, order-by determinism, validators. No business logic.

### Other runtime

| File | Role |
| --- | --- |
| `batch/` | `$batch`: `batch-builder.ts` combines queries into one PostgreSQL multi-statement payload, `count-sql-parser.ts` + `batch-result.ts` split results back. |
| `transaction.ts` | Transaction executor wrapper. |
| `result-transformers.ts` | Top-level entry that applies reducers/transformers to raw driver rows. |
| `generated-runtime.ts` | Driver execution helpers: postgres.js query runner, better-sqlite3 prepared-statement cache (`SQLITE_STMT_CACHE`), raw execute, stream reduce plumbing. |
| `shard.ts` | Injectable shard slot: `createShardedReader(toSQL, controller)`. The library generates SQL; the host's `ShardController` maps an EXPLICIT key to an executor. No argument inference, no default executor — a refusing controller means the SQL never runs anywhere. |
| `utils/s3-fifo.ts` | S3-FIFO eviction used by caches. |
| `maintenance/` | Empty. Reserved. |

## Invariants

1. Never return wrong results: unsupported shape -> fall back to Prisma (or raise explicit no-fallback error for extended orderBy forms Prisma cannot parse).
2. Result shapes must match Prisma, including value types (Decimal, BigInt, DateTime).
3. Strategy choice affects speed only, never correctness.
4. Shard routing is the host's decision: the slot takes an explicit key from the call site and never inspects query args; resolution failure is loud, never a silent default executor.
