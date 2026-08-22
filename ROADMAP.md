# Roadmap

Status: `done` / `in progress` / `planned` / `spike` (needs feasibility check first).

## Done

- Repo hygiene: untrack patch files and stale benchmark summaries, ignore them.
- npm description aligned with verifiable claims (no more "2-7x").
- npm provenance enabled in release workflow.
- Maintainer ladder documented in CONTRIBUTING.md.
- ARCHITECTURE.md.
- Benchmark methodology section on docs site.

## Done

- Repo hygiene: untrack patch files and stale benchmark summaries, ignore them.
- npm description aligned with verifiable claims (no more "2-7x").
- npm provenance enabled in release workflow.
- Maintainer ladder documented in CONTRIBUTING.md.
- ARCHITECTURE.md.
- Benchmark methodology section on docs site.

### Parity verify CLI (`prisma-sql-verify`) — built
Runs every corpus query through base Prisma and the speed extension on the same database, deep-compares with type-aware equality (Date, BigInt, Decimal), exits 1 on mismatch. Smoke-tested against the local SQLite test schema on its first run it flagged a DateTime representation drift in nested includes (`+00:00` string vs Prisma's `Z`) — tool works. That finding needs a product decision: normalize at the result-transformer layer or document as known difference.

## Planned

### `$batch` for SQLite
better-sqlite3 is synchronous and local, so there is no network roundtrip to save. The win is different: execute N accumulated statements in one synchronous pass with prepared-statement reuse, then stitch results. Smaller win than Postgres, but removes per-query promise overhead for dashboard-style call sites.

### Cloudflare Workers: full extension support
Today Workers only get standalone SQL generation (`createToSQL`) because postgres.js needs Node sockets or compat flags. Spike needed: HTTP-based driver (D1 REST / Hyperdrive / Neon serverless driver) behind the same executor interface used by `generated-runtime.ts`. If the executor interface holds, full acceleration follows without builder changes.

## Explicitly rejected (for now)

### Silent auto-tuning of strategy constants
Decision: defaults stay static in code. Nothing changes strategy behavior by itself. Reasons:
1. Stats collection costs time; on projects without seamless deploys that means longer downtime windows.
2. A bad measurement silently applied can erase all performance gains.
Allowed instead:
- Measurement stays opt-in (`prisma-sql-collect-stats`, already non-blocking, catalog-stats fast mode).
- Any measured constant must be written into explicit config by the operator before it takes effect.
- Sanity clamps on measured values before use.
