# Performance decisions

Recreated record. Exact deleted standalone file: not found in reachable history, reflogs, stashes, or unreachable objects. Facts here come from:

- `613be07`: strategy benchmark table, formulas, large-child production case, `src/builder/select/strategies.ts` and `src/builder/select/strategy-estimator.ts`.
- `766a591^`: removed cardinality-planner and query-shaping guidance in `readme.md`.
- `75c64c`: explicit no-silent-auto-tuning rule in `ROADMAP.md`.

This file now source of truth. Strategy behavior change requires all three: focused benchmark evidence, focused choice/parity tests, matching update here. No timing-only change if result correctness weakens.

## Invariants

- Correct result first. Strategy changes speed only.
- Large-child guard runs before cost/rule choice.
- No global `json_agg` ban. Correlated won bounded deep cases. Also lost by about 1000x in one production large-child case. Guard protects that case.
- No fake cardinality. PostgreSQL planner may collect `RELATION_STATS` and `MODEL_STATS`. SQLite planner stats remain empty; fan-out/parent defaults apply and large-child guard stays inactive.
- PostgreSQL `pg_class.reltuples = -1` means not analyzed/unknown. Never divide it or mark coverage trusted. Emit coverage `0`; runtime uses default fan-out. Regression found during v8 benchmark work: fake `avg=1/max=5/coverage=1` selected slow correlated shallow includes.
- Fast relation stats reuse validated model row counts (`max(reltuples, n_live_tup)`) before bare catalog fallback. No table scan added. Prevent second catalog query from discarding usable `n_live_tup` and reverting to unknown/fake fan-out.
- No silent auto-tune. Fast catalog/model collection may run when datasource exists. Precise edge scans and benchmark probes stay opt-in. Measured constants become behavior only through explicit operator config. Clamp values before use.
- Sub-ms gap noise. Historical small-data margins often below 2ms. Change only with repeated, same-machine, same-version, same-seed evidence.

## Strategies

- `F`: flat join. One query. Client dedup. Best for one-to-one and some single-parent shapes. Risk: list fan-out cartesian growth.
- `W`: where-in. Parent query, then batched `IN` query per level. Best safe default for unbounded/high fan-out. Cost: extra roundtrips.
- `C`: correlated. One query with per-parent nested aggregation. Best for bounded deep shapes. Risk: repeated child scans on huge child table.

Removed paths: array-agg won 0 benchmark cases. Lateral picker path won 0 and became orphaned. Source: `613be07`.

## Constants

Defaults in `strategy-estimator.ts`:

- `R = roundtripRowEquivalent = 73`
- `jsonRowFactor = 1.5`
- bounded correlated factor `Sb = 0.5`
- unbounded correlated factor `Su = 3.0`
- nested `where` penalty `Q = 3.0`
- missing-stat fan-out `10`
- missing parent count `50`
- dynamic child `take` estimate `10`
- trusted relation-stat coverage `>= 0.1`
- single-parent flat depth `<= 2`
- large child rows `> 100_000`
- small parent count `< 1000`

Per relation node:

```text
fan = trusted RELATION_STATS.avg, else 10
take = positive child take, dynamic estimate 10, else Infinity
eff = min(fan, take)
```

`W` exact recursive model:

```text
costW = (1 + relation-level query count) * R + rows
rows at node = incoming parent rows * eff
child node receives that row count
```

`C` exact recursive model:

```text
S = Sb when bounded, else Su
S = S * Q when node has nested where
node = eff * S + eff * child-node-cost
costC = R + parentCount * sum(root node costs)
```

`jsonRowFactor` belongs to flat-join/cardinality calibration. Current `pickIncludeStrategy` compares `W` and `C` only after hard rules.

## Decision order

1. Empty include -> `W`.
2. Can flat + root all one-to-one + no blocking root args -> `F`.
3. `findFirst`/`findUnique` + can flat + depth <= 2 + no blocking root args -> `F`.
4. Large-child guard -> `W`.
5. Child pagination + depth >= 2 -> `C`.
6. No child pagination + depth >= 2 -> `W` empirical safety override.
7. Child pagination + depth 1 -> `W`; child `where` stays `W`.
8. Depth 1 + child `where` -> `W`.
9. Otherwise compare `costC < costW`; if true `C`, else `W`.

Blocking root args: cursor, non-empty distinct, include/select `_count`.

PostgreSQL implements `F`, `W`, `C`. SQLite picker compares segmented `W` with direct correlated JSON query `C`; flat capability passed false. Same formulas/defaults, but local SQLite roundtrips have no network cost. Current deep bounded choice retained by focused same-seed evidence: v7 four-shape candidate mean `0.478-0.684ms`, old segmented mean `2.525-2.937ms` on same machine/run setup. Full matrix must confirm.

Why step 6 bypasses cost: collected seed stats near fan-out 1 made model choose `C`, but same-environment v7 check made depth-2 `10.037ms` and depth-3 unbound `64.608ms`. Restored `W`; historical `613be07` also records `W` wins for unbounded depth 2/3/4. Formula misses repeated correlated JSON work here.

Why step 5 stays `C`: temporary `W` comparison made v7 depth-2/3/4 paginated `4.636/8.036/7.632ms`. `C` comparison made them `1.519/1.308/1.026ms`. Keep bounded deep correlated despite Prisma-relative variance.

## Large-child guard

Parent estimate:

```text
explicit root take
else min(root MODEL_STATS.rowCount, 50) when known
else 50
```

Walk every nested to-many. If parent estimate `< 1000` and any child model row count `> 100_000`, choose `W`. Missing/empty `MODEL_STATS`: guard inactive, one warning. This override stays before paginated/cost rules.

Why: correlated query took about 26s in reported production shape with multi-GB child table. Index knowledge absent. Bounded `W` safer. Operator with verified indexes may raise `largeChildTableRows` explicitly.

## Historical benchmark winners

`613be07` small-data matrix, 24 cases:

- `W` 12: posts; depth-1 mid/high/wide/unbound; depth-2, wide/unbound; depth-3/4 unbound; include+where; findUnique depth-2; complex nested.
- `C` 8: include 3/4 levels; depth-2/3/4 paginated; select+include; depth-1 low-fan; ultra deep.
- `F` 4: profile; include+select nested; depth-2 high-fan; findFirst depth-2.

These are evidence, not eternal constants. Prisma/runtime/package changes require same-environment recheck. Never compare timings from different runs as proof.

## Query shaping

From `readme.md` before `766a591`:

- Page parents.
- Cap nested collections with `take`.
- Add selective nested `where`.
- Split unrelated heavy branches into `$batch`.
- Encode real one-to-one FK uniqueness.
- Index relation FKs, nested filter/order/cursor fields.
- Keep nested predicates sargable. Prefer indexed equality/range. Avoid unindexed broad contains, OR-heavy filters, deep unbounded includes.
- Use deterministic nested order.

## PostgreSQL row streaming

- Keep postgres.js query `.forEach(...)` in generated and shared runtime paths.
- Not cosmetic array iteration. Driver decodes each row into callback instead of buffering query result.
- Where-in parent stream starts chunk resolution while more parent rows arrive. Keeps pipeline overlap.
- Progressive reducer depends on ordered parent-key transitions. Preserve SQL order and callback order.
- Do not replace with `await client.unsafe(...)`, `.then`, or array `.forEach`. That buffers first and loses overlap/memory bound.

## Change protocol

1. Keep seed, database, Node, Prisma version, warmup, iteration count same.
2. Measure old and candidate repeatedly. Preserve correctness parity.
3. Prioritize repeatable multi-ms regressions. Ignore sub-ms noise.
4. Run focused strategy tests and affected E2E parity.
5. Run full supported Prisma x dialect matrix green.
6. Update benchmark JSON only from complete exit-zero run.
7. Update this file with decision and evidence. No telemetry project. No silent threshold change.
