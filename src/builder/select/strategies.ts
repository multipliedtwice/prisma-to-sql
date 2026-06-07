/**
 * ═══════════════════════════════════════════════════════════
 * INCLUDE STRATEGIES
 * ═══════════════════════════════════════════════════════════
 *
 * Three strategies handle to-many/to-one include shapes:
 *
 *   F = flat-join   (LEFT JOIN parent → child, dedup client-side)
 *   W = where-in    (peel relations to N+1 batched IN queries)
 *   C = correlated  (per-parent (SELECT json_agg(...) WHERE fk = parent.id))
 *
 * Picker output type is 'flat-join' | 'where-in' | 'correlated'
 * (renamed from 'fallback' in earlier code for clarity — it was always
 * a deliberate strategy, never a safety net.)
 *
 * Previous array-agg and lateral paths have been removed: array-agg won
 * zero benchmark scenarios and bypassed the picker via canUseJoinInclude;
 * lateral was orphaned (never returned by the picker).
 *
 *
 * ═══════════════════════════════════════════════════════════
 * BENCHMARK WINNERS (24 scenarios, all sub-30ms on small data)
 * ═══════════════════════════════════════════════════════════
 *
 * where-in   (12 wins): include posts, depth-1 mid/high/wide/unbound,
 *                       depth-2, depth-2 wide/unbound, depth-3/4 unbound,
 *                       include+where, findUnique depth-2, complex nested
 *
 * correlated (8 wins):  include 3/4 levels, depth-2/3/4 paginated,
 *                       select+include, depth-1 low-fan, ultra deep
 *
 * flat-join  (4 wins):  include profile, include+select nested,
 *                       depth-2 high-fan, findFirst depth-2
 *
 * Margins are tight on small data (<2ms differences). At scale the picker
 * needs more than this benchmark gives — see the large-child guard below.
 *
 *
 * ═══════════════════════════════════════════════════════════
 * LARGE-CHILD GUARD (size-aware override)
 * ═══════════════════════════════════════════════════════════
 *
 * Problem: at production scale, correlated can lose by 1000x when each
 * per-parent subquery scans a multi-GB child table. The cost model alone
 * cannot see this — it has fan-out estimates but no absolute table size.
 *
 * Solution: a hard guard runs at the top of pickIncludeStrategy, before
 * any cost-comparison rules. It uses MODEL_STATS.rowCount (collected by
 * cardinality-planner.ts, persisted in planner.generated.ts).
 *
 * Inputs:
 *   - topLevelParentEstimate
 *       = take (if specified)
 *       = min(MODEL_STATS[rootModel].rowCount, defaultParentCount) if known
 *       = defaultParentCount otherwise
 *   - For each to-many relation in the include tree (walked recursively),
 *     childRows = MODEL_STATS[rel.relModel.name].rowCount
 *
 * Rule:
 *   if topLevelParentEstimate < smallParentCountThreshold
 *      AND any to-many in tree has childRows > largeChildTableRows:
 *     → return 'where-in'
 *
 * Defaults:
 *   - largeChildTableRows = 100_000
 *   - smallParentCountThreshold = 1000
 *
 * Both override-able via setStrategyConfig({ ... }).
 *
 * When MODEL_STATS is empty (generator run without DB connection), the
 * guard logs a one-time warning and is inactive. Existing behavior preserved.
 *
 *
 * ═══════════════════════════════════════════════════════════
 * CASE WALK (after patch)
 * ═══════════════════════════════════════════════════════════
 *
 *   depth | child-pagination | child-rows-large | result
 *   ──────┼──────────────────┼──────────────────┼─────────────────────────
 *     1   |       no         |       no         | where-in (cost)
 *     1   |       no         |       yes        | where-in (guard)
 *     1   |       yes        |       no         | where-in
 *     1   |       yes        |       yes        | where-in (guard) ← fixes
 *                                                              the production
 *                                                              26s case
 *    ≥2   |       no         |       no         | cost picks W or C
 *    ≥2   |       no         |       yes        | where-in (guard)
 *    ≥2   |       yes        |       no         | correlated (rule)
 *    ≥2   |       yes        |       yes        | where-in (guard overrides)
 *
 * Tradeoff in the last row: where-in adds D roundtrips. If a good FK
 * + ORDER BY index exists, correlated would have been faster. Without
 * runtime index info we choose the bounded strategy. Projects with good
 * index coverage on huge tables can raise `largeChildTableRows` to bias
 * back toward correlated.
 *
 *
 * ═══════════════════════════════════════════════════════════
 * DECISION TREE
 * ═══════════════════════════════════════════════════════════
 *
 *  1. canFlatJoin + all-one-to-one          → flat-join
 *  2. singleParent + canFlatJoin + depth≤2  → flat-join
 *  3. large-child guard fires               → where-in
 *  4. childPagination + depth ≥ 2           → correlated
 *  5. childPagination + depth=1 + childWhere → where-in
 *  6. childPagination + depth=1             → where-in
 *      (note: the old `selectNarrowing → fallback` rule was deleted;
 *       it caused the user's 26s production case and won a 0.49ms
 *       benchmark scenario at best)
 *  7. depth=1 + childWhere                  → where-in
 *  8. costC < costW                         → correlated
 *  9. else                                  → where-in
 *
 *
 * ═══════════════════════════════════════════════════════════
 * COMPOSITE FOREIGN KEYS (tuple IN)
 * ═══════════════════════════════════════════════════════════
 *
 * Relations with multi-column foreign keys are peeled by segment-planner
 * the same as single-column FK relations. The where-in query uses a
 * row-value IN clause:
 *
 *   WHERE (fk1, fk2) IN (($1, $2), ($3, $4), ...)
 *
 * Implementation: Prisma's where DSL has no row-value IN, so the child
 * SQL is built with buildSQL (preserving user where, orderBy, etc.) and
 * the tuple-IN clause is injected post-build via a quote/depth-aware
 * scanner that finds the top-level WHERE / terminator (ORDER BY, LIMIT,
 * etc.) and splices in the additional clause. Parameter placeholders for
 * the tuples start at `existingParams.length + 1` — no reindexing of the
 * existing params is needed.
 *
 * Map keys for stitching use a BigInt/Date-safe serializer over
 * JSON.stringify (BigInt → {__bigint:"..."}, Date → {__date: ms}).
 *
 * Tuple count per query is capped at floor((paramLimit - reserved) /
 * keyColumnCount); if uniqueParentTuples exceeds this, the executor
 * issues multiple child queries and merges results.
 *
 * Requires Postgres (all versions) or SQLite >= 3.15.
 */
