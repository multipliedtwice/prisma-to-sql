/**
 * ═══════════════════════════════════════════════════════════
 * COMPREHENSIVE STRATEGY BENCHMARK (all 5 strategies)
 * ═══════════════════════════════════════════════════════════
 *
 * Strategies: flat-join (F) | array-agg (A) | where-in (W) | correlated (C) | lateral (L)
 * All times in ms, col3 (static/prebaked). Drizzle (D) where available.
 * ★ = winner
 *
 * ── DEPTH-1, NO CHILD PAGINATION ──────────────────────────
 *
 * include posts (one-to-many, 5 parents)
 *   F:1.59ms  A:2.77ms  W:1.15ms★  C:1.97ms  L:2.28ms  D:2.36ms
 *
 * include profile (one-to-one, 5 parents)
 *   F:0.31ms★  A:0.48ms  W:0.37ms  C:0.60ms  L:0.32ms  D:0.50ms
 *
 * include+select nested (field narrowing, no child take)
 *   F:0.85ms★  A:1.50ms  W:0.90ms  C:1.50ms  L:6.12ms  D:1.88ms
 *
 * depth-1 low-fan (labels ~4, take 5)
 *   F:0.34ms  A:0.30ms  W:0.88ms  C:0.20ms★  L:0.30ms  (noise floor, all <1ms)
 *
 * depth-1 mid-fan (tasks ~15, take 5)
 *   F:1.44ms  A:2.61ms  W:1.07ms★  C:1.87ms  L:2.39ms
 *
 * depth-1 high-fan (tasks ~25, take 5)
 *   F:1.19ms  A:4.23ms  W:0.93ms★  C:1.81ms  L:2.22ms
 *
 * depth-1 wide (3 sibling lists, take 3)
 *   F:6.38ms  A:4.66ms  W:1.19ms★  C:1.35ms  L:2.08ms
 *
 * depth-1 unbound (no parent take)
 *   F:17.04ms  A:27.25ms  W:9.17ms★  C:16.64ms  L:39.10ms
 *
 * ── DEPTH-2+, NO CHILD PAGINATION ────────────────────────
 *
 * depth-2 (Project→tasks→comments, take 3)
 *   F:2.67ms  A:8.74ms  W:2.02ms★  C:7.74ms  L:9.16ms
 *
 * depth-2 high-fan (User→tasks→comments, take 3)
 *   F:1.23ms★  A:6.40ms  W:1.35ms  C:11.49ms  L:6.74ms
 *
 * depth-2 wide (tasks→3 child rels, take 2)
 *   F:2.86ms  A:11.54ms  W:1.84ms★  C:10.91ms  L:12.13ms
 *
 * depth-2 unbound
 *   F:28.09ms  A:277.53ms  W:13.81ms★  C:259.98ms  L:289.63ms
 *
 * depth-3 unbound (take 2)
 *   F:9.94ms  A:59.62ms  W:5.10ms★  C:54.13ms  L:61.23ms
 *
 * depth-4 unbound (take 1)
 *   F:5.41ms  A:30.92ms  W:3.95ms★  C:30.39ms  L:32.52ms
 *
 * ── WITH CHILD PAGINATION ─────────────────────────────────
 *
 * include 3 levels (take 2/3)
 *   F:1.37ms  A:1.38ms  W:3.79ms  C:1.14ms★  L:5.79ms  D:1.67ms
 *
 * include 4 levels (take 2/2/2)
 *   F:1.15ms  A:1.05ms  W:5.40ms  C:1.04ms★  L:4.93ms  D:1.54ms
 *
 * include+where (tasks where+take 5)
 *   F:1.59ms  A:1.58ms  W:0.95ms★  C:0.38ms(s)/1.58ms(d)  L:2.30ms  D:4.26ms
 *   Note: C static 0.38ms is outlier vs dynamic 1.58ms. W wins reliably.
 *
 * depth-2 paginated (tasks take 5)
 *   F:1.63ms  A:1.28ms  W:1.56ms  C:1.21ms★  L:1.67ms
 *
 * depth-3 paginated (take 2/3)
 *   F:1.75ms  A:1.64ms  W:4.76ms  C:1.17ms★  L:1.50ms
 *
 * depth-4 paginated (take 2/2/2)
 *   F:1.39ms  A:1.04ms  W:5.34ms  C:1.01ms★  L:1.22ms
 *
 * select+include (select 2 fields + include take 3)
 *   F:0.27ms  A:0.24ms  W:0.72ms  C:0.23ms★  L:0.28ms  D:0.35ms
 *
 * ── SINGLE PARENT ─────────────────────────────────────────
 *
 * findFirst depth-2
 *   F:0.98ms★  A:4.10ms  W:1.33ms  C:2.91ms  L:4.02ms
 *
 * findUnique depth-2 (Project→tasks→comments)
 *   F:1.20ms  A:2.96ms  W:0.98ms★  C:3.32ms  L:3.88ms
 *
 * ── COMPLEX / DEEP ────────────────────────────────────────
 *
 * complex nested select
 *   F:1.89ms  A:2.15ms  W:1.64ms★  C:1.97ms  L:5.23ms
 *
 * ultra deep query
 *   F:7.80ms  A:5.08ms  W:7.54ms  C:4.95ms★  L:9.29ms
 *
 *
 * ═══════════════════════════════════════════════════════════
 * WINNER TALLY
 * ═══════════════════════════════════════════════════════════
 *
 * where-in (12 wins):
 *   include posts, depth-1 mid/high/wide/unbound,
 *   depth-2, depth-2 wide/unbound, depth-3/4 unbound,
 *   include+where, findUnique depth-2, complex nested
 *
 * correlated (8 wins):
 *   include 3/4 levels, depth-2/3/4 paginated,
 *   select+include, depth-1 low-fan (noise), ultra deep
 *
 * flat-join (4 wins):
 *   include profile, include+select nested,
 *   depth-2 high-fan, findFirst depth-2
 *
 * array-agg (0 wins): never best. Eliminated.
 *
 * lateral (0 wins): never best. Close 2nd on depth-3/4
 *   paginated (1.50ms/1.22ms vs cor 1.17ms/1.01ms) but loses
 *   catastrophically on unbound (10-18x slower than wIn).
 *
 *
 * ═══════════════════════════════════════════════════════════
 * COST MODEL (row-equivalent units)
 * ═══════════════════════════════════════════════════════════
 *
 * Variables from cardinality-planner.ts:
 *   R     = roundtripRowEquivalent (default 73, calibrated per-db)
 *   J     = jsonRowFactor (default 1.5, json_agg overhead per row)
 *   fan_i = estimated fan-out for relation i (from RELATION_STATS)
 *   P     = parentCount (from take, or table estimate for unbound)
 *   D     = include depth (number of nested relation levels)
 *   T_i   = child take for relation i (Infinity if unbound)
 *   eff_i = min(fan_i, T_i) — effective rows per parent per level
 *
 * ── STRATEGY COST FORMULAS ────────────────────────────────
 *
 * where-in:
 *   Roundtrips = 1 + D (parent query + 1 batch query per level)
 *   Rows processed = SUM over levels(P × eff_i)
 *   costW = (1 + D) × R + SUM_i(P × eff_i)
 *
 *   Strength: rows processed via batch IN(), no per-parent overhead.
 *   Weakness: each depth level adds a full roundtrip cost R.
 *
 * correlated (fallback to prisma subqueries):
 *   Roundtrips = 1 (single query, postgres runs subqueries internally)
 *   Subquery cost per parent per relation:
 *     With child take:    sub_i = eff_i × S  (S ≈ 0.8, index seek + LIMIT)
 *     Without child take: sub_i = fan_i × S  (unbounded scan, expensive)
 *   costC = R + P × SUM_i(sub_i)
 *
 *   Strength: 1 roundtrip; bounded by LIMIT when paginated.
 *   Weakness: P × D subquery executions inside postgres.
 *   When unbounded: fan_i can be 100s→ P × fan_i explodes.
 *
 * flat-join:
 *   Roundtrips = 1
 *   Joined rows = P × PRODUCT(eff_i) (cartesian across relations)
 *   costF = R + P × PRODUCT(eff_i) × J + dedup(P, PRODUCT(eff_i))
 *
 *   dedup ≈ 0 when P = 1 (findFirst/findUnique)
 *   dedup ≈ 0 when all eff_i = 1 (one-to-one)
 *   PRODUCT explodes with multiple to-many siblings (wide queries).
 *
 *   Strength: 1 roundtrip, 1 query, zero overhead for P=1.
 *   Weakness: cartesian product multiplies rows exponentially.
 *
 * ── COST-BASED DECISION ───────────────────────────────────
 *
 * Derived from benchmark data fitting:
 *   R ≈ 73 row-equivalents (≈ 0.3ms)
 *   J ≈ 1.5
 *   S ≈ 0.8 (correlated subquery overhead factor)
 *
 * Pick minimum of:
 *
 *   costW = (1 + D) × R + SUM_i(P × eff_i)
 *   costC = R + P × SUM_i(eff_i × S)
 *   costF = R + P × PRODUCT(eff_i) × J    (only when canFlatJoin)
 *
 * Simplifications that match benchmark winners:
 *
 * 1. P = 1 (findFirst/findUnique):
 *    costF = R + PRODUCT(eff_i) × J
 *    costW = (1+D) × R + SUM(eff_i)
 *    → F wins when PRODUCT(eff_i) × J < D × R + SUM(eff_i)
 *    → At D=2, eff=[15,5]: F = 73+112 = 185, W = 219+20 = 239 → F wins ✓
 *
 * 2. All one-to-one (all eff_i = 1):
 *    costF = R + P × J  (minimal)
 *    costW = (1+D) × R + P × D
 *    → F wins when P × J < D × R + P × D
 *    → Always true for D >= 1 since R = 73 >> J = 1.5
 *    → Matches: include profile ✓
 *
 * 3. hasChildPagination + D >= 2:
 *    costC = R + P × SUM(T_i × S)     (bounded by small takes)
 *    costW = (1+D) × R + SUM(P × T_i) (multiple roundtrips)
 *    → C wins when D × R > P × SUM(T_i) × (S - 1)
 *    → At D=3, T=[2,3,2], P=5: C = 73+5×5.6 = 101, W = 292+35 = 327 → C wins ✓
 *    → At D=4, T=[2,2,2,2], P=5: C = 73+5×6.4 = 105, W = 365+40 = 405 → C wins ✓
 *
 * 4. No child pagination, D >= 1, mid/high fan:
 *    costW = (1+D) × R + P × SUM(fan_i)
 *    costC = R + P × SUM(fan_i) × S
 *    → W wins when (1+D)×R + P×F_sum < R + P×F_sum×S
 *    → D×R < P × F_sum × (S-1)
 *    → Since S < 1 for batched vs subquery: W always wins
 *    → Matches: all unbound, all mid/high fan ✓
 *
 * 5. hasChildPagination + D = 1 + hasChildWhere:
 *    costW = 2R + P × eff   (where clause benefits from batch)
 *    costC = R + P × eff × S
 *    → Close, but where clause filtering is more efficient batched
 *    → Matches: include+where W:0.95ms vs C:1.58ms ✓
 *
 * 6. hasChildPagination + D = 1 + selectNarrowing + no childWhere:
 *    costC = R + P × eff × S  (very small eff due to narrow select+take)
 *    costW = 2R + P × eff     (extra roundtrip dominates small eff)
 *    → C wins: select+include C:0.23ms vs W:0.72ms ✓
 *
 *
 * ═══════════════════════════════════════════════════════════
 * DECISION TREE (implements cost model above)
 * ═══════════════════════════════════════════════════════════
 *
 * 1. hasChildPagination + depth >= 2           → correlated
 *    costC ≈ R + P × SUM(T_i × 0.8)  ≪  costW ≈ (1+D) × R + P × SUM(T_i)
 *    Saves D × R (D extra roundtrips) at cost of S overhead per subquery.
 *    Benchmarked: 2.7-4.7x faster than where-in.
 *
 * 2. hasChildPagination + selectNarrowing + !hasChildWhere → correlated
 *    Small eff makes extra roundtrip cost dominate in where-in.
 *    Benchmarked: 3.1x faster (0.23ms vs 0.72ms).
 *
 * 3. hasChildPagination + depth 1              → where-in
 *    Only 2 roundtrips; batch WHERE benefits > subquery overhead.
 *    Benchmarked: include+where W:0.95ms vs C:1.58ms.
 *
 * 4. singleParent + canFlatJoin + depth <= 2   → flat-join
 *    P=1 eliminates dedup cost entirely. costF = R + PRODUCT(eff) × J.
 *    Benchmarked: findFirst depth-2 F:0.98ms vs W:1.33ms.
 *
 * 5. allOneToOne + canFlatJoin                 → flat-join
 *    PRODUCT(eff)=P (no multiplication). costF = R + P × J.
 *    Benchmarked: include profile F:0.31ms vs W:0.37ms.
 *
 * 6. everything else                           → where-in
 *    Safe default. Never catastrophically slow.
 *    Worst case vs optimal: ~1.1x (depth-2 high-fan F:1.23ms vs W:1.35ms).
 */
