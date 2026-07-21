# External-DB view-index lookup benchmarks

Measurements behind the `wip/fdw-clickhouse` lookup rework (2026-07-20/21).

**Setup.** Single host; `postgres:16` and `clickhouse/clickhouse-server:24.8` in
docker on localhost; remote table = 500k rows (`id BIGINT PRIMARY KEY, body TEXT`
/ `MergeTree ORDER BY id`) unless noted. Workload tiers by match density:
`abramov` = 200 docs, `krivda` = 2k, `gul` = 25k (1 in 20 rows), `pudge` = 100k.
A lookup batch is up to 2048 keys; the engine emits ~103 real keys per batch at
gul density (one columnstore row group's hits). Times are medians over 3-21
reps, hot cache; where box noise mattered, runs were interleaved. Bare-engine
numbers via `psql`/`clickhouse-client` in-container.

## 0. One table: our code vs the bare engine

All five lookup kinds on ONE stack at the same moment (both engines in docker,
5 fresh indexes over 500k-row tables, gul workload). "Bare" = the exact inner
statement our code ships, ~103 keys (one real batch), run with the engine's own
client in-container, median of 5. "Our code" = gul query wall time / 243
batches — includes FTS matching, batch emit, duckdb passthrough bind, remote
round trip, and our routing probe. Absolute numbers drift with box load
(a quiet-box run measured pg ctid at 1.74 ms/batch); the pairwise bare-vs-ours
comparison within this snapshot is the datum.

| lookup kind | shipped statement (inner) | bare, 103 keys | our code, per batch | our overhead |
|---|---|---|---|---|
| pg ctid | `ctid = ANY('{(p,t),..}'::tid[])` | 0.55 ms | 4.1 ms | +3.5 ms |
| pg single key | `"uniq" = ANY('{..}'::int8[])` | 0.57 ms | 3.7 ms | +3.1 ms |
| pg composite | `("shard","id") IN (SELECT * FROM unnest('{..}'::int8[], '{..}'::int8[]))` | 0.86 ms | 4.7 ms | +3.9 ms |
| CH single key | `` `uniq` IN [..] `` | ~1 ms | 4.9 ms | +3.9 ms |
| CH composite | `` (`shard`,`id`) IN [(..),..] `` (since replaced, see 7b) | ~3 ms | 3.7 ms | +0.7 ms |

(CH client timing granularity is 1 ms.) The overhead column is everything
around the remote query: duckdb parse/bind/plan of the outer statement, the
passthrough bind round trips (pg PQprepare+describe; CH describe-cache hit +
pool lease ping), COPY/stream setup and decode, and our render + slot-map
probe + gather. On a quiet box it measures ~1.2-1.5 ms; under load it inflates
toward ~3-4 ms while the bare statement barely moves -- i.e. the remaining cost
is per-batch client-side machinery, which is exactly what batch accumulation
(lever 1 in section 8) amortizes.

## 1. End-to-end lookup evolution (gul tier: 25k docs, 245 batches)

| stage | pg ctid | pg key column | CH key |
|---|---|---|---|
| filter never pushed (rowid IN silently dropped -> full scan per batch) | 31.2 s | 23.3 s | ~33 s * |
| after RenderCtidFilter optional-filter unwrap (pushes, but padded 2048-param IN + per-Execute rebind + 4 ctid-range slices) | 19.0 s | 22.9 s | — |
| array passthrough (`postgres_query` / `clickhouse_query`) | **0.41 s** | **0.36 s** | 1.41 s (detached conns) |
| + CH transaction connection + describe cache | — | — | **1.21 s** |
| **net speedup** | **76x** | **64x** | **~27x** |

\* extrapolated from the measured 134 ms/batch scan-path cost.

Per batch: pg ~78-95 ms -> **1.74 ms**; CH ~134 ms -> **~3.9 ms**.

## 2. Root cause: duckdb re-optimizes prepared statements per Execute

`PreparedStatement::CanCachePlan` returns false for any plan containing a
`LOGICAL_GET`, so a parameterized statement that scans a table is re-bound and
re-optimized on every Execute — PREPARE only caches the parse. Cost scales with
expression-tree size (~70 us per IN element per Execute):

| statement through serened (full pipeline) | time |
|---|---|
| `id IN (103 literals)` | 13 ms |
| `id IN (2048 literals, 103 distinct)` | 149 ms |
| `id IN (2048 distinct literals)` | 145 ms |
| same batch via `postgres_query` + array literal | **6.0 ms** |
| same batch via `clickhouse_query` + `IN [array]` | **2.3 ms** |

The passthrough works because the key list is ONE string constant to duckdb —
a ~10-node statement regardless of key count.

## 3. Postgres shape shootout — EXECUTE only (server-side PREPARE, generic cached plan, 2048 sparse keys)

| class | shape | plan | median |
|---|---|---|---|
| bare | `ctid = ANY($1::tid[])` | Tid Scan | **0.30 ms** |
| bare | `id IN (2048 literals)` | Index Scan | 0.94 ms |
| bare | `id = ANY($1::bigint[])` | Index Scan | 0.99 ms |
| bare | `ctid IN (2048 literals)` | Parallel Seq Scan (cliff baked into cached plan) | 8.1 ms |
| slot-free | `unnest($1::tid[]) WITH ORDINALITY` join | NL + Tid Scan | 0.65 ms |
| slot-free | `unnest($1::bigint[]) WITH ORDINALITY` join | NL + Index Scan | 1.25 ms |
| slot-free | `array_position($1, key)` + ANY | O(n^2) per-row array scan | 9.5-10 ms |

Slot-elimination premium on pg: +0.3 ms/batch. (Not taken: slot map kept.)

## 4. Postgres ctid plan cliff (why ctid ever lost)

pg costs 2048 "random" tid fetches above a parallel seq scan on small tables
with default `random_page_cost=4`; the *literal* IN keeps the bad plan even
when prepared. Parameterized shapes dodge it (planner never sees the count).

| scenario | plan chosen for `ctid IN (2048)` | ctid | PK index scan |
|---|---|---|---|
| 500k rows, default settings | Parallel Seq Scan | 10.6 ms | 3.5 ms |
| 500k rows, `random_page_cost=1.1` | Tid Scan | **1.9 ms** | 3.5 ms |
| 500k rows, batch <= 1024, default | Tid Scan | 1.2 ms | — |
| 5M rows, default settings | Tid Scan | **2.2 ms** | 3.8 ms |

Execution-only mechanism comparison: Tid Scan 0.37 ms vs PK Index Scan 1.35 ms
(3.6x) — ctid is the faster mechanism wherever the planner uses it.

## 5. ClickHouse shape shootout (bare `clickhouse-client`, 2048 keys)

| class | shape | median |
|---|---|---|
| bare | `id IN (v1, v2, ...)` tuple literals | ~4-6 ms |
| bare | **`id IN [array literal]`** | **~3-4 ms** |
| bare | `id IN {ids:Array(Int64)}` param | ~3-4 ms |
| bare | `has([array], id)` | **~285 ms** — no PK analysis, never use |
| slot-free | `ARRAY JOIN arrayEnumerate` join + IN prune | ~6 ms (== bare IN via same session; ord is free) |
| slot-free | `transform(id, keys, ords, 0)` + IN | +1 ms |
| slot-free | `indexOf(keys, id)` + IN | +4 ms |

## 6. Engine per-query floors (persistent connection, `pgbench` / `clickhouse-benchmark`)

| shape | postgres | ClickHouse | ratio |
|---|---|---|---|
| `SELECT 1` | **0.006 ms** (168k QPS) | **0.69 ms** (1460 QPS) | ~115x |
| 2048-key lookup | 1.11 ms (899 QPS) | 3.18 ms (314 QPS) | ~2.9x |

ClickHouse pays pipeline construction + thread orchestration per query and
reads whole 8192-row granules covering the key range (~41k rows read per
2048-key query vs pg's 2048 btree tuples). You do not tune this away — you
send fewer, bigger queries.

## 7. Composite keys: form comparison

Bare, full statement (parse+plan+execute, 2048 tuples, 500k rows, composite PK):

| pg form | plan | median |
|---|---|---|
| `(shard,id) IN ((a,b),...)` row-IN literal | OR-expansion | ~52 ms |
| `(shard,id) IN (VALUES ...)` | Hash Semi Join | ~13 ms |
| **`(shard,id) IN (SELECT * FROM unnest('{..}'::int8[], '{..}'::int8[]))`** | NL + composite-PK Index Scan | **~2.0 ms** |

Bare, EXECUTE only (cached generic plan — proves it is not a parse artifact):

| pg form | execute median |
|---|---|
| row-IN literal | 43.3 ms |
| `IN (VALUES ...)` | 13.1 ms |
| **unnest zip** | **2.26 ms** |

pg has no set fast-path for row-value IN lists (it OR-expands them); a
composite `= ANY(array)` is impossible (`record[]` literals: "input of
anonymous composite types is not implemented"; the expressible
`ARRAY[(a,b),..]` form plans a seq scan — record equality cannot use the
composite btree).

### 7b. ClickHouse composite: tuple literals vs N-arrays zip

`clickhouse-benchmark`, 2048 tuples, persistent connection; both forms read
the same ~41k granule rows (identical PK pruning) — the delta is parsing 4096
tuple-literal nodes vs 2 flat arrays:

| CH form | p50 / QPS |
|---|---|
| `(shard,id) IN [(a,b),...]` tuple literals | 9-10 ms, 92 QPS |
| **`(shard,id) IN (SELECT a, b FROM (SELECT [..] AS xs, [..] AS ys) ARRAY JOIN xs AS a, ys AS b)`** | **4 ms, 207 QPS (2.25x)** |

The zip form is now what ships: both dialects carry composite keys as one flat
array per key column ("columnar"), zipped server-side — pg by `unnest`,
ClickHouse by parallel `ARRAY JOIN`.

### 7c. Would unifying single-key onto the zip form cost anything?

(2048 keys; pg EXECUTE-only with $1 array param, alternating; CH
clickhouse-benchmark, 100 iters.)

| engine | direct single-key form | unified zip form | verdict |
|---|---|---|---|
| pg | `id = ANY($1)` — 0.95 ms (Index Scan SAOP) | `id IN (SELECT * FROM unnest($1))` — 1.62 ms (NL + probes + dedup) | **keep = ANY: zip costs +71%** |
| CH | `id IN [arr]` — 3.80 ms | zip subquery — 3.88 ms | identical within noise |

pg's `= ANY` compiles to a ScalarArrayOp index scan (one machinery-free btree
descent per key); the IN-subquery form adds a Unique/Sort dedup and join
plumbing. ClickHouse builds the same hash set either way. So the single-key
special case stays for pg (and CH keeps the symmetric direct form for free).

### 7d. Key-type matrix: our code vs bare, all supported key types

Same method as section 0 (bare = exact shipped inner statement; our code =
gul wall / 243 batches of ~103 keys); bare at 2048 keys to make type effects
visible. All nine through-code runs returned identical sums (correctness).

| key type | shipped query (inner) | bare, 2048 keys | our code, per batch |
|---|---|---|---|
| pg BIGINT | `id = ANY('{..}'::int8[])` | 1.03 ms (IN literals: 0.97) | 3.8 ms |
| pg TEXT | `skey = ANY('{..}'::text[])` | 4.05 ms (IN literals: 4.07) | 3.2 ms |
| pg DATE | `dkey = ANY('{..}'::date[])` | 1.21 ms | 3.7 ms |
| pg TIMESTAMP | `tskey = ANY('{..}'::timestamp[])` | 1.26 ms | 3.8 ms |
| pg composite (TEXT, BIGINT) | unnest zip | 3.40 ms | 5.2 ms |
| CH Int64 | `IN [..]` | 3.84 ms | 4.9 ms |
| CH String | `IN ['..',..]` | 3.74 ms | 5.4 ms |
| CH DateTime | `IN ['2000-01-01 ..',..]` | 3.82 ms | 5.0 ms |
| CH composite (String, Int64) | ARRAY JOIN zip | 6.97 ms (tuple literals: 13.3 — 1.9x worse) | 4.0 ms |

Findings:

- Every supported type round-trips correctly through the generic renderers
  (double-quoted pg array elements re-parsed by `::T[]`; quoted CH literals
  coerced to the column type).
- pg TEXT keys are ~4x costlier than ints bare at 2048 (collation-aware
  btree comparisons + bigger literals); DATE/TIMESTAMP price like ints.
- pg `= ANY($1)` vs `IN (literals)` are EQUAL execute-only for both int and
  text — the array form stays (smaller text, one node).
- CH is type-insensitive (hash set; string hashing ~free); the composite zip
  beats tuple literals ~2x on mixed types too.
- Through our code every type lands in the same 3-5 ms/batch band: per-batch
  transport overhead dominates and type effects (~0.1 ms at 103 keys) vanish —
  no type-specific hotspots in the render/probe path.

### 7e. Proper candidate grid — one metric, stated reps, both key counts

**Method.** One host, one moment, both engines in docker on localhost.
Bare = SERVER-SIDE wall time per query: pg from `log_min_duration_statement=0`
durations of EXECUTE on statements PREPAREd once per session with
`force_generic_plan` (pure execute, plan cached); CH from
`system.query_log` `query_start_time_microseconds -> event_time_microseconds`
on one connection per candidate (CH has no plan cache; parse is inherently
part of every CH execute). 210 executions per cell, first 10 discarded,
p50/p90 over the remaining 200. Key counts: 103 (the real batch size the
engine emits at gul density) and 2048 (max batch). "Our code" = client wall
of the full gul query (FTS + 243 batches + probe), 7 runs, p50/p90;
per-batch = p50/243.

**Postgres — server-side, ms** ([ship] = form our code emits)

| candidate | p50 @103 | p90 @103 | p50 @2048 | p90 @2048 |
|---|---|---|---|---|
| ctid `IN (tid literals)` | 0.013 | 0.013 | 0.207 | 0.210 |
| ctid `= ANY($1::tid[])` [ship] | 0.018 | 0.018 | 0.299 | 0.305 |
| ctid unnest-IN | 0.045 | 0.049 | 0.839 | 0.854 |
| int `IN (literals)` | 0.050 | 0.051 | 0.951 | 0.969 |
| int `= ANY($1)` [ship] | 0.054 | 0.056 | 0.986 | 1.009 |
| int unnest-IN | 0.079 | 0.081 | 1.562 | 1.582 |
| text `IN (literals)` | 0.197 | 0.203 | 3.899 | 4.018 |
| text `= ANY($1)` [ship] | 0.202 | 0.205 | 3.955 | 4.015 |
| text unnest-IN | 0.215 | 0.218 | 4.327 | 4.685 |
| date `IN (literals)` | 0.050 | 0.051 | 0.964 | 0.991 |
| date `= ANY($1)` [ship] | 0.059 | 0.062 | 1.152 | 1.241 |
| date unnest-IN | 0.086 | 0.092 | 1.676 | 1.855 |
| ts `IN (literals)` | 0.052 | 0.054 | 0.949 | 0.979 |
| ts `= ANY($1)` [ship] | 0.063 | 0.065 | 1.198 | 1.214 |
| ts unnest-IN | 0.090 | 0.094 | 1.756 | 1.777 |
| comp row-IN literals | 0.243 | 0.260 | 45.197 | 46.661 |
| comp `IN (VALUES)` | 0.169 | 0.173 | 16.565 | 17.234 |
| comp unnest zip [ship] | 0.177 | 0.181 | 3.332 | 3.364 |

**ClickHouse — server-side, ms**

| candidate | p50 @103 | p90 @103 | p50 @2048 | p90 @2048 |
|---|---|---|---|---|
| int `IN (literals)` | 0.884 | 1.000 | 3.454 | 3.962 |
| int `IN [array]` [ship] | 0.856 | 0.965 | 2.854 | 3.548 |
| int `IN {param}` | 0.888 | 1.005 | 2.170 | 2.573 |
| int zip subquery | 1.032 | 1.118 | 2.844 | 3.280 |
| str `IN (literals)` | 0.910 | 0.993 | 3.774 | 4.526 |
| str `IN [array]` [ship] | 0.886 | 0.985 | 2.922 | 3.442 |
| str `IN {param}` | 0.900 | 0.980 | 2.274 | 2.520 |
| str zip subquery | 1.132 | 1.272 | 3.421 | 4.031 |
| dt `IN (literals)` | 0.882 | 1.000 | 3.974 | 4.514 |
| dt `IN [array]` [ship] | 0.811 | 0.923 | 3.050 | 3.596 |
| dt `IN {param}` | 0.882 | 0.973 | 1.902 | 2.183 |
| dt zip subquery | 1.170 | 1.305 | 4.080 | 4.574 |
| comp `IN [(tuples)]` | 3.634 | 4.013 | 13.404 | 14.340 |
| comp ARRAY JOIN zip [ship] | 2.042 | 2.273 | 6.541 | 7.159 |
| comp `arrayJoin(arrayZip(..))` | FAILED * | | 7.483 | |

\* type inference broke at 103 keys (small Int64 literals inferred UInt8 ->
tuple type mismatch, CANNOT_CONVERT_TYPE). The parallel ARRAY JOIN form
handled identical input at both sizes — the arrayZip spelling is
type-fragile and out of the running.

**Our code — full gul query (25k docs, 243 batches), client wall, 7 runs**

| index kind | total p50 | total p90 | per batch |
|---|---|---|---|
| pg ctid | 997 ms | 1034 | 4.10 ms |
| pg BIGINT | 898 | 916 | 3.69 |
| pg TEXT | 1034 | 1037 | 4.25 |
| pg DATE | 913 | 922 | 3.76 |
| pg TIMESTAMP | 934 | 937 | 3.84 |
| pg composite (TEXT,BIGINT) | 1264 | 1280 | 5.20 |
| CH Int64 | 1201 | 1213 | 4.94 |
| CH String | 1321 | 1348 | 5.44 |
| CH DateTime | 1233 | 1244 | 5.07 |
| CH composite (String,Int64) | 984 | 994 | 4.05 |

Takeaways at the REAL batch size (103 keys):

- pg executes every shipped form in 0.018-0.20 ms server-side — i.e. under
  5% of our per-batch cost; even comp row-IN is fine at 103 (0.24 ms) and
  only detonates at 2048 (45 ms). Shape choice matters at large batches,
  which is exactly where accumulation will take us — the shipped forms are
  chosen for the 2048 end.
- CH single-key forms are all ~0.85-0.9 ms at 103 (the engine's per-query
  floor dominates); differentiation appears at 2048 where `{param}` wins
  (int 2.17 / str 2.27 / dt 1.90 vs [array] 2.85/2.92/3.05) — ~25-38%
  under the shipped array literal. `params :=` support in clickhouse_query
  is the quantified next transport step.
- CH composite ARRAY JOIN zip beats tuple literals ~1.8x at BOTH sizes
  (2.04 vs 3.63 @103; 6.54 vs 13.40 @2048).
- Our-code per batch (3.7-5.4 ms) vs bare-at-103 (0.02-2.0 ms): 60-95% of
  every batch is transport + engine floor + our machinery, not the lookup
  query — the flattening that makes batch accumulation (section 8) the
  dominant lever.

## 8. Current per-batch anatomy and remaining levers

Current shipped queries: pg `ctid = ANY('{(p,t),..}'::tid[])` /
`key = ANY('{..}'::T[])` / unnest-zip for composite; CH `IN [..]` /
tuple-IN for composite, with `schema_query` (constant `WHERE 0` variant)
feeding a catalog-level DESCRIBE cache.

CH per-batch ~3.6-4.9 ms = CH per-query floor (~1.0 ms) + protocol round trips
+ 12KB text parse (~1-1.5 ms) + duckdb glue + pool lease ping (0.33 ms).
pg per-batch ~1.74 ms = statement parse (~0.5-1 ms) + plan + NL probes + glue.

Levers, ranked:

1. **Batch accumulation** — `HitBatcher::CloseGroup`'s dense fast path emits
   each columnstore row group (~103 hits at gul density) as its own batch:
   right for local reads (saves a scatter-copy), inverted for remote lookups
   (costs a 2-5 ms query). Flag to accumulate to 2048 => 245 -> 13 queries:
   gul CH ~1.2 s -> ~0.2 s, pg ~0.4 s -> ~0.1 s.
2. `postgres_query` statement caching / `$n` params (constant text): ~0.5-1 ms/batch.
3. Pool health-check freshness window (skip `Ping` on recent return): 0.33 ms/batch.
4. Raw child-vector render/probe in Materialize (kill Value boxing): ~1 ms/batch on composite.
