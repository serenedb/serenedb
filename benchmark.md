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

### 7e. Definitive grid: every lookup kind x every candidate, bare + our code

Single stack, single moment (both engines in docker, 10 indexes). Bare pg =
one session, server-side PREPARE, `force_generic_plan`, 2048 keys, median of
10 executes after warm-up (EXECUTE-only). Bare CH = `clickhouse-benchmark`
-i 200 on a persistent connection (param rows: 20x `clickhouse-client
--time`, 1 ms resolution; CH has no prepared plans — parse is part of every
CH execute). "Our code" = gul query wall / 243 batches of ~103 keys, median
of 5 runs — includes FTS, emit, transport, probe. [ship] = the form our code
emits; our-code numbers exist only for shipped forms.

**Postgres**

| kind | candidate | bare, 2048 keys | our code, per batch |
|---|---|---|---|
| ctid | `IN (2048 tid literals)` | 0.247 ms * | |
| ctid | `= ANY($1::tid[])` [ship] | 0.334 ms | **4.00 ms** |
| ctid | `IN (SELECT * FROM unnest($1))` | 0.891 ms | |
| BIGINT | `IN (literals)` | 1.156 ms | |
| BIGINT | `= ANY($1::int8[])` [ship] | 1.015 ms | **3.62 ms** |
| BIGINT | unnest-IN | 1.812 ms | |
| TEXT | `IN (literals)` | 4.414 ms | |
| TEXT | `= ANY($1::text[])` [ship] | 4.199 ms | **4.18 ms** |
| TEXT | unnest-IN | 4.414 ms | |
| DATE | `IN (literals)` | 1.258 ms | |
| DATE | `= ANY($1::date[])` [ship] | 1.183 ms | **3.68 ms** |
| DATE | unnest-IN | 1.784 ms | |
| TIMESTAMP | `IN (literals)` | 1.255 ms | |
| TIMESTAMP | `= ANY($1::timestamp[])` [ship] | 1.240 ms | **3.80 ms** |
| TIMESTAMP | unnest-IN | 1.820 ms | |
| composite (TEXT,BIGINT) | row-IN literals | 46.325 ms | |
| composite | `IN (VALUES ...)` | 17.038 ms | |
| composite | unnest zip [ship] | 4.101 ms | **5.15 ms** |

\* the ctid literal form is bimodal across table shapes: 0.25 ms here (this
wider table costed Tid Scan under the seq scan) but 8.1 ms on the narrow
table of section 3, where the seq-scan cliff bakes into the cached plan.
`= ANY($1)` plans Tid Scan regardless of key count — shipped for stability.

**ClickHouse**

| kind | candidate | bare, 2048 keys | our code, per batch |
|---|---|---|---|
| Int64 | `IN (v,v,..)` literals | 3.832 ms | |
| Int64 | `IN [array]` [ship] | 3.139 ms | **4.90 ms** |
| Int64 | `IN {ks:Array(Int64)}` | **2.20 ms** (200 reps, us-precise) | |
| Int64 | zip subquery | 3.224 ms | |
| String | `IN (..)` literals | 3.972 ms | |
| String | `IN [array]` [ship] | 3.209 ms | **5.31 ms** |
| String | `IN {param}` | **2.39 ms** (200 reps, us-precise) | |
| String | zip subquery | 3.602 ms | |
| DateTime | `IN (..)` literals | 4.059 ms | |
| DateTime | `IN [array]` [ship] | 3.391 ms | **4.98 ms** |
| DateTime | `IN {param}` | ~3 ms (20x --time only) | |
| DateTime | zip subquery | 4.241 ms | |
| composite (String,Int64) | `IN [(v,v),..]` tuples | 12.419 ms | |
| composite | parallel ARRAY JOIN zip [ship] | 5.765 ms | **4.00 ms** |
| composite | `arrayJoin(arrayZip(..))` | 7.483 ms | |

Grid takeaways:

- Every shipped form is the best or within noise of the best in its group on
  its engine; the one deliberate trade is ctid `= ANY` vs the bimodal literal
  form (stability over best-case).
- pg: unnest-IN costs a consistent +0.6-0.8 ms on single keys (set dedup +
  join plumbing) — hence the single-key/composite split. TEXT keys price the
  same (~4.2-4.4 ms) on every form: collation-aware btree comparison
  dominates, transport shape is irrelevant.
- CH: `IN [array]` beats element-wise literals by ~0.6-0.8 ms on every type;
  `{param}` BEATS the array literal by a further ~0.8 ms (~27%) — re-measured
  at 200 reps with us-precise server-side timing (query_log
  query_start_time_microseconds -> event_time_microseconds): int 2.20 vs
  3.02 ms avg, str 2.39 vs 3.06 — the param value bypasses SQL grammar
  parsing entirely. Strongest single argument for adding `params :=` to
  clickhouse_query (also makes the per-batch query text constant). Composite
  zip is ~2.2x the tuple literals; the arrayZip spelling sits between.
- Our-code per batch sits 3.6-5.3 ms across ALL kinds while bare spans
  0.25-5.8 ms — per-batch transport overhead flattens everything, which is
  why batch accumulation (section 8) dominates every remaining
  per-query-shape optimization. Note our composite CH batch (4.00 ms) runs
  FASTER than its 2048-key bare form (5.77 ms): batches carry only ~103 keys.

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
