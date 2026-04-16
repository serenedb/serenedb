## Optimizer / scan rework -- next plans

What's left after the optimisation-stage work in
[OPTIMIZER_COVERAGE.md](OPTIMIZER_COVERAGE.md). Organised by phase.
Each bullet is tagged with its rough priority for runtime-stage work.


### Small TODO
#### Better naming

Better naming, think what function exists and rename to something like this
- rocksdb_primary_fullscan
- rocksdb_secondary_fullscan
- rocksdb_primary_points_lookup
- rocksdb_primary_ranges_scan
- rocksdb_secondary_points_lookup
- rocksdb_secondary_ranges_scan
- iresearch_fullscan
- iresearch_lookup
- iresearch_scores_topk_scan
- iresearch_ann_ranges_scan
- iresearch_ann_topk_scan

Avoid strategy field? Just fill additional strings, like score function, offsets/etc

#### Secondary index and NULLs

Check nulls for rocksdb rule with secondary index on nullable columns (cannot be point lookup and is_null special handling).


---

## Runtime executors (blocker for correctness of specialised scans)

The specialised scans pass the optimisation-stage contract --
EXPLAIN shows the right strategy and claim info -- but all of them
still dispatch to the full-table loop at runtime. Flipping them to
their actual implementation is the biggest remaining chunk, and it's
where the "rules never drop predicates" safety valve in
[rocksdb_plan.cpp](server/connector/optimizer/rocksdb_plan.cpp) will
finally be removed (so EXPLAIN stops showing a redundant `FILTER`
above the specialised scan).

### rocksdb-side

- **`pk_point`** -- RocksDB MultiGet over `ResolvedPoint` -> PK bytes.
  Bind data already carries the resolved points; encode to keys at
  `init_global` and batch-MultiGet per chunk. Port the Velox-era
  `MultiGetContext` / `PointLookupPKColumnBuilder` /
  `FillColumnFromMultiGet` from `data_source.cpp` -- they just need
  Velox `RowVector` -> DuckDB `DataChunk` / `Vector` swap. Once live,
  drop the matched equality expressions from the surrounding
  `LogicalFilter` in the rule.
- **`pk_range`** -- bounded prefix iterator. For each `ResolvedRange`,
  compute `[begin_bytes, end_bytes)` from prefix values + the trailing
  `ColumnRange` bound. Multi-range case iterates per range. Same
  filter-drop cleanup as point.
- **`sk_point` / `sk_range`** -- SK probe -> PK list -> MultiGet.
  Executor path splits into two passes (SK iterator -> PK stream ->
  MultiGet). `SkPointScan` / `SkRangeScan` carry `shard_id` +
  `is_unique` so the executor knows which shard to probe and whether
  early-terminate on a single hit.
- **Mutation-via-index** (Phase 8 in my old plan): once `sk_point` /
  `sk_range` execution lands, `DELETE WHERE sk_col = 5` can drive the
  delete sink through the SK path. Requires a `SupportsIndexColumnBinding()`
  virtual (default false, override on `DuckTableEntry` and
  `SereneDBTableEntry`) plus patches at
  `third_party/duckdb/src/planner/binder/statement/bind_delete.cpp:79`
  and `bind_merge_into.cpp:399` switching from `IsDuckTable()` to the
  new virtual.

### iresearch-side

- **`fulltext_score` / `fulltext_topk`** -- construct a real
  `irs::Scorer` (default BM25 with k1=1.2, b=0.75) at rule time so
  the runtime doesn't need the "sentinel pointer" bridge. Plumb
  `score_top_k` into `iresearch_search_query.execute(...).top_k(N)`
  at scan time. Rewrite the `sdb_score(tableoid)` projection
  expression to a `BoundReferenceExpression` reading the scan's
  score output column so the stub function never actually runs.
- **`iresearch_search+offsets`** -- emit one `LIST(BIGINT)` column per
  `OffsetsRequest`; alternate start/end values. Rewrite the
  `sdb_offsets(col)` projection expression to a `BoundReferenceExpression`
  onto that column. Reuse the Velox-era `OffsetsCollector`
  (`search_scan_data_source.cpp`) with a DuckDB `ListVector` sink
  instead of Velox.
- **`ann_topk` / `ann_range`** -- wire the HNSW scan at runtime using
  `bind_data.ann.index_id` + `GetSereneDBContext.EnsureSearchSnapshot(id)`
  to resolve the shard lazily (cleaner than capturing the
  `shared_ptr<shard>` at plan time). Needs **real ARRAY indexing**
  (below) to produce useful results.

### Scorer / tokenizer parameterisation

Right now `sdb_score(tableoid)` is parameterless and always resolves
to default BM25. Follow-ups when the SQL surface lands:

- `sdb_score(tableoid, 'bm25', k1, b)` -> tuned BM25.
- `sdb_score(tableoid, 'tfidf')` -> TF-IDF.
- The rule stores the chosen scorer directly in
  `SearchScan.scorer` (no sentinel) and derives
  `scorer_description` from it.

The plan is to keep `sdb_score` as one polymorphic function with
scorer kind as a string literal argument -- opaque `irs::Scorer` on
the bind-data side; no per-scorer variant.

---

## Indexing side (blocks ANN runtime correctness)

### HNSW vector indexing

`CREATE INDEX ... USING inverted(..., emb hnsw)` currently succeeds
but the writer path
([search_sink_writer.cpp::SwitchColumnImpl](server/connector/search_sink_writer.cpp))
accepts `LogicalTypeId::ARRAY` only when the child type is FLOAT.
`SetupColumnWriter<ARRAY>` + `Field::PrepareForVectorValue` +
`WriteVectorValue` wire the insert path into iresearch's HNSW
directory format -- this is in place on `main` already. What's
missing: runtime-side HNSW scan execution and validation that ANN
topk / range queries return ordered results. End-to-end ANN tests
can only ship once the runtime executor is wired.

### Vector-native writer path (drop Kind templates)

Current writer dispatches through a `LogicalTypeId Kind` template
(`SwitchColumnImpl` -> `SetupColumnWriter<Kind>`). Pattern should
move to duckdb::Vector-native access with fast paths for flat /
constant and `UnifiedVectorFormat` fallback, per
`feedback_vector_fast_paths` (saved memory). Consolidates: the
per-kind template tree collapses to direct physical-type reads
off the `Vector`, and logical-type + op-class decide tokenizer
choice (not a single enum). This is a big but self-contained
rework -- best done after the HNSW runtime lands so both pieces
move together.

---

## Plan-node cleanups

### Drop `FILTER` above specialised scans

Once every specialised scan actually honours its claims, flip the
TODO in `rocksdb_plan.cpp` / pushdown spots:

```cpp
// TODO(phase2/4-follow-up): once the scan executor honours the
// specialised scan sources, claim the matched predicates here:
//   filter.expressions.clear();
//   if (result.remaining_filter) {
//     filter.expressions.push_back(std::move(result.remaining_filter));
//   }
//   if (filter.expressions.empty()) {
//     plan = std::move(filter.children[0]);
//   }
```

The iresearch rule already does this; the rocksdb rule doesn't,
because its executors are still stubs.

### Delete `SereneDBPushdownComplexFilter` stub

Currently a no-op under `#if 0` in
[duckdb_table_function.cpp](server/connector/duckdb_table_function.cpp).
Once the rule handles everything (it already does the phrase /
term / ANN / range claims), drop the callback registration entirely
and delete the `#if 0` block. Needs one grep pass to make sure
nothing else expects the pushdown-side claim.

### Delete the old scan-source variant cases

After the iresearch rule fully subsumes the inline pushdown path,
`ScanSource = variant<FullTableScan, SearchScan, SecondaryIndexScan,
ANNScan, RangeSearchScan, PkPointScan, PkRangeScan, SkPointScan,
SkRangeScan>` can lose `SecondaryIndexScan` (replaced by
`SkPointScan{shard_id, is_unique, ...}` or the empty-filter SK
range), and the giant
`SereneDBScanFunction` dispatcher inside `duckdb_table_function.cpp`
shrinks to a table of function-name -> executor pointers.

### System-table scan function (Phase 7)

`SystemTableEntry` (pg_catalog / information_schema) currently
routes through bespoke `SystemTableInit` / `SystemTableScan` /
`SystemTableGetBindInfo` callbacks. Migrate to
`CreateSystemScanFunction()` so it shares the same callback surface
as the rest of the connector. Doesn't affect rule coverage, but
cleans the connector layer.

### Lazy virtual-column emission (Phase 6)

Today `tableoid` is always written and `rowid` is padded with
dummy values. After the refactor: each function inspects
`input.column_ids` / `input.projection_ids` and writes `rowid` /
`tableoid` only when referenced. One shared helper used across
specialised functions.

---

## Deferred / design-level follow-ups

### PK rowid specialisation

Today `rowid` is always `LogicalType::BLOB`. Follow-ups:

- `LogicalType::BIGINT` when the PK fits a single int64 (single
  `int4` / `int8` / OID-like column).
- Narrower int widths (1 / 2 / 4 / 16 bytes) where the PK column's
  range is known.
- `ARRAY<UTINYINT, N>` for fixed-width composite PKs so mutation
  sinks can hash the PK without a heap allocation.
- Decoding individual PK columns from rowid bytes so late-materialise
  paths don't have to re-read columns.

All deferred until the specialised-scan executors land -- they'll
shape exactly how the rowid is produced per strategy.

### Parallel scan within a strategy

All specialised scans are single-threaded today
(`order_preservation_type = FIXED_ORDER`, one worker). Parallel
execution requires `get_partition_data` / `get_partition_stats` on
the TableFunction. Deferred.

### Filter-combiner pushdown hook

`TableFunction::pushdown_expression` is left null everywhere.
Revisit when `sdb_score` / `sdb_offsets` projections need to be
pushed through a filter combiner (e.g. for distance-column passthrough
on ANN).

### Late materialisation

`late_materialization = true` + a dedicated materialize-by-rowid
operator would let DuckDB rewrite
`scan(LHS: rowid) -> semi-join -> scan(RHS: full)` in two passes.
Deferred because the current RHS re-scan is a full RocksDB prefix
iteration, which is wrong for us -- we'd need a MultiGet-based
materialise operator first.

### Cost-model-based rule selection

`rocksdb_plan.cpp::StrictlyBetter` is a static-preference tiebreak
(points > ranges; fewer remaining filters; PK > SK). Once we have
stats (SK histograms, iresearch term stats), promote to a real cost
model. Not urgent -- current preference catches the common cases.

### `supports_pushdown_extract` for iresearch columnstore

Reserved for a future where the iresearch columnstore serves
individual column reads directly (skip the rocksdb MultiGet layer
when the inverted index already stores the value).

### FK-binding virtual

`bind_create_table.cpp:590` uses `IsDuckTable()` for a FK check.
Revisit once FK support lands in SereneDB. Low priority.

---

## Plan-shape limitations (by design, might revisit)

- **OR handling in the rocksdb rule** is solid (sweep-based merge
  produces disjoint ranges / points for `pk IN / OR` with overlap
  resolution and contiguous-coverage -> full-scan detection).
- **Cross-table iresearch claims** aren't attempted -- the rule only
  fires on a single-table `LogicalGet`. Joined-table search (e.g.
  `SELECT ... FROM a JOIN b ON a.id = b.id WHERE sdb_phrase(b.text, ...)`)
  works only because iresearch claims inside `b`'s scan; nothing
  special needed.
- **CREATE INDEX re-use** in rule: the iresearch rule auto-detects the
  first inverted index on the table when `FROM table_name` is used.
  It doesn't choose among multiple inverted indexes -- the first one
  wins. Fine as long as tables have at most one inverted index
  (current assumption). Revisit if we support multiple.
