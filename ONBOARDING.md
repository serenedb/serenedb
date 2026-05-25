# Onboarding — `pashandor789/dropscanorderscoretopk`

Hand-off note for the next Claude (or human) picking up this branch.
Pair this with `CLAUDE.md` (general project notes) and `CONTRIBUTING.md`
(build/style/PRs).

## What this branch is for

Restructure the iresearch (inverted-index) scan layer:

1. **One table function, not four.** Collapse `iresearch_scan` /
   `iresearch_count` / `iresearch_ann_scan` / `iresearch_ann_range_scan`
   into a single `CreateIResearchScanFunction`. Runtime dispatch happens
   on `bind_data.scan_source->Kind()` (`FullTable` / `Search` / `Count` /
   `Ann` / `RangeSearch`). Optimizer rules no longer swap `get.function`
   — they only swap `scan_source`.
2. **Parallelise BM25 top-K** using the same external-heap pattern ANN
   already had. Each worker keeps its own `NthPartitionScoreCollector`
   over a slice of `gstate.hits`; a shared `global_kth_score` atomic
   feeds WAND threshold pruning across threads.
3. **No in-scan merge / sort.** When `LogicalTopN` is preserved above
   the scan, the scan never sorts globally — each thread emits its own
   unsorted slice; DuckDB's `LogicalTopN` does the global merge. This
   rule applies to both BM25 (`SearchFullScanFunction`) and ANN
   (`SearchAnnScanFunction`) top-K paths.
4. **Simplify `iresearch_plan.cpp`.** Many ex-rules moved into runtime
   callbacks (`set_top_n_hint`, `pushdown_complex_filter`) or got
   replaced by `ExpressionIterator::EnumerateChildren` recursions.

## Build, test, launch

### Compile

```bash
cd build && ninja serened
# or: cmake --build build --target serened
```

For iresearch unit tests:

```bash
cmake --build build --target iresearch-tests
./build/bin/iresearch-tests --gtest_filter='*doc_collector*:*block_scoring*'
```

### sqllogic (production regression)

Always pass `--fast` (see `feedback_test_runner`):

```bash
# Need a running serened on --single-port -- spin one up first:
rm -rf /tmp/serened_data && mkdir -p /tmp/serened_data
./build/bin/serened /tmp/serened_data \
  --server.endpoint='pgsql+tcp://0.0.0.0:7890' &

# In another shell (or after a `sleep 6`):
./tests/sqllogic/run.sh --fast true \
  --test ./tests/sqllogic/sdb/pg/index/inverted_index_topk.test \
  --test ./tests/sqllogic/sdb/pg/index/inverted_index_wand.test \
  --single-port 7890 --jobs 4
```

Branch-relevant tests to keep an eye on:

- `inverted_index_topk.test`, `inverted_index_topk_include.test`
- `inverted_index_wand.test`
- `inverted_index_score.test`, `inverted_index_score_layout.test`
- `inverted_index_ann_include.test`, `inverted_index_hnsw_nulls.test`
- `inverted_index_optimize_top_k.test`, `inverted_index_range_include.test`
- `inverted_index_view*.test` (view-backed scan path)
- `inverted_index_explain_source.test` (EXPLAIN node names)
- `offsets.test`, `geo_search.test`

### Smoke server

Pick a free port (don't reuse 5432 / 7890 if already taken). Always
clean up afterwards:

```bash
./build/bin/serened ./build_data \
  --server.endpoint='pgsql+tcp://0.0.0.0:7890' \
  --log.foreground-tty=true &
psql -h 127.0.0.1 -p 7890 -U postgres -d postgres
# when done:
kill -9 $(lsof -t -i:7890) 2>/dev/null
rm -rf build_data
```

## What changed (file map)

### Unified dispatch
- `server/connector/duckdb_table_function.{h,cpp}` — one
  `CreateIResearchScanFunction`; `IResearchScanInitGlobal/InitLocal/Function`
  switch on `scan_source->Kind()`. `IsCountOnlyScan` + `PromoteToCountScan`
  detect the count(*) shape at runtime (replaces the old
  `TryConvertAggregateToCount` optimizer rule).
- `server/connector/duckdb_search_full_scan.{hpp,cpp}` — parallel BM25
  top-K with `SearchFullScanLocalState`, per-thread
  `NthPartitionScoreCollector`, sliced `gstate.hits`, atomic
  `global_kth_score`, `parallel_topk` flag, `MaxThreads` override.
- `server/connector/duckdb_search_ann_scan.{h,cpp}` — `MergeResult`
  deleted (~80 lines), last-thread barrier gone; per-thread
  `PostProcessLocal` + `EmitLocal`. Range-scan emit mutex dropped.
- `server/connector/duckdb_scan_base.{hpp,cpp}` — `cs_materializers_mu`
  mutex on lazy materializer build; `ClaimNextLiveSegment` promoted out
  of the anon namespace.
- `server/connector/duckdb_index_scan_entry.cpp` —
  `ViewInvertedIndexScanEntry::GetVirtualColumns()` now exposes
  `COLUMN_IDENTIFIER_EMPTY` (the root fix that lets `RemoveColumnsFromLogicalGet`'s
  `GetAnyColumn()` placeholder land on EMPTY rather than a real column).

### Optimizer collapse
- `server/connector/optimizer/iresearch_plan.{h,cpp}` — one optimizer
  extension (`RewriteSearchCallsToColumnRefs`, anchored at
  `UNUSED_COLUMNS - Before`). Public API is just 4 entry points:
  `IresearchPushdownComplexFilter`, `RegisterIresearchPlanOptimizer`,
  `BindingIsScoreColumn`, `TryAttachAnnTopK`.
- `server/connector/optimizer/iresearch_ann_topk.cpp` — **deleted**,
  folded back into `iresearch_plan.cpp` (it depended on a half-dozen
  `sdb::optimizer` helpers that lived there, and a separate header
  `iresearch_plan_internal.h` only existed to bridge them).
- `server/connector/optimizer/iresearch_plan_internal.h` — **deleted**.

Rules removed:
- `TryConvertAggregateToCount` + `IsCountStarLikeAggregate` (~90 lines).
- `ReconcileSearchOffsets` (~50 lines).
- `Walk` / `WalkOrder` template (was already dead).
- `PushdownAnnTopK` (replaced by `set_top_n_hint` callback + `TryAttachAnnTopK`).
- `RewriteScoreCallInChildren` 73-line hand-rolled expression-class
  switch — collapsed into `ExpressionIterator::EnumerateChildren`.
- `SimplifyScoreGtZero`'s same hand-rolled switch — same treatment.

Renames:
- `LowerToSearchScan` → `RewriteSearchCallsToColumnRefs` (clearer:
  this is the optimizer extension that rewrites bm25/ts_offsets calls
  in expressions into BoundColumnRefs to synthetic scan columns).
- `NthPartitionScoreCollector::Finalize()` → `TotalMatches()` (was just
  `return _count;` — the sort step is gone, the name should reflect
  what it returns).

### Vendored DuckDB
- `third_party/duckdb` — `set_top_n_hint` table-function callback +
  `TopNHintPushdown` rule. The hook fires inside `TopNHintPushdown`,
  which runs before our `RewriteSearchCallsToColumnRefs` optimizer
  extension.

## Mental model — flow of a query

```
SQL ─┐
     │ DuckDB bind
     ▼
LogicalGet(SereneDB, scan_source = FullTable)   ← default at bind time
     │
     │ (a) pushdown_complex_filter callback:
     │     IresearchPushdownComplexFilter
     │     - text predicates → SearchScan
     │     - ANN range predicate → RangeSearch
     ▼
LogicalGet(SereneDB, scan_source = Search | RangeSearch | FullTable)
     │
     │ (b) DuckDB optimiser: TopNHintPushdown matches
     │     LogicalTopN > Project > [Filter*] > LogicalGet
     │     and calls set_top_n_hint (SereneDBSetTopNHint):
     │     - FullTable + distance ORDER BY → TryAttachAnnTopK
     │                                       (swaps scan_source → ANNScan,
     │                                        absorbs claimable text
     │                                        conjuncts into ann.text_filter)
     │     - Search + DESC on score column  → stamp search_scan.score_top_k = K
     ▼
LogicalGet(scan_source = Search | RangeSearch | FullTable | Ann)
     │
     │ (c) RewriteSearchCallsToColumnRefs (optimizer extension,
     │     anchored UNUSED_COLUMNS - Before): bm25(...) / ts_offsets(...)
     │     calls in expressions → BoundColumnRef to synthetic
     │     score / offsets column on the scan.
     ▼
LogicalGet ... [UNUSED_COLUMNS prunes synthetic cols nobody reads]
     ▼
Physical: SereneDBPhysicalTableFunction → CreateIResearchScanFunction
     ▼ dispatch on scan_source->Kind():
       FullTable / Search → SearchFullScan{InitGlobal,InitLocal,Function}
                            (BM25 streaming or parallel top-K)
       Count              → SearchCountScan{InitGlobal,Function}
       Ann                → SearchAnnScan{InitGlobal,InitLocal,Function}
       RangeSearch        → SearchRangeScan{InitGlobal,InitLocal,Function}
```

## Rules / gotchas

- **No in-scan merge / sort.** When `LogicalTopN` sits above the scan,
  the scan must NOT sort globally. Each thread emits its own slice,
  unsorted. The upstream `LogicalTopN` does the global merge.
  (See `~/.claude/projects/-home-ivanovp-serenedb/memory/feedback_no_in_scan_merge.md`.)
- **`ColumnReader::ScanCursor` is forward-only.** Before materialising
  from the columnstore, sort the per-thread hit slice by `(segment_idx, doc)`.
- **`cs_materializers` lazy build is mutex-guarded.** Multiple workers
  can first-touch the same segment slot; without the mutex this races
  on `vector::resize()` and `make_unique<...>()`.
- **No unnecessary `static_cast`** — only what the compiler requires
  (`feedback_no_unnecessary_casts`).
- **`--fast` on sqllogic** by default (`feedback_test_runner`).
- **EXPLAIN baselines** for inverted-index tests show `IRESEARCH_SCAN`
  uniformly (one table function name), not `IRESEARCH_COUNT` /
  `IRESEARCH_ANN_SCAN` / `IRESEARCH_ANN_RANGE_SCAN`. If you re-add a
  rule that splits these, expect cascading `.test` baseline diffs.
- **`set_top_n_hint` is the right place for TopN-above-Get rewrites.**
  Don't walk the plan yourself from inside an optimizer extension —
  DuckDB's `TopNHintPushdown` already matches the shape and hands you
  `(LogicalTopN, LogicalGet, bind_data)`.
- **`ExecuteTopK*` in `libs/iresearch/.../doc_collector.hpp` is
  test-only.** It still sorts internally (it has no `LogicalTopN`
  above it). The production scan path does NOT go through it; the
  no-in-scan-sort rule isn't violated.

## Current state

All target sqllogic + iresearch unit tests pass at HEAD of the
working tree:

- 33/33 `iresearch-tests` (doc_collector, block_scoring).
- 15/15 inverted-index sqllogic (topk, topk_include, score,
  score_layout, wand, view, view_pruning, view_variants,
  explain_source, range_include, optimize_top_k, ann_include,
  hnsw_nulls, offsets, geo_search).

Nothing in-flight from the most recent session.
