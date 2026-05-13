# TODO_NEXT -- new columnstore review

Scope: `libs/iresearch/include/iresearch/columnstore/`, the integration in
`server/connector/columnstore_materializer.*`, the new code in
`duckdb_search_full_scan.cpp`, `segment_writer.{cpp,hpp}`,
`segment_reader_impl.cpp`, `merge_writer.cpp`, and the DuckDB-side patches
in the WIP commit. Bug-fixes outside columnstore (catalog/pg_comm_task/
transactions/etc.) are out of scope.

---

## Bugs (sure)

- **HNSW graphs are not rebuilt on merge.** `columnstore::MergeInto`
  ([`merge.cpp:38`](../libs/iresearch/include/iresearch/columnstore/merge.cpp))
  iterates only `Reader::Columns()` (typed columns). The ARRAY column data is
  transferred, but `cs_writer->AttachHNSW(...)` is never called on the output
  writer. After consolidation the merged segment's footer slot 102 is empty;
  `Reader::HNSW(id)` returns nullptr and ANN queries against the merged
  segment fail or fall back wrongly. Fix: in `MergeInto`, when a source has
  an HNSW reader for a field, AttachHNSW on the output and let Writer::Commit
  rebuild the graph from the just-written ARRAY data. Add merge+ANN test.

## Performance (sure)

- **HNSW build's PersistentColumnData clone via serialize-then-deserialize
  round-trip** ([`format.cpp:392-413`](../libs/iresearch/include/iresearch/columnstore/format.cpp)).
  `DataPointer` is move-only, so the code MemoryStream-serializes the whole
  column entry and deserializes it to get a copy. Cost is one full footer
  walk per HNSW column at commit time. Add a `PersistentColumnData::Clone()`
  (DataPointer clone = `stats.Copy()` + re-call `serialize_state(segment)`)
  or restructure HNSW build to operate on the in-flight writer state
  directly without a clone.
- **`HNSWReader::Search` builds a fresh `ChunkedVectorCache` +
  `ColumnDistance` per call** ([`hnsw.cpp:441-443`](../libs/iresearch/include/iresearch/columnstore/hnsw.cpp)).
  Each ctor allocates an s3_fifo Cache, two NodeHashMaps, the pin stack,
  and (lazily) a 2 MB chunk slot region. For per-segment ANN queries
  this is paid per query. Move cache to a thread-local or member so
  warmup amortises across queries.
- **HNSW graphs are eagerly faiss-deserialised at every Reader construction**
  ([`format.cpp:558-585`](../libs/iresearch/include/iresearch/columnstore/format.cpp)).
  Queries that never touch HNSW pay this. Make HNSW lazy -- only
  deserialise on first `HNSW(field_id)` call. This also fixes the
  MergeWriter-throws-away-HNSW waste (next item).
- **MergeWriter opens per-source `columnstore::Reader` which eagerly
  faiss-deserialises every HNSW graph** ([`merge_writer.cpp:754-756`](../libs/iresearch/include/iresearch/index/merge_writer.cpp))
  and then throws them away -- HNSW is rebuilt from scratch on the merged
  data. One full HNSW deserialise per source per merge, wasted. Lazy
  HNSW (above) fixes it; or add a "skip HNSW" Reader flag.

## Simplifications (sure)

- **`HNSWInfo` footer fields are written/read field-by-field across 8
  `WriteProperty`/`ReadProperty` calls** ([`format.cpp:474-484,628-638`](../libs/iresearch/include/iresearch/columnstore/format.cpp)).
  Either give `HNSWInfo` a `Serialize`/`Deserialize` or use a
  `WriteProperty(0, "info", info)` once with the inspector specialisation.
  (HNSW item, owned separately.)
---

## Bugs (notsure)

- **`HNSWReader::_hnsw` is `mutable` because `faiss::HNSW::search` isn't
  const** (`hnsw.hpp:91`). The graph is shared across threads; faiss
  probably treats search as readonly on the graph nodes (state lives in
  the per-call `VisitedTable`). Verify by reading the faiss search path,
  or wrap with a shared_mutex. Otherwise multi-threaded ANN with one
  reader could race. (HNSW item, owned separately.)
- **`HNSW _pin_stack` is `std::array<ChunkSlot*, 2>` with hard depth 2**
  ([`hnsw.cpp:308`](../libs/iresearch/include/iresearch/columnstore/hnsw.cpp)).
  Asserts at depth 3. Comment says "Build outer + symmetric_dis inner"
  is enough today -- re-check after any faiss bump. Consider switching
  to a small_vector with assert-only depth-check. (HNSW item, owned
  separately.)
- **`SegmentReaderImpl::UpdateMeta` shares `_data` (incl. `cs_reader`)
  with the predecessor** ([`segment_reader_impl.cpp:218`](../libs/iresearch/include/iresearch/index/segment_reader_impl.cpp)).
  Two SegmentReaderImpl instances read through the same `cs_reader`.
  This mirrors how the old columnstore handles `UpdateMeta`, so the
  shape is intentional and not new. `ReopenColumnStore` (line 197)
  builds a fresh ColumnData and is fine. The remaining concern is
  whether the HNSW-side seek path needs a `Dup()` here (rare path);
  if it does, the fix is to `Dup()`/`Reopen()` the IndexInput inside
  cs_reader on share. Cross-check against old-cs's UpdateMeta when
  the HNSW owner picks this up.

## Performance (notsure)

- **`PartialBlockManager::ClearBlocks()` is called after every
  `FlushRowGroup`** to avoid an assert in `BlockHandle` dtor
  ([`column_writer.cpp:574`](../libs/iresearch/include/iresearch/columnstore/column_writer.cpp)).
  Comment: "without this PBM's destructor leaves BlockHandles loaded with
  an inconsistent memory charge". This smells like a misuse pattern --
  re-examine whether the PartialBlockManager should be recreated per RG
  instead, and whether there's a cleaner DuckDB API. May save the
  per-RG bookkeeping cost.
- **`HNSWReader::Search`'s `ChunkedVectorCache` capacity is 64 slots x 8192
  floats** -- defaults from a sweep that targeted `vector_search.test_slow`
  shape. May not be optimal for high-`d` workloads with large `m`. Worth
  a per-`info.d` heuristic or a config knob.
- **Both data and validity cursors are kept per binding in
  `ColumnstoreMaterializer`** even for `skip_validity=true` columns
  ([`columnstore_materializer.h:319`](../server/connector/columnstore_materializer.h)).
  Skip validity cursor allocation when `reader.ValidityGroupCount()==0`.
- **HNSW `prepare_level_tab(rows + doc_limits::min(), false)`**
  ([`hnsw.cpp:394`](../libs/iresearch/include/iresearch/columnstore/hnsw.cpp))
  -- faiss allocates a level entry for node 0 which is unused.
  `doc_limits::min() == 1`, so one wasted level entry per HNSW column.
  Minor; could remap to 0-based locally.

## Simplifications (notsure)

(All resolved in this pass; see Already-applied + Considered + Follow-ups.)

---

## Follow-ups (out of this PR, file as issues)

Things not in scope for this PR -- some need to error cleanly + have a
test today, some are pure follow-up optimisations / features. All file
as separate issues.


- **`STRUCT` / `MAP` / `UNION` / `VARIANT` INCLUDE columns.** Catalog
  blocks them today ([`index.cpp:349-360`](../server/catalog/index.cpp))
  -- good -- but `column_writer.cpp::FlushNode` default would call
  `CompressColumn` -> `PickCodec` -> "no codec accepted the row group for
  type STRUCT" if catalog ever stops being the gate. Add an explicit
  early reject in `Writer::OpenColumn` mirroring the catalog check, and
  a test confirming the catalog rejection produces a clean SQL error.
- **`GEOMETRY` INCLUDE columns.** Same status -- catalog rejects, writer
  doesn't. Mirror check + test.
- **Schema evolution between merged segments.** `MergeInto` asserts
  `col->Type() == first_col->Type()` in DEBUG only
  ([`merge.cpp:70`](../libs/iresearch/include/iresearch/columnstore/merge.cpp)).
  Convert to runtime error. Add test forcing two segments with the same
  field_id but different LogicalType.
- **Filter pushdown / zone-map skipping at scan time.** Codec
  `compression_filter_t` is unused; `ColumnReader::DataPointer(rg).statistics`
  are exposed but no consumer uses them. ORDER-BY / WHERE on an INCLUDE
  column doesn't skip row groups. Document as Phase 2.
- **ORDER BY column + LIMIT pushdown for columnstore columns.** DuckDB's
  `TableFunction` has projection_pushdown / pushdown_complex_filter
  callbacks. The columnstore could heap-walk per-RG zone maps and
  materialise only the surviving rows. Today everything is materialised
  then sorted. Phase 2.
- **`bulk_scan_active` short-circuits when any segment has deletes**
  ([`duckdb_search_full_scan.cpp:670-683`](../server/connector/duckdb_search_full_scan.cpp)).
  Comment says the bulk path doesn't yet stitch a docs_mask selection
  into the typed Scan. Add either the docs_mask stitch or document the
  fallback behaviour in user-facing docs. Test that delete-and-scan still
  returns correct rows via the streaming path.
- **Long string overflow > 4 GB.** `IndexOutputOverflowWriter` writes a
  uint32_t length and the `SDB_ENSURE` rejects beyond. Test that a 5GB
  string (or whatever lower cap we settle on) produces a clean error
  instead of UB.
- **Concurrent multi-threaded ANN on one HNSWReader.** Unclear if safe
  (see Bugs/notsure). Document the contract and add a test or guard.
- **Corrupted `.cs` file (truncated mid-write, footer offset garbage).**
  Reader silently treats as missing. Decide: log + reject, or return
  error from Open. Add a fuzzer-style test.
- **Per-segment HNSW info mismatch across merge sources.** If two source
  segments have the same field_id with different HNSW params (d, m,
  metric), merge currently doesn't even attempt; once HNSW merge is
  implemented (top bug), decide whether to reject or coerce. Test.
- **DECIMAL / TIMESTAMP_TZ / time-zoned types.** Should work through
  the generic primitive path but no test confirms. Add one round-trip
  test per missing type.
- **Per-column min/max stats not yet propagated to DuckDB.** Surface
  `DataPointer::statistics` (BaseStatistics per column per RG) through
  the table function's `statistics_t` callback so DuckDB's
  `SumPropagateStats` picks `SumState<long>` for SUM of bounded
  INT8/16/32. Cardinality half is already landed
  (see Already-applied); the column-stats half is missing. Without it
  i8/i16/i32 -- and types containing them (array of i32, struct with
  i32 field) -- stay at 0.5-0.7x of native in the perf bench. Add the
  callback + a perf test that confirms `SumState<long>` is picked.
- **Forced compression: lenient (DuckDB-like) mode not supported yet.**
  Today `PickCodec` throws `ERROR_BAD_PARAMETER` when the forced codec's
  analyze rejects the row group. DuckDB's `ForceCompression`
  ([column_data_checkpointer.cpp:109]) keeps `COMPRESSION_UNCOMPRESSED`
  available as a fallback so the write still succeeds when the codec
  doesn't fit the data. Both are useful: strict ("this codec or fail")
  for users who want a hard guarantee, lenient ("prefer this codec,
  fall back to uncompressed") matches DuckDB's behavior. Add a
  per-column option (e.g. `compression_strict` /
  `compression_with_fallback`) that picks between the two paths in
  `PickCodec`. Test both modes -- strict still errors, lenient produces
  uncompressed segments when codec rejects.
- **`merge.cpp` mask check is row-by-row `mask->contains(src_doc)`**
  ([`merge.cpp:131-138`](../libs/iresearch/include/iresearch/columnstore/merge.cpp)).
  Premise of the original TODO ("DocumentMask is a bitset, AND a slice
  in bulk") is wrong today: `DocumentMask` is
  `absl::flat_hash_set<doc_id_t>`
  ([index_meta.hpp:39](../libs/iresearch/include/iresearch/index/index_meta.hpp#L39)),
  so per-doc `contains()` is already the cheapest path. Becomes valid
  once the planned deleted-mask redesign lands (dense bitset for small
  segments, roaring-alive vs roaring-deleted switch -- tracked as a
  separate issue already): codegen a fast-path in `merge.cpp` per
  concrete mask type so the bulk path can sweep run-of-keepers /
  AND a slice without per-doc hash lookups.
- **Memory accounting for the new columnstore.** Two related gaps,
  both to fold into the broader "cs memory accounting" issue:
  - `SegmentWriter::memory_active` / `memory_reserved`
    ([segment_writer.cpp:62-72](../libs/iresearch/include/iresearch/index/segment_writer.cpp))
    don't include cs writer staging. Each `ColumnWriter` holds a
    `_staging` Vector sized to row_group_size; with many INCLUDE
    columns this is significant. Same for `FieldsData::memory_active`
    not including `NormColumnWriter`'s pending vector (one
    `vector<uint32_t>` of row_group_size per norm field; 100 norm
    fields x 122880 x 4B = 50 MB unaccounted).
  - `SegmentReaderImpl::CountMappedMemory`
    ([segment_reader_impl.cpp:228](../libs/iresearch/include/iresearch/index/segment_reader_impl.cpp))
    doesn't include the `.cs` mmap footprint. Bytes show in RSS but
    not in this counter, throwing off memory-budgeted maintenance
    decisions.
- **`UPDATE` rewrites cs INCLUDE columns from the entire chunk even
  when no value changed**
  ([duckdb_physical_update.cpp:574-585,668-680](../server/connector/duckdb_physical_update.cpp)).
  For 10-row updates x 100 INCLUDE columns that's 1000 codec analyze
  tournaments per batch. RocksDB partial-column updates only touch
  changed columns; cs design is full-rewrite. Measure on UPDATE-heavy
  workloads first; if it matters, route unchanged INCLUDE cols through
  a verbatim-passthrough.
- **`PersistedNormReader::GetBatch` does `upper_bound` on the first
  call even for batches that all live in one RG.** Cache the last RG
  span on the reader to short-cut entry. Effect unclear -- needs a
  bench before deciding whether to land.
- **Perf + on-disk size benchmark for the new norm columns.** Once
  the norm-column wiring stabilises (currently still in progress on
  this branch), run a bench comparing the new norm storage vs the
  legacy norm path on size and read/write throughput. Pin numbers so
  the legacy norm deletion is justified.
- **Rethink the `StoredBytesAccumulator` per-row design.** Today PK
  and other STORE'd columns funnel through
  `SearchSinkInsertBaseImpl::StoredBytesAccumulators*`
  ([search_sink_writer.cpp:332-418](../server/connector/search_sink_writer.cpp))
  -- the SearchSink builds a per-batch `duckdb::Vector` one slot at a
  time via `StringVector::AddStringOrBlob` / `SetNull` and only at
  `Flush` time hands the assembled Vector to
  `ColumnWriter::Append`. That's row-level access masquerading as a
  batch interface, complete with: a per-batch `Vector` re-emplace
  (~32 KB BLOB malloc/free per accumulator per batch), a per-row
  branch in `Append` vs `AppendNull`, and an extra string-heap copy
  at `VectorOperations::Copy` time when the accumulator Vector is
  Append'd into the writer's `_staging`. The intended end state is
  batch-oriented: have the SearchSink hand the original DuckDB
  `Vector` (or a `Slice`) to `ColumnWriter::Append` directly, the
  same way INCLUDE/HNSW columns already do via `WriteFullColumnImpl`.
  Per-row PK fixup (e.g. generated-PK from rowid) becomes either a
  one-pass batch transform on the source Vector or a typed Append
  overload on the writer. Outcome: drop the per-batch buffer
  re-emplace (the original TODO concern) AND drop one full string
  copy on every PK/STORE column. Out of this PR; file as its own
  refactor.
- **Fix iresearch gtest tests.** `tests/libs/iresearch/...` (the
  `iresearch-tests` ninja target) doesn't compile on this branch --
  `legacy_compat.cpp` has drifted, and other test files have likely
  drifted alongside. Repair as a separate pass; `ninja serened` is
  enough for the columnstore PR's verification. (See user-memory
  `project_iresearch_tests_broken.md`.)
- **Raw non-owning pointers in cs internals** (`ColumnReader::_db,_in`,
  `ColumnWriter::_db,_out,_entry`, `Writer::Impl::dir,db`,
  `NormColumnReader::_column`). Project convention is references for
  init-once non-nullable pointees. Per-file mechanical change; deferred
  out of this PR to keep the cs diff focused on the format/refactors.

---

## Considered, NOT a bug (do not "fix")

- **`MergeInto` iterates `for (auto [field_id_v, first_col] : first_seen)`
  over a `flat_hash_map`** ([`merge.cpp`](../libs/iresearch/include/iresearch/columnstore/merge.cpp))
  -- iteration order is non-deterministic, so the output footer's column
  order varies between runs. Looks like it could matter; it doesn't.
  Readers key columns by id (separate `by_id` / `norm_by_id` / `hnsw_by_id`
  maps in `Reader::Impl`), so the on-disk order is irrelevant for
  correctness. There is no byte-for-byte segment-comparison test that
  would care. Sorting before iterating adds a heap allocation + sort for
  zero observable benefit. Leave as-is.

- **Norm `_total_sum` running-sum overflow** at
  [`norm_reader.cpp:39`](../libs/iresearch/include/iresearch/columnstore/norm_reader.cpp).
  The check is `SDB_ASSERT(_total_sum + p.sum >= _total_sum)`. It looks
  like a runtime overflow guard, but the arithmetic is unreachable on
  any real workload: each row group's `sum` is bounded by row_count
  (capped well below 2^32) times max_norm (uint32_t = 2^32 - 1), so the
  total stays comfortably inside uint64. `SDB_ASSERT` is the right tool
  (debug-only check, zero release cost) -- do NOT promote to
  `SDB_ENSURE`; the runtime branch on every Reader open buys us
  nothing.

- **`OpenColumn` / `AttachHNSW` cross-call equality checks**
  ([`format.cpp` re-open/re-attach paths](../libs/iresearch/include/iresearch/columnstore/format.cpp)).
  These guard SearchSink-internal invariants (the same caller opens the
  same id with the same type/HNSWInfo across batches). Not user input;
  not file content. Same with `AttachHNSW`'s "column must be opened
  first" and Commit's `col_entry` lookup -- both are reading from a
  map we just populated ourselves. All `SDB_ASSERT`.

- **Upstreaming the WIP DuckDB patches in `third_party/duckdb`**
  (`50ae8e72`: `compression_init_analyze_t` signature change to take
  `CompressionAnalyzeContext`, the `OverflowStringReader` hook on
  `UncompressedStringSegmentState`, and the `Executor::ExecuteTask`
  callback patch). Considered; not pursued. The patches are small,
  serenedb-specific plumbing -- DuckDB upstream has no need for them
  and accepting them would be churn for an external project. Carry
  locally; document divergence at the patch sites only.

- **`std::ranges::sort(perm, {}, proj)`** at
  [`duckdb_search_full_scan.cpp:586`](../server/connector/duckdb_search_full_scan.cpp).
  Uses the projection form, which CONTRIBUTING.md explicitly allows as
  the fallback. Compliant; no rewrite needed.

- **`PersistentColumnData::child_columns` is a vector even though
  ARRAY/LIST/MAP have at most one child**. STRUCT has N, so the vector
  shape already covers the general case; switching to
  `unique_ptr<PersistentColumnData>` for ARRAY/LIST/MAP would force a
  per-branch type discriminator. Trade-off either way; keep the vector
  to avoid churn when STRUCT lands.

- **`field_id` is `uint32_t` in memory but the footer writes it as
  `uint64_t`** (`format.cpp:431,448,474,579,598,628`). 4 bytes wasted
  per id x few columns x segments -- negligible. Width mismatch is
  cheap insurance against ever needing to grow the type; leave at 64
  on disk.

- **`Reader::HasColumn` / `HasNormColumn` / `HasHNSW` are three
  separate methods.** No caller uses them together; collapsing would
  force a synthetic enum / type tag. Keep three.

- **`Writer::AllocateColumnId() noexcept` is a one-line `next_id++`.**
  Inlining in the header would need to expose `Writer::Impl`; the
  whole point of the PImpl is to NOT expose it. Cost of the
  out-of-line call is one increment + ret -- negligible against the
  column-open work that follows. Keep as-is.

- **`std::function<unique_ptr<duckdb::OverflowStringWriter>()>`
  factory on `ColumnDataCheckpointData`**
  ([column_writer.cpp:374-376](../libs/iresearch/include/iresearch/columnstore/column_writer.cpp)).
  One-shot lambda per VARCHAR-UNCOMPRESSED row group. Removing the
  `std::function` indirection would require a DuckDB API change
  (`SetOverflowStringWriter(unique_ptr)` direct setter) on top of the
  patches we already carry. Not worth a new DuckDB-side patch for a
  per-RG allocation that's nowhere near hot. Keep.

- **`Reader::_impl->in` shared between `OpenSegment()` and concurrent
  callers.** `PointReadCursor` already takes a `Reopen()`'d input so
  it's independent. The materializer/merge paths share `_in` but only
  call positional `ReadBytes(offset, ...)`; the BytesViewInput backend
  (mmap-backed) just memcpy's from the underlying view and updates
  `_pos` as a side-effect that no other call observes. Sequential
  reads (which mutate `_pos`) only happen inside the single-threaded
  `Reader::Reader` ctor. Safe in practice.

- **VARCHAR overflow round-trip.** `IndexOutputOverflowWriter::WriteString`
  writes immediately and hands back the file offset as `block_id`,
  which the codec serializes into the segment dictionary. We intercept
  *all* overflow writes via the factory we install on
  `ColumnDataCheckpointData`, so the codec never relocates a string
  after we've recorded its offset. Stable; no fix needed.

- **`columnstore::Reader::Reader` footer-buf allocation sized from
  on-disk metadata.** Already gated by
  `footer_offset >= header_len && footer_offset < footer_offset_pos`;
  worst-case bound is the file length minus header / iresearch
  footer, which is what a legitimate footer would consume. The mmap
  fast path (now via `IndexInput::ReadView`) doesn't allocate at all.
  Hard cap not worth adding.

- **No `fsync` directly between `_columnstore->Commit()` and opening
  `_cs_reader`** ([segment_writer.cpp:142-153](../libs/iresearch/include/iresearch/index/segment_writer.cpp)).
  Verified: the `.cs` IS fsynced -- same code path as old
  `.csi`/`.csd`. `columnstore::Writer` is constructed with the
  segment's TrackingDirectory ([segment_writer.cpp:203](../libs/iresearch/include/iresearch/index/segment_writer.cpp)),
  so `Writer::Commit` creates the `.cs` through the tracker;
  `SegmentWriter::Commit` collects it via
  `_dir.FlushTracked(meta.byte_size)` ([segment_writer.cpp:170](../libs/iresearch/include/iresearch/index/segment_writer.cpp)).
  `IndexWriter::Commit` -> `GetFilesToSync`
  ([index_writer.cpp:585-592](../libs/iresearch/include/iresearch/index/index_writer.cpp))
  then iterates `segment.meta.files` and fsyncs each entry, which is
  exactly the loop that handles `.csi`/`.csd` for old-cs. The
  reader opened in segment_writer right after `Writer::Commit()`
  predates that fsync but it reads from the same process's
  Directory; the segment isn't visible to other processes until
  `IndexWriter::Commit` finishes its sync.

- **`columnstore_materializer.cpp::Scan` LIST path unconditionally
  `SetListSize(out_vec, 0)`** before per-row appends. Today's caller
  passes one IotaRange per batch with `output_start == 0`, so the
  reset matches the contract; the worry about "multiple Scan calls
  truncating prior rows" doesn't apply to any live caller. The Scan
  path was rewritten when the recursive `MaterializeNode` landed --
  the API now mirrors `SelectByDocIds`'s shape. No fix needed.

- **`FieldData::compute_features` appends `_stats.len` to the norm
  writer with no `> 0` guard** ([field_data.cpp:646-648](../libs/iresearch/include/iresearch/index/field_data.cpp)).
  Zero-token docs land norm=0; BM25's skip-zero semantics make that
  the documented BM25 contract, not an accident. Already vetted on
  the BM25/TFIDF read side. Leave.

**Rule of thumb being applied here**: choose by *site cost* and
*diagnostic value*, not by reader-vs-writer category.

- `SDB_ENSURE` when the check is in a cold or one-shot path AND a
  meaningful error message helps when it does fire. Real runtime
  failure modes (file corruption, unsupported codec from a newer
  format, user-supplied compression that doesn't exist for the type,
  forced-codec analyze rejection) belong here. Also fine for
  "theoretically unreachable today, but if a future codec / footer
  layout ever produces this we want to fail loudly with context"
  guards -- as long as the site is cheap and the message names what
  changed. Example: `ColumnReader::OpenSegmentImpl`'s
  `!p.segment_state` check -- one-shot per segment open, and the
  message names the codec type, so the diagnostic earns its keep.

- `SDB_ASSERT` when the condition is mathematically unreachable on
  bounded inputs (saturation arithmetic, contracts between
  cooperating SereneDB components in the same transaction, internal
  invariants like "the same caller opens the same id with the same
  type across batches"), OR when the check sits on a hot enough path
  that the release-mode branch matters and the case is "impossible
  unless our own code is buggy". Example: the norm-sum overflow check
  fires on every Reader open and can only trip if our own
  segment-doc-count cap and uint32 norm bounds are both broken --
  zero diagnostic value, real release cost. `SDB_ASSERT`.

Don't pay a release-mode runtime branch for an unreachable case; do
keep `SDB_ENSURE` where the message would actually help an operator
who hit the failure.

---

## Already applied in this review (skip)

Style / contract pass landed directly in the columnstore files during
this review:

- **Decorative `// ---` separators** removed (`format.cpp`, `hnsw.cpp`).
- **WHAT-comments** stripped across `column_writer/column_reader/format/
  merge/hnsw/norm_*/materializer.{h,cpp}`. Kept only the few WHY-comments
  (codec-handle re-pin reason, validity-state TOCTOU note, footer-offset
  sanity, BlockHandle teardown, schema-evolution domain meaning).
- **Bare `SDB_ASSERT(cond)`** -- kept bare where the expression is
  self-documenting; added a message only where the failure scenario or
  domain meaning isn't visible from the expression (overflow check on
  norm sum, schema evolution between merge sources, HNSW re-attach with
  mismatched params, 4 GB segment limit, `serialize_state` not plumbed,
  monotonic-input contract on Append, ReadHNSW byte-range contract).
- **`std::any_of` -> `absl::c_any_of`** at `duckdb_search_full_scan.cpp:361,438`.
- **`std::to_string` -> `absl::StrCat`** at `column_reader.cpp` (with
  `absl/strings/str_cat.h` include).
- **`NormColumnWriter` default size** now uses `kDefaultRowGroupSize`
  instead of the magic `122880` literal.
- **`CONTRIBUTING.md`** updated to spell out the comment + assert-message
  rules with concrete heuristics (mechanical-translation test for
  comments; failure-mode-only message for asserts).
- Build verified with `ninja serened` after each batch of edits.

License headers in new files: out of scope per project convention --
`scripts/check_decorative_separators.py` handles header / separator
policing.

Autonomous follow-up pass (this session):
- Bug: `SDB_ASSERT(p.segment_state == nullptr)` -> `SDB_ENSURE`
  (`column_reader.cpp:OpenSegmentImpl`); release builds now refuse
  to silently drop codec state.
- Bug: `OpenColumn` / `AttachHNSW` cross-call invariants converted from
  `SDB_ASSERT` to `SDB_ENSURE`; mismatched re-attach now surfaces a
  runtime error instead of UB in release.
- Bug: `std::runtime_error` -> project `SDB_ENSURE` / `IoError` in
  `column_reader.cpp`, `format.cpp` (3 sites).
- Bug: `Norm _total_sum` running-sum overflow promoted to `SDB_ENSURE`
  (norm_reader.cpp:39).
- Bug: HNSWReader constructor now `SDB_ENSURE`s
  `ArraySize() == info.d` (hnsw.cpp:397).
- Bug: `s3_fifo::Evict()` falls through to `EvictMain` when
  `EvictSmall` returns false; HNSW cache under pin pressure no longer
  stalls.
- Perf: `OpenColumn` / `AttachHNSW` / `OpenNormColumn` dedup is now
  `flat_hash_map<field_id, idx>` lookups (was linear scan).
- Cleanup: `NormColumnWriter::FlushRowGroup` per-value writes go through
  `IndexOutput::WriteByte` / `WriteU16` / `WriteU32` (native little-endian
  via `WriteNumU`, with `BufferedOutput` buffering the calls internally).
  Drops the manual `absl::little_endian::FromHost*` flip + `WriteBytes`
  shim that was holdover from when iresearch wrote big-endian.
- Perf: `ColumnReader::RowGroupElementStart` walks backward from `rg`
  to the largest cached entry, then fills forward; sequential calls are
  now O(1) amortised (was O(rg) per call, O(N^2) over N calls).
- Perf: streaming full-scan reuses one BLOB Vector across batches
  (`SearchFullScanGlobalState::streaming_pk_vec`) instead of per-segment
  allocation.
- Simplification: lifted `kLengthsType` / `kValidityType` to one
  anonymous-namespace constant each in `column_reader.cpp`.
- Simplification: replaced bulk-scan string compare
  `search.filter_summary == "All"` with the typed flag
  `SearchScan::match_all`.
- All changes verified with `ninja serened` after each batch.

Materialization refactor pass (this session):
- Bug: codec scan-state reuse -- `ColumnReader::ScanCursor::Scan` now
  bumps both `state.offset_in_column` and `state.internal_index` after
  every `Scan`. FixedSize / Validity codecs read from
  `GetPositionInSegment() = offset_in_column - row_start` and don't
  auto-advance it on `Scan`, so consecutive `Scan` calls without an
  intervening `Skip` previously re-read the same bytes (corrupted
  ARRAY rows after the first in `MaterializeArrayRange`).
- Bug: `MaterializeListRow` / `PointReadCursor::FetchRow` /
  `SegmentPkRandomFetcher::Fetch` destructured the old
  `LocateResult { rg, in_rg, rg_end }` shape after the rename to
  `RgWindow { rg, begin, end }`. The `in_rg` local silently received
  the global RG start (always 0 for RG 0). Replaced with named-field
  access: `window.rg`, `in_rg = row_pos - window.begin`.
- Refactor: collapsed 5 caller patterns (`MaterializeArrayRange`,
  `MaterializeListRow`, `MaterializeColumnSide`, `merge.cpp`,
  `hnsw.cpp` cache load) onto a single `RangeScan` abstraction that
  caches `(RgWindow, ScanCursor)` and crosses RG boundaries
  internally. `ScanRowsBatched` does run-detection over
  sorted-ascending doc_ids; `ScanCursor` is the low-level escape
  hatch for HNSW's non-monotonic cache.
- Refactor: `LocateInOffsets` split into forward-jump and
  backward-jump branches with narrowed binary search ranges
  (forward: `upper_bound` starts at `hint.rg + 2`; backward: ends at
  `hint.rg`). Replaced runtime branches with `SDB_ASSERT`s on the
  always-true bounds.
- Simplification: removed `RgWindow::Contains` (single caller in
  `RangeScan::Scan` -- inlined as two-sided comparison).
- Simplification: dropped `cs_internal::MaterializeColumnSide`
  (`bool is_data` parameter flipped to `!is_data` for the RangeScan
  ctor). `MaterializeColumnRange` now does the two passes inline:
  data first, then validity if `HasValidity()`.
- Simplification: dropped `std::optional<RangeScan>` wrapper in
  `merge.cpp` -- `RangeScan` ctor is trivial (pointer + bool), so
  construct unconditionally and gate the `Scan` call on
  `col->HasValidity()`.
- All changes verified with `ninja serened` and the targeted
  `inverted_index_list_include`, `inverted_index_array_include`,
  `inverted_index_columnstore_codecs`, `vector_search`,
  `inverted_index`, `inverted_index_score`,
  `inverted_index_compression_option`, and `inverted_index_topk` SQL
  tests.

Perf-bench / cardinality / cross-query-reuse pass (this session):
- Perf: cs Reader is now cached per segment via
  `SegmentReaderImpl::CsReader()` (virtual on `SubReader`, override on
  `SegmentReaderImpl` and `SegmentReader`).  `ColumnstoreMaterializer`
  borrows it instead of constructing a fresh Reader per query;
  `SegmentPkSequentialFetcher`, `SegmentPkRandomFetcher`, the ANN
  filter (`duckdb_ann_filter.cpp::Reset`), and `MergeWriter::Flush`
  (`SourceContext::cs_reader` is now non-owning) all borrow the same
  cached Reader -- those fetchers also dropped the `db` argument since
  the Reader comes from the cache.  Footer parse (~9-14% of i32 hot
  CPU) gone for reads; one footer parse per source per merge saved
  during consolidation.
- Perf: `SCAN_ENTIRE_VECTOR` opt-in.  `RangeScan::Scan` takes a
  `may_use_entire` hint; `ScanRowsBatched` passes `true` only on the
  contiguous-range fast path (single Scan call into the vector).  Random
  doc-id path stays `SCAN_FLAT_VECTOR` to avoid the assert when a
  follow-up FLAT write hits a DICT vector.  Net win for varchar (dict_fsst
  hands back a DICTIONARY_VECTOR instead of materialising 100M
  string_t).
- Perf: pin-aux for zero-copy lifetime.  After SCAN_ENTIRE_VECTOR, we
  pin the segment's BlockHandle via `BufferManager::Pin` and attach it
  to the result vector through `AddAuxiliaryData(PinnedBufferHolder)`
  so the buffer survives the next `ScanCursor` rotation -- without it,
  `FixedSizeScan`'s `FlatVector::SetData(result, segment_data, ...)`
  use-after-frees once the next RG opens.
- Bug: `HasValidity()` now skips RGs whose validity codec is
  `COMPRESSION_EMPTY`.  Computed once in the four ColumnReader
  constructors via `AnyNonEmptyValidity(_validity_pointers)`.
  All-valid columns no longer trigger the validity scan, which in turn
  unblocks `SCAN_ENTIRE_VECTOR` for dict_fsst varchars.
- Perf: **max cardinality propagation** so DuckDB picks
  `SumState<long>` over `SumState<hugeint_t>` when the output bound
  fits int64.  `InvertedIndexCardinality`, `TableScanBindData::Cardinality`,
  `ViewScanBindData::Cardinality` switched from the 1-arg
  `NodeStatistics(estimated)` ctor (which leaves
  `has_max_cardinality=false` and silently kills DuckDB's
  `SumPropagateStats`) to the 2-arg `NodeStatistics(rows, rows)` --
  snapshot row counts are exact, not estimates.  Closes the bool gap
  (CASE-WHEN-of-constants output bound is `[0,1]`, max sum = N rows;
  fits long).  i8/i16/i32 still go via hugeint because their bound
  depends on column min/max, not cardinality alone -- column-stats
  propagation is the next PR (see top of Performance section).
- Refactor: `ConsecutiveRunLength()` helper in `column_reader.hpp`
  replaces 3 copies of the
  `while (rows[i+run]-rows[i+run-1]==1) ++run` loop
  (`ScanRowsBatched`, `MaterializeNode` ARRAY branch, `MaterializeNode`
  LIST/MAP branch with an upper-bound predicate).
- Refactor: `MaterializerNodeState` -- recursive state struct mirrored
  after `duckdb::ColumnScanState::child_states` -- now lives on
  `ColumnstoreMaterializer::Binding` so the same `RangeScan` /
  `ListOffsetState` / pre-allocated child states survive across
  batches.  Fixes the per-row LIST `RangeScan` allocation that was
  the previous Performance-section item.
- Refactor: `ColumnstoreMaterializer` rebuilt-per-batch in streaming &
  top-K paths is gone -- both paths now reuse the segment's cached
  `CsReader()` and the per-binding state.
  ([`duckdb_search_full_scan.cpp`](../server/connector/duckdb_search_full_scan.cpp),
  [`duckdb_scan_base.cpp`](../server/connector/duckdb_scan_base.cpp))
- Verified-no-op: "codec analyze runs ALL codecs" matches DuckDB's
  `ColumnDataCheckpointer::DetectBestCompressionMethod`
  ([column_data_checkpointer.cpp:166]): same per-RG tournament over
  `GetCompressionFunctions()`, no mid-flight pruning.  Pre-pruning is
  at codec-registration time, not per call.

ANN / range-scan include-column materializer pass (this session):
- Bug fix: ANN + INCLUDE no longer silently routes through RocksDB.
  `SearchAnnScanGlobalState` / `SearchRangeScanGlobalState` now go
  through the shared `ColumnstoreMaterializer` overlay for INCLUDE'd
  columns and only fall back to `IndexSource` for the residual.
- Perf: every `MakeIndexSource` call site (full-scan streaming /
  top-K, sk full-scan / range / point-lookup, ANN, range) now
  receives `state.external_projected_columns` (same shape as
  `projected_columns` but with cs slots zeroed to INVALID_INDEX), so
  RocksDB doesn't double-materialize columns the cs overlay will
  overwrite.
- Refactor: hoisted the projection classifier into a shared
  `ClassifyColumnstoreProjections(state, bind_data)` helper in
  `duckdb_scan_base.{hpp,cpp}`. The cs fields (`cs_projections`,
  `cs_field_ids`, `cs_output_slots`, `has_external_projections`,
  `external_projected_columns`) live on `CommonScanGlobalState` so
  every search scan picks them up.
- Refactor: extracted the score-ordered cs overlay (perm by
  (segment, doc), per-segment Reader open, score-order
  DICTIONARY_VECTOR slice) from the top-K full-scan path into a
  shared `MaterializeIncludeColumnsScoreOrder(gstate, reader,
  seg_doc_score_order, output)` helper; reused by ANN's EmitResult
  and range scan's emit step.
- Tests: `inverted_index_ann_include.test` and
  `inverted_index_range_include.test` -- both EXPLAIN-check
  the per-column source annotation (`(i)` / `(l)` mixed-mode,
  suppressed `Lookup` row when all projections are INCLUDE'd) and
  end-to-end-check the materialized values.
- Perf: `ColumnWriter::_staging` now constructs with
  `VectorDataInitialization::UNINITIALIZED` (ctor + post-FlushRowGroup
  re-init). Saves ~480 KB per BIGINT RG / ~2 MB per VARCHAR RG of
  memset per writer per row group. Gap-padding still clears the
  validity bitmap for null slots, so codecs that respect validity
  skip those rows; data bytes for never-written slots stay garbage
  but are never read.
- Perf: `merge.cpp` mask-prune no longer allocates a throwaway
  `duckdb::Vector kept_vec{Type, kept}` per row-group batch.
  `ColumnWriter::Append` gained a `SelectionVector` overload; merge
  builds the selection and hands it straight to the writer, which
  calls `VectorOperations::Copy(source, target, sel, ...)` into
  `_staging` -- one fewer per-batch Vector ctor + Slice + Flatten on
  the consolidation hot path. The two `Append` overloads share a
  `PadNullsTo` helper for the gap-handling prologue.
- Verified-no-op: `ColumnReader::ListOffsets` no longer materialises a
  `_list_offsets_per_rg` cache. Current `ListOffsetState`
  ([column_reader.hpp:230-236](../libs/iresearch/include/iresearch/columnstore/column_reader.hpp))
  is a streaming cursor: one open `ScanCursor` over the current RG,
  a single-element scratch `Vector buf`, and `prev_offset` / `next_pos`
  advancement -- no materialised array of all offsets per RG. The
  "~960 KB per LIST RG, no eviction" concern from the original TODO
  doesn't apply; nothing to evict.

Simplifications pass (this session):
- Simplification: dropped the `concept DocIdRange` in
  [columnstore_materializer.h](../server/connector/columnstore_materializer.h).
  `template<DocIdRange DocIds>` -> `template<typename DocIds>`; the
  duck-type contract (`.size()` + `operator[](size_t) -> uint64_t`) is
  now documented on `IotaRange` and enforced at instantiation, matching
  CONTRIBUTING's "prefer template + static_assert over concepts".
- Simplification: `ColumnWriter::Append` / `AppendChunk` now take
  `const duckdb::Vector&` / `const duckdb::DataChunk&` -- source
  parameters were always logically read-only; the non-const signature
  was forcing a `const_cast<duckdb::Vector&>` at the only writer call
  site
  ([search_sink_writer.cpp:319-320](../server/connector/search_sink_writer.cpp)),
  which is now gone. `VectorOperations::Copy` already takes
  `const Vector&` source so no internal change needed.
- Simplification: cs column name removed entirely. Dropped the `name`
  field/parameter from `ColumnReader`, `ColumnWriter`,
  `FooterColumnEntry`, `Writer::OpenColumn`, and `MakeColumnReader`;
  dropped the `WriteProperty(1, "name", ...)` in `Writer::Commit` and
  the matching `ReadProperty<string>(1, "name")` in the footer read.
  cs typed columns are looked up by id everywhere; the persisted name
  was footer-only metadata that nothing read. (HNSW + norm columns
  keep their name -- owned separately.)
- Simplification: collapsed the four `ColumnReader` constructors
  (primitive / ARRAY / LIST-or-MAP / STRUCT) and the matching
  `MakeColumnReader` switch arms into one ctor + one factory body. The
  ctor dispatches on `_type.id()` to compute `_data_offsets` /
  `_row_count` / `_rg_element_starts` per type-shape; the factory
  builds the optional `element_child` / `struct_children` / `array_size`
  triple before delegating.
- Simplification: split `Reader::Reader` (was ~140 lines) into
  orchestration + helpers in
  [format.cpp](../libs/iresearch/include/iresearch/columnstore/format.cpp):
  - Free anon-namespace helpers `OpenAndCheckHeader(dir, filename)` and
    `ReadFooterBytes(in, filename)`.
  - Private member fns `Reader::BuildColumnReaders` /
    `BuildNormReaders` / `BuildHnswReaders` (declared in `format.hpp`),
    each iterating one footer slot.
  The ctor is now header check + footer-bytes read + 3 build calls.
- Simplification: split `MergeWriter::Flush` into orchestration + two
  anonymous-namespace helpers in
  [merge_writer.cpp](../libs/iresearch/include/iresearch/index/merge_writer.cpp):
  - `ComputeDocMappingsAndFieldMeta(readers, segment, ...)` -- per-
    source `doc_map` (identity vs compacted), field-meta map,
    `CompoundFieldIterator::Add`, sets `segment.docs_count` /
    `live_docs_count`.
  - `OpenColumnstoreContexts(db, dir, segment_name, readers, ...)` --
    borrows each source's cached cs Reader from
    `SubReader::CsReader()` and constructs the output cs Writer when
    `db != nullptr`.
  Flush itself is now ~30 lines of orchestration + the existing
  `WriteFields` / `MergeInto` / `Commit` calls.
- Perf: `ReadFooterBytes` now tries `IndexInput::ReadView(offset, size)`
  first and returns a span over the file mapping directly when the
  directory supports it (mmap-backed). Only falls back to allocating a
  `std::vector<byte_type>` + `ReadBytes` when ReadView returns nullptr.
  Avoids a footer-sized heap copy per `.cs` Reader open on every mmap
  directory.
- Simplification: empty norm columns no longer reach the footer.
  `Writer::Commit` does `std::erase_if(_impl->norm_writers, [](const
  auto& nw) { return nw->Pointers().empty(); })` before writing the
  norm-columns list. `FieldsData::emplace` allocates a
  `NormColumnWriter` eagerly per field with the Norm feature; the
  ones that never receive an indexed doc are dropped in-place at
  commit time so the footer carries no row_groups=[] no-ops and the
  writer's in-memory state is freed. field_id growth from the
  pre-allocation is intentional and harmless.
