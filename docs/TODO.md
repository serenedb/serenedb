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

> **STATS-PROPAGATION NEXT PR.** Cardinality half is **landed** (see
> Already-applied). Column min/max half remains: surface
> `DataPointer::statistics` (BaseStatistics per column per RG) through
> the table function's `statistics_t` callback so DuckDB picks
> `SumState<long>` for SUM of bounded INT8/16/32. Without this i8/i16/i32
> and types containing them (array of i32, struct with i32 field) stay
> at 0.5-0.7x of native in the perf bench.

- **HNSW build's PersistentColumnData clone via serialize-then-deserialize
  round-trip** ([`format.cpp:392-413`](../libs/iresearch/include/iresearch/columnstore/format.cpp)).
  `DataPointer` is move-only, so the code MemoryStream-serializes the whole
  column entry and deserializes it to get a copy. Cost is one full footer
  walk per HNSW column at commit time. Add a `PersistentColumnData::Clone()`
  (DataPointer clone = `stats.Copy()` + re-call `serialize_state(segment)`)
  or restructure HNSW build to operate on the in-flight writer state
  directly without a clone.
- **`ColumnWriter::_staging` is ZERO_INITIALIZE'd after every row-group
  flush** ([`column_writer.cpp:578-579`](../libs/iresearch/include/iresearch/columnstore/column_writer.cpp))
  -- that's row_group_size x element_size of zeroing per RG (~480 KB for
  BIGINT, ~32 KB for VARCHAR string_t per RG; per writer per RG, multiplied
  by writers and RGs in a segment). Only the validity bitmap needs
  predictable contents; data bytes are fully overwritten by Append +
  gap-padding. Either zero only the validity buffer, or stop zeroing data
  and trust the gap-padding/Append fills.
- **`merge.cpp` allocates `duckdb::Vector kept_vec{Type, kept}` per inner
  iteration of the mask-prune path** (`merge.cpp:140`). Hoist the
  destination outside the loop or let `ColumnWriter::Append` accept a
  `SelectionVector` so we don't need a separate Vector at all.
- **`merge.cpp` mask check is row-by-row `mask->contains(src_doc)`**
  (`merge.cpp:131-138`). `DocumentMask` is a bitset -- pre-compute
  selection by AND'ing the relevant bitset slice or sweeping the next
  run-of-keepers, then bulk-append.
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
- **`ColumnReader::ListOffsets` cache has no eviction policy / byte
  budget** ([`column_reader.cpp`](../libs/iresearch/include/iresearch/columnstore/column_reader.cpp),
  `_list_offsets_per_rg`). ~960 KB per LIST row group of UBIGINTs at
  122880 rows; wide LIST schemas or many open segments balloon. Add a
  per-reader byte budget and a simple LRU eviction.
- **`SegmentWriter::memory_active` / `memory_reserved` don't include
  columnstore writer staging** ([`segment_writer.cpp:62-72`](../libs/iresearch/include/iresearch/index/segment_writer.cpp)).
  Each `ColumnWriter` holds a `_staging` Vector sized to row_group_size;
  with many INCLUDE columns this is significant. Same for
  `FieldsData::memory_active` -- doesn't include `NormColumnWriter`'s
  pending vector (one `vector<uint32_t>` of row_group_size per norm
  field; 100 norm fields x 122880 x 4B ≈ 50 MB unaccounted).
- **`SegmentReaderImpl::CountMappedMemory` doesn't include columnstore
  mmap memory** ([`segment_reader_impl.cpp:228`](../libs/iresearch/include/iresearch/index/segment_reader_impl.cpp)).
  The `.cs` file is mmap-backed; bytes show in RSS but not in this
  counter.
- **`UPDATE` rewrites cs INCLUDE columns from the entire chunk even when
  no value changed** ([`duckdb_physical_update.cpp:574-585,668-680`](../server/connector/duckdb_physical_update.cpp)).
  For 10-row updates with 100 INCLUDE columns this is 1000 codec analyze
  tournaments per batch. RocksDB partial-column updates only touch
  changed columns; cs design is full-rewrite. Measure on UPDATE-heavy
  workloads; possibly route unchanged INCLUDE cols through a verbatim-
  passthrough.
- **`StoredBytesAccumulator` Vector reallocated per batch `Init`**
  ([`search_sink_writer.cpp:392-409`](../server/connector/search_sink_writer.cpp)).
  For uniform STANDARD_VECTOR_SIZE batches this is per-batch alloc/free.
  Keep the Vector alive when the size matches; only re-emplace on size
  change.
- **MergeWriter::Flush still opens a per-source `columnstore::Reader`**
  ([`merge_writer.cpp`](../libs/iresearch/include/iresearch/index/merge_writer.cpp)).
  All other read sites (`ColumnstoreMaterializer`, both PK fetchers,
  ANN filter) now borrow `SegmentReaderImpl::CsReader()` -- the
  per-segment cached Reader added by the cross-query reuse pass.
  Wire merge through the same cache; saves one footer parse per source
  per merge.

## Simplifications (sure)

- **Three `ColumnReader` constructors that diverge only in which arg list
  + which composite they handle** ([`column_reader.cpp:38,66,101`](../libs/iresearch/include/iresearch/columnstore/column_reader.cpp)).
  Plus three matching `MakeColumnReader` switch arms in
  [`format.cpp:90-122`](../libs/iresearch/include/iresearch/columnstore/format.cpp).
  Collapse into one ctor (default-empty data/validity for ARRAY) and one
  `MakeColumnReader` body.
- **`HNSWInfo` footer fields are written/read field-by-field across 8
  `WriteProperty`/`ReadProperty` calls** ([`format.cpp:474-484,628-638`](../libs/iresearch/include/iresearch/columnstore/format.cpp)).
  Either give `HNSWInfo` a `Serialize`/`Deserialize` or use a
  `WriteProperty(0, "info", info)` once with the inspector specialisation.
- **`Reader::Reader` is a 140-line constructor doing header check + footer
  parse + per-section walk** ([`format.cpp:530-671`](../libs/iresearch/include/iresearch/columnstore/format.cpp)).
  Split into `OpenAndCheckHeader`, `ReadFooterBytes`, `BuildColumnReaders`,
  `BuildNormReaders`, `BuildHnswReaders`. Each becomes individually
  testable and the ctor becomes orchestration.
- **`AllocateColumnId() noexcept` is a one-line `next_id++`** --
  inline it in the header (needs `Impl` exposure; deferred).
- **`std::function<std::unique_ptr<duckdb::OverflowStringWriter>()>` factory
  on `ColumnDataCheckpointData`** is a one-shot lambda; pass the writer
  directly if the DuckDB patch we already carry permits.
- **`concept DocIdRange`** at `columnstore_materializer.h:54`. Rule:
  "Prefer template + static_assert over concepts when possible". Convert
  or drop the constraint (call sites are `std::span<const irs::doc_id_t>`
  and an inline `PermDocIds` struct).
- **Raw non-owning pointers** in `ColumnReader::_db`, `_in`,
  `ColumnWriter::_db`, `_out`, `_entry`, `Writer::Impl::dir`/`db`,
  `NormColumnReader::_column`. Project convention is references for
  init-once non-nullable pointees.
- **`std::ranges::sort` at `duckdb_search_full_scan.cpp:586`** -- uses the
  projection form `std::ranges::sort(perm, {}, proj)`. CONTRIBUTING.md
  explicitly allows ranges with projection as the fallback, so this is
  arguably compliant; reconfirm and either keep or convert to
  `absl::c_sort(perm, cmp_lambda)`.
- **`MergeWriter::Flush` is one giant function** ([`merge_writer.cpp:683-797`](../libs/iresearch/include/iresearch/index/merge_writer.cpp)).
  Split into per-source-context prep / WriteFields / MergeInto / Commit
  for testability.
- **`const_cast<duckdb::Vector&>(vec)`** at
  [`search_sink_writer.cpp:308-320`](../server/connector/search_sink_writer.cpp)
  -- `ColumnWriter::Append` takes non-const Vector to call
  `ToUnifiedFormat`. Make Append accept const + do `ToUnifiedFormat`
  itself to drop the cast.
---

## Bugs (notsure)

- **`Reader::_impl->in` is shared between `OpenSegment()` (called from
  materializer / merge / norm reader) and `PointReadCursor`'s owned
  Reopen'd input.** The cursor path uses its own input. The materializer
  path uses the shared `_in`. Concurrency: `IndexInput::ReadBytes(offset,
  buf, size)` is positional, which is required for thread-safety; verify
  every backend (`MMapIndexInput`, `BufferedIndexInput`, etc.) preserves
  that contract for concurrent calls. The Reader-side `_in->Seek + ReadHNSW`
  during `Reader::Reader` happens before any ColumnReader callers can run,
  so that's safe, but document it.
- **`HNSWReader::_hnsw` is `mutable` because `faiss::HNSW::search` isn't
  const** (`hnsw.hpp:91`). The graph is shared across threads; faiss
  probably treats search as readonly on the graph nodes (state lives in
  the per-call `VisitedTable`). Verify by reading the faiss search path,
  or wrap with a shared_mutex. Otherwise multi-threaded ANN with one
  reader could race.
- **`HNSW _pin_stack` is `std::array<ChunkSlot*, 2>` with hard depth 2**
  ([`hnsw.cpp:308`](../libs/iresearch/include/iresearch/columnstore/hnsw.cpp)).
  Asserts at depth 3. Comment says "Build outer + symmetric_dis inner"
  is enough today -- re-check after any faiss bump. Consider switching
  to a small_vector with assert-only depth-check.
- **VARCHAR overflow round-trip** -- `IndexOutputOverflowWriter::WriteString`
  writes immediately and hands back the file offset as `block_id`. The
  codec serializes that `block_id` into the segment dictionary. Verify the
  codec doesn't reorder/relocate overflow strings after `WriteString`
  returns (which would invalidate our offsets). Probably fine because we
  intercept *all* overflow writes; recheck.
- **`columnstore::Reader::Reader` allocates a `std::vector<byte_type>
  footer_buf` sized from on-disk metadata.** It's already sanity-checked
  for `footer_offset < footer_offset_pos`, but a corrupted size that
  passes the check could still allocate up to file size. Probably bounded
  enough; consider a hard cap.
- **`SegmentReaderImpl::UpdateMeta` shares `_data` (incl. `cs_reader`)
  with the predecessor** ([`segment_reader_impl.cpp:218`](../libs/iresearch/include/iresearch/index/segment_reader_impl.cpp)).
  Two SegmentReaderImpl instances now read through the same `cs_reader`.
  `columnstore::Reader` has no internal locking; the HNSW seek path
  (`_in->Seek`) is racy under concurrent scans on the two readers.
  `ReopenColumnStore` (line 197) builds fresh ColumnData and is fine.
  Decide: copy on UpdateMeta, or make cs_reader Seek-free / locked.
- **No fsync between `_columnstore->Commit()` and opening `_cs_reader`**
  ([`segment_writer.cpp:142-153`](../libs/iresearch/include/iresearch/index/segment_writer.cpp)).
  For directories that defer writes (some S3-style backends), the read
  might miss the write. Today's impls flush on close; document the
  assumption or fsync explicitly.
- **Bulk-scan condition uses string compare
  `search.filter_summary == "All"`** ([`duckdb_search_full_scan.cpp:672`](../server/connector/duckdb_search_full_scan.cpp)).
  Brittle on optimizer-generated summary; if the wording ever changes
  the bulk path silently disables. Use a typed `search.IsMatchAll()`
  flag.
- **`columnstore_materializer.cpp::Scan` LIST path unconditionally
  `SetListSize(out_vec, 0)`** ([`columnstore_materializer.cpp:71`](../server/connector/columnstore_materializer.cpp))
  before per-row appends. If a caller ever issues multiple Scan calls
  into the same chunk, prior LIST rows are truncated. Today's caller
  writes at slot 0 once per chunk; align with `SelectByDocIds`'s
  `output_start == 0` guard.
- **`FieldData::compute_features` appends `_stats.len` to the norm
  writer with no `> 0` guard** ([`field_data.cpp:646-648`](../libs/iresearch/include/iresearch/index/field_data.cpp)).
  Zero-token docs land norm=0; BM25 skip-zero semantics make this OK
  in practice, but the implicit contract should be guarded or
  documented in the public API.
- **`FieldsData::emplace` allocates a per-segment norm column id even
  for fields that may receive zero docs** ([`field_data.cpp:1004-1023`](../libs/iresearch/include/iresearch/index/field_data.cpp)).
  `NormColumnWriter::Finalize` writes a zero-row entry to the footer.
  Either prune empty norm columns at finalize or document the
  dead-entry case.

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
- **`PersistedNormReader::GetBatch` does `upper_bound` on the first call
  even for batches that all live in one RG.** Cache the last RG span on
  the reader to short-cut entry. May or may not matter -- measure.
- **HNSW `prepare_level_tab(rows + doc_limits::min(), false)`**
  ([`hnsw.cpp:394`](../libs/iresearch/include/iresearch/columnstore/hnsw.cpp))
  -- faiss allocates a level entry for node 0 which is unused.
  `doc_limits::min() == 1`, so one wasted level entry per HNSW column.
  Minor; could remap to 0-based locally.

## Simplifications (notsure)

- **`PersistentColumnData::child_columns` is a vector but ARRAY/LIST only
  ever have one child** (STRUCT would have N). For V1, a
  `std::unique_ptr<PersistentColumnData>` is cleaner; switch to vector
  once STRUCT lands. Trade-off either way; leaning toward keep-as-vector
  to avoid future churn.
- **`field_id` is `uint32_t` but the footer writes it as `uint64_t`**
  (`format.cpp:431,448,474,579,598,628`). 4 bytes wasted per id x ~few
  columns x segments -- negligible. Decide once: 32 or 64 in the footer.
- **`Reader::HasColumn` / `HasNormColumn` / `HasHNSW` are three separate
  methods.** No callers use them together, so probably keep.
- **cs column name = `std::to_string(column_id)`** at
  [`search_sink_writer.cpp:166-169`](../server/connector/search_sink_writer.cpp)
  while iresearch field names use BE-encoded bytes (see
  `SetNameToBuffer`). cs entries are looked up by id; the name is just
  metadata. Drop, or align with BE-bytes convention so a hex dump of the
  `.cs` footer matches the `.ti` file.

---

## Not supported yet (must error cleanly + have a test)

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

## Related cleanup (out-of-scope but worth tracking)

- The DuckDB patches in `third_party/duckdb` (WIP commit `50ae8e72`)
  changed `compression_init_analyze_t`'s signature
  (`CompressionAnalyzeContext`) and added the `OverflowStringReader` hook
  on `UncompressedStringSegmentState`. Upstream them or document divergence.
  The `Executor::ExecuteTask` callback patch from the previous session is
  separate; both should be tracked in one place.

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
  `SegmentPkSequentialFetcher`, `SegmentPkRandomFetcher`, and the
  ANN filter (`duckdb_ann_filter.cpp::Reset`) now take just
  `(reader, seg_idx)` -- dropped the `db` argument since the Reader
  comes from the cache.  Footer parse (~9-14% of i32 hot CPU) gone.
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
