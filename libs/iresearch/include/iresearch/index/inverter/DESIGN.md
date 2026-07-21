# Columnar inversion engine

This directory implements the in-memory inversion engine behind
`SegmentWriter`: term-id interning plus a columnar occurrence log, replacing
the deleted Lucene-2014 byte-pool design (`field_data`/`postings`; absolute
bench numbers are the regression baseline). Flush feeds the unchanged format
(`burst_trie` + block-128 postings) and is byte-identical to the old engine's
output for the same token streams.

## The two-level model

Everything at this seam is either **level-1 metadata** (static, catalog/bind
time: tokenizer type + `TokenTraits` + index features + store; validated at
CREATE INDEX, drives routing -- verbatim keyword block path, single-token 1-1
ingest, WKB input binding) or a **level-2 runtime hint** (per batch,
describing the data in flight: `dense_pos`, `one_to_one`). Traits never
change per fill; hints are set by the tokenizer's fill dispatch and let the
indexing pipeline pick faster paths without runtime lane switches.

## Data structures

**`TermDictionary`** -- a SwissTable of bare u32 ids into an entry array; the
array IS the dense term-id space (4-byte ids in the log, flat histogram/rank
arrays at flush). `Entry : HashedTerm` is exactly 32 bytes:
`{duckdb::string_t term, size_t hash, doc_id_t inline_docs[2]}`. The hash
lives in the entry, so map slots are 4 bytes (probe working set /4 vs a
ref+hash slot; measured 1.3-1.4x faster resolution on hit-dominated shapes
than entry-in-set layouts -- see invert_kernels BM_Dict{Ref,Direct,PairMap}).
Term bytes intern once per unique term into a shared arena; probes compare
via `string_t`'s length+prefix u64 early-out; the hash is computed once per
token and only ever re-read (placement, prefetch, rehash). `Resolve*` is
intern-only.

There is no occurrence counter. For `TokenLayout::Terms` fields (docs-only /
freq -- the default keyword filter index and the PK field) a term's first
`kInlineOccs = 2` occurrences live in `inline_docs` (zero slot = vacant);
`TryInline` captures, the log holds the rest, and flush folds the inline
docs into the term's scattered region ahead of the log's. All-unique columns
(PKs) build no occurrence log at all.

**`PostingLog<L>`** -- per-field, monomorphized on the layout: each
specialization owns exactly the SoA columns its layout indexes (term_ids
always; pos + the explicit-doc bitmap for pos layouts; offs when indexed)
over geometric blocks carved from a shared per-writer arena.
`FieldInverter` holds the three specializations in a variant and dispatches
once per ingest call -- every per-token/per-value layout branch is
compile-time. Doc ids are not stored per occurrence: `Run{first_doc, ndocs}`
headers (8B) plus per-doc token counts map log positions to docs. Gaps of up
to `kMaxBridgedGap = 3` docs (typical when inline capture swallows whole
docs) are bridged with zero-count slots (4B) instead of breaking the run.
Positions default to *dense* (position == within-doc ordinal, nothing
written); a doc that receives an explicit position is promoted -- its bit set
and its implied ramp backfilled -- and the flag bitmap allocates only on the
first promotion, so all-dense fields carry none. The log owns every
promotion decision: batch pushes ramp dense-onto-promoted docs internally
(`PushRamp`), single-token pushes route through `PushOne`.

**`FieldInverter` / `FieldsInverter`** -- one inverter per *field* (field-major
is the design); `FieldsInverter` owns the shared arenas (duckdb
BufferAllocator-backed, IResourceManager-accounted) and the field map with
per-field term-count history as the next segment's reserve hint. Per-doc /
per-value ingest cursors live in one documented object (`DocCursors`) with
layout-slimmed resets.

## Ingest

Ingest entries on `FieldInverter`, by column class (level-1 routing; wrapped
1:1 by `SegmentWriter` / `Document::Insert*`):

- verbatim keyword / PK blocks -> `InvertKeywordBlock` (docs span or ramp
  from `first_doc`); Terms-layout keyword blocks without norms take
  `CaptureKeywordTerms` -- inline capture only, log untouched for
  first-occurrence-only (PK-shaped) columns.
- constant term (null, bool) -> `InvertConstantBlock`: one resolve covers any
  number of docs.
- analyzed columns -> `InvertBlock(batch, runs)`: term resolution runs once
  over the whole block (adaptive: fused probe while the dictionary is under
  `kFusedProbeThreshold` distinct terms, prefetch-pipelined once it goes
  cache-cold), then per run `TokenRunTerms` (dictionary-inline loop) or
  `TokenRunPos` (positional pipeline). Batches flagged `one_to_one` (every
  run is one doc's single token -- set by the dispatch for
  `TokenTraits::SingleToken()` kernels, cleared by kernel reject paths, and
  shape-checked `runs.size() == count` before trusting) take
  `InvertOneToOne`: the fused strided loop for Terms, constant-folded
  `TokenRun(n=1)` for pos layouts (+10.7% on realistic single-token
  columns).
- numeric trie slabs -> `InvertBlock(terms, docs, tokens_per_doc)`: strided,
  runless, Terms-only; the connector stages the precision-trie terms into
  its own slab (`SearchSinkInsertBaseImpl::InsertNumericColumn`).

Invariant: flush derives its cursors from the log itself, so every reject
must precede any slot capture or log push -- validation first, accounting
last. Kernels emit value-absolute pos/offs across resumptions; `DocCursors`
captures the value bases at each value's first batch so multi-batch
continuations rebase by the value start.

## Batch tokenization (`analysis/batch/`)

`TokenBatch` (POD, capacity 1024) is the unit of transfer: term bytes
(`string_t`), pos, offs lanes + the level-2 hints (`dense_pos`,
`one_to_one`); doc segmentation travels beside it as a `DocRun` span. A
kernel is one function: `template<TokenLayout L> bool
DoFill(std::string_view, TokenEmitter&)` plus `Traits()`;
`analysis::TypedTokenizer` owns the one `ResolveLayout`, sets the batch
hints from traits, and runs the bracketing column loop
(`BeginValue`/`DoFill`/`EndValue`) -- the layout is a `Fill` parameter,
stored nowhere. `TokenEmitter` (kernel-facing: `Emit<L>`/`EmitInterned<L>`
carry the only lane guards; `Next`/`Intern`/`Reserve`/`Store`/`buf`) and
`TokenWriter` (driver-facing: bracketing, `Bind(consumer[, store])`,
`Finish`/`Discard`/`Runs`) split one object into two capability sets.
`Store()` delivers a value's blob straight to the bound store sink
(`OnStore(doc, blob)`), which the connector's `InverterSink` points at the
blob-column appender -- no intermediate state. `InverterSink`
(`server/connector/inverter_sink.hpp`) is the ingest consumer: it adapts
`Consume` -> `Document::InsertBlock` and owns the pooled writer.

## Flush

Per field: histogram the log's term ids into an id-indexed cursor array ->
fold each live term's inline-doc count in -> rank terms by {8-byte
big-endian prefix key, id} (key-first `std::sort` under 2048 terms, LSD
radix above: constant digits skipped, equal-key runs term-compared) ->
prefix-sum in rank order, materializing per-rank region `bounds`
(`TermBegin/End(rank) = bounds[rank] / bounds[rank+1]`; the cursor array is
scatter-internal write heads only) -> emit inline docs ahead of each term's
log occurrences -> stable counting-sort scatter of the log into term-major
{docs, pos, offs} regions living in fixed blocks (`ScatterView` two-level
indexing -- consumers are doc-at-a-time iterators, contiguity is never
needed; dense docs reconstruct positions from the within-doc ordinal, no
pos-column reads) -> columnar Term/Doc/Pos readers feed the unchanged
`burst_trie::FieldWriter`; the doc iterator is a pure run-scan (no
inline/scatter merge). All flush scratch (`ScatterScratch`: block pool,
cursors, bounds, rank arrays -- declared beside its users in
`columnar_flush.hpp`) is writer-owned, reused across fields within a flush,
and released at flush end. Zero-occ entries (resolved-only leftovers) are
filtered. Log columns are fill-time bitpacked (SIMD min-FOR per 1024-value
block; constant blocks store zero bytes; offset ends stored as span lengths,
~3 bits), unpacked block-at-a-time. Method bodies live in
`columnar_flush.cpp` -- the header carries no `burst_trie` dependency.

## Measured (tests/bench/micro/invert_kernels, Release)

- Keyword per-value: ~3x the old engine; block entry + Terms fast path:
  ~72M values/s high-cardinality, neutral on low-cardinality.
- Flush scatter phase: 22ms / 4M occ (~179M occ/s) -- prefix-key LSD radix
  rank + id-indexed cursors + block-pool output (2.8x the first columnar
  flush).
- 1-1 fast path (`BM_ColumnarOneToOne`): +10.7% on low-cardinality
  single-token columns (75.1 -> 83.2 Mt/s), +1.9% on unique-term (PK-shaped,
  dictionary-bound) streams.
- split->lower->stopwords pipeline fast path: 71M tokens/s, byte-identical
  postings to the generic chain.
- High-cardinality keyword memory: -14% vs pre-inline/bridging (runs -99%);
  PK-shaped columns: occurrence log eliminated entirely.

End-to-end ingest is tokenizer-bound (the batch kernels exist for that
reason); benchmark inversion with `invert_kernels` only -- end-to-end
harnesses are dominated by input parsing.

## Verification

- `tests/libs/iresearch/index/inverter_tests.cpp`: exact-postings anchors,
  block==per-value differentials (verbatim, text, numeric, constant),
  synonym/overlap ground truth, inline-spill, run-bridging and packed-column
  roundtrip unit tests, `InverterOneToOneTest.MatchesGeneralPath` (fast path
  output-identical to the general loop across layouts x position modes) --
  run under MALLOC_CHECK_=3 (Fastpack/simdfor buffer contracts bite
  silently).
- `tests/libs/iresearch/analysis/tokenizer_tests.cpp` `token_sink_tests`:
  run protocol (value runs, kOpenValue continuation, run-capacity forced
  cycle, one_to_one arming/clearing).
- `inverter_oracle_test.cpp` (env-gated `SDB_ORACLE_DIR`): byte-identical
  segment files across feature classes vs a reference build.
- sqllogic `sdb/pg/index/*` and `recovery/wal_index_recovery_*` cover the SQL
  surface and WAL-replay identity through the sink block writers (runnable
  locally inside the `serenedb-build-ubuntu` image, which carries cargo).

## Write-path core (2026-07-22 restructure)

One Terms body and one positional pipeline replace the five formerly cloned
per-doc push bodies; the five public entries are unchanged.

- `PushDocTerms<Log, kN>`: the only Terms push (Reset -> budget -> TryInline
  compaction -> `PushBatch`). `kN` pins the token count at compile time
  (`kDynTokens` = runtime n); `TokenRun<1>` is the one_to_one fast path --
  the former hand-inlined clone is gone, the constant-folding survives via
  the template axis. `CaptureKeywordTerms` stays as the deliberate
  no-Reset bulk path for norm-free keyword blocks.
- `PushDocRun<Log, kN>` = `ValidatePos`/`ValidateOffs` (const, read-only by
  type) -> `CheckDocBudget` -> `CommitRun` (void: no failure path after the
  first log mutation). The `Offs` feature bit is set commit-side only.
- Singletons: `CheckDocBudget` (one overflow guard), `RoutePos` in
  `PostingLogPosBase` (one dense/explicit promotion decision;
  `PromoteCurrentDoc` is its mechanism).
- Push-carries-doc: every mutating log entry takes `doc_id_t` and opens its
  own doc slot; `BeginDoc` is protected, `TouchDoc` records an explicit
  empty doc. "A doc whose tokens are all inline never touches the log" is
  structural, not a convention.
- `DocState` (nested in `FieldInverter`): all doc-scoped uncommitted state
  (stats, last_doc, cursors, value bases) -- one POD, one `ResetDoc<L>`.
  Ownership rule: doc-scoped uncommitted -> DocState; field-scoped ->
  FieldInverter; committed -> the log. Hot note: `last_doc` is the first
  member (the per-value transition compare is the hottest load; placing it
  behind the zeroed stats cost a consistent ~3% on keyword arms).
- Inliner contract: the cores and `TokenRun` are IRS_FORCE_INLINE and the
  one_to_one loop is IRS_NO_INLINE -- replacing a hand clone with a template
  core requires pinning the inliner; equal instruction counts with lower
  IPC in an A/B means fusion regressed (verify with objdump call counts
  into the loop, not by trusting `if constexpr`).
- Verified: flush oracle byte-identical vs the pre-restructure tree;
  interleaved ABBA neutral-or-better on every invert/scatter arm with the
  one_to_one wins preserved; memory counters bit-identical.

## ClickHouse head-to-head (MergeTreeIndexText, CH 26.7, 2026-07-22)

Harness: `text(tokenizer='splitByNonAlpha')` + `MATERIALIZE INDEX`
(mutations_sync=2, their isolated index-build phase) vs
`CREATE TEXT SEARCH DICTIONARY (template='split_by_non_alpha')` +
`CREATE INDEX ... USING inverted`. Same corpora, token streams identical by
construction. Both sides effectively single-core on these corpora.

- zipf text (2M rows / 237 MB / ~50M tokens): CH 5.19 s (6.88 s at
  max_threads=1) vs 1.47 s filter-shape = 3.5x (4.7x single-core);
  1.98 s with full freq+pos+norms (capabilities CH does not have) = 2.6x.
  Index bytes comparable (~115 MB both).
- low-card keyword (10M rows, 1 token/row, 1k vocab): CH 0.36 s vs 0.92 s --
  but ~69% of our build is the PK term field (10M unique terms; CH builds
  no PK structure, it rides implicit row numbers). Body-only we are ~0.3 s.
  The 82 MB `.idx` is the PK term dictionary (~8 B/term), not posting
  bloat; the PK column itself compresses to ~0.6 MB.
- CH capability gap (by design, it is a filter index): no term frequency
  (postings dedup per row), no norms, positions experimental-off, no
  offsets, no BM25. Their structural edges are elsewhere: merge-not-rebuild
  on part merges and bitpacked-delta postings with per-block skip metadata.

## PK-field cost: findings and open direction

- Baseline lowcard profile: SwissTable growth 22.7% flat, probe/insert
  ~55% cumulative, entries growth 19.3% -- all PK-dictionary construction.
- Per-chunk exact `ReserveTerms` is a trap: repeated exact reserves turn
  geometric growth into a linear rehash cascade (measured 0.92 -> 2.84 s).
  A reserve must fire once, from the operator's total row estimate.
- Unique-mode dictionary (append-only: no hash, no probe, empty map) is
  implemented but DORMANT: `TermDictionary::Append`,
  `FieldInverter::MarkUniqueTerms`, and the flush-side duplicate fold
  (`FoldDuplicateTerms` -> `term_starts` remap; duplicate PK entries sort
  adjacent by id = doc order, regions are contiguous by construction).
  The sink does not arm it: the live dictionary map is queried by
  same-segment remove/delete-by-PK, so an empty map silently fails to
  remove in-flight docs (caught by
  `DuckDBSearchSinkWriterTest.InsertDeleteInsertWithExisting`; live smokes
  cannot reach in-flight-segment paths). Re-arming requires a
  removal-aware design -- the candidate is lazy map construction on the
  first same-segment lookup (removals are rare, ingest stays append-only).
- With the table removed the bottleneck moves: per-row PK key
  materialization (~31% memmove) and commit/refresh (~25%) dominate; those
  are the next levers regardless of the dictionary design.
