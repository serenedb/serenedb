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
ingest, WKB input binding) or a **level-2 hint** (per binding, describing the
data a column will deliver: `FieldInverter::Configure(TokenTraits)`, armed
at the ingest binding from the analyzer's `TokenTraits`). Traits never
change per fill; hints let the indexing pipeline pick faster paths without
runtime lane switches, and the 1-1 gate re-checks each batch's shape before
trusting its hint.

## Data structures

**`TermDictionary`** -- a SwissTable of bare u32 ids into an entry array; the
array IS the dense term-id space (4-byte ids in the log, flat histogram/rank
arrays at flush). `Entry = HashedTerm` is exactly 24 bytes:
`{duckdb::string_t term, size_t hash}`. The hash lives in the entry, so map
slots are 4 bytes (probe working set /4 vs a ref+hash slot; measured
1.3-1.4x faster resolution on hit-dominated shapes than entry-in-set
layouts -- see invert_kernels BM_Dict{Ref,Direct,PairMap}). Term bytes
intern once per unique term into a shared arena; probes compare via
`string_t`'s length+prefix u64 early-out; the hash is computed once per
token and only ever re-read (placement, prefetch, rehash). Insertion is
intern-only -- there is no occurrence counter.

Unique-bound (PK-shaped) fields skip the map entirely: `MarkUniqueTerms`
arms the append-only mode, `AppendUnique` interns without hashing or
probing, and each term's single doc is captured in the field's id-indexed
`_inline_docs` vector -- such fields build no occurrence log at all (a
field is all-log or all-inline, never both). The rare duplicate PK terms
become duplicate entries; they rank adjacent at flush (id order = doc
order) and `FoldDuplicateTerms` folds each group into one emitted term.
Removal safety: nothing resolves against the live map -- same-segment
delete-by-PK is `SearchRemoveFilter`-based against flushed readers --
pinned by `DuckDBSearchSinkWriterTest.InsertDeleteInsertWithExisting`.

**`PostingLog<L>`** -- per-field, monomorphized on the layout: each
specialization owns exactly the SoA columns its layout indexes (term_ids
always; pos + the explicit-doc bitmap for pos layouts; offs when indexed)
over geometric blocks carved from a shared per-writer arena.
`FieldInverter` holds the three specializations in a variant and dispatches
once per ingest call -- every per-token/per-value layout branch is
compile-time. Doc ids are not stored per occurrence: `Run{first_doc, ndocs}`
headers (8B) plus per-doc token counts map log positions to docs. Gaps of up
to `kMaxBridgedGap = 7` docs (nulls, rows whose tokens all filtered away)
are bridged with zero-count doc slots (bitpacked, ~2 bits each) instead of
breaking the run.
Positions default to *dense* (position == within-doc ordinal, nothing
written); a doc that receives an explicit position is promoted -- its bit set
and its implied ramp backfilled -- and the flag bitmap allocates only on the
first promotion, so all-dense fields carry none. The log owns every
promotion decision (`RoutePos`): batch pushes ramp dense-onto-promoted docs
internally, single-token pushes route through `PushOne`.

**`FieldInverter` / `FieldsInverter`** -- one inverter per *field* (field-major
is the design); `FieldsInverter` owns the shared arenas (duckdb
BufferAllocator-backed, IResourceManager-accounted) and the field map with
per-field term-count history as the next segment's reserve hint. Per-doc /
per-value ingest cursors live in one documented object (`DocState`) with
layout-slimmed resets.

## Ingest

Ingest entries on `FieldInverter`, by column class (level-1 routing; wrapped
1:1 by `SegmentWriter` / `Document::Insert*`):

- PK blocks (unique terms) -> `InvertPrimaryKeyBlock`: append-only interning
  plus per-term inline doc capture, no log (see `TermDictionary` above).
- verbatim keyword blocks -> `InvertKeywordBlock` (UVF: dense valid runs, or
  a sel walk under dictionary encoding) / `InvertKeywords` (streamed pairs);
  norm-free Terms-layout dense runs take `CaptureKeywordTerms`, the no-Reset
  bulk `PushOne` loop.
- bool columns -> `InvertBoolBlock` (UVF, lazy term-id resolve per polarity);
  null docs -> `InvertNullBlock` (UVF invalid rows / all-null doc ramp),
  staged null pairs ride `InvertKeywords`.
- analyzed columns -> `InvertBlock(batch, runs)`: term resolution runs once
  over the whole block (adaptive: fused probe while the dictionary is under
  `kFusedProbeThreshold` distinct terms, prefetch-pipelined once it goes
  cache-cold), then per run `PushDoc` dispatches into `PushDocTerms`
  (Terms body) or `PushDocRun` (positional pipeline). Unique-bound columns
  (`Configure` from `TokenTraits::unique`) whose block passes the shape
  check (no value continuation on either edge and
  `runs.size() == batch.count`) take `InvertOneToOne`, the pinned
  single-token-per-run loop (+10.7% on realistic single-token columns).
- numeric columns -> `InvertNumericBlock` (UVF: fused encode+resolve over
  valid runs) / `InvertNumerics` (streamed pairs, kernel-staged): the
  precision-trie terms are built block-at-a-time into stack slabs
  (`AppendNumericTermsBlock`), resolved in one pass, Terms-only.

Invariant: flush derives its cursors from the log itself, so every reject
must precede any slot capture or log push -- validation first, accounting
last. Kernels emit value-absolute pos/offs across resumptions; `DocState`
captures the value bases at each value's first batch so multi-batch
continuations rebase by the value start.

## Batch tokenization (`analysis/`)

`TokenBatch` (POD, capacity 1024) is the unit of transfer: term bytes
(`string_t`), pos and offs lanes; doc segmentation travels beside it as a
`DocRun` span, and the level-2 hints live on the field binding
(`Configure(TokenTraits)`), not in the batch. A
kernel is one function: `template<TokenLayout L> bool
DoFill(duckdb::string_t, TokenSink&)` plus `Traits()`;
`analysis::TypedTokenizer` owns the one `ResolveLayout` and runs the
bracketing column loop (`BeginValue`/`DoFill`/`EndValue`) -- the layout is
a `Fill` parameter, stored nowhere else. `TokenSink` is ONE class: the
staged batch/arena, driver verbs (`Bind(consumer, store)`,
`BeginValue`/`EndValue`, `Finish`/`Discard`/`DiscardValue`/`Runs`), and the
emit surface
(`Emit<L>` for ready views and whole-value `string_t` handles, the builder
`Emit<L>(size, build, ...)` for bytes constructed into sink memory --
built straight into the batch slot's inline bytes for
`size <= INLINE_LENGTH` (stores only, no staging round-trip), arena-built
otherwise -- the slice/case-convert forms (`EmitSlice` as the single-token
mirror of flat `EmitK`'s descriptor, `EmitCaseConverted` and
`EmitSliceCaseConverted` for ASCII case-converted emits), and the `EmitK<L>` bulk
forms: flat `EmitK(k, base, gen)` for slices of a stable base, staged
`EmitK(k, size, stage, gen)` which allocates (`AllocateStaged`:
`size + kTermViewSlack`, views at `mem + begin`; single-term arena
allocations use `AllocateTerm`: `max(size, kTermViewSlack)`, views
anchored at `mem`) and stages a block via `stage(mem)` per
slot-guaranteed wave for volatile sources) whose explicit layout argument
carries the only lane guards. Verb choice is mechanical: one ready view ->
`Emit(term, ...)`; one constructed term -> `Emit(size, build, ...)`; many
slices of a stable base -> flat `EmitK(k, base, gen)`; many terms built
into sink memory -> staged `EmitK(k, size, stage, gen)`. Single emits take
lane tags (a bare-integer position, an explicit `Offs{start, end}`; both
optional -- no pos means the dense ramp, no offs means the whole-value
span). Bulk emits take no tags: the generator returns a slot descriptor,
any aggregate with `begin`/`end` term bounds plus optional `pos`/`offs`
fields (`EmitKSlot`/`EmitKSlotPos`/`EmitKSlotOffs` are the canonical
spellings); an absent pos leaves the lane dense, an absent offs defaults
to the bounds under the flat form (a slice of the value is its offsets)
and to the whole-value span under the staged form. Kernels see no
reservation or per-value cursor state (the sink's own value bracket --
`_doc`/`_value_size`/`_run_start` -- belongs to the drivers); the arena is
fully private -- every byte enters the batch through `Emit`/`EmitK`. Per-value ingest drivers use the doc-bracketing
`Tokenizer::Fill(value, doc, sink, layout)` overload and never touch
`BeginValue`/`EndValue`; delivered batches are the only inspection surface
(tests assert inside their consumers). `DocRuns` carries `tail_open`
beside the run span. Dense-vs-explicit position reading is a trait of the producing
kernel (`TokenTraits::explicit_pos`): consumers take it from `Traits()` at
their own bind, the sink carries no echo of it.
`Store()` delivers a value's blob straight to the bound store sink
(`OnStore(doc, blob)`), which the ingest binding points at the blob-column
appender -- no intermediate state. `SegmentWriter::TokensTarget`
(`iresearch/index/segment_writer.hpp`) is the ingest consumer: it adapts
`Consume` -> `FieldInverter::InvertBlock` via `WithSlot`, and
`SegmentWriter` owns the pooled sink.

## Flush

Per field: histogram the log's term ids into an id-indexed cursor array ->
rank live (nonzero-occ) terms by {8-byte big-endian prefix key, id}: a
key-sorted fill (PK-shaped ascending interning) is detected and skips the
sort entirely, an over-shared key prefix (URLs, "pk_" ids) is re-keyed past
the common bytes (`RekeyPastSharedPrefix`), otherwise key-first `std::sort`
under 2048 terms, LSD radix above (constant digits skipped, equal-key runs
term-compared); append-only dictionaries fold duplicate entries into one
emitted term (`FoldDuplicateTerms` -> `term_starts`) -> prefix-sum in rank
order, materializing per-rank region `bounds`
(`TermBegin/End(rank) = bounds[rank] / bounds[rank+1]`; the cursor array is
scatter-internal write heads only) -> stable counting-sort scatter of the
log into term-major {docs, pos, offs} regions living in fixed blocks
(`ScatterView` two-level indexing -- consumers are doc-at-a-time iterators,
contiguity is never needed; dense docs reconstruct positions from the
within-doc ordinal, no pos-column reads; pos and offs-start unpack from
within-doc delta streams). All-inline (PK) fields skip everything after the
rank: bounds are the rank identity and docs gather straight from the
dictionary's inline capture, sorted fills even keep identity rank order. The
ranked terms then feed the unchanged `burst_trie::FieldWriter` through
batched pull (`ColumnarTermIterator::NextTermsWithPostings` hands out
`TermPostings` views -- contiguous inside one scatter block, blocked
row-range otherwise -- consumed by `WritePostings`/`VisitRuns`; the classic
per-term iterator surface refuses use). All flush scratch (`ScatterScratch`: block pool,
cursors, bounds, rank arrays -- declared beside its users in
`columnar_flush.hpp`) is writer-owned, reused across fields within a flush,
and released at flush end. Zero-occ entries (resolved-only leftovers) are
filtered. Log columns are fill-time bitpacked (SIMD min-FOR per 1024-value
block; constant blocks store zero bytes; offset ends stored as span lengths,
~3 bits), unpacked block-at-a-time. Method bodies live in
`columnar_flush.cpp` -- the header carries no `burst_trie` dependency.

The log's columns advance together only while no push ever fails midway; an
allocation throw between two column pushes leaves them desynced, escapes the
writer's `_valid` latch, and a pooled writer holding prior transactions'
committed docs would flush the desync. The scatter therefore fails such a
flush instead of writing garbage postings: a column reader's cold refill
(once per 1024 values) throws on exhaustion instead of dereferencing an
empty span, the final per-column totals are cross-checked (docs walked vs
doc slots, ids consumed vs the term-id column, positions vs the pos column,
offs lane lengths up front), and u32 cursor-width overflow aborts the same
way. The per-token loop is unchanged; the per-doc cost is one accumulator
add.

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
  synonym/overlap ground truth, unique-dictionary fold/identity scatter
  tests, run-bridging and packed-column roundtrip unit tests,
  `InverterOneToOneTest.MatchesGeneralPath` (fast path output-identical to
  the general loop across layouts x position modes),
  `InverterRejectTest.*` (a rejected batch leaves the log untouched:
  reject-then-valid flushes byte-identical to a never-rejected stream) --
  run locally under MALLOC_CHECK_=3 or rely on the ASAN CI config
  (Fastpack/simdfor buffer contracts bite silently).
- `tests/libs/iresearch/analysis/tokenizer_tests.cpp` `token_sink_tests`:
  run protocol (value runs, tail_open continuation, run-capacity forced
  cycle).
- `inverter_oracle_test.cpp` (env-gated `SDB_ORACLE_DIR`): byte-identical
  segment files across feature classes vs a reference build.
- sqllogic `sdb/pg/index/*` and `recovery/wal_index_recovery_*` cover the SQL
  surface and WAL-replay identity through the sink block writers (runnable
  locally inside the `serenedb-build-ubuntu` image, which carries cargo).

## Write-path core (2026-07-22 restructure)

One Terms body and one positional pipeline replace the five formerly cloned
per-doc push bodies.

- `PushDocTerms`: the only Terms push (Reset -> budget -> `PushBatch`).
  `CaptureKeywordTerms` stays as the deliberate no-Reset bulk path for
  norm-free keyword blocks; `InvertOneToOne` is the pinned single-token
  loop over `PushDoc`.
- `PushDocRun` = `ValidatePos`/`ValidateOffs` (const, read-only by
  type) -> `CheckDocBudget` -> `CommitRun` (void: no failure path after the
  first log mutation). The `Offs` feature bit is set commit-side only.
- Singletons: `CheckDocBudget` (one overflow guard), `RoutePos` in
  `PostingLogPosBase` (one dense/explicit promotion decision;
  `PromoteCurrentDoc` is its mechanism).
- Push-carries-doc: every mutating log entry takes `doc_id_t` and opens its
  own doc slot; `BeginDoc` is protected, `TouchDoc` records an explicit
  empty doc. "A PK field's docs never touch the log" is structural, not a
  convention.
- `DocState` (nested in `FieldInverter`): all doc-scoped uncommitted state
  (stats, last_doc, cursors, value bases) -- one POD, one `ResetDoc<L>`.
  Ownership rule: doc-scoped uncommitted -> DocState; field-scoped ->
  FieldInverter; committed -> the log. Hot note: `last_doc` is the first
  member (the per-value transition compare is the hottest load; placing it
  behind the zeroed stats cost a consistent ~3% on keyword arms).
- Inliner contract: the cores are IRS_FORCE_INLINE and the one_to_one loop
  is IRS_NO_INLINE -- replacing a hand clone with a shared core requires
  pinning the inliner; equal instruction counts with lower IPC in an A/B
  means fusion regressed (verify with objdump call counts into the loop,
  not by trusting `if constexpr`).
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
  LANDED and armed via `InvertPrimaryKeyBlock` -> `MarkUniqueTerms` /
  `AppendUnique`, with the flush-side duplicate fold
  (`FoldDuplicateTerms` -> `term_starts` remap; duplicate PK entries sort
  adjacent by id = doc order, so regions stay contiguous by construction).
  Its first landing was reverted because the live dictionary map was then
  queried by same-segment remove/delete-by-PK, so an empty map silently
  failed to remove in-flight docs. The removal contract that re-admitted
  it: nothing resolves against the live map -- same-segment delete-by-PK is
  `SearchRemoveFilter`-based against flushed readers -- pinned by
  `DuckDBSearchSinkWriterTest.InsertDeleteInsertWithExisting` (live smokes
  cannot reach in-flight-segment paths). Any future same-segment lookup
  must first build the map lazily (removals are rare, ingest stays
  append-only).
- With interning append-only the bottleneck moves: per-row PK key
  materialization (~31% memmove) and commit/refresh (~25%) dominate; those
  are the next levers regardless of the dictionary design.
