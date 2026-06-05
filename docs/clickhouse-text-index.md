# ClickHouse text index: tokenization, inversion, merge -- vs our iresearch path

Source read of ClickHouse master (`d5db9a7ed7f`, the new `TYPE text(...)` index, not the
old experimental GIN) against our iresearch write path. Citations are `file:line` in
`/home/mironov/projects/ClickHouse` or this repo.

TL;DR of what is structurally different from us:

1. **Postings carry doc-ids only.** No freq, no positions, no offsets. The entire
   in-memory inversion is `StringHashMap<PostingListBuilder>` + `Arena`, where a
   posting builder is a 24-byte union: inline `array<UInt32, 6>` that upgrades to a
   Roaring bitmap on the 6th distinct row. No byte-slice chains, no int-pool stream
   pointers, no per-byte branches.
2. **Token dedup is the hot probe, and CH made it ~free.** `StringHashMap` packs
   tokens <=24 bytes *inline in the hash cell* (as UInt64/UInt128/UInt192), so probe
   equality is a register compare -- no pointer chase, no arena touch on lookup. Our
   `Postings::emplace` equality does two dependent cache misses
   (`_postings[ref].term` -> byte pool memcmp) on every H2 match.
3. **Lowercasing is not the tokenizer's job.** A `preprocessor` index option holds an
   arbitrary deterministic SQL expression (`lowerUTF8(col)`), executed vectorized over
   the whole column slice before tokenization. Tokenizers are case-preserving byte/
   codepoint scanners, one of them SIMD.
4. **Merges k-way-merge the sorted dictionaries and never re-tokenize** (same idea as
   our consolidation), with a fallback decision tree: parts missing the index are
   re-tokenized individually inside the merge pipeline; row-reducing merges (TTL,
   dedup, lightweight delete) rebuild the whole index on the result part.

---

## 1. Tokenization

### 1.1 Tokenizer inventory

Six tokenizers behind one `ITokenizer` interface (`src/Interpreters/ITokenizer.h:26-34`),
registered in `TokenizerFactory` (`TokenizerFactory.cpp:154-284`). `tokenizer = ...` is a
**required** index option; others: `preprocessor`, `dictionary_block_size` (512),
`dictionary_block_frontcoding_compression` (1), `posting_list_block_size` (1 MiB),
`posting_list_codec` ("none") (`MergeTreeIndexText.cpp:1635-1640, 1690-1756`).

| Tokenizer | Algorithm | Unit |
|---|---|---|
| `splitByNonAlpha` | separator = `isASCII(c) && !isAlphaNumericASCII(c)`; all bytes >=0x80 are token content, so UTF-8-safe without decoding (`ITokenizer.cpp:98-121`) | bytes |
| `ngrams(N)` (default N=3) | overlapping window of **N codepoints** via `UTF8::seqLength`, advances 1 codepoint per token (`ITokenizer.cpp:22-36`) | codepoints |
| `splitByString([seps])` | memcmp each separator at position, vector order matters (`ITokenizer.cpp:213-266`) | bytes |
| `array` | whole value = one token (`ITokenizer.cpp:297-307`) | -- |
| `sparseGrams(min,max,cutoff)` | content-defined variable-length grams, UTF-8 aware, stateful (cloned per thread) (`ITokenizer.cpp:333-355`) | codepoints |
| `asciiCJK` | UAX#29-ish: ASCII alnum+`_` runs with `: . '` connectors; every non-ASCII codepoint is its own single-char token (`ITokenizer.cpp:470-574`) | mixed |

### 1.2 The SIMD scanner

`SplitByNonAlphaTokenizer::forEachTokenImpl` (`ITokenizer.h:202-299`) scans 16 bytes per
iteration. SSE4.2 path: one `_mm_cmpestrm` with `_SIDD_CMP_RANGES` against
`{0-9, A-Z, a-z, 0x80-0xFF}` produces a bitmask of "token byte" positions; SSE2 fallback
builds the same mask from signed compares (>=0x80 is negative => in-range). Token
boundaries come from `countr_zero` over the mask. **Precondition: the buffer is
right-padded with >=15 readable bytes** -- guaranteed by ClickHouse columns (PODArray
`pad_right`). Disabled under MSan because it reads padding.

### 1.3 Token emission: zero-copy callback, no virtual call per token

The hot interface is `forEachToken(tokenizer, data, len, callback)` where
`callback(const char*, size_t)` receives slices into the source buffer
(`ITokenizer.h:458-506`). Dispatch is a type switch on `tokenizer.getType()` -- the
per-token path has neither a virtual call nor an allocation. The copying APIs
(`stringToTokens` etc.) are explicitly documented "inefficient, constants only".

### 1.4 Case folding = preprocessor expression, not tokenizer

No tokenizer lowers case; there are no stop words, no stemming, no token length limits
anywhere. Normalization is the `preprocessor` option -- an arbitrary SQL expression
validated to be: a function (not bare identifier), String/FixedString out, same array
dimensionality, deterministic, no `arrayJoin` (`MergeTreeIndexTextPreprocessor.cpp:110-161`).

- Build time: `processColumn` runs the ExpressionActions DAG over the whole block slice
  (vectorized) before row iteration (`MergeTreeIndexText.cpp:1530`).
- Query time: `processConstant` runs the same expression on needles before tokenizing
  them (`MergeTreeIndexConditionText.cpp:520-542`).
- For `Array(String)` columns the expression is auto-wrapped in `arrayMap(x -> expr(x), col)`.
- A special `is_lower_or_upper` flag is set only when the expression is *exactly*
  `lower|upper|lowerUTF8|upperUTF8(col)` -- it gates the ILIKE case-insensitive
  dictionary-regex optimization (`MergeTreeIndexTextPreprocessor.cpp:186-201`).

Consistency between index and query is structural: one `ITokenizer` instance and one
preprocessor are shared by the aggregator and the index condition
(`MergeTreeIndexText.cpp:1608-1616`), and `SELECT tokens(...)`/`hasAnyTokens` use the
same factory.

### 1.5 Contrast with our analyzer stack

Ours (iresearch `text`/`segmentation`/`ngram`/`pipeline`/...) is a strict superset in
features: ICU locale-aware `toLower`, NFC, accent folding, snowball stemming, stopwords,
per-token positions/offsets, analyzer composition. CH's bet is the opposite: tokenizers
are dumb, fast, byte-oriented scanners; anything semantic is pushed into one vectorized
column expression. The costs we pay that CH doesn't: ICU UTF-16 round trip per word in
`text`, per-token virtual `next()` dispatch through the analyzer graph, and attribute
plumbing (`IncAttr`/`OffsAttr`) even when the field only needs filtering. Our
`segmentation` analyzer already has the ASCII fast path idea
(`segmentation_tokenizer.cpp:156-167`).

---

## 2. Write path: the invert process

### 2.1 Driver

A text index is a skip index with granularity forced to 100,000,000
(`ASTIndexDeclaration.cpp:107-111`) -- i.e. **the whole part is one granule**. The
generic skip-index driver (`MergeTreeDataPartWriterOnDisk.cpp:214-267`) feeds blocks to
`MergeTreeIndexAggregatorText::update`; the flush-per-granularity branch never fires
mid-part, and the single granule is serialized at part finalize in
`fillSkipIndicesChecksums` (`:329-336`).

```
update(block):                                MergeTreeIndexText.cpp:1509-1570
  preprocessed = preprocessor->processColumn(col, pos, rows)   // lowerUTF8 etc, vectorized
  for each row (or array element; null skipped):
    addDocument(string_view):                 :1429-1448
      forEachToken(tokenizer, data, len, λtoken):
        ArenaKeyHolder kh{token, *arena}
        tokens_map.emplace(kh, it, inserted)  // StringHashMap<PostingListBuilder>
        it->getMapped().add(current_row, posting_lists)
    incrementCurrentRow()
```

Doc id = `UInt32` row number within the part; parts >4.29B rows throw
(`MergeTreeIndexText.cpp:1518-1523`).

### 2.2 The dedup map -- why their probe is cheap

`TokenToPostingsBuilderMap = StringHashMap<PostingListBuilder>`
(`MergeTreeIndexText.h:146`). `StringHashMap` is the length-bucketed string table
(`src/Common/HashTable/StringHashTable.h:322-404`): submaps for keys of 1-8 / 9-16 /
17-24 bytes store the key bytes **packed inline in the cell** as
UInt64/UInt128/StringKey24; only >=25-byte keys go to a generic `string_view` submap.

- Probe equality for short keys is `bitEquals` => integer `==` (`HashTable.h:75-82`).
  The candidate key is in the same cache line the hash probe already loaded.
- For short keys the Arena is never touched on lookup: dispatch calls
  `keyHolderDiscardKey`, a no-op for `ArenaKeyHolder`
  (`StringHashTable.h:372-396`, `HashTableKeyHolder.h:107-109`). Token bytes are copied
  into the Arena only when a *new long* key is persisted.
- The long-key submap gates memcmp with a saved hash
  (`HashMap.h:174`).

Compare our `Postings::emplace` (`libs/iresearch/include/iresearch/index/postings.cpp:72-82`):
the set stores `ValueRef{ref, hash}`; `TermEq` equality dereferences
`_postings[lhs.ref].term` (cache miss #1, separate vector) then memcmps bytes in the
byte pool (cache miss #2, third allocation) -- and **never consults the stored 64-bit
hash** before the memcmp (`postings.hpp:108-112`); the only pre-filter is abseil's 7-bit
H2 tag. Same number of probes per token; the cost asymmetry is entirely in the equality
step.

### 2.3 The postings accumulator

`PostingListBuilder` (`MergeTreeIndexText.h:90-143`, `.cpp:1392-1427`):

```cpp
union { std::array<UInt32, 6> small;                    // 24 bytes
        struct { PostingList* postings; roaring::BulkContext context; } large; };
UInt8 small_size;
```

- Row ids arrive non-descending; same-row repeats are dropped by a single compare with
  the last element.
- <=5 distinct rows live in the inline array -- zero heap. The **6th distinct row**
  allocates a `roaring::Roaring` in `std::list<PostingList> posting_lists` (std::list
  for pointer stability) and bulk-loads the 6 values. (Watch the off-by-one: `isSmall()`
  is `small_size < 6`; the on-disk "embedded" threshold below is `<= 6` -- same number,
  different comparison, unrelated constants.)
- After upgrade, inserts go through `addBulk` with a `BulkContext` that keeps the cursor
  on the last roaring container -- O(1) for the ascending-row pattern.

That's the whole inversion state: no freq, no positions, no per-term stream pointers, no
slice chains. The long tail of rare tokens (most of any zipfian dictionary) costs 24
inline bytes + the arena'd key, total.

### 2.4 Memory lifecycle

- **Normal INSERT path: no cap.** The accumulator grows for the whole part;
  `memoryUsageBytes()` is reported to the MemoryTracker but never triggers a flush. The
  implicit bound is part size.
- **Materialize/merge build path: capped by token count.** `BuildTextIndexTransform`
  cuts a temporary segment every 100M processed tokens
  (`TextIndexUtils.cpp:143,152-153`), serializes it, then continues with
  `setCurrentRow(num_processed_rows)` so row ids stay part-absolute across segments.
  Temp segments are later k-way merged like part segments.
- **Reset is full-free**: `tokens_map = {}`, `posting_lists.clear()`, fresh `Arena`
  (`MergeTreeIndexText.cpp:1477-1485`). Ours is the opposite: `FieldsData::reset()`
  rewinds the byte/int pool cursors and keeps the blocks
  (`field_data.cpp:1061-1066`); pools shrink only if a recycled `SegmentWriter`'s
  `memory_reserved()` exceeds the limit on acquire (`index_writer.cpp:1633-1636`).
  Our retain-and-rewind is fine for steady load (no realloc churn); their no-cap is
  worse for huge parts -- our 256 MB `segment_memory_max` + flush
  (`index_writer.cpp:745-748`) plays the role of their 100M-token segment cut.

### 2.5 Flush to disk

`build()` (`MergeTreeIndexText.cpp:1456-1475`) walks the map once into
`vector<pair<string_view, PostingListBuilder*>>` and does one `std::ranges::sort` --
sorting deferred to flush, same as our `get_sorted_postings`.

`serializeBinaryWithMultipleStreams` -> `serializeTokensAndPostings`
(`:1273-1331`) writes three substreams (`getSubstreams`, `:1585-1593`):

- **`.idx`** -- header: version (`Initial=0`/`WithCodec=1`), codec type, and the
  **sparse index**: first token of every dictionary block + its file offset, serialized
  as a ColumnString + ColumnUInt64 (binary-searchable, like a PK).
- **`.dct`** -- dictionary blocks of `dictionary_block_size` (512) sorted tokens. Each
  block starts a fresh compression frame so blocks decompress independently. Tokens are
  **front-coded** by default (varint LCP + suffix; `:861-882`). After the tokens, a
  `TokenPostingsInfo` per token: header flags (varint), cardinality, then either
  embedded postings or per-block `(offset into .pst, min row, max row)`.
- **`.pst`** -- posting lists, *not* wrapped in the generic compressed buffer
  (`MergeTreeIndicesSerialization.h:33-38`).

Cardinality-adaptive postings encoding (`serializePostings`, `:958-1028`; flags
`MergeTreeIndexText.h:154-169`):

| Cardinality | Encoding |
|---|---|
| <= 6 | `RawPostings\|EmbeddedPostings` -- varint row ids inlined **into the dictionary block**; no `.pst` read at all |
| <= 12 | `RawPostings\|SingleBlock` -- varint row ids in `.pst` (roaring's 48-byte floor isn't worth it) |
| <= posting_list_block_size | `SingleBlock`; portable roaring, or codec if configured |
| larger, codec=none | split via zero-copy views into roaring's container array (`splitPostings`, `:884-931`), one portable roaring per block + min/max range each |
| larger, codec=bitpacking | `IsCompressed\|HasBlockIndex`, see below |

**Bitpacking codec** (`MergeTreeIndexTextPostingListCodec.*`, `BitpackingBlockCodec.h`):
delta-encode (adjacent_difference) then frame-of-reference bitpack in 128-id blocks
(simdcomp SSE2 on x86, byte-identical scalar fallback elsewhere; `[1B max_bits][payload]`
per block, no PFOR patching -- one outlier widens the whole block). Logical segments of
`posting_list_block_size` ids carry `{codec, payload_bytes, cardinality, first_row_id}`
headers plus a **V2 index section**: column-oriented arrays of per-block `last_row_id`
and relative offset -- that's what the read cursor binary-searches
(segments -> blocks -> within decoded block) without decoding intervening blocks.
Read-side niceties worth knowing for our indexing plans: dense-segment `memset`
shortcut, already-covered/all-zero output skips for OR/AND, adaptive AND
(brute-force byte counters vs leapfrog chosen by density), embedded small lists decoded
zero-copy.

---

## 3. Merge path

`MergeTextIndexStage` is the third of four merge stages, after all columns are merged
(`MergeTask.h:590-598`).

### 3.1 Decision tree (`MergeTask.cpp:2816-2822`, `:2242-2253`)

```
merge_may_reduce_rows?            // TTL drops, dedup, cleanup, LWD, non-ordinary mode
├─ yes -> full rebuild: BuildTextIndexTransform on the merged result stream,
│        merged_part_offsets = nullptr (row ids already final)
└─ no  -> for each input part:
         ├─ part has index on disk (checksum probe) -> use its files directly
         │  as TextIndexSegment{part_storage, index_file, part_index}
         └─ part lacks index -> BuildTextIndexTransform inserted into THAT part's
            read plan inside the column-merge pipeline (re-tokenizes only this part,
            spills temp segments every 100M tokens)
         then: MergeTextIndexesTask k-way merges all segments
```

If `materialize_skip_indexes_on_merge=false` the index is dropped from the result and
rebuilt lazily later (`:907-913`).

### 3.2 Row-id remap

During the column merge a virtual `_part_index` column is read from every part and
`MergedPartOffsets` records, in merge output order, `offset_maps[part].insert(row++)`
(`MergeTask.cpp:1492-1502`). Merges that don't reduce rows preserve relative order
within a part, so each map is monotonic and stored frame-of-reference-packed.
`adjustPartOffsets` then maps every old row id to its position in the new part
(`TextIndexUtils.cpp:326-339`). Gaps can't occur: any row-dropping merge took the
rebuild branch instead. This mirrors our `DocRemap` (`merge_writer.hpp:52-62`) -- base-id
shift when fully live, explicit map with deletes.

### 3.3 The k-way merge (`MergeTextIndexesTask`, `TextIndexUtils.cpp:405-460`)

- One `SortCursor` per input segment over the tokens column of its **current dictionary
  block**, in a `SortingQueue` -- dictionary blocks are streamed lazily one at a time per
  source (`readDictionaryBlock`, `:291-303`), so memory is bounded by
  `#segments x dictionary_block_size` tokens plus the current token's postings.
- Per queue pop: if the token differs from the last output token, flush the accumulated
  posting list (re-encoded through the same `serializePostings`, so the new part gets
  fresh embedded/raw/roaring/bitpacked decisions) and start a new one; flush a
  dictionary block every 512 output tokens; record the sparse index entry per block.
- Postings of the current token: embedded ones come from the dictionary block, others
  `seekToMark` into `.pst` and deserialize; each is decoded to roaring, remapped, then
  `output_postings |= *posting` (`:442-446`). **Decode + re-encode, not block splice** --
  same trade-off as our consolidation's decode->remap->re-encode.
- `executeStep` is time-sliced (`background_task_preferred_step_execution_time_ms`);
  indexes are merged serially, single-threaded; a TODO notes multi-stage merge to cut
  memory (`TextIndexUtils.h:69-70`). The parallelism lives upstream: per-part build
  transforms run inside the parallel column-merge pipeline.

### 3.4 Mutations (`MutateTask.cpp`)

Untouched column -> index files hardlinked (`:2063-2075`). Touched or newly materialized
-> rebuild by re-tokenization via `BuildTextIndexTransform` over the mutation stream,
then a consolidation `MergeTextIndexesTask` over its own temp segments with
`merged_part_offsets = nullptr` (`:1741-1742, 1878-1899`). Mutations never merge old
index segments.

---

## 4. Our slow inversion, mechanically (what to fix)

The write-path costs visible in our code, ranked:

1. **Per-byte branchy slice writer.** Every varint byte goes through
   `BlockPoolSlicedInserter::operator=` which tests `if (*_where)` per byte and may
   trigger slice growth (copy trailing bytes, write 4-byte forward pointer, memset new
   slice, repoint) (`block_pool.hpp:844-853, 786-813`). Slice levels
   5->14->20->30->40->40->80->80->120->200 mean a hot term's postings are scattered across
   dozens of chained slices -- cache-hostile on write *and* on the flush re-read
   (`next_slice`, `:512-526`).
2. **Two indirections before writing anything.** Each `add_term` seeks the int pool
   (div/mod per access) to load stream-end offsets, constructs a fresh
   `sliced_inserter`, writes, stores the offset back (`field_data.cpp:704-770`).
3. **Equality = two dependent cache misses** (§2.2). One-line mitigation available:
   gate `TermEq` with the already-stored `ValueRef::hash` before the memcmp; real fix is
   co-locating key bytes with the slot (CH-style inline short keys).
4. **Per-token indirect dispatch** through `_proc_table` member-fn pointers
   (`field_data.cpp:970`) and analyzer virtual `next()`.
5. **Cookies in sorted mode**: extra byte+varint per doc transition + greedy-slice
   chase (`field_data.cpp:855-877`).

Things that are *not* the problem:

- **Flush** already fully re-encodes (slice varints -> block-128 + skip lists + burst-trie
  blocks, FST built in `EndField`) -- the on-disk format is decoupled from the in-memory
  inverter. A new accumulator only has to feed the `BasicTermReader`/`DocIterator`
  contract consumed by `FieldWriterImpl::write`/`PostingsWriterImpl::Write`.
- **Consolidation** (`MergeWriter::Flush`, `merge_writer.cpp:745-831`) is already the
  CH-shaped good path: k-way by field then term, doc-id remap, streams postings, never
  tokenizes. It depends only on reader interfaces, not on
  `FieldData`/`Postings`/`block_pool` internals -- it survives any write-path redesign.

## 5. Design notes for the new write path

- The CH accumulator shape -- *string-keyed map with inline short keys -> tiny inline
  vector -> growable bitmap/container per term* -- is directly applicable to our doc-id
  stream even though we must also keep freq/pos. Split per-term state by feature: the
  doc-id container can be small-array->roaring (or delta buffer); freq/pos can go into
  per-term append-only chunked buffers (power-of-two chunk growth, batch varint writes
  into a contiguous scratch) instead of 5/14/20-byte slice chains. Fields with
  `IndexFeatures::FREQ|POS` off should pay exactly the CH cost.
- Keep our per-segment memory cap + temp-segment spill; CH's INSERT path (unbounded,
  whole part in RAM) is the weaker design, and their own merge-side builder reinvents
  our segment cut (100M tokens) anyway.
- Tokenization: the byte-oriented SIMD scan + zero-copy callback works because columns
  guarantee 15 bytes of right padding -- if we want the same fast path over DuckDB string
  vectors, padding has to be arranged or the tail handled scalar. The
  preprocessor-as-column-expression idea maps naturally onto our analyzer split:
  vectorize the case-fold/normalize stage over the whole vector once, then run the dumb
  splitter per row.
- Cardinality-adaptive postings (embedded <=6 in the dictionary, raw varint <=12,
  FOR-bitpacked 128-blocks with a per-block last_row_id index above) plus the read-side
  skip tricks (dense memset, covered-skip, adaptive AND) are the parts worth stealing
  for the new columnar-store-based indexing, independent of the write-path question.

### Expression vs in-index tokenization

We already support indexing expressions, so the CH question -- where does tokenization
live -- maps onto: run everything through DuckDB vectorized execution and hand the index
finished tokens, or keep a splitter in the index. The boundary that holds up is
**1:1 vs 1:N**:

- **1:1 document transforms (lower/unaccent/normalize/regexp_replace) -> expression.**
  Flat vector in, flat vector out, vectorized once per block, replayed on constant
  needles at query time. This is CH's preprocessor, and DuckDB already has the
  functions.
- **1:N split -> index.** The split's only consumer is the dedup hash map; routing it
  through the expression engine materializes a LIST<VARCHAR> per row (list entry +
  string_t per token) that the index immediately re-reads and discards. The in-index
  splitter emits zero-copy views straight into the hash insert. The split also owns
  what an opaque expression cannot express: per-token positions/offsets (phrase,
  highlight), array-element doc-id semantics, and dedup-aware emission. The
  pure-expression model is Postgres' `to_tsvector` GIN design, whose known failure mode
  (tsvector materialization cost, stored tsvector columns as workaround) is the
  empirical argument against it.
- **The combination subsumes expression-only**: CH's `tokenizer = array` is the
  degenerate splitter -- an expression that already produced tokens (LIST column,
  `regexp_split_to_array`) feeds the index in array mode. The reverse direction cannot
  recover zero-copy or positions.

**Per-token transforms (stemming, stopwords, lower) -- run them at flush over the
dictionary, vectorized.** `stem(lower(tokenize(...)))` is the common case; instead of
an in-index token-filter chain (logic duplicated against DuckDB functions) or
expression-side tokenization (list materialization in the hot loop), factor the
pipeline into three slots:

```
pre_expr   doc-level 1:1, vectorized over the column block -- only transforms that must
           precede splitting (lowerUTF8 before ngram, unaccent affecting boundaries)
split      fused in the index: zero-copy emit -> dedup hash; positions and
           array->doc-id mapping live here
post_expr  token-level transforms, executed vectorized over the UNIQUE-TERM dictionary
           at segment flush (ExpressionExecutor over the flat string vector), colliding
           terms merged by roaring OR; the same expression runs on needle tokens at
           query time
```

Why flush-time post_expr wins: transform work becomes proportional to unique terms per
segment, not token occurrences -- 10-100x less stemming for natural language (Lucene/
iresearch stem every occurrence; CH has no stemming). Stopwords post-dedup drop the
term + postings at flush; occurrences already consumed positions = Lucene posInc
semantics for free, zero per-occurrence checks. Every primitive (`stem`, `lower`,
`unaccent`, stopword filter) is implemented once as a DuckDB scalar function -- SQL
users, the needle path, and the index share it. (DuckDB's `lower` is currently the
weakest of the surveyed engines -- patch it once, everyone benefits.) Cost: the
in-memory map keys are surface forms, so the pre-flush dictionary is somewhat larger;
per-function placement (pre vs post) is a knob. Once `stem` participates in index
semantics its behavior is part of the on-disk contract -- pin the snowball version
(same pinning problem analyzers have today).

No new vector type is needed for the expression-side representation: `LIST<VARCHAR>`
is already "flat child VARCHAR vector + list_entry_t per row", any scalar function runs
vectorized over the child, `StringVector::AddHeapReference` (vector.hpp:154) makes
zero-copy token views into the source heap legal, and string_t inlines <=12-byte tokens
into its 16-byte slot anyway (string_type.hpp:54-59). The needed conventions: position
= ordinal in list; NULL = removed token that still advances position (so filter stages
preserve phrase semantics); byte offsets (highlighting) would need a parallel child if
kept.

**Planner matching**: with a declared transform expression, implicit LIKE/ILIKE
rewrites against the base column require recognizing the expression -- CH's
`is_lower_or_upper` flag is the ad-hoc recognizer this degenerates into
(`MergeTreeIndexTextPreprocessor.cpp:186-201`). Explicit match functions bound to
the index config avoid the problem entirely; implicit rewrites need a recognized
transform whitelist.
