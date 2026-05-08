# Posting V2 — Review Notes

Context: review against the design gist
(https://gist.github.com/MBkkt/2e92d201e5188effe66d264a622b101a) of the WIP
posting format on this branch (writer / reader / skip_list / iterator_doc /
iterator_score / wand_writer / format_block_128 / scorer.hpp).

## What was fixed in this pass

These are already on this branch — listed so you know the current state.

1. **Segfault during consolidation on terms with `docs_count == 128`.**
   The boundary `kBlockSize == 128 == Skip0()` was treated inconsistently:
   - `EndTerm`: `has_skip_list = Skip0() < docs_count` (excluded 128)
   - `Encode`: `docs_count > Skip0()` (excluded 128)
   - `decode`: `_block_size < docs_count` (excluded 128)
   - iterator `Prepare`: `docs_count >= kBlockSize` (**included** 128)

   So for 128-doc terms `e_skip_start` was never written/decoded but the
   iterator still seeked to `doc_start + e_skip_start_garbage` to read the
   wand root. Fixed by switching all writer/encode/decode comparisons to
   `<=` / `>=` to align with the iterator's `>=`.

2. **Inverted asserts** in `EndDocument` (`!=` instead of `==`) — was already
   fixed in `29e0bbe`, mentioning for completeness.

3. **`std::optional<uint32_t> norm` in `WandWriter::WandData`** — replaced
   with plain `uint32_t norm` defaulting to 0. The on-disk format already
   signals presence via `norm_code` in the wand encoding byte; the in-memory
   optional was redundant bookkeeping (extra discriminator + branchy access).
   `norm == 0` naturally means "norm == freq" because the stored value is
   the delta `entry.norm - entry.freq`.

4. **`std::optional<WandWriter::WandData>` in `InlineSkipEntry`** — replaced
   with a plain field plus a `bool has_wand` flag. The optional was only
   ever materialised under `HasWand` and dereferenced under the same
   condition.

5. **`ReadBytes` / `WriteBytes` for 1–4 byte values on the hot path**
   (`ReadByteSize1234`, `ReadByteSize124ForSkipEntry`, `SerializeFor1234`,
   `Serialize124ForSkipEntry`, plus call sites in `WriteInlineSkipEntry`,
   `WriteInlinePosPayMetadata`, `WriteWandData`, `ReadInlineSkipZone`)
   — replaced with `switch(code)` over `ReadByte`/`ReadI16`/`ReadI32` /
   `WriteByte`/`WriteU16`/`WriteU32`. On `BytesViewInput` these inline as
   raw memory loads/stores; the previous form went through a virtual
   variadic-length read.

6. **Double-assignment typo** in `ReadPosPayMetadata`:
   `result.pay_ptr = result.pay_ptr = ReadByteSize1234(...)`.

## Plan items not implemented

Listed roughly in expected impact order.

1. **`max_doc` in term metadata.** The plan called for it in `.tm` so:
   - conjunction/filtered queries can short-circuit when ranges don't overlap;
   - `FillBlock` can do the fast-path check `max_doc <= window_end` in a
     single comparison instead of detecting it from the block;
   - skip levels can be sealed using actual `max_doc` instead of an EOF
     sentinel.

   `TermMetaImpl` doesn't carry `max_doc` today.

2. **`all_delta_1` doc-encoding tag** (consecutive doc IDs → 0 bytes data,
   not 1 byte via `de_delta_all_same_08` with value 1). High value: this
   is *the* common case for unique terms.

3. **`all_ones` freq-encoding tag** (every freq=1 → 0 bytes data, not
   1 byte via `e_all_same_08` with value 1). Same shape as #2; common case
   for the "term appears exactly once per doc" pattern.

4. **No-WAND-in-skip-array.** Plan: *"Skip array has no WAND data — only
   stores pointers for seeking."* Today the level-1 writer emits wand data
   per entry and the reader reads/skips it. Pure overhead for the seek
   path (conjunction tail, filtered queries, COUNT, consolidation read).

5. **Last-block-has-no-skip-zone distinction.** Plan: non-last block has
   skip zone; last block doesn't (max_doc is in `.tm`, wand_root serves as
   the implicit per-block summary). Today every full block — including
   the last — emits a 6–15 byte skip zone. The 128-boundary bug we just
   fixed was a symptom of this same conflation.

6. **Term-metadata 2-byte uniform encoding header.** Plan called for a
   single `uint16` LE header per term with 2-bit size classes per field,
   then size-class–dispatched fixed-width writes. Today `Encode` /
   `decode` still emit/parse VInts on every field, which directly violates
   plan principle #1 ("no varints on the hot path"). Term-dictionary
   decode is hit on every term lookup.

7. **Skip-by-max-doc fast path (Path 1).** Plan: when skipping a block
   purely on `max_doc`, touch only ~6 bytes (`skip_enc + max_doc +
   rest_block_size`). Today `ReadInlineSkipZone` always reads wand
   `freq`/`norm` and stores them in the entry, even when the very next
   line is going to discard everything and `Skip(rest_block_size)`. With
   the current wand-bytes-in-skip-zone layout this requires either
   reading the encoding byte and computing wand size (then skipping wand
   bytes without decoding), or reordering the skip zone so `rest_block_size`
   sits earlier and lets us skip wand entirely.

8. **Compact wand root.** Plan: in the common case `freq == 1` and
   `norm == freq` the wand root is just the 1-byte `wand_enc` (both data
   widths = 0). Today `WriteWandData` always writes 1–4 bytes for `freq`.

9. **Backward-seek in `ReadPosPayMetadata`.** Plan order:
   `pos_pay_enc` first, then `pos_block_idx`, then `pos_ptr`, then
   `pay_ptr` — single forward pass. Current layout puts the encoding byte
   at the end, so the reader does `ReadByte → Seek(pos - n - 1) →
   re-read forward → Skip(1)`. Backward `Seek` defeats stream prefetch
   on `IndexInput`. **On-disk format change.**

10. **`ByteSize1234` / `Read|SerializeFor1234` introduces a 3-byte
    payload that the plan never had** (plan was 1/2/4 only via the
    `ByteSize124` family). Used for `max_doc_delta`, inline `pos_ptr` /
    `pay_ptr`, inline wand `freq`, and level-1 `max_doc_delta` — i.e.
    every hot read. Drop it: encoding becomes `{0:1B, 1:2B, 2:4B,
    3:reserved}`, the helper switch shrinks by one branch (and one
    awkward `WriteU16+WriteByte` / `ReadI16+ReadByte` case). **On-disk
    format change.**

11. **Adaptive skip fanout** (`F ≈ sqrt(block_count)`). Today fixed at 32.
    Plan called this acceptable for V1; mentioned for completeness.

## Code-quality cleanups still pending

No on-disk format change for any of these unless noted.

12. **`Features` is runtime-typed inside the writer.** Several places use
    `if (_features.HasFrequency())` etc. on the inline hot path
    (`EndDocument` precompute, encoding-size computation, condition
    inside `WriteInlinePosPayMetadata`). The iterator's `IteratorTraits`
    /`FieldTraits` are compile-time. The writer template should grow the
    same compile-time traits so these branches go away. Not a tiny diff,
    but it's the right shape.

13. **Double-bookkeeping in `EndDocument`.** Pre-computes
    `rest_block_size` and `pos_pay_meta_serialized_size`, then asserts
    that the actual position diff after writing matches. Easy to drift —
    was the immediate enabler of the 128-boundary bug. Replace with:
    record `pos_before`, write everything, set
    `rest_block_size = pos_after - pos_before`. (Backpatch into the skip
    zone or buffer the skip zone in a small scratch and emit at the end.)

14. **Old `SkipWriter` / `SkipReader` / `SkipReaderBase` classes** in
    `skip_list.hpp` are no longer used by the current writer/reader
    (`// SkipWriter _skip;` is commented out in the writer base).
    Several hundred lines of dead code; should be removed.

15. **~80 lines of commented-out code in `BitUnion`** (`reader.hpp:280–345`).
    Either finish the implementation or delete; the live body just calls
    `Iterator(...)->advance()` in a loop, which is fine to ship as a
    starting point.

17. **Half-finished refactor in `PostingIteratorBase`**
    ([`iterator_doc.hpp:135–142`](libs/iresearch/include/iresearch/formats/posting/iterator_doc.hpp#L135-L142)):
    `[[no_unique_address]] utils::Need<...>` for `_collected_freqs` is
    next to `std::conditional_t<...>` for `_freqs`, with a commented-out
    `utils::Need` form for `_freqs` immediately below. Pick one
    mechanism.

18. **`MakeBlockFromTail` `memmove + fill_n` per tail write**
    ([`format_block_128.hpp:894–901`](libs/iresearch/include/iresearch/formats/posting/format_block_128.hpp#L894-L901)).
    The pad-and-bitpack pattern is from the plan, but the per-call
    `memmove` of up to ~127 elements + `fill_n` is on the per-term tail
    path. Probably fine, worth profiling once #1–#5 land.

19. **`SerializeFor1234` was a one-liner `WriteBytes`**. After the
    `switch`-rewrite in this pass it has a real body — keep it inlined
    (`IRS_FORCE_INLINE` already added) and prefer it over open-coding
    the switch at every call site.

20. **`InlineSkipEntry::has_wand`** (added in this pass) is a
    serialization-time concern only. If/when the writer grows
    compile-time `HasWand` (see #12), the field can go.

## Suggested order of work

1. #13 (drop double-bookkeeping) — small, prevents regressions.
2. #14, #15, #16, #17 — pure cleanup, makes everything else easier to read.
3. #1 (`max_doc` in `.tm`) — unlocks #5 and a class of query-side wins.
4. #4 (no wand in skip array) — straight win on the seek hot path.
5. #6 (term-meta 2-byte header) — bigger diff but per-term-lookup gain.
6. #2, #3, #8 (encoding-tag wins).
7. #9, #10 (on-disk format changes) — batch into a single reindex.
8. #7 (skip-by-max-doc fast path) — depends on #4 / #9 layout.
9. #12 — refactor when the dust settles.
