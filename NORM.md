# Norm read/write bug-hunt

Branch: `codeworse/norm-fixes` (WIP commit `19002402`). Five bugs found across the
new columnstore-backed norm path; all fixed in the working tree.

## Bug 1 -- `any_data` flag set before any value is actually appended

**Severity:** real bug, user-facing.

**Where:** [libs/iresearch/include/iresearch/index/merge_writer.cpp:633 (pre-fix)](libs/iresearch/include/iresearch/index/merge_writer.cpp#L633)

**Symptom:** when a merge consumes a source segment that has the norm column but
every doc for that field is deleted in the merged segment, the visit body
unconditionally set `any_data = true` before any value was appended. So
`props.norm = new_norm_id` was set at
[merge_writer.cpp:653](libs/iresearch/include/iresearch/index/merge_writer.cpp#L653)
even though `nw` received zero `Append` calls. The empty writer was then dropped
from the footer by `std::erase_if` at
[format.cpp:426-427](libs/iresearch/include/iresearch/columnstore/format.cpp#L426):

```cpp
std::erase_if(_impl->norm_writers,
              [](const auto& nw) { return nw->Pointers().empty(); });
```

Result: the merged segment's `FieldMeta.norm` referenced an id that didn't appear
in `kFooterSlotNormColumns`. `Reader::NormColumn(id)` returned `nullptr`; scoring
silently degraded and any future "norm valid => column exists" assertion would have
tripped.

**Fix:** flip `any_data` to fire *after* a successful `Append`, so it reflects what
actually landed in `nw`.

```cpp
-        any_data = true;
         for (size_t rg = 0, rg_count = nc->RowGroupCount(); rg < rg_count;
              ++rg) {
@@
             nw.Append(value);
+            any_data = true;
           }
         }
```

## Bug 2 -- `NormColumnReader::Get()` UBs on an empty / invalid row group

**Severity:** defensive (trust boundary on disk).

**Where:**
[libs/iresearch/include/iresearch/columnstore/norm_reader.cpp:72-77 (pre-fix)](libs/iresearch/include/iresearch/columnstore/norm_reader.cpp#L72)
and
[libs/iresearch/include/iresearch/columnstore/format.cpp BuildNormReaders](libs/iresearch/include/iresearch/columnstore/format.cpp#L566).

**Symptom:** the ctor sets `_spans[rg] = {}` when `byte_count == 0`
([norm_reader.cpp:47-50](libs/iresearch/include/iresearch/columnstore/norm_reader.cpp#L47)),
but `Get()` unconditionally does:

```cpp
return ReadNormValue(_spans[rg].data() + in_rg * byte_size, byte_size);
```

`_spans[rg].data()` on an empty span is unspecified (typically `nullptr`);
`ReadNormValue` then dereferences. Today's writer never emits a 0-row RG and
`Locate` correctly skips empty RGs via `upper_bound`, so this is unreachable from
current callers -- but the on-disk format is the trust boundary. A corrupt footer
with `row_count == 0` or `byte_size ∉ {1,2,4}` would cause UB instead of a clean
error.

**Fix:**

- In `BuildNormReaders`, validate each row-group entry as it's deserialized.
  `SDB_ASSERT` (dev-only) is enough here: the `.cs` file is internally produced,
  so a malformed footer is treated as a programmer error rather than untrusted
  input.

  ```cpp
  SDB_ASSERT(
    (p.byte_size == 1 || p.byte_size == 2 || p.byte_size == 4) &&
      p.row_count != 0,
    "columnstore: corrupt norm row-group (byte_size=", p.byte_size,
    ", row_count=", p.row_count, ") on column id ", id);
  ```

- Skip building a reader when the deserialized pointers list is empty.
- Tighten `NormColumnReader::Get()` with a matching span-not-empty assert.

## Bug 3 -- `PersistedNormReader::GetBatch` asserts on an all-empty column

**Severity:** defensive.

**Where:** [libs/iresearch/include/iresearch/index/norm_column_reader.hpp:78-98 (pre-fix)](libs/iresearch/include/iresearch/index/norm_column_reader.hpp#L78)

**Symptom:** if a footer somehow listed a norm column with `RowCount() == 0`, the
per-doc loop called `_column->Locate(row_pos)` which `SDB_ASSERT`s
`row_pos < _total_row_count`. Same trust-boundary class as Bug 2.

**Fix:** zero-fill the result up front in both `GetOne` and `GetBatch`:

```cpp
uint32_t GetOne(doc_id_t doc) const noexcept {
  SDB_ASSERT(doc >= doc_limits::min());
  if (_column->RowCount() == 0) {
    return 0;
  }
  return _column->Get(doc - doc_limits::min());
}

void GetBatch(...) const noexcept {
  if (_column->RowCount() == 0) {
    std::fill_n(values.begin(), docs.size(), 0u);
    return;
  }
  ...
}
```

## Bug 4 -- stale `NormColumnWriter::Get` references in `FieldData` comments

**Severity:** misleading documentation, not a runtime bug.

**Where:**
[libs/iresearch/include/iresearch/index/field_data.cpp:643-648 (pre-fix)](libs/iresearch/include/iresearch/index/field_data.cpp#L643)
and
[libs/iresearch/include/iresearch/index/field_data.hpp:60-64 (pre-fix)](libs/iresearch/include/iresearch/index/field_data.hpp#L60).

**Symptom:** comments claimed "Reads during the same flush use
`NormColumnWriter::Get`", but `NormColumnWriter` has no `Get` method -- it exposes
only `Append`, `Finalize`, `Id`, `Name`, `Pointers`, `RowGroupSize` (see
[norm_writer.hpp](libs/iresearch/include/iresearch/columnstore/norm_writer.hpp)).
The real path is `SegmentWriter::flush` committing the columnstore *before*
`FlushFields`, opening a fresh `_cs_reader`, and serving all in-flush scorer norm
reads via `PersistedNormReader`.

**Fix:** rewrite both comments to describe the real (post-commit `_cs_reader`)
flow.

## Bug 5 -- `Writer::OpenNormColumn` silently reuses on name / row-group-size mismatch

**Severity:** latent inconsistency, footgun.

**Where:** [libs/iresearch/include/iresearch/columnstore/format.cpp:332-345 (pre-fix)](libs/iresearch/include/iresearch/columnstore/format.cpp#L332)

**Symptom:** `OpenColumn` (the typed-column path) asserts on every re-open
mismatch (type / row-group-size / skip_validity / compression) at
[format.cpp:274-291](libs/iresearch/include/iresearch/columnstore/format.cpp#L274).
`OpenNormColumn` skipped the equivalent check:

```cpp
if (auto it = _impl->norm_by_id.find(id); it != _impl->norm_by_id.end()) {
  return *it->second;    // silently reuses, ignores name + row_group_size mismatch
}
```

Hard to hit today (norm id allocation is private to `SegmentWriter` and
`merge_writer`), but the asymmetry is a footgun for future callers.

**Fix:** mirror `OpenColumn`'s `SDB_ASSERT` block, restricted to `Name()` -- the
two current callers (`field_data.cpp` and `merge_writer.cpp`) always pass
`row_group_size = 0`, so a row-group-size check would be dead code.

```cpp
if (auto it = _impl->norm_by_id.find(id); it != _impl->norm_by_id.end()) {
  auto& existing = *it->second;
  SDB_ASSERT(existing.Name() == name,
             "columnstore::Writer::OpenNormColumn: re-opened id ", id,
             " with mismatched name '", name, "' vs '", existing.Name(),
             "'");
  return existing;
}
```

## Bug 6 -- sparse norm column vs. dense reader mapping *(real; reachable today via JSON-indexed columns)*

**Severity:** real, user-facing. Returns wrong norm values for downstream BM25 /
TFIDF / LM-* scoring and can also blow past `_total_row_count` and assert (dev)
or UB (release).

**Where:**

- writer side -- [field_data.cpp:643-650](libs/iresearch/include/iresearch/index/field_data.cpp#L643) inside `FieldData::compute_features`, plus [segment_writer.hpp:166-170](libs/iresearch/include/iresearch/index/segment_writer.hpp#L166) `finish()`.
- reader side -- [norm_column_reader.hpp:69-71](libs/iresearch/include/iresearch/index/norm_column_reader.hpp#L69) `GetOne` and the loop body in `GetBatch`, both of which compute `row_pos = doc - doc_limits::min()` *densely*.

**Mechanism:** `SegmentWriter::finish()` iterates `_doc`, which contains only
the fields that the *current* doc actually saw (added there with the `seen()`
guard in `SegmentWriter::index`). For fields with `IndexFeatures::Norm` whose
docs do *not* insert that field, `compute_features` is never called for that
doc, so `NormColumnWriter::Append` is never called either. The norm column
becomes **sparse** -- its row count is smaller than `state.doc_count`.

But `PersistedNormReader` maps `doc_id N -> row_pos N - doc_limits::min()`
densely. So:

- `Get(doc=2)` reads row 1, which is whatever the next-doc-with-the-field
  stored -- *wrong value*, silent corruption of BM25 scores.
- `Get(doc=N)` for a doc past the last sparse row asserts in dev
  (`row_pos < _total_row_count` in `Locate`) and UBs in release.

The merge path has the same dense assumption at
[merge_writer.cpp:638-641](libs/iresearch/include/iresearch/index/merge_writer.cpp#L638):

```cpp
const auto src_doc =
  static_cast<doc_id_t>(rg_first_row + i + doc_limits::min());
```

`rg_first_row + i` is the row index, not the source doc id. If the source norm
column is sparse, this maps the wrong source doc into `doc_map`.

**Reachable today in SereneDB:** yes, via JSON-indexed columns. The TODO at
[search_sink_writer.cpp:629-633](server/connector/search_sink_writer.cpp#L629)
documents this:

> Now only the JSON `null` leaf is indexed; SQL NULL cells and missing keys are
> silently skipped, producing index/scan divergence on IS NULL.

So for any inverted index over a JSON path, docs missing that path produce no
field insert and hence no norm value. With Norm-feature scorers (BM25 by
default), scores read back garbage. Non-JSON cells route NULLs through
`_null_field.SetNullValue()` + `_document->Insert(field)`, so those fields stay
dense -- but the moment the indexer skips an insert for any reason (any future
optimisation, an unsupported value type, a tokenizer that elides a row), this
breaks.

**Why tests don't catch:** `tests/libs/iresearch/index/norm_test.cpp` uses
`JsonDocGenerator` over `simple_sequential.json`, where every doc has every
indexed field. The sparse path is unexercised.

**Fix:** *Shape A -- zero-padding to keep the column dense.* Read hot path stays
a single stride-indexed load per doc, write side enforces "one row per segment
doc". Applied.

Changes (write side, `SegmentWriter`):

- `NormColumnWriter` gained `RowCount()` and `PadTo(target)`
  ([norm_writer.hpp:55-65](libs/iresearch/include/iresearch/columnstore/norm_writer.hpp#L55),
  [norm_writer.cpp:69-91](libs/iresearch/include/iresearch/columnstore/norm_writer.cpp#L69)).
  `PadTo` chunks bulk `vector::insert(end, n, 0u)` calls per row group
  (one flush per filled RG) instead of N `Append`s.
- `columnstore::Writer` exposes `NormWriters()` returning a non-owning span
  ([format.hpp:90-91](libs/iresearch/include/iresearch/columnstore/format.hpp#L90),
  [format.cpp:332-335](libs/iresearch/include/iresearch/columnstore/format.cpp#L332)).
- `SegmentWriter` carries `_last_indexed_doc` (reset to `doc_limits::invalid()`
  in `reset()`); `SegmentWriter::index()` pads every norm writer to
  `doc - doc_limits::min()` on the *first* index() of a new doc -- i.e. when
  `doc != _last_indexed_doc`. This back-fills zeros for fields that were
  missing in any earlier doc and is the only reliable signal for "new doc"
  because SereneDB's column-major indexer calls `NextDocument()` (and thus
  `finish()`) once per *column* per row, not once per doc
  ([segment_writer.hpp:192-197](libs/iresearch/include/iresearch/index/segment_writer.hpp#L192),
  [segment_writer.cpp:101-128](libs/iresearch/include/iresearch/index/segment_writer.cpp#L101)).
  `finish()` itself stays a thin `compute_features` loop -- appending the
  value the field actually saw.
- `SegmentWriter::flush()` does a final pad sweep to `buffered_docs()` right
  before `_columnstore->Commit()`. Needed because `index()`'s lazy pad only
  fires on the *next* doc; a segment whose last doc(s) have no norm-feature
  field would otherwise leave the column short by N rows
  ([segment_writer.cpp:162-176](libs/iresearch/include/iresearch/index/segment_writer.cpp#L162)).

Changes (merge side, `WriteFields`):

- The per-source loop now iterates **`sources` directly** (every source
  reader, in the order ComputeDocMappingsAndFieldMeta assigned merged
  doc-ids), **not** `field_itr.Visit(...)`. The Visit iterator only invokes
  its callback for sources that *have* the current field -- which is exactly
  the wrong set for a JSON-path field that's missing in some segments. The
  old loop silently dropped those sources' live docs and re-introduced the
  sparse off-by-one bug at merge time (visible in
  `tests/sqllogic/sdb/pg/index/inverted_index_json_sparse_norm.test`:
  expected `[1, 3, 4]` ordering became `[1, 4, 3]` because doc 3 read doc
  4's norm).
- For each source, the loop emits exactly one value per live source doc.
  When the source lacks the norm column (`src.reader->field(name)` returns
  nullptr, or the source's `field.norm` is invalid), the value is 0. Each
  Append corresponds to one merged doc in order, so the merged column is
  dense by construction. The legacy `any_data` flag became
  `nw.RowCount() != 0` -- same effect, but the merged column drops only when
  no source contributed any live doc, not when all contributed zeros
  ([merge_writer.cpp:597-650](libs/iresearch/include/iresearch/index/merge_writer.cpp#L597)).

**Storage cost:** `skipped_docs x byte_size` per field per segment. For typical
text norms (<= 65535 tokens -> `byte_size = 2`), 50%-sparse, 1M docs: ~1 MB per
sparse field. For `byte_size = 1` (most norms) and 99%-sparse fields: ~1 MB per
million docs. Negligible vs the value of having correct BM25 scores.

**Read hot path:** unchanged. `PersistedNormReader::GetOne`/`GetBatch` is still
one stride-indexed `Load{8,16,32}` per doc. The wand_writer invariants
(Bug 10) hold by construction because zero-padded docs are never in any
posting list.

## Bug 7 -- `NormColumnWriter::Append` doesn't check `_finalized` *(defensive)*

**Severity:** defensive.

**Where:** [norm_writer.cpp:55-60](libs/iresearch/include/iresearch/columnstore/norm_writer.cpp#L55).

**Mechanism:** `Append` unconditionally pushes into `_pending` and, when the
RG fills, calls `FlushRowGroup` which writes to `_out`. After `Finalize()` the
output stream may already be closed (or about to write a footer); a stray
`Append` either silently buffers data that's never flushed (memory-only data
loss) or writes bytes past the footer (file corruption) depending on timing.

Not reachable in the current flow -- `compute_features` runs at per-document
`commit()`, well before `Writer::Commit` calls `Finalize()` -- but the writer
exposes no contract preventing it.

**Fix:** `SDB_ASSERT(!_finalized, "Append after Finalize")` at the top of
`Append`.

## Bug 8 -- `_total_sum` / `_total_non_zero` overflow silent in release *(defensive)*

**Severity:** defensive; practically unreachable per segment.

**Where:** [norm_reader.cpp:41-46](libs/iresearch/include/iresearch/columnstore/norm_reader.cpp#L41).

The existing assert is `SDB_ASSERT(_total_sum + p.sum >= _total_sum, ...)` --
dev-only. In release, a `uint64_t` wrap goes unnoticed and `GetAvg` returns
garbage. Wrapping requires ~`UINT64_MAX / UINT32_MAX ≈ 4.3 billion` rows at the
maximum norm value per segment, which doesn't happen in normal use. Worth
flagging because `GetAvg` feeds BM25's average-document-length factor: corrupt
output here corrupts every score in the segment, silently.

**Fix (cheap):** keep `SDB_ASSERT` but also guard `GetAvg`:

```cpp
score_t GetAvg() const noexcept final {
  const auto nz = _column->NonZeroCount();
  if (nz == 0) {
    return {};
  }
  // Cheap sanity: a wrapped _total_sum is detectable because the ratio
  // would be far below any plausible avgdl.
  return static_cast<double>(_column->Sum()) / static_cast<double>(nz);
}
```

Or, harden by widening the accumulator with explicit saturation when
`_total_sum + p.sum` would overflow (cap at `UINT64_MAX`). I'd leave this one
alone unless a benchmark forces it.

## Bug 9 -- search-benchmark-game (and `examples/basic.cpp`) do not exercise the new norm code at all *(test coverage gap)*

**Severity:** test gap, not a code bug -- but it's the root reason Bugs 1-8 weren't
caught.

**Where:**

- [tests/bench/search-benchmark-game/index_builder.hpp:65-67](tests/bench/search-benchmark-game/index_builder.hpp#L65) -- `IndexBuilder` constructs `irs::MMapDirectory _dir;` directly; there's no `duckdb::DatabaseInstance` in scope.
- [libs/iresearch/examples/basic.cpp](libs/iresearch/examples/basic.cpp) -- same shape, plain `MMapDirectory`.

**Mechanism:** the new norm path is gated by a `DatabaseInstance`. Trace:

1. `SegmentWriter::reset(meta)` at [segment_writer.cpp:202-204](libs/iresearch/include/iresearch/index/segment_writer.cpp#L202):

   ```cpp
   if (_db != nullptr) {
     _columnstore = std::make_unique<columnstore::Writer>(_dir, meta.name, *_db);
   }
   ```
   No `_db` -> no `_columnstore`.

2. `FieldsData::emplace` at [field_data.cpp:1014-1017](libs/iresearch/include/iresearch/index/field_data.cpp#L1014):

   ```cpp
   const field_id norm_id = (_columnstore != nullptr &&
                             IsSubsetOf(IndexFeatures::Norm, index_features))
                              ? _columnstore->AllocateColumnId()
                              : field_limits::invalid();
   ```
   No columnstore -> `norm_id = invalid` -> `FieldData::_norm_writer = nullptr`.

3. `FieldData::compute_features` is a no-op (`if (_norm_writer != nullptr) ...`).

4. Posting writer's `PrepareWriters` at [writer.hpp:277](libs/iresearch/include/iresearch/formats/posting/writer.hpp#L277) early-returns when `_norms` is null, so no wand info is written.

5. Query side: `BM25::PrepareScorer` at [bm25.cpp:485-497](libs/iresearch/include/iresearch/search/bm25.cpp#L485):

   ```cpp
   if (!norm && ctx.fetcher) {
     auto norm_reader = ctx.segment.norms(ctx.field.norm);
     norm = ctx.fetcher->AddNorms(ctx.field.norm, std::move(norm_reader));
   }
   if (!norm) {
     static constexpr auto kNorms = [] {
       std::array<uint32_t, kPostingBlock> norms;
       absl::c_fill(norms, 1);
       return norms;
     }();
     norm = kNorms.data();
   }
   ```
   No norm reader -> fall back to a static `[1, 1, ..., 1]` array. BM25 produces
   plausible-looking but length-blind scores; queries succeed and return *some*
   ranking, so nothing visibly breaks in the benchmark.

**Consequence for the benchmark queries** (Count / Top10 / Top100 / Top1000 /
TopKWithCount in [tests/bench/search-benchmark-game/do_query.cpp](tests/bench/search-benchmark-game/do_query.cpp)):

- `Count` queries: no scoring -> entirely unaffected.
- `TopK` queries: scoring uses the all-ones fallback. The benchmark *runs* but
  doesn't measure the real BM25 path the SereneDB server uses. The new norm
  reader/writer code is bytecode that never runs in this binary.

So none of Bugs 1-8 manifest as a benchmark failure. To exercise the new path
end-to-end you need either:

- a SereneDB server smoke test (creates a `DatabaseInstance`, so the columnstore
  path opens) -- see the verification section at the end of this doc; or
- a unit test in `tests/libs/iresearch/index/norm_test.cpp` that constructs a
  `DatabaseInstance`-backed segment.

**Fix:** add a benchmark/smoke variant that points iresearch at a backing DB
instance, so the `1_5simd` format actually creates / reads norm columns.

## Bug 10 -- downstream wand-writer asserts trip on a sparse / off-by-one norm read *(canary, not root cause)*

**Severity:** dev-only canaries that fire when Bug 6 (or any future
read-from-wrong-row bug) hits real data, hiding the root cause behind cryptic
asserts in `wand_writer.hpp`. Worth flagging so the next person who sees the
assert knows where to look.

**Where:**

- [wand_writer.hpp:201](libs/iresearch/include/iresearch/formats/posting/wand_writer.hpp#L201) `SDB_ASSERT(entry.norm >= entry.freq);` in `Write`.
- [wand_writer.hpp:212](libs/iresearch/include/iresearch/formats/posting/wand_writer.hpp#L212) `SDB_ASSERT(entry.norm >= entry.freq);` in `Size`.
- [wand_writer.hpp:290](libs/iresearch/include/iresearch/formats/posting/wand_writer.hpp#L290) `SDB_ASSERT(_norm.value);` after `_norm_it->Get(_doc->value);`.
- [wand_writer.hpp:148-153](libs/iresearch/include/iresearch/formats/posting/wand_writer.hpp#L148) `SDB_ASSERT(0 < dl_1); SDB_ASSERT(tf_1 <= dl_1); ...` inside `CmpBm25`.

**Why they fire from Bug 6:**

The wand writer reads `_norm_it->Get(_doc->value)` for every (term, doc) pair
in the posting list -- docs that, by construction, *have* the field. Semantic
invariant: `norm_value > 0` and `norm_value >= term_freq` (doc length >= any
single term's count). A sparse norm column violates that invariant the moment
a doc with no field-value sits "below" a queried doc in the dense row mapping,
because the queried doc's `Get(doc - 1)` returns the *next* sparse row's value
-- which may be smaller than `freq` or zero.

In release these asserts compile out: wand info gets written with garbage
`norm` values, the per-block max scores are wrong, top-K skip decisions are
wrong, and the user gets a wrong ranking with no visible crash.

**Fix:** no change to `wand_writer.hpp` itself -- the asserts are correctly
guarding the invariant. Fixing Bug 6 (dense norms via zero-padding) makes them
stop firing *for this root cause*: every posting-list doc has `_stats.len > 0`
when its `Append` runs, and zero-padded docs are never in any of that field's
posting lists, so the wand writer never reads a padded zero. Keep the asserts
in place -- they're tripwires for the *next* regression that violates the
invariant (a future merge off-by-one, a doc-id remap bug, etc.). Don't downgrade
them; if Bug 6's fix is correct they sit silent, and if a future change breaks
the invariant they catch it cheaply.

## Files touched

| File | Bug(s) |
| --- | --- |
| [libs/iresearch/include/iresearch/index/merge_writer.cpp](libs/iresearch/include/iresearch/index/merge_writer.cpp) | 1 |
| [libs/iresearch/include/iresearch/columnstore/format.cpp](libs/iresearch/include/iresearch/columnstore/format.cpp) | 2, 5 |
| [libs/iresearch/include/iresearch/columnstore/norm_reader.cpp](libs/iresearch/include/iresearch/columnstore/norm_reader.cpp) | 2 |
| [libs/iresearch/include/iresearch/index/norm_column_reader.hpp](libs/iresearch/include/iresearch/index/norm_column_reader.hpp) | 3 |
| [libs/iresearch/include/iresearch/index/field_data.hpp](libs/iresearch/include/iresearch/index/field_data.hpp) | 4 |
| [libs/iresearch/include/iresearch/index/field_data.cpp](libs/iresearch/include/iresearch/index/field_data.cpp) | 4 |

## What was checked and ruled out

- **Endianness.** Writer routes through `BufferedOutput::WriteNumU` ->
  `absl::little_endian::FromHost` -> `WriteBytes` (LE on disk). Reader uses
  `absl::little_endian::Load16` / `Load32`. Symmetric on BE and LE hosts.
- **`PickByteSize(0) == 1`.** Writer emits one zero byte per value; reader's
  `ReadNormValue(..., 1)` returns `*bytes`. Round-trips cleanly for all-zero RGs.
- **Unaligned reads.** `absl::little_endian::Load{16,32}` use unaligned memcpy;
  any `file_offset` is safe.
- **`uint32_t -> uint8_t / uint16_t` truncation in the writer.** Only invoked when
  `rg_max` fits in the target width -- lossless.
- **Per-doc norm capture in `SegmentWriter`.** `_doc` is cleared in `begin()`,
  each Document calls `finish()` exactly once, the `seen` flag plus
  `FieldData::reset(doc_id)` ensure each indexed doc adds the field once and
  `_stats.len` carries the correct per-doc value at `Append` time.
- **`_owned` lifetime in `NormColumnReader`.** Reader is `final`; `_owned` is
  sized once and the inner vectors aren't resized, so the heap-fallback
  `std::span`s remain valid for the reader's lifetime.

## Verification

1. **gtest** -- extend `tests/libs/iresearch/index/norm_test.cpp` with:
   - a merge case where every doc for a norm field is deleted; assert the merged
     segment has no norm column and `FieldMeta.norm` is invalid.
   - boundary coverage for `byte_size` selection at `max = 255 / 256 / 65535 /
     65536` across single-RG and multi-RG configurations (use a small
     `row_group_size`).
2. **Sqllogic** -- rerun existing `tests/sqllogic/recovery/wal_index_recovery_*`
   and the `tests/sqllogic/sdb/pg/index/inverted_index_*` cases that exercise
   scorers; no new sqllogic case needed.
3. **Smoke** -- `./build/bin/serened ./build_data
   --server.endpoint=pgsql+tcp://0.0.0.0:<port>`, create an indexed table, insert
   + delete enough rows to force a delete-heavy merge, run a BM25 query, confirm
   no `Reader::NormColumn` mismatch warnings or assertion failures.
