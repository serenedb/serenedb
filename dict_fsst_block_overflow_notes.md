# dict_fsst and the block overflow: the storage model, the algorithm, the four defects, the fix

Working notes for serenedb/duckdb#60 and serenedb/serenedb#1005. Everything here was verified in
this workstream; the commands to reproduce each claim are at the end.

---

## 1. Where dict_fsst sits in the columnar store

The write path that matters here is the **checkpoint**. Until then, rows live in in-memory row
groups; `CHECKPOINT` is what runs compression and produces the persistent columnar layout:

```
database file
 └─ row group            (122,880 rows by default; ATTACH ... ROW_GROUP_SIZE n can raise it,
 │                        and serenedb's own stores use large row groups -- the corpus showed
 │                        515k-row segments)
 │   └─ column           (one per table column, checkpointed independently)
 │       └─ column segment(s)   <- ONE segment == at most ONE storage block
 └─ blocks               (256KB allocation, 262,136 usable bytes after the checksum)
```

At checkpoint, each column of each row group goes through two passes: an **analyze** pass that picks
a compression function (dict_fsst wins for repetitive/prefix-heavy VARCHAR), then a **compress**
pass that feeds the chosen function the column's rows in vectors of 2,048. The compression function
owns the decision of *where segments end*: it accumulates rows into an in-memory state and must
flush a segment **before** its serialized layout exceeds one block. That decision -- the *cut* -- is
the entire subject of this document. Everything below lives in
`third_party/duckdb/src/storage/compression/dict_fsst/`.

Two facts shape all of it:

- **A segment is written once, at the end.** While accumulating, nothing is serialized; the state
  must *predict* the size of a layout it has not built yet.
- **The layout is not row-additive.** Several regions are `dict_count`-wide or `tuple_count`-wide
  and bitpacked, so their cost is `count × width` -- and the *width* can change. One row that
  changes a width re-prices every row already in the segment. This is the failure class: all four
  defects are one-row whole-segment re-prices that the row-at-a-time size tracking could not see.

## 2. The five layouts

dict_fsst is not one format but a mode chosen per segment at write time:

| mode | dictionary bytes | row -> value mapping | readable by |
| --- | --- | --- | --- |
| `DICTIONARY` | raw strings | bitpacked selection, `tuple_count × bits(entry_n)` | vanilla duckdb |
| `FSST_ONLY` | FSST-compressed | **none** -- legal only while every row is unique and non-NULL, row *i* IS entry *i* | vanilla duckdb |
| `DICT_FSST` | FSST-compressed | bitpacked selection | vanilla duckdb |
| `DICT_FSST_PLUS` | FSST + prefix-cleaved, entries sorted | bitpacked selection | serenedb only |
| `FSST_PLUS` | FSST + prefix-cleaved, entries in row order | **none** (all-unique only) | serenedb only |

FSST itself: a trained symbol table maps byte sequences (up to 8 bytes) to 1-byte codes; code 255
escapes a literal. It compresses *within* a string. The **cleave** (the `_PLUS` modes) additionally
compresses *across* strings: dictionary entries that share a prefix are grouped, the shared prefix
is stored once in a prefix table, and each entry stores `(prefix_id, suffix)`.

The plus layouts add `dict_count`-wide bitpacked fields (`prefix_id` per entry, suffix lengths) and
a per-group field (prefix lengths). This is why group count is not free: every group adds metadata,
and enough groups widen `bits(prefix_count)` for **all** entries.

The `STORAGE_VERSION` decides eligibility: `serenedb_v1` (serenedb's native `__sdb_store` databases,
the `.col` columnstore, and attaches that request it) sets `allow_plus`; a plain duckdb attach keeps
the segment on native modes so the file stays duckdb-readable.

## 3. The cut machinery, end to end

State per segment (all in `DictFSSTCompressionState`): the dedup'd dictionary (`dict.raw`, and once
encoded `dict.encoded`), the per-row selection `dictionary_indices`, and the cut bookkeeping.

Per row, `Compress()` does:

```
AddScanRow                     dedup; new entries append (and FSST-encode incrementally once ready)
if not encoded yet:            MaybeEncodeOrCutSmall
   raw dict >= 4KB and stable/near-block -> EncodeAll + first real measure (certify if it fits)
   raw dict stays tiny                   -> byte trigger; flush when within max_raw + 4KB of block
                                            [defect 4 lived here]
else:
   NearBlock?                  cheap per-row trigger: bytes grown since last measure >= 2KB,
                               OR a width changed, OR all-unique just died   [defect 1 fixed here]
   committed == PLAIN  -> CutPlain:    exact native size; flush with/without the current row
   else                -> CutCleaved:  CleavedUpperBound gate            [defect 2 lived here]
                                       -> RefreshCleave (real measure)
                                       -> fits with margin: certify fit_rows/fit_commit
                                       -> fits:             flush now (cached)
                                       -> over:             drop one row and retry, else
                                          FlushRewind to the certificate  [defect 3 lived here]
```

Key concepts:

**Measure vs estimate.** `RefreshCleave()` is the *measurement*: it computes the exact serialized
size of every still-viable candidate layout and picks the winner (`cut_mode`). It costs an LCP scan,
a Cartesian-tree DP and a walk, so it cannot run per row. `NearBlock` and `CleavedUpperBound` are
the *estimates* that decide when a measurement is worth it. The entire soundness question of this
bug class is: what may happen between two measurements?

**The commitment (`committed`).** The first near-block measurement decides which family wins --
`PLAIN` (native), `PLUS_SORTED`, or `PLUS_ROW` -- so later cuts skip the loser's work. A `PLUS_ROW`
commitment dies if uniqueness dies (row order needs no selection buffer, which is only legal
all-unique).

**The certificate (`fit_rows`, `fit_raw_count`, `fit_commit`).** Whenever a measurement fits with
margin, the state records "this many rows, this many entries, under this commitment, provably fit."
`FlushRewind` is the recovery path for an overshoot that dropping one row cannot fix: pop the
segment back to the certificate, flush that, and re-add the popped rows to the next segment.

**The cleave (`CleaveDP` + walk).** For a candidate order (row order, or the maintained sorted
order), compute adjacent LCPs of the FSST-encoded entries, build the Cartesian tree of the LCP
array, and run a DP: for every subtree (= contiguous entry range whose internal minimum LCP is the
subtree root), either take the whole range as ONE group sharing `lcp[root]` bytes, or split into the
children. `take_whole[i]` marks the choice; a walk over those choices yields the grouping. The DP's
objective is bytes saved -- since the fix, bytes saved **net of `GROUP_COST` per group** (§5).
LCPs respect FSST escapes (a group boundary may not split an escape pair -- `GuardedLcp`).

## 4. The four defects

All four are the same sentence with a different subject: *a single row re-priced the whole segment,
and the row-at-a-time tracking between measurements could not see it.*

**Defect 1 -- losing all-unique (and the encoded-length width).** While all rows are unique the
best native layout is `FSST_ONLY` with no selection buffer (on plus-enabled storage the row-order
`FSST_PLUS` cleave is likewise selection-free -- defect 3 is its side of the same flip). The first
duplicate (or NULL) makes a
bitpacked `tuple_count`-wide selection buffer appear at once: measured, a segment went from 239,424
comfortable bytes to 307,520 -- 45KB past the block -- while the byte tracker saw ~2KB of growth.
Similarly one long value widens the `dict_count`-wide string-lengths field for every entry at once.
`CutPlain`'s drop-one-row recovery assumes size is monotonic per row, so it returns 2 bytes of a
45KB overshoot.

**Defect 2 -- the estimate that could not exist.** The FSST+ gate needs an upper bound on "what
would a cleave measure right now." But the cleave re-partitions from scratch and one inserted entry
can restructure the LCP tree anywhere, so the group count has *no incremental relation* to the last
measurement. The original code extrapolated it (not a bound -> gate silent -> overflow). The only
sound bound for an unconstrained cleave -- half the entries -- costs +45.6% runtime, because at 33k
entries it charges ~47KB of phantom metadata and the gate then measures on every row.

**Defect 3 -- the rewind flushed a candidate its certificate never priced.** The flip row kills the
`PLUS_ROW` candidate and pushes the sorted candidate over the block; the cut rewinds to `fit_rows`
-- which restores the exact all-unique state the certificate was measured in -- but the poisoned
commitment stayed, so the flush serialized a *sorted* layout with a selection buffer the certified
row layout never paid for: certified 245,994, written 270,293, block 262,136.

**Defect 4 -- the path with no guard at all (pre-existing, found by the corpus).** A dictionary
under 4KB raw never FSST-encodes; its segment is guarded only by the small-dict byte trigger. At a
few bits of selection per row, such segments legitimately hold hundreds of thousands of rows -- and
the entry that pushes the entry count across a power of two widens the selection for every one of
them. Corpus trace: 281,238 rows at 7-bit width, all checks green; the 128th distinct value arrives;
the segment is 283,656 bytes against a 262,136 block *in one row*. This existed in every prior
release. It needs a large row group and the (2^k)-th distinct value arriving late, so whether a load
hits it depends on scan order -- it fired on 1-to-3 of every 3 fresh corpus loads and never in the
hand-written suite until distilled deliberately.

## 5. The fix

Two principles, then the parts.

**Principle 1: watch layout properties for a change, not bytes for growth.** Whole-segment
re-prices are invisible to byte accumulation but trivially visible as width/flag transitions.

**Principle 2: never write an unmeasured layout.** Estimates only decide *when* to measure;
measurements decide cuts; the flush serializes exactly what the last measurement priced. A wrong
estimate may cost an extra measurement or a rewind -- never an overflow. This is what turns the
defect *class* off, rather than the four instances.

The parts, in `compression.cpp`:

- **`NearBlock`** additionally fires on `bits(max_enc_len)` changing and on the all-unique flip.
  The flip needs no stored state: uniqueness is monotone in a segment, and
  `!was_new && entry_n + 1 == tuple_count` identifies exactly the first row that failed to add an
  entry. Cost: two integer comparisons per row.

- **`GROUP_COST` in the cleave DP** (`whole >= vl + vr + GROUP_COST`): a group must save more than
  its metadata costs to form at all. This kills the pathological many-tiny-groups cleaves (a bait
  shape -- 60k entries whose sorted neighbours share exactly one byte -- now correctly lands
  native), and it is what makes a *budget* workable at all:

- **`prefix_cap`, the priced budget.** `CleavedUpperBound` prices the prefix-id width at
  `prefix_cap`; every `RefreshCleave` raises `prefix_cap` to at least the group count the DP
  actually wants (with a doubling of headroom, carried across segments of the column) **before
  anything emits**. So the written layout never holds more groups than the estimate in force was
  priced against, and the emit-side clamp exists only as a backstop. The dictionary-bytes half of
  the bound stays incremental and sound by the absorb argument: for sorted `x < e < y`,
  `lcp(x,y) = min(lcp(x,e), lcp(e,y))`, so a newcomer inside a group's range shares that group's
  prefix and the anchored grouping remains feasible; the meta-priced DP can only do better.

- **`fit_commit`.** Certificates record the commitment they were measured under, and `FlushRewind`
  restores it. `MaybeEncodeOrCutSmall`'s encode-time measurement only certifies when it actually
  fits (it used to certify unconditionally and even discarded the measured size).

- **The width guard on the never-encoded path** (defect 4). When a NEW entry crosses a bitpacking
  boundary and the post-bump size no longer clears the block: pop that row, flush the pre-bump
  segment -- it passed the trigger one row earlier, so it provably fits -- and re-add the row to the
  fresh segment. Exactly `CutPlain`'s overshoot mechanics, one branch earlier. A benign bump (wider
  layout still fits) deliberately does not cut.

- **Measure/emit split** -- the performance half. `RefreshCleave` used to *materialize* up to two
  full candidate layouts (entry vectors, prefix tables; hundreds of KB of writes) per near-block
  row, of which the cut consumed one integer. Now `Dictionary::CleaveMeasure` runs the same
  LCP/tree/DP plus a *counting* walk (`CleaveStats`: group count, prefix/suffix bytes, maxes --
  everything `DictFSSTPlusLayout::Compute` needs), and the layout is materialized once per segment
  at flush. Both consumers run the **same** `CleaveWalk` with different callbacks, so the measured
  and written layouts cannot structurally diverge. The row-order LCP array persists across cleaves
  (`row_lcp` -- row order only appends; the sorted order re-scans because the merge shifts
  positions).

- **Unified finalize.** Forced plus modes go through the same `RefreshCleave`-then-materialize path
  as auto (their private unmeasured `Cleave`+write branch is gone), and `FinalizeCompress`
  re-measures the final segment, falling back to the certificate rewind if it stopped fitting.

**Why it ends up faster than the unfixed engine** (1,459ms vs 1,505ms on the adversarial 3M-row
shape; the sound-but-loose interim fix cost 2,192ms): the measurement loop lost its materialization
and most of its LCP re-scanning, which outweighed the cost of the added correctness checks. Bonus:
packing improved on two sweep shapes (one segment fewer each) because the DP stopped forming groups
that cost more than they saved.

## 6. Interaction with the columnar store and inverted indexes

**Row groups bound the exposure.** A segment never spans a row group, so at duckdb's default
122,880 rows the never-encoded path cannot build a wide enough segment for defect 4's jump to clear
the block (7-bit x 122,880 rows is ~107KB -- the bump lands far under 262KB). serenedb's own stores
use large row groups -- the corpus trace showed 515k-row segments -- which is precisely why the
corpus could arm defect 4 while attached-duckdb-default tests could not. The regression tests set
`ROW_GROUP_SIZE 1048576` to reproduce deterministically, and keep one default-row-group control
that must pass on any build.

**serenedb_v1 storage is where the FSST+ machinery is live.** `__sdb_store` databases (every
`CREATE DATABASE`), the `.col` columnstore, and `STORAGE_VERSION 'serenedb_v1'` attaches enable
`allow_plus`, i.e. defects 2 and 3 and the whole cleave/budget apparatus. Native duckdb attaches
run natives only -- defects 1 and 4 still apply there, and the fix covers both storage versions
(the test matrix runs the flip and width shapes on each).

**Inverted indexes: the fix applies to the columns, not to the index format -- by design.** An
inverted index's internal structures (the iresearch segment: FST-based term dictionary, postings,
its own columnstore for stored values/norms) are a separate codec under `libs/iresearch` and never
pass through dict_fsst; they neither need nor receive this fix. What the index *indexes*, however,
is an ordinary table column that lives in duckdb segments and is compressed by dict_fsst like any
other -- low-cardinality keyword columns, doc-key columns of exactly the `<kind>:<nanoid>` shape
that started all this. Those are covered. The proof is direct: the 564k-document corpus load that
validated the fix runs the full serenedb search schema (term-dict indexes included) into native
serenedb storage, and the 538-test broad sweep includes the 75 `sdb/pg/index` tests on the fixed
engine.

## 7. What proves it

| claim | evidence |
| --- | --- |
| the four defects are real and armed | 9 of 15 `recovery/dict_fsst*` tests FAIL on unmodified `v2026.07.07`, carrying the original `layout.total <= block_size` assert; the 6 passes are the files documented as coverage-only |
| the fix closes them | the same 15 tests pass on the fixed engine, both pg-wire engines, one serened per file |
| defect 4 predates this work | `cut_small_dict_width` fails pre-fix AND on the first two commits of #60, passes with the guard |
| real-world load | corpus: 6 of 6 fresh 564,316-doc loads clean (was 3-of-3 failing originally; ~1-in-3 after the first three fixes), `force_compression='uncompressed'` workaround removed |
| no collateral | 538/538 across `sdb/pg/index`, `sdb/pg/dml`, `any/pg/simple`, both engines |
| no compression regression | rows-per-segment sweep identical on 7 shapes, better on 2; junk-prefix bait lands native; zero value mismatches anywhere |
| performance | 1,459ms vs 1,505ms unfixed vs 2,192ms loose-bound (3M-row shape, mean of 3, assertions live) |

Reproduce:

```bash
# the suite (one serened per file; do NOT batch these on one shared server --
# force_dict_fsst_mode is a global setting)
cd tests/sqllogic && BUILD_DIR=build_clangd ./run_recovery_tests.sh recovery/dict_fsst*.test

# any single test, fresh server
./build_clangd/bin/serened /tmp/x --listen='postgres://0.0.0.0:55700' &
cd tests/sqllogic && ./run.sh --single-port 55700 --test recovery/dict_fsst_cut_small_dict_width.test

# the timing shape
./run.sh --single-port 55700 --engines pg-wire-simple --test recovery/dict_fsst_plus_block_overflow.test
```

PRs: engine fix **serenedb/duckdb#60** (3 commits on `gnusi/dict-fsst-unique-flip-block-overflow`,
base `v2026.07.07`); tests + pin **serenedb/serenedb#1005** (`gnusi/dict-fsst-unique-flip-test`,
rebased onto current `main`). Merge order: #60 first, then re-point the submodule in #1005 off the
PR-branch commit.
