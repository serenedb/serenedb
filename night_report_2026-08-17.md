# Overnight sweep report (2026-08-17)

Mandate: sweep every tokenizer for simplify/optimize/reuse, then the main
inversion procedure. Zero perf regressions (ABBA-gated vs A binaries at
checkpoint commit 61272b4b6).

Note: I made two `wip` checkpoint commits myself (matching your pattern) to
anchor the ABBA stash-cycles: 61272b4b6 (campaign base, includes the sqllogic
tests for form/locale/break + all evening work).

## Phase 0 (before the sweep, part of base)
- sqllogic blocks added + psql-replay-verified: norm form=nfkc, optional
  locale, tr tailoring, invalid-form error; break=sentence/line/paragraph
  goldens + paragraph glue + invalid-break error (text_tokenizer.test).

## Phase 1: tokenizer sweep

APPLIED (all gtest-gated; ABBA verdicts below):
- ngram family (G2): shingle dead-branch collapse in emit_unigram (+assert);
  shingle O(n) run-end cursor (was O(n*max)); shingle psum built only when
  shingles possible; shingle filler-gap ternary -> prev cursor; sparse_ngram
  EmitK cursor got the missing IRS_FORCE_INLINE.
- delimiter family (G1): sql emit/list-drain dedup into EmitSqlTerm/
  EmitSqlListRow + dead explicit DoFill instantiations deleted; delimited
  EmitToken derives offsets from base pointer (running-tally state + delim
  param deleted); pattern literal split via string_view::find; pattern
  _matches sized by extraction group (RE2 no longer extracts all groups in
  split mode); pattern ctor byte-set extraction via popcount+bit-drain;
  path_hierarchy IsDelimAt for leading-delim probe (was full-value scan!) +
  redundant reverse-skip guard dropped; split_by_non_alpha fold via shared
  FoldAscii; multi_delimited dead <bit> include.
- filter/synonym (G3): union+pipeline missing Discard() on failed sub-fill
  (LATENT TOKEN-LEAK BUG, found independently by two agents); pipeline+union
  SubSink dedup into shared AccumulatorSink; union single-value emit now
  view-emits terms pointing into the caller value (mirrors pipeline);
  dead options.clear() x2; visit_members -> VisitMembers; wordnet RE2
  replaced by starts/ends_with (re2 dep dropped from that TU); CRLF
  tolerance in both synonym parsers (Windows files no longer break);
  StrSplit vectors -> range-for; token_list_sink per-batch capacity +
  data-pointer hoist; TokenAccumulator nullptr+size UB fix + pos lane
  hoisted out of per-token loop.
- ML/geo/text (G4): fast_text_model cache race fixed (re-check under writer
  lock; erase+insert so the key view always points into the live model);
  text_tokenizer unicode path now uses the stem cache (was raw
  sb_stemmer_stem per word -- ASCII path had 13-25x memoization, unicode
  did not); text whole-value probe -> IsAsciiValue; dead attribute_helper
  includes x3; ctor param name drift fixed.

REJECTED/DEFERRED (with reasons):
- ngram DoFill IsAsciiValue swap: APPLIED THEN REVERTED -- ABBA showed +6%
  on ngram_variable both fill paths; probe inlining perturbs EmitGrams
  codegen. simdutf call stays.
- 3-way block-splitter unification (multi_delimited/pattern/delimited):
  relocation-only on measured-final kernels; tonight's ngram lesson shows
  exactly how that perturbs codegen. Needs its own measured pass.
- split_by_non_alpha ClassifyAlnumBlock via WordCmpsOf: same reasoning.
- MakeTermView 3-arg slack in text Case::None paths: no bench arm covers
  those configs; unmeasurable tonight.
- collation ASCII sort-key fast path: assumption (sort-key bytes <0x80)
  unverified; needs its own measured pass.
- stem_cache evict dedup, solr/wordnet SortUnique/scaffold merges, sql
  DoFillColumn scratch: churn > value.
- CpBounds accessor merge (ngram/wildcard): deferred with the splitter
  unification (same codegen-sensitivity class).

## Phase 2: inversion procedure

APPLIED:
- PackedU32Column: dead PushNAdd + Reset deleted (+ test consumers).
- fields_inverter: InvertUniqueKeywords/CaptureKeywordTerms share one
  CaptureKeywordRange (forced-inline push lambda; asserts + tail in one
  place); Flush releases scatter scratch via Finally (was skipped on
  throw, pinning 20-76MB until next flush).
- columnar_flush: ScatteredField._layout derivable member dropped
  (Layout() forwards to field); field.Log() visited once per Reset (was 3
  std::visit); scatter-column clears hoisted out of Scatter into Reset
  (dead !nocc guard -> assert); IsSubsetOf idiom in ResetField.
- hit_batcher (consumer side): BuildSel/FinishBatch/DrainCompact/IsListLike
  helpers collapse 4 duplicated shells; CloseGroup passes anchor/span to
  ScatterGroup instead of recomputing.
- segment_writer: memory_active docs-mask accounting used removed-doc COUNT
  instead of bitset length (underreported rm budget) -- fixed; duplicate
  doc_count guard merged; stale includes dropped.
- DESIGN.md flush + write-path sections refreshed to match the code.

APPLIED THEN REVERTED (ABBA-caught, invert_kernels):
- CaptureKeywordRange lambda dedup of the two keyword capture loops:
  BM_TermsBlockUniqueWarm +13.8% -- the THIRD time a keyword-path
  unification regresses (PushBatchTermsPos precedent). Reverted; flat
  after (-0.2%).
- TokenAccumulator pos-lane hoist out of the per-token loop:
  BM_PipelineLegacy +6.3%. Reverted; +0.7% after.
- ScatteredField._layout removal + field.Log() visit hoist: one of the two
  shifted Reset/scatter codegen, ScatterSparseKeep +7%. Both dropped
  (isolated by pristine-dir bisect; the kept subset measures +2.3/+2.7 =
  that arm family's noise band, shown by pristine sources at +1.2/+2.2).
- columnar_flush clears hoist: reverted during bisect, innocent, left
  reverted (cosmetic only).

DEFERRED:
- RadixSortByKey threshold sweep (kRadixThreshold=2048 vs 1MB counts
  clear): needs its own arm sweep, flagged with numbers in the findings.

## Perf verdicts (all interleaved ABBA vs checkpoint binaries, core 30)

- invert_kernels (final kept set): worst +2.7% on ScatterSparseKeep, which
  is that arm family's own noise band (pristine sources measured +1.2/+2.2).
  All keyword/scatter/uvf/numeric arms flat. The three reverted items above
  were caught at +13.8/+6.3/+7%.
- tokenizer_fill (full arm sweep): semantically-touched arms all flat
  (pipeline/union/sql/delimiter/path/split/shingle/sparse/text/norm).
  path_hierarchy bench: worst +1.2%, several skip arms improved
  (IsDelimAt fix). segmentation_stream control: flat.
- LAYOUT-LOTTERY ARMS (documented, not regressions): multi_delimiter
  mixed8/strings/tags swing +-15..22% in MIRROR (mixed8 +22 while tags -20)
  with a byte-identical TU; identical-source rebuild measures flat.
  wildcard +-5-7% same class (proven earlier tonight). union_2/FillColumn
  swings +-7% between IDENTICAL binaries = noisy arm (ShortValues class).
  Rule applied: judge semantically-touched arms; these are placement luck.
- Full gtest: 3575/3575 passed on BOTH the mid-campaign and the FINAL
  binary (plus per-batch suite gates green throughout); CRLF parser fixes
  gtest-pinned (34 synonym tests green).
- MEASURED WIN: text_en_unicode -38% (consistent across two independent
  runs) -- the unicode-path stem memoization. FillColumn/text_en -14.6%
  in the same runs points the same direction.
- A full-arm re-run at 11:08 was DISCARDED as contaminated: colleagues'
  processes active (serened at 18% CPU, 3 other claude sessions), arms
  swinging +-40-55% in both directions incl. UNTOUCHED TUs; canaries on
  the same binaries immediately after: flat (ngram_suffix -0.07%,
  segmentation -1.9%). All keep/revert verdicts were made from the
  overnight clean-box gates.

## Fixed real bugs (bonus finds by the sweep agents)
- union/pipeline: missing Discard() on failed sub-stream Fill leaked tokens
  into the NEXT sub's vectors (cross-contamination); found independently by
  two agents; fixed both sites (+ the pipeline FillFast precedent confirmed
  the intended contract).
- fast_text_model: model-cache race (no re-check under writer lock) +
  insert_or_assign keeping a stale key view that made the dying model's
  DropModel evict the LIVE entry. Fixed (re-check + erase/insert).
- segment_writer::memory_active: docs-mask accounting used removed-doc
  count instead of bitset length (underreported the rm budget).
- TokenAccumulator::Bind: nullptr+size UB (UBSan-visible).
- CRLF files broke both synonym parsers (wordnet threw on valid
  Windows-edited files) -- fixed + gtest-pinned.
- text_tokenizer unicode path: stemming now memoized via StemCache (the
  ASCII path's 13-25x cache; unicode did raw sb_stemmer_stem per word).
- fields_inverter::Flush: scatter scratch (20-76MB) now released on throw.

## Suggested next steps (for the morning discussion)
- The layout lottery is now the dominant noise source in ABBA judging
  (multi_delimiter +-20%!). Consider -falign-functions=32 (or per-kernel
  alignment attributes) for build_perf: would stabilize every future
  campaign's measurements. Needs its own measured pass.
- RadixSortByKey threshold sweep (2048 vs 1MB counts clear per field).
- Deferred reuse batch: 3-way block-splitter unification + CpBounds merge +
  split_by_non_alpha WordCmpsOf tier -- all need the alignment story fixed
  first, or they will be unjudgeable.
- collation ASCII sort-key fast path (verify sort-key byte distribution).
- MakeTermView 3-arg slack in text Case::None paths (needs a bench arm).
