# Text primitives

`analysis/text/` is the layer of byte-level text kernels the tokenizers are
built from: block classification, word segmentation, normalization, case
conversion, and the drivers that turn a scan into sink emits. Everything here
is independent of any particular tokenizer class. The tokenizer classes
themselves, the `TokenSink`/`TokenBatch`/`TokenStage` infrastructure, and the
interned resources stay one level up in `analysis/`.

## Layers

Bottom-up. Each directory is one namespace under `irs::analysis`.

| Directory | Namespace | What lives there |
|---|---|---|
| `iresearch/utils/utf8_*` (outside this tree) | `irs::utf8_utils` | Codepoint decode/encode, general categories, simple case tables. `text/` depends on it, never the reverse. |
| `classify/` | `classify` | 32-byte SIMD block primitives (`Block`, `Load`, `MoveMask`, `ClassifyEqBlock`, `ClassifyAnyEqBlock`, `DrainClassified`), whole-value ASCII probes (`IsAsciiValue`, `IsAsciiShort`), locale case-safety predicates (`AsciiCaseSafe`, `SimpleCaseSafe`), UTF-8 codepoint bounds. |
| `words/` | `words` | UAX#29 word boundaries: the ASCII run scanner (`ScanAsciiRuns`, `ScanAscii`), the Unicode DFA (`ScanUnicode`) with its generated property table, the block word masks, and the alnum-run splitter. |
| `normalize/` | `normalize` | NFC/NFKC fast path: lead-byte classification, `Denormalized`, `StripSafe`, `Compose`, `Decompose`, `StripNonspacingMarks`; `icu.hpp` holds the ICU normalizer/transliterator fallbacks (`MakeStripTransliterator`, `NormalizeCaseStrip`). |
| `case/` | `casing` | Byte-to-byte case conversion: `CaseConvertAscii`, `CaseConvertUtf8` and its bound. The emit-side hooks (`EmitCaseConverted*`) stay in `analysis/token_sink.hpp`. |
| `sz/` | `sz` | Wrapper over third-party StringZilla: sentence and newline finders, normalization entry points, the AVX-512 probe. |
| `segment/` | `segment` | The `Accept`/`Convert` option enums and the `*FillValue` drivers that apply a scanner, the accept filter and the convert mode, and emit into a `TokenSink`. |
| `term_view.hpp` | `irs` | Term-view slot builders (`MakeTermView*`, `InlineTermHandle`): the canonical `string_t` image of a token, shared by the sink and the dictionaries. |
| `dict/` | `dict` | Dictionaries: stopword loading, the stem cache, and `string_table.hpp`'s self-contained `StringSet`/`StringMap` backing stopword sets and synonym maps — two `flat_hash` tiers, inline-handle keys plus owned `std::string` long keys. Independent of the scanning stack. See `tests/bench/micro/word_bloom.cpp` for why the hash-free radix that used to front them was removed. |

## Dependency direction

```
utils/utf8 -> classify -> words -> normalize, case -> segment
                            sz  -> normalize, segment
dict: standalone
```

Known upward edges, stated so they are not extended by accident:
`segment/fill.hpp` includes `analysis/token_sink.hpp` (it emits into a
`TokenSink`); `normalize/icu.hpp` includes `analysis/tokenizer.hpp` for
`irs::Case`.

## Codegen law

The fill drivers in `segment/fill.hpp` (`WordFillValue`, `SentenceFillValue`,
`LineFillValue`, `WholeFillValue`) are `IRS_NO_INLINE`
compilation roots; `words::Scan*`, the `classify` primitives and
`casing::CaseConvertAscii` are `IRS_FORCE_INLINE` and fuse inside them. Moving
the drivers to force-inline flips the inlining root to the scan and loses
per-token register allocation of sink state (+11% on the segmentation arms);
force-inlining everything bloats every `DoFill` instantiation (+6-9% on the
pipeline column arms). Any change here is checked with the `tokenizer_fill`
micro bench and an `nm -S` size comparison of the hot `FillValue` symbols.

## Tests and benches

Unit tests: `tests/libs/iresearch/analysis/text/`. Micro benches:
`tests/bench/micro/{ascii_probe,segmentation_stream,word_bloom,split_by_non_alpha,tokenizer_fill}.cpp`.
