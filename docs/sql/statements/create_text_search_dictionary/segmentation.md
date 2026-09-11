---
title: "segmentation"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# segmentation

The `segmentation` template splits text into tokens using the language-agnostic word-boundary algorithm defined by [Unicode Standard Annex #29 (Unicode Text Segmentation)](https://www.unicode.org/reports/tr29/).

It derives boundaries from the Unicode properties of the characters themselves rather than from whitespace or any per-language dictionary, so a single dictionary works across scripts. The algorithm runs in-process with no locale and no dictionary, which also bounds what it can do: a script that UAX#29 does not join breaks at every character, so Han text is emitted one character per token. For languages that do not separate words with spaces — Chinese, Japanese, Thai — use [`icu_text`](./icu_text.md), which segments with the ICU break iterator for a locale. For ASCII text where words are already space-separated, a [`delimiter`](./delimiter.md) split is simpler and faster.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `CASE` | string | `'none'` | Case conversion applied to each emitted token: `'none'`, `'lower'`, `'upper'` |
| `BREAK` | string | `'alpha'` | Unit of segmentation and which segments are kept. Word segments, filtered: `'alpha'` (segments holding a letter or a digit), `'graphic'` (segments holding a non-whitespace, non-control character), `'all'` (every segment, whitespace runs included). Larger units, every segment kept: `'sentence'`, `'line'`, `'paragraph'` |

## Tokenization

The six `BREAK` values fall into two families.

`alpha` (the default), `graphic` and `all` all cut the value at every UAX#29 word boundary and differ only in which segments are emitted. `alpha` keeps a segment that contains at least one letter or digit — Unicode primary category `L` or `N` — and so discards punctuation and whitespace entirely. `graphic` keeps a segment that contains at least one character that is neither whitespace nor an ASCII control character, which additionally keeps each punctuation mark as its own token. `all` keeps every segment, so a run of consecutive spaces is one token and each punctuation character is a token of its own.

`sentence`, `line` and `paragraph` change the unit of segmentation instead of filtering. No accept test applies: every segment is emitted, with leading and trailing bytes up to and including the space character trimmed off, and a segment that trims away to nothing is dropped. `sentence` emits UAX#29 sentences. `line` emits one token per line, breaking on LF, VT, FF, CR, CRLF (counted as a single break), U+0085 NEXT LINE, U+2028 LINE SEPARATOR and U+2029 PARAGRAPH SEPARATOR. `paragraph` breaks only on a run of two or more of those breaks — a blank line — or on a single U+2029, so a lone line break stays inside the token.

Each emitted token gets one index position, in input order, and its offsets are its byte range in the value. All four [feature flags](./index.md#feature-flags) are supported: the tokenizer records offsets, so `OFFSET` is available alongside `FREQUENCY`, `POSITION` and `NORM`. `CASE` is applied last, and only to the segments that are kept. `'none'` emits the source bytes exactly. `'lower'` and `'upper'` apply simple per-codepoint case mapping; there is no locale option here, so the mapping is locale-independent and context-free — no Turkish dotless `i`, no Greek final-sigma context. Each codepoint maps to exactly one codepoint, so `'upper'` leaves `ß` unchanged instead of expanding it to `SS`; the mapped codepoint can still encode to a different number of bytes, so a converted token is not always as long as its source. Bytes that are not valid UTF-8 are copied through unchanged rather than rejected, and empty input yields no tokens. `CASE` and `BREAK` accept their values case-insensitively, so `'Lower'` and `'ALPHA'` also work.

The following table shows how the input `The Quick fox-trot.` is tokenized under each of the three word modes:

| `BREAK` | Tokens |
|---|---|
| `alpha` | `The`, `Quick`, `fox`, `trot` |
| `graphic` | `The`, `Quick`, `fox`, `-`, `trot`, `.` |
| `all` | `The`, ` `, `Quick`, ` `, `fox`, `-`, `trot`, `.` |

Because boundaries come from Unicode properties, the same dictionary splits ASCII words exactly where you would expect, and it needs no per-language configuration — but it also finds no boundary that Unicode does not mark.

This dictionary keeps the segments holding a letter or a digit and lowercases each one:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/segmentation/example_001" />

### Graphic segments

`BREAK = 'graphic'` keeps punctuation as separate tokens:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/segmentation/example_003" />

### All segments

`BREAK = 'all'` emits every segment, whitespace included, and here uppercases the result:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/segmentation/example_002" />

## See also

- [icu_text](./icu_text.md) — locale-aware segmentation for scripts UAX#29 leaves unjoined
- [delimiter](./delimiter.md) — split space-separated text on a literal character
- [text](./text.md) — full linguistic pipeline (case folding, stemming, stopwords)
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
