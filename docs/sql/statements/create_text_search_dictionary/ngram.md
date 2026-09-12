---
title: "generate_ngrams"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# generate_ngrams

The `generate_ngrams` template breaks each token into overlapping fixed-length character sequences — n-grams — so searches can match on fragments rather than whole words. With the default `MINGRAM` of 2 and `MAXGRAM` of 3, the word `search` yields `se`, `ea`, `ar`, `rc`, `ch` and `sea`, `ear`, `arc`, `rch`, letting a query find it from a partial or slightly misspelled input. This makes the template a good fit for fuzzy matching, autocomplete and typo-tolerant search.

`PRESERVEORIGINAL` additionally keeps the whole token alongside its grams, and `STARTMARKER`/`ENDMARKER` tag the start and end of the source token so prefixes and suffixes can be distinguished from interior matches. `MODE` narrows the stream to the grams anchored at the start of the token, at its end, or at both, which turns the same template into an edge n-gram tokenizer for autocomplete. The index grows with the width of the `MINGRAM`–`MAXGRAM` range, so keep it as narrow as your matching needs allow.

For substring search over code, logs or identifiers, prefer [`generate_sparse_ngrams`](./sparse-ngram.md), which answers the same fragment queries while keeping the index far more compact.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `MINGRAM` | integer | `2` | Minimum n-gram length, in symbols |
| `MAXGRAM` | integer | `3` | Maximum n-gram length, in symbols |
| `PRESERVEORIGINAL` | boolean | `false` | Emit original token alongside n-grams |
| `INPUTTYPE` | string | `'utf8'` | Unit the gram lengths count: `'utf8'` counts codepoints, `'binary'` counts bytes |
| `STARTMARKER` | string | `''` | Text prepended to the grams that start at the first symbol of the token; empty means no marker |
| `ENDMARKER` | string | `''` | Text appended to the grams that reach the last symbol of the token; empty means no marker |
| `MODE` | string | `'all'` | Which grams to generate: `'all'`, `'only_prefix'`, `'only_suffix'`, `'only_prefix_and_suffix'` |

Both lengths are clamped silently and are not otherwise validated: `MINGRAM = 0` behaves as `1`, and a positive `MAXGRAM` below `MINGRAM` behaves as `MAXGRAM = MINGRAM`. The template supports all four [feature flags](./index.md#feature-flags) — `FREQUENCY`, `POSITION`, `NORM` and `OFFSET` — subject to the usual dependencies: `OFFSET` requires `POSITION`, and both `POSITION` and `NORM` require `FREQUENCY`.

## Tokenization

With the default `MODE = 'all'` the template emits, for each input token, every contiguous symbol window whose length falls between `MINGRAM` and `MAXGRAM`, sliding one symbol at a time across the whole word. With `MINGRAM = 2` and `MAXGRAM = 3`, `search` produces every 2- and 3-symbol window, so a query for any of those fragments finds the word — the basis for fuzzy and typo-tolerant matching. In this mode the windows are not restricted to either end of the word.

The other modes keep only the windows anchored to an edge of the token and emit nothing interior: `'only_prefix'` keeps the start-anchored grams, `'only_suffix'` the end-anchored ones, and `'only_prefix_and_suffix'` both. `'only_prefix'` chained after [`split_text`](./text.md) turns each word into its edge n-grams, the basis for autocomplete. In the edge modes the whole token is part of the stream whenever its length falls between `MINGRAM` and `MAXGRAM`, and `PRESERVEORIGINAL` adds it otherwise.

| Input | Options | Tokens |
|---|---|---|
| `search` | `MINGRAM = 2`, `MAXGRAM = 3` | `{se,sea,ea,ear,ar,arc,rc,rch,ch}` |
| `search` | `MINGRAM = 2`, `MAXGRAM = 3`, `PRESERVEORIGINAL = true` | `{se,sea,search,ea,ear,ar,arc,rc,rch,ch}` |
| `cat` | `MINGRAM = 2`, `MAXGRAM = 3`, `STARTMARKER = '^'`, `ENDMARKER = '$'` | `{^ca,^cat,cat$,at$}` |
| `hello` | `MINGRAM = 2`, `MAXGRAM = 4`, `MODE = 'only_prefix'` | `{he,hel,hell}` |
| `abcd` | `MINGRAM = 1`, `MAXGRAM = 2`, `MODE = 'only_suffix'`, `PRESERVEORIGINAL = true` | `{abcd,cd,d}` |
| `abcd` | `MINGRAM = 1`, `MAXGRAM = 2`, `MODE = 'only_prefix_and_suffix'`, `PRESERVEORIGINAL = true` | `{a,ab,abcd,cd,d}` |

A gram is a verbatim slice of the input: nothing is lowercased, stripped of accents or normalized, so `HeLLo` with `MODE = 'only_prefix'` and 2–3 symbols yields `{He,HeL}`. Lengths count codepoints by default; with `INPUTTYPE = 'binary'` they count bytes, so a gram can cut a multi-byte character in half and its term text is then not valid UTF-8. Input that is not valid UTF-8 is never rejected — symbol boundaries fall back to a lead-byte walk — and an empty value yields no tokens.

Every token carries the byte range of its source gram in the value, so [`ts_offsets()`](../../functions/search/highlighting.md#ts_offsets) and [`ts_highlight()`](../../functions/search/highlighting.md#ts_highlight) work over a `generate_ngrams` dictionary; marker text is not part of that range. Positions follow the anchoring: in `'all'` mode the grams that share a start symbol share a position, `'only_prefix'` and `'only_suffix'` put every token at position 1, and `'only_prefix_and_suffix'` keeps the prefixes at position 1 and the suffixes at position 2.

Preview the gram stream with `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_003" />

`PRESERVEORIGINAL = true` keeps the whole word in the stream alongside its grams, so an exact match still scores:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_004" />

`STARTMARKER` and `ENDMARKER` tag only the boundary grams — those at the start of the word carry the start marker and those at the end carry the end marker — so a prefix or suffix query can be distinguished from an interior match. When both are set and the whole token is emitted, it appears twice, once with each marker:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_005" />

## Examples

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_001" />

### Unigrams and bigrams

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_002" />

## See also

- [`generate_sparse_ngrams`](./sparse-ngram.md) — variable-length grams for compact substring search
- [`split_text`](./text.md) — the word splitter to chain `generate_ngrams` after for edge n-grams
- [`generate_wildcard_ngrams`](./wildcard.md) — boundary-marked n-grams for wildcard and prefix matching
- [`generate_ngrams()`](../../functions/search/tokenizers.md#generate_ngrams) — the template as a function, applied to a value or a list in any query
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
