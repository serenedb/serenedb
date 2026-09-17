---
title: "generate_sparse_ngrams"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# generate_sparse_ngrams

The `generate_sparse_ngrams` template indexes text for **substring search** — finding an arbitrary fragment anywhere inside a value, the way `LIKE '%fragment%'` does, but accelerated by an inverted index instead of a full scan.

It targets text where splitting on word boundaries does not help: source code, log lines, URLs, file paths, identifiers, serial numbers. For that kind of data a plain word tokenizer cannot answer "which rows contain `i=42`", and an [`generate_ngrams`](./generate_ngrams.md) dictionary that could do so would bloat the index with every overlapping window. `generate_sparse_ngrams` keeps the index compact by emitting only a small, carefully chosen set of variable-length grams, while still letting any substring query be answered exactly.

**As a function:** `generate_sparse_ngrams(value, max_ngram_length := 16, covering := false)` — the value first, then the options in the order below. See [tokenizer functions](./index.md) for how a value, a list and a chain of calls behave.

<SqlLogicTest id="sql/functions/search/tokenizers/generate_sparse_ngrams/function_form" />

## How you use it

Substring search needs **two dictionaries** built from this template — they differ only in the `COVERING` option:

- an **indexing** dictionary (`COVERING = false`) attached to the column, and
- a **querying** dictionary (`COVERING = true`) used to tokenize the search string.

A query then matches every row whose indexed grams contain all of the query's grams (`ts_all`). That conjunction returns a small candidate set, which a `LIKE` predicate filters down to exact matches. The index does the expensive narrowing; `LIKE` only runs on the few rows that survive.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `COVERING` | boolean | `false` | Which side the dictionary serves. Leave `false` for the dictionary attached to the indexed column. Set `true` for the dictionary used to tokenize query strings, so their grams can be `AND`-ed together to find containing rows. |
| `MAX_NGRAM_LENGTH` | integer | `16` | Largest gram length, in characters. It caps the grams the indexing side emits and bounds how long the querying side lets a covering gram grow, so a covering gram can come out shorter than the cap. Longer grams are more selective, so queries return fewer false candidates to verify, at the cost of a larger index. Values below `3` are rejected when the dictionary is created. |

The template supports the `FREQUENCY` and `NORM` [feature flags](../../../statements/create_text_search_dictionary/index.md#feature-flags), so `BM25()` can rank rows by how much of the query they contain; `NORM` requires `FREQUENCY`. `POSITION` and `OFFSET` are not supported: setting either fails when the dictionary is created. [`ts_offsets()`](../highlighting.md#ts_offsets) and [`ts_highlight()`](../highlighting.md#ts_highlight) still work over a `generate_sparse_ngrams` dictionary — with no offsets in the index they re-analyze the value at query time.

## Tokenization

Unlike [`generate_ngrams`](./generate_ngrams.md), which emits every sliding window in its length range, `generate_sparse_ngrams` picks a small set of variable-length grams that still let any substring query be answered. The `COVERING` option controls which set: the indexing side (`false`) stores enough grams to cover the value, while the querying side (`true`) emits only the few grams a search string must share with a row.

The selection runs over hashes of adjacent character pairs, so every gram boundary falls between characters. A gram is a verbatim slice of the value: nothing is case-folded, accent-stripped or Unicode-normalized, and there is no locale or case option to change that. Lengths count codepoints, so a gram never cuts a multi-byte UTF-8 sequence in half and its term text is valid UTF-8 whenever the value is; input that is not valid UTF-8 is never rejected, character boundaries just fall back to a lead-byte walk. Every gram spans at least 3 characters, which is why shorter values produce nothing at all: `he` yields `{}` and `hel` yields `{hel}`, and the two-character `日本` yields `{}` even though it is 6 bytes. A search fragment shorter than 3 characters likewise has no grams to require, so it cannot be answered from the index at all and has to be found with `LIKE` alone.

| Input | Options | Tokens |
|---|---|---|
| `hello world` | indexing (`COVERING = false`) | `{hel,ell,llo,"lo ","o w","lo w"," wo","lo wo",wor,orl,worl,rld}` |
| `hello world` | querying (`COVERING = true`) | `{hel,ell,llo,rld,worl,"lo wo"}` |
| `hello world` | indexing, `MAX_NGRAM_LENGTH = 4` | `{hel,ell,llo,"lo ","o w","lo w"," wo",wor,orl,worl,rld}` |
| `日本語テキスト` | indexing (`COVERING = false`) | `{日本語,本語テ,語テキ,本語テキ,日本語テキ,テキス,キスト}` |

Tokens come out in the order of the selection walk, not sorted by where they start in the value. Here the querying set is a subset of the indexing set, so requiring all of a query's grams (`ts_all`) narrows the scan to rows whose stored grams cover the search string; those rows are candidates, and `LIKE` confirms the exact matches. The verified token streams appear in the examples below.

## Examples

Create the indexing dictionary and inspect the grams it stores per value with `ts_lexize`. It enables `FREQUENCY` and `NORM` so `BM25()` can rank later:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_sparse_ngrams/example_001" />

Create the querying dictionary with `COVERING = true`. For the same string it emits a much smaller set — the grams to require together when searching:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_sparse_ngrams/example_002" />

### Substring search

Attach the indexing dictionary to a column, then search for a substring by requiring all of its query grams and confirming exactness with `LIKE`. `ts_highlight(code)` wraps the matched region so you can see what was found:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_sparse_ngrams/example_003" />

### Fuzzy "looks like this" search

Requiring only some of the query grams with `ts_any(..., k)` instead of all of them makes the search approximate. Rows that share more grams with the query rank higher under `BM25()`, so the closest matches surface first, and `ts_highlight(code)` shows how much of each row matched:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_sparse_ngrams/example_004" />

### Deriving the querying dictionary

The querying dictionary repeats the indexing dictionary's gram settings with `COVERING` switched on, and carries no feature flags of its own because nothing is indexed with it:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_sparse_ngrams/example_005" />

### Tuning gram length

Lowering `MAX_NGRAM_LENGTH` caps how long a gram can grow. Here the longer `lo wo` gram the default keeps is dropped, yielding a smaller index at the cost of less selective queries:

<SqlLogicTest id="sql/functions/search/tokenizers/generate_sparse_ngrams/example_006" />

## See also

- [`generate_ngrams`](./generate_ngrams.md) — every n-gram in a length range, optionally anchored to the start or the end of the token
- [`generate_wildcard_ngrams`](./generate_wildcard_ngrams.md) — `LIKE`-style pattern matching over the terms of a nested tokenizer
- [CREATE TEXT SEARCH DICTIONARY](../../../statements/create_text_search_dictionary/index.md)
- [CREATE INDEX](../../../statements/create_index/index.md) — attach a dictionary to a column with an inverted index
