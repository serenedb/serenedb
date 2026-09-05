---
title: "Sparse Ngram"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Sparse Ngram

The `sparse_ngram` template indexes text for **substring search** — finding an arbitrary fragment anywhere inside a value, the way `LIKE '%fragment%'` does, but accelerated by an inverted index instead of a full scan.

It targets text where splitting on word boundaries does not help: source code, log lines, URLs, file paths, identifiers, serial numbers. For that kind of data a plain word tokenizer cannot answer "which rows contain `i=42`", and a fixed-width [`ngram`](./ngram.md) dictionary that could would bloat the index with every overlapping window. `sparse_ngram` keeps the index compact by emitting only a small, carefully chosen set of variable-length grams, while still letting any substring query be answered exactly.

## How you use it

Substring search needs **two dictionaries** built from this template — they differ only in the `COVERING` option:

- an **indexing** dictionary (`COVERING = false`) attached to the column, and
- a **querying** dictionary (`COVERING = true`) used to tokenize the search string.

A query then matches every row whose indexed grams contain all of the query's grams (`ts_all`). That conjunction returns a small candidate set, which a `LIKE` predicate filters down to exact matches. The index does the expensive narrowing; `LIKE` only runs on the few rows that survive.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `COVERING` | boolean | `false` | Which side the dictionary serves. Leave `false` for the dictionary attached to the indexed column. Set `true` for the dictionary used to tokenize query strings, so their grams can be `AND`-ed together to find containing rows. |
| `MAXNGRAMLENGTH` | integer | `16` | Largest gram length, in bytes. Longer grams are more selective, so queries return fewer false candidates to verify, at the cost of a larger index. Minimum `3`. |

Inputs shorter than 3 bytes produce no grams. The template supports the `FREQUENCY` and `NORM` index features, so `bm25()` can rank rows by how much of the query they contain; `POSITION` and `OFFSET` are not supported.

## Tokenization

Unlike [`ngram`](./ngram.md), which emits every fixed-width window, `sparse_ngram` picks a small set of variable-length grams that still let any substring query be answered. The `COVERING` option controls which set: the indexing side (`false`) stores enough grams to cover the value, while the querying side (`true`) emits only the few grams a search string must share with a row.

| Input | Options | Tokens |
|---|---|---|
| `hello world` | indexing (`COVERING = false`) | `{hel,ell,llo,"lo ","o w","lo w"," wo","lo wo",wor,orl,worl,rld}` |
| `hello world` | querying (`COVERING = true`) | `{hel,ell,llo,rld,worl,"lo wo"}` |
| `hello world` | `MAXNGRAMLENGTH = 4` | `{hel,ell,llo,"lo ","o w","lo w"," wo",wor,orl,worl,rld}` |

The querying set is a strict subset of the indexing set, so requiring all of a query's grams (`ts_all`) selects exactly the rows whose indexed grams contain them. The verified token streams appear in the examples below.

## Examples

Create the indexing dictionary and inspect the grams it stores per value with `ts_lexize`. It enables `FREQUENCY` and `NORM` so `bm25()` can rank later:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sparse-ngram/example_001" />

Create the querying dictionary with `COVERING = true`. For the same string it emits a much smaller set — the grams to require together when searching:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sparse-ngram/example_002" />

### Substring search

Attach the indexing dictionary to a column, then search for a substring by requiring all of its query grams and confirming exactness with `LIKE`. `ts_highlight(code)` wraps the matched region so you can see what was found:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sparse-ngram/example_003" />

### Fuzzy "looks like this" search

Requiring only some of the query grams with `ts_any(..., k)` instead of all of them makes the search approximate. Rows that share more grams with the query rank higher under `bm25()`, so the closest matches surface first, and `ts_highlight(code)` shows how much of each row matched:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sparse-ngram/example_004" />

### Deriving the querying dictionary

Rather than configuring the querying dictionary by hand, derive it from the indexing one with [`copy_from`](./copy-from.md) and flip `COVERING`. This keeps the two in sync as the indexing options evolve:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sparse-ngram/example_005" />

### Tuning gram length

Lowering `MAXNGRAMLENGTH` caps how long a gram can grow. Here the longer `lo wo` gram the default keeps is dropped, yielding a smaller index at the cost of less selective queries:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/sparse-ngram/example_006" />

## See also

- [`ngram`](./ngram.md) — fixed-width character n-grams
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
- [CREATE INDEX](../create_index/index.md) — attach a dictionary to a column with an inverted index
