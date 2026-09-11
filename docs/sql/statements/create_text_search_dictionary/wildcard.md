---
title: "wildcard"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# wildcard

The `wildcard` template indexes text for wildcard and prefix matching.

It wraps an inner tokenizer to split the input into terms, then emits boundary-marked character n-grams of each term so that `LIKE`-style patterns can be answered from the index instead of by a full scan. Each term is wrapped in one marker byte at each end before the grams are cut, so a leading-anchored prefix and a trailing-anchored suffix stay distinguishable from a match in the middle of a term. The template also stores the terms it saw, and a candidate the grams select is re-checked against that stored copy whenever the grams alone cannot decide the pattern, so a match is exact rather than gram-approximate. For plain substring search over code and logs, [`sparse_ngram`](./sparse-ngram.md) is usually the better fit.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `NGRAMSIZE` | integer | `3` | Gram length in codepoints, measured over the marker-wrapped term. Minimum `2` |
| `TOKENIZER_TEMPLATE` | string | **required** | Template of the nested tokenizer that produces the terms |
| `TOKENIZER_*` | — | — | Options for the nested tokenizer, each prefixed with `TOKENIZER_` |

An `NGRAMSIZE` below `2` is rejected with `"ngramsize" must be at least 2`; there is no upper bound. `TOKENIZER_TEMPLATE` is required for a new dictionary — omitting it gives `required parameter "template" was not found` — but [`copy_from`](./copy-from.md) inherits `NGRAMSIZE` and the whole nested tokenizer from the source dictionary. The template supports the `FREQUENCY` and `POSITION` [feature flags](./index.md#feature-flags), with `POSITION` requiring `FREQUENCY`; `NORM` and `OFFSET` are rejected at `CREATE TEXT SEARCH DICTIONARY` time with `Unsupported index features are specified: <mask>`.

## Tokenization

The nested tokenizer (selected with `TOKENIZER_TEMPLATE` and configured through its `TOKENIZER_`-prefixed options) first splits the input into terms. Every term is then wrapped in a single marker byte (`0xFF`) at each end, and windows of `NGRAMSIZE` codepoints slide across the wrapped term one codepoint at a time. Each window spans `NGRAMSIZE` codepoints or stops at the trailing marker, whichever comes first, so the last gram of a term is two symbols long. A term yields exactly one gram more than it has codepoints.

Writing the marker as `⟨M⟩`, with `NGRAMSIZE = 3` the term `search` yields `⟨M⟩se`, `sea`, `ear`, `arc`, `rch`, `ch⟨M⟩` and `h⟨M⟩`, and `cat` yields `⟨M⟩ca`, `cat`, `at⟨M⟩` and `t⟨M⟩`. The marker byte is part of the gram text, so the boundary grams of a term are not valid UTF-8 — these grams are an internal representation rather than something you inspect with `ts_lexize`, and you query them indirectly through `LIKE`-style patterns.

Gram lengths always count codepoints, whatever the nested tokenizer is; there is no `INPUTTYPE` option here. Input that is not valid UTF-8 is not rejected: symbol boundaries fall back to a lead-byte walk. The wildcard layer changes no bytes of its own, so all case, accent and normalization behaviour belongs to the nested tokenizer. Alongside the grams the template stores the encoded term stream of the value; a value whose nested tokenizer produced no terms emits no tokens and stores nothing.

## Searching

Index a column with the wildcard dictionary, then match it with [`ts_like`](../../functions/search/full-text.md#ts_like). A SQL `LIKE` predicate on that column, with or without `ESCAPE`, is lowered to the same filter, so it too is answered from the indexed grams instead of a full scan. The grams only select candidate documents. Because the terms themselves are stored, a candidate is re-checked against that copy whenever the grams alone cannot decide the match, so the result is exact rather than gram-approximate. The pattern always applies to one whole nested term at a time — with a delimiter tokenizer that means per word, never across the value. Enabling `POSITION` requires the grams of a pattern fragment to occur adjacently instead of merely co-occurring, which leaves fewer candidates to re-check. Every wildcard hit receives the same constant score, so `BM25()` and `TFIDF()` do not rank wildcard matches against each other.

A **substring** pattern matches anywhere in a term:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wildcard/example_001" />

A **prefix** pattern is anchored to the start of a term:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wildcard/example_002" />

## See also

- [sparse_ngram](./sparse-ngram.md) — compact substring search over code and logs
- [ngram](./ngram.md) — fixed-length character n-grams
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
