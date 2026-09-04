---
title: "wildcard"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# wildcard

The `wildcard` template indexes text for wildcard and prefix matching.

It wraps an inner tokenizer to split the input into terms, then emits boundary-marked character n-grams of each term so that `LIKE`-style patterns can be answered from the index instead of by a full scan. The grams carry start/end markers (not visible in the token text) so that a leading-anchored prefix is distinguishable from a match in the middle of a term. For plain substring search over code and logs, [`sparse_ngram`](./sparse-ngram.md) is usually the better fit.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `NGRAMSIZE` | integer | `3` | N-gram length used for wildcard/prefix indexing (minimum 2) |
| `TOKENIZER_TEMPLATE` | string | **required** | Template of the nested tokenizer that produces the terms |
| `TOKENIZER_*` | — | — | Options for the nested tokenizer, each prefixed with `TOKENIZER_` |

## How it tokenizes

The nested tokenizer (selected with `TOKENIZER_TEMPLATE` and configured through its `TOKENIZER_`-prefixed options) first splits the input into terms. Each term is then expanded into its character n-grams up to `NGRAMSIZE` long — the shorter boundary grams at the start and end of the term alongside the full-length interior grams.

Conceptually, with `NGRAMSIZE = 3`, the term `search` expands into the grams `se`, `sea`, `ear`, `arc`, `rch`, `ch`, `h`. Each gram also carries a non-printable start/end marker (so a leading-anchored prefix is distinguishable from a mid-term match), which is why these grams are an internal detail rather than something you inspect with `ts_lexize` — you query them indirectly through `LIKE`-style patterns.

## Searching

Index a column with the wildcard dictionary, then match it with [`ts_like`](../../functions/search/full-text.md#ts_like) (or [`ts_starts_with`](../../functions/search/full-text.md#ts_starts_with) / [`ts_regexp`](../../functions/search/full-text.md#ts_regexp)). The pattern is answered from the indexed grams instead of a full scan. A **substring** pattern matches anywhere in a term:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wildcard/example_001" />

A **prefix** pattern is anchored to the start of a term:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/wildcard/example_002" />

## See also

- [sparse_ngram](./sparse-ngram.md) — compact substring search over code and logs
- [ngram](./ngram.md) — fixed-length character n-grams
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
