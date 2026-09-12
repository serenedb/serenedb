---
title: "union"
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# union

The `union` template runs several independent sub-tokenizers over the same input and merges their tokens into one stream. Use it when a column needs to be searchable in more than one way at once — for example as a whole keyword *and* as character n-grams — without maintaining separate indexes.

A union is a list of analyzers, `[split_csv(' '), keyword()]`, and each element may itself be a chain: `[generate_ngrams(2, 2) | normalize_tokens(case := 'lower'), keyword()]`. At least one member is required. Where [`pipeline`](./pipeline/index.md) feeds one analyzer's output into the next, `union` runs them in parallel over the original input and combines the results.

## Options

The template has no options of its own; the members are the list elements. A member may be any template call, a chain, another list, or a bare dictionary name that runs a stored dictionary's analyzer. A list element that is not an analyzer — a string, say — fails with `a list stage must hold analyzers`, and so does the empty list `[]`.

The template supports the `FREQUENCY`, `POSITION` and `NORM` [feature flags](./index.md#feature-flags); `POSITION` and `NORM` each require `FREQUENCY`. `OFFSET` is not supported: setting it fails when the dictionary is created, with `Unsupported index features are specified: <mask>`. [`ts_offsets()`](../../functions/search/highlighting.md#ts_offsets) and [`ts_highlight()`](../../functions/search/highlighting.md#ts_highlight) are not available for a `union` dictionary either — the tokenizer produces no offsets, so both fail rather than re-analyzing the value.

## Tokenization

Every member analyzes the original input independently — nothing is chained — and the member streams are merged into one stream ordered by position. Pairing [`keyword`](./keyword.md) (which keeps the value verbatim) with a 2-gram [`generate_ngrams`](./ngram.md) member makes `abcd` searchable both as the exact term and by any of its bigrams. Pairing a [`split_csv`](./csv.md) member with `keyword` indexes `hello world` both as its individual words and as the whole phrase, so exact-phrase and per-word queries both hit.

| Input | Members | Tokens |
|---|---|---|
| `abcd` | `keyword` + `generate_ngrams` (`MINGRAM = MAXGRAM = 2`) | `{abcd,ab,bc,cd}` |
| `hello world` | `split_csv` (`' '`) + `keyword` | `{hello,"hello world",world}` |

At each position the union emits every token member 1 has there, then member 2's, and so on. Positions are each member's own numbering, starting at `1`, and they pass through unchanged — the union does not renumber. The same position therefore repeats across members, and members of different granularity drift apart: a word-splitting member advances one position per word while an n-gram member advances one position per gram. Duplicate terms are not removed, so when two members emit the same term at the same position both are emitted.

A member that produces no tokens for a value contributes nothing, and the other members run to the end of their own streams. Empty input is not special-cased: each member sees the empty string and the union emits whatever the members return for it. The union itself never transforms bytes: case folding, accent handling and Unicode normalization are each member's own business.

## Examples

Index each value both verbatim and as 2-grams:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/union/example_001" />

Index text both as individual words and as the whole phrase:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/union/example_002" />

The same dictionary written as a list in the expression form:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/union/example_003" />

## See also

- [`pipeline`](./pipeline/index.md) — chain analyzers in sequence (vs. union's parallel merge)
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
