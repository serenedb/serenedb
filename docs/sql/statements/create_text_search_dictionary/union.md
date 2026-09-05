---
title: "union"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# union

The `union` template runs several independent sub-tokenizers over the same input and merges their tokens into one stream. Use it when a column needs to be searchable in more than one way at once — for example as a whole keyword *and* as character n-grams — without maintaining separate indexes.

Each member is configured with a `TOKENIZER⟨N⟩_` prefix, numbered densely from `1`: `TOKENIZER1_TEMPLATE` selects the first sub-tokenizer and its `TOKENIZER1_*` options configure it, `TOKENIZER2_TEMPLATE` the second, and so on. At least one member is required. Where [`pipeline`](./pipeline/index.md) feeds one analyzer's output into the next, `union` runs them in parallel over the original input and combines the results.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `TOKENIZER⟨N⟩_TEMPLATE` | string | **required** | Template of the Nth sub-tokenizer (numbered densely from 1) |
| `TOKENIZER⟨N⟩_*` | — | — | Options for the Nth sub-tokenizer, prefixed with `TOKENIZER⟨N⟩_` |

## Tokenization

Every member analyzes the original input, and their outputs are pooled into a single token set. Pairing [`keyword`](./keyword.md) (which keeps the value verbatim) with a 2-gram [`ngram`](./ngram.md) member makes `abcd` searchable both as the exact term and by any of its bigrams. Pairing a [`delimiter`](./delimiter.md) member with `keyword` indexes `hello world` both as its individual words and as the whole phrase, so exact-phrase and per-word queries both hit.

| Input | Members | Tokens |
|---|---|---|
| `abcd` | `keyword` + `ngram` (`MINGRAM = MAXGRAM = 2`) | `{abcd,ab,bc,cd}` |
| `hello world` | `delimiter` (`' '`) + `keyword` | `{hello,"hello world",world}` |

Index each value both verbatim and as 2-grams:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/union/example_001" />

Index text both as individual words and as the whole phrase:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/union/example_002" />

## See also

- [`pipeline`](./pipeline/index.md) — chain analyzers in sequence (vs. union's parallel merge)
- [`minhash`](./minhash/index.md) — composition template that emits similarity signatures
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
