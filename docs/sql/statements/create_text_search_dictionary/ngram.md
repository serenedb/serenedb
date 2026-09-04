---
title: "ngram"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# ngram

The `ngram` template breaks each token into overlapping fixed-length character sequences — n-grams — so searches can match on fragments rather than whole words. With the default `MINGRAM` of 2 and `MAXGRAM` of 3, the word `search` yields `se`, `ea`, `ar`, `rc`, `ch` and `sea`, `ear`, `rch`, letting a query find it from a partial or slightly misspelled input. This makes the template a good fit for fuzzy matching, autocomplete and typo-tolerant search.

`PRESERVEORIGINAL` additionally keeps the whole token alongside its grams, and `STARTMARKER`/`ENDMARKER` tag the start and end of the source token so prefixes and suffixes can be distinguished from interior matches. The index grows with the width of the `MINGRAM`–`MAXGRAM` range, so keep it as narrow as your matching needs allow.

For substring search over code, logs or identifiers, prefer [`sparse_ngram`](./sparse-ngram.md), which answers the same fragment queries while keeping the index far more compact.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `MINGRAM` | integer | `2` | Minimum n-gram length |
| `MAXGRAM` | integer | `3` | Maximum n-gram length |
| `PRESERVEORIGINAL` | boolean | `false` | Emit original token alongside n-grams |
| `INPUTTYPE` | string | `'utf8'` | Input encoding: `'binary'`, `'utf8'` |
| `STARTMARKER` | string | — | Prefix marker at n-gram boundary |
| `ENDMARKER` | string | — | Suffix marker at n-gram boundary |

## Tokenization

For each input token the template emits every contiguous character window whose length falls between `MINGRAM` and `MAXGRAM`, sliding one character at a time across the whole word. With `MINGRAM = 2` and `MAXGRAM = 3`, `search` produces every 2- and 3-character window, so a query for any of those fragments finds the word — the basis for fuzzy and typo-tolerant matching. Unlike the edge n-grams of [`text`](./text.md), these grams are not anchored to the start of the word.

| Input | Options | Tokens |
|---|---|---|
| `search` | `MINGRAM = 2`, `MAXGRAM = 3` | `{se,sea,ea,ear,ar,arc,rc,rch,ch}` |
| `search` | `MINGRAM = 2`, `MAXGRAM = 3`, `PRESERVEORIGINAL = true` | `{se,sea,search,ea,ear,ar,arc,rc,rch,ch}` |
| `cat` | `MINGRAM = 2`, `MAXGRAM = 3`, `STARTMARKER = '^'`, `ENDMARKER = '$'` | `{^ca,^cat,cat$,at$}` |

Preview the gram stream with `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_003" />

`PRESERVEORIGINAL = true` keeps the whole word in the stream alongside its grams, so an exact match still scores:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_004" />

`STARTMARKER` and `ENDMARKER` tag only the boundary grams — those at the start of the word carry the start marker and those at the end carry the end marker — so a prefix or suffix query can be distinguished from an interior match:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_005" />

## Examples

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_001" />

### Unigrams and bigrams

<SqlLogicTest id="sql/statements/create_text_search_dictionary/ngram/example_002" />

## See also

- [`sparse_ngram`](./sparse-ngram.md) — variable-length grams for compact substring search
- [`text`](./text.md) — word tokenizer with optional prefix-anchored edge n-grams
- [`wildcard`](./wildcard.md) — boundary-marked n-grams for wildcard and prefix matching
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
