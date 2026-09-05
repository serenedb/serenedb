---
title: "keyword"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# keyword

The `keyword` template emits the entire input as a single verbatim token — it performs no splitting, normalization or stemming. It is the right choice for values that must match exactly and as a whole: tags, status codes, enum values, identifiers and other atomic strings.

Because the token is the raw input, a query matches only when it is byte-for-byte identical, including case and spacing. For an exact-match column that should still be case- or accent-insensitive, use [`norm`](./norm.md), which normalizes the single token; for per-word search, use [`text`](./text.md).

## Options

The `keyword` template takes no options.

## Tokenization

The template performs no analysis at all: whatever string it receives becomes a single token, byte for byte. Spaces, punctuation and case are all preserved, so a query matches only when it is identical to the indexed value as a whole.

| Input | Tokens |
|---|---|
| `New York City` | `{"New York City"}` |
| `Hello World 42` | `{"Hello World 42"}` |
| `ERR_TIMEOUT` | `{ERR_TIMEOUT}` |

The token is quoted in the output whenever it contains spaces or other characters that would otherwise be ambiguous in the array literal. Preview it with `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/keyword/example_003" />

## Examples

Create a verbatim dictionary:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/keyword/example_001" />

Preview how it tokenizes — the whole string, spaces and all, becomes one token:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/keyword/example_002" />

## See also

- [norm](./norm.md) — a single token, normalized for case and accents
- [text](./text.md) — split into words
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
