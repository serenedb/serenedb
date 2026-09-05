---
title: "norm"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# norm

The `norm` template normalizes the whole input — folding case and, optionally, stripping accents for the given `LOCALE` — and returns it as a single token without splitting it into words. Because the entire value becomes one token, it behaves like a normalized keyword: two strings match only if they are equal after normalization.

Use it for exact-match or keyword columns — tags, codes, names, enum-like values — that should still compare case-insensitively or accent-insensitively, rather than for free-text search. For per-word tokenization with the same normalization, use [`text`](./text.md).

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | — | ICU locale |
| `CASE` | string | `'none'` | Case conversion: `'none'`, `'lower'`, `'upper'` |
| `ACCENT` | boolean | `true` | Preserve accent marks (`false` folds them away) |

## Tokenization

`norm` always emits exactly one token: the input with case and accents normalized per the options. Spaces and punctuation are kept verbatim — the value is never split. The table below shows how the same input transforms under different option combinations.

| Input | CASE | ACCENT | Output token |
|---|---|---|---|
| `CAFÉ` | `'lower'` | `false` | `cafe` |
| `CAFÉ` | `'none'` | `true` (default) | `CAFÉ` (unchanged) |
| `café` | `'upper'` | `true` | `CAFÉ` |

Because two values collide only when their normalized forms are identical, a `norm` dictionary with `CASE = 'lower'` and `ACCENT = false` makes `CAFÉ`, `Café` and `cafe` all match.

<SqlLogicTest id="sql/statements/create_text_search_dictionary/norm/example_001" />

### Uppercase normalization, accents preserved

Folding to upper case while keeping accent marks turns `café` into `CAFÉ`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/norm/example_002" />

## See also

- [text](./text.md) — per-word tokenization with the same case and accent normalization
- [keyword](./keyword.md) — keeps the value as one token without normalizing it
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
