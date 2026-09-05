---
title: "multi_delimiter"
sidebar_label: Multi Delimiter
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# multi_delimiter

The `multi_delimiter` template splits the input wherever it encounters any of several delimiter characters, given as the `DELIMITERS` list. It suits fields that mix separators — for example splitting `key:value; key2:value2` on `:`, `;` and space yields the individual keys and values.

Apart from accepting a set of delimiters rather than one, it behaves like [`delimiter`](./delimiter.md): it emits the pieces verbatim, so chain it into a [`pipeline`](./pipeline/index.md) if you also need case folding or stemming.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `DELIMITERS` | string list | **required** | Comma-separated delimiters (e.g., `'":", ";", " "'`) |

## Tokenization

The template cuts the input wherever it finds any character in the `DELIMITERS` set and emits the pieces verbatim — no further analysis. It behaves like [`delimiter`](./delimiter.md) but accepts several separators at once, which suits fields that mix them.

| Input | Delimiters | Tokens |
|---|---|---|
| `key:value; key2:value2` | `:` `;` space | `{key,value,key2,value2}` |
| `2026-06-18 logs/app` | `/` `-` space | `{2026,06,18,logs,app}` |

Splitting `key:value; key2:value2` on the colon, semicolon and space separators recovers the individual keys and values in one pass. Preview the split with `ts_lexize`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/multi-delimiter/example_002" />

## Examples

<SqlLogicTest id="sql/statements/create_text_search_dictionary/multi-delimiter/example_001" />

Because the delimiter list is itself comma-separated, a comma cannot be used as a delimiter here. To split on a single comma, use the [`delimiter`](./delimiter.md) template instead.

## See also

- [delimiter](./delimiter.md) — split on a single delimiter
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
