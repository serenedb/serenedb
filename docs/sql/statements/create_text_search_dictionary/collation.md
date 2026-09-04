---
title: "collation"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# collation

The `collation` template converts the input into a single locale-aware collation key for the configured `LOCALE`, rather than into search tokens. A collation key is a transformed form of the string whose byte order matches the locale's sorting rules, so comparing or ordering the keys yields linguistically correct results — for example placing `ä` where the locale expects it relative to `a` and `z`.

Use this template when you need locale-correct sorting or equality over an indexed column. It produces one token per value and is not intended for free-text matching.

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `LOCALE` | string | — | ICU locale |

## Tokenization

`collation` emits exactly one token per value: an opaque binary collation key. The key's bytes are not human-readable and you never inspect them directly — their value is that comparing two keys reproduces the locale's sort order. For an `en_US` locale the key for `apple` sorts before the key for `banana`, just as the words do.

| Input | LOCALE | Output |
|---|---|---|
| `apple` | `en_US.UTF-8` | binary collation key (sorts before `banana`) |
| `banana` | `en_US.UTF-8` | binary collation key (sorts after `apple`) |

The example below builds a collation dictionary and confirms that the key for `apple` orders before the key for `banana`:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/collation/example_001" />

## See also

- [keyword](./keyword.md) — keeps the literal value as one token for exact matching
- [norm](./norm.md) — normalizes a value for case- or accent-insensitive equality
- [CREATE TEXT SEARCH DICTIONARY](../create_text_search_dictionary/index.md)
