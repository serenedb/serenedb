---
title: "pattern"
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# pattern

The `pattern` template tokenizes text with an [RE2](https://github.com/google/re2) regular expression.

It works in two modes selected by the `GROUP` option. In **extract** mode (`GROUP = 0` for the whole match, or `N > 0` for the Nth capture group) every match becomes a token. In **split** mode (`GROUP = -1`, the default) the pattern marks the separators and the text between matches becomes the tokens. This makes it useful both for pulling structured tokens out of free text — identifiers, codes, mentions — and for splitting on separators too complex for a fixed [`delimiter`](./delimiter.md).

## Options

| Option | Type | Default | Description |
|---|---|---|---|
| `PATTERN` | string | **required** | RE2 regular expression used to match (extract mode) or to mark separators (split mode) |
| `GROUP` | integer | `-1` | What to emit: `-1` = split on each match, `0` = the whole match, `N > 0` = the Nth capture group |

## Tokenization

In **split** mode the pattern describes the separators between tokens, so the tokens are the gaps. In **extract** mode the pattern describes the tokens themselves, so anything not matched is dropped — and with `GROUP = N` only the Nth parenthesized capture group of each match is kept.

The table below shows the same idea from both directions, plus capture-group extraction:

| Mode | `PATTERN` | `GROUP` | Input | Tokens |
|---|---|---|---|---|
| split | `[-_.]` | `-1` | `SereneDB-2024_v1.2` | `SereneDB`, `2024`, `v1`, `2` |
| split | `\s+` | `-1` | `alpha  beta   gamma` | `alpha`, `beta`, `gamma` |
| extract | `[A-Z][A-Za-z0-9]{2,}` | `0` | `The Quick Brown fox jumps over Lazy Dog` | `The`, `Quick`, `Brown`, `Lazy`, `Dog` |
| extract | `([a-zA-Z]+)(\d+)` | `2` | `abc123def456ghi` | `123`, `456` |

### Extract every capitalized word (`GROUP = 0`)

Each whole match becomes a token; the lowercase `fox`, `jumps` and `over` are not matched and so are dropped:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_001" />

### Split on runs of whitespace (`GROUP = -1`)

Here the pattern `\s+` marks the separators and the runs of text between them are emitted:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_002" />

### Split an identifier on several delimiters (`GROUP = -1`)

A character class splits on `-`, `_` or `.` in a single pass — something a fixed `delimiter` cannot do:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_003" />

### Keep only a capture group (`GROUP = 2`)

With `GROUP = 2` each match emits just its second capture group — the trailing digits:

<SqlLogicTest id="sql/statements/create_text_search_dictionary/pattern/example_004" />

## See also

- [delimiter](./delimiter.md) / [multi_delimiter](./multi-delimiter.md) — split on literal characters
- [segmentation](./segmentation.md) — Unicode word-boundary splitting
- [CREATE TEXT SEARCH DICTIONARY](./index.md)
