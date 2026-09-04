---
title: Case-Sensitivity and Diacritics
sidebar_position: 3
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Case-Sensitivity and Diacritics

Control how text is normalized before indexing and searching. Dictionary configuration determines whether searches are case-sensitive and whether accented characters match their base forms.

See [Setup](./index.md#setup) for the shared dataset used in all examples.

## How it works

When text is indexed, the dictionary's `CASE` and `ACCENT` options normalize tokens:

| Option | Value | Effect |
|---|---|---|
| `CASE` | `'lower'` | Convert all tokens to lowercase |
| `CASE` | `'upper'` | Convert all tokens to uppercase |
| `CASE` | `'none'` | Preserve original case |
| `ACCENT` | `false` | Strip diacritics (é → e, ü → u) |
| `ACCENT` | `true` | Preserve diacritics |

Use `ts_lexize` to see exactly how a dictionary transforms text:

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_001" />

## Case-insensitive search

The `basic_dict` uses `CASE = 'lower'`, so searches are case-insensitive:

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_002" />

## Case-sensitive search

The `exact_dict` uses `CASE = 'none'`, preserving original case:

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_003" />

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_004" />

## Accent-insensitive search

The `basic_dict` uses `ACCENT = false`, stripping diacritics:

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_005" />

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_006" />

## Accent-sensitive search

The `exact_dict` uses `ACCENT = true`, preserving diacritics:

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_007" />

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_008" />

## Creating custom dictionaries

Different use cases call for different normalization. Here are common patterns:

### Search-friendly (case + accent insensitive)

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_009" />

### Identifier matching (case + accent sensitive)

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_010" />

### Uppercase normalization

<SqlLogicTest id="cookbook/search/case-sensitivity-and-diacritics/example_011" />

## See also

- [CREATE TEXT SEARCH DICTIONARY](../../sql/statements/create_text_search_dictionary/index.md) — all dictionary templates and options
- [Phrase and Proximity Search](./phrase-and-proximity-search.md) — using phrase search with dictionaries
