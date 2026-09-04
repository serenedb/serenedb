---
title: Fuzzy Search
sidebar_position: 8
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Fuzzy Search

Fuzzy search finds approximate matches despite typos, spelling variations or alternative forms. SereneDB supports two approaches, each suited to different use cases.

## Similarity measures

### Levenshtein distance

Measures the minimum number of single-character edits (insertions, deletions, substitutions) to transform one string into another.

| From | To | Distance | Edits |
|---|---|---|---|
| `galaxy` | `galxy` | 1 | 1 deletion |
| `search` | `serach` | 2 | 2 substitutions |

### Damerau-Levenshtein distance

Extends Levenshtein by also counting **transpositions** (adjacent character swaps) as a single edit. This is more forgiving for common typos:

| From | To | Levenshtein | Damerau-Levenshtein |
|---|---|---|---|
| `galaxy` | `glaaxy` | 2 | 1 (transposition) |
| `search` | `saerch` | 2 | 1 (transposition) |

### N-gram similarity

Breaks strings into substrings of fixed length (bigrams, trigrams, etc.) and measures how many substrings are shared. Works better for longer strings and partial matches.

Example with bigrams (n=2):

| String | Bigrams |
|---|---|
| `hello` | `he`, `el`, `ll`, `lo` |
| `help` | `he`, `el`, `lp` |

Shared bigrams: `he`, `el` → similarity = 2/5 = 0.4

## When to use which

| Approach | Best for | Typical use case |
|---|---|---|
| **Levenshtein** | Short strings, exact typo correction | User name search, product codes, tags |
| **N-gram** | Longer strings, partial matching | Autocomplete, "did you mean?", document titles |

## Levenshtein matching with `ts_levenshtein`

Finds terms within a given edit distance. Uses Damerau-Levenshtein by default (transpositions count as one edit).

### Setup

Any text dictionary works — stemming should typically be disabled for fuzzy matching:

<SqlLogicTest id="cookbook/search/fuzzy-search/example_001" />

### Basic usage

<SqlLogicTest id="cookbook/search/fuzzy-search/example_002" />

### Disable transpositions

Use strict Levenshtein (no transposition counting):

<SqlLogicTest id="cookbook/search/fuzzy-search/example_003" />

### Prefix matching

Require a prefix before applying fuzzy matching — useful for autocomplete:

<SqlLogicTest id="cookbook/search/fuzzy-search/example_004" />

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| *column* | column | | Indexed text column |
| *term* | string | | Search term |
| *distance* | integer | | Max edit distance (0–4) |
| *transpositions* | boolean | `true` | Count transpositions as single edit |
| *prefix* | string | `''` | Required prefix before fuzzy matching |

The number of dictionary terms the predicate expands to is capped by the [`sdb_levenshtein_max_terms`](../../sql/indexes/inverted/maintenance.md#session-settings) session setting (default `64`, per index segment), keeping the terms closest to the query. `SET sdb_levenshtein_max_terms = 0` matches every term within the edit distance.

## N-gram matching with `ts_ngram`

Finds terms by n-gram similarity. Requires an index built with an `ngram` dictionary.

### Setup

<SqlLogicTest id="cookbook/search/fuzzy-search/example_005" />

### Basic usage

<SqlLogicTest id="cookbook/search/fuzzy-search/example_006" />

### Tuning n-gram size

- **Bigrams** (mingram=2, maxgram=2): More matches, less precision. Good for short terms.
- **Trigrams** (mingram=3, maxgram=3): Fewer matches, more precision. Better for longer terms.

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| *column* | column | | Indexed text column (must use `ngram` dictionary) |
| *term* | string | | Search term |
| *threshold* | float | `0.7` | Minimum n-gram similarity (0.0–1.0) |

## Combining with other filters

Both fuzzy functions work with `AND`, `OR` and other search predicates:

<SqlLogicTest id="cookbook/search/fuzzy-search/example_007" />

## See also

- [CREATE INDEX](../../sql/statements/create_index/index.md)
- [CREATE TEXT SEARCH DICTIONARY](../../sql/statements/create_text_search_dictionary/index.md)
- [Phrase and Proximity Search](./phrase-and-proximity-search.md)
- [Spell Correction](./spell-correction.md): correct the input before you query instead of matching fuzzily at query time
- [Synonyms](./synonyms.md): match different words for the same thing rather than typos of one word
