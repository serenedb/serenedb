---
title: Spell Correction
sidebar_position: 9
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Spell Correction

A search box gets typos. Instead of running a query for "jaket" and returning nothing, correct the input against the terms you actually indexed and rerun with the fix. SereneDB does the correction with a fuzzy match over the [inverted index](../../sql/indexes/inverted/index.md) dictionary: [`ts_levenshtein`](../../sql/functions/search/full-text.md#tsquery-constructors) enumerates the terms within an edit distance of what the user typed and [`ts_dict_score`](../../sql/functions/search/term-dictionary.md) ranks them by similarity, so the closest term is the correction.

This is the mirror image of [Fuzzy Search](./fuzzy-search.md). Fuzzy search runs the real query with typo tolerance baked in. Spell correction fixes the input first, then runs a normal exact query with the corrected term, which keeps the hot query path fast and precise.

Each row of the query log holds one past search stored as a keyword so a term's document count is how many times that word got searched.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/spell-correction/setup" />

</details>

## Suggest corrections

`ts_levenshtein('jaket', 2)` enumerates the indexed terms within edit distance 2 of the typo and `ts_dict_score` reports each candidate's similarity. Ordering by similarity puts the intended word on top.

<SqlLogicTest id="cookbook/search/spell-correction/example_001" />

## Pick the correction

Take the top candidate and you have the "did you mean" to feed back into the real query. `ORDER BY` similarity then `LIMIT 1`.

<SqlLogicTest id="cookbook/search/spell-correction/example_002" />

## Break ties by popularity

When the typo sits the same distance from two real words the similarity ties, so lean on the log: prefer the word people actually search. Here "bost" is one edit from both "boot" and "boat", and the document count breaks the tie toward the popular one.

<SqlLogicTest id="cookbook/search/spell-correction/example_003" />

## See also

- [Fuzzy Search](./fuzzy-search.md): match documents with the typo tolerance at query time instead of correcting first
- [Autocomplete](./autocomplete.md): complete a prefix rather than correct a whole word
- [Term Dictionary](../../sql/functions/search/term-dictionary.md): how `ts_dict_score` follows the driving fuzzy matcher
