---
title: Stemming and Stopwords
sidebar_position: 6
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Stemming and Stopwords

A search for "run" should find "running" and "runs". Noise words like "the" and "a" should never bloat the index. A [text dictionary](../../sql/indexes/inverted/text-analysis.md) handles both: [`stemming = true`](../../sql/statements/create_text_search_dictionary/stem.md) reduces words to their root with the Snowball stemmer picked by `locale`, while [`stopwords`](../../sql/statements/create_text_search_dictionary/stopwords.md) drops a list of words before they ever reach the index. The same dictionary analyzes the indexed text and the query, so the inflection you store and the term you search for meet in the middle.

Three short articles share one indexed `body` column whose dictionary stems each word and strips the stopwords.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/stemming/setup" />

</details>

## Match every inflection

The dictionary folds "running" and "runs" down to the stem `run` as it indexes each row, so all three rows store the same term. The query term is stemmed the exact same way, which means one query form finds every inflection without you spelling them out.

<SqlLogicTest id="cookbook/search/stemming/example_001" />

## Stemming is not fuzzy matching

Stemming is precise, not fuzzy. "runners" stems to `runner`, which is a different root from `run`, so searching for `runner` matches only that row. If you want to match on misspellings or near-typos instead, reach for [Fuzzy Search](./fuzzy-search.md) or [Spell Correction](./spell-correction.md). The Snowball algorithm is rule based too, so irregular forms like "ran" are left alone rather than folded back to `run`.

<SqlLogicTest id="cookbook/search/stemming/example_002" />

## See the analysis

[`ts_lexize`](../../sql/functions/search/full-text.md) runs a string through the dictionary and hands back the terms it produces, which is the fastest way to see what stemming and stopword removal actually do. Regular inflections collapse to their stem, the irregular "ran" survives untouched and the stopwords disappear entirely.

<SqlLogicTest id="cookbook/search/stemming/example_003" />

## What ends up in the index

`ts_dict_agg` lists the terms the index actually stores. The stopwords are gone and everything else is a stem, so the dictionary stays small and every query hits the same normalized form.

<SqlLogicTest id="cookbook/search/stemming/example_004" />

## See also

- [Text analysis](../../sql/indexes/inverted/text-analysis.md): how a text dictionary tokenizes, stems and filters
- [Fuzzy Search](./fuzzy-search.md): match near-typos and misspellings, not inflections
- [Case Sensitivity and Diacritics](./case-sensitivity-and-diacritics.md): fold case and accents in the same dictionary
