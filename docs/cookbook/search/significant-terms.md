---
title: Significant Terms
sidebar_position: 20
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Significant Terms

Find the words that make a subset of your corpus distinctive: terms that show up far more often inside one category than they do across the whole index. It runs on [term dictionary](../../sql/functions/search/term-dictionary.md) aggregates over an [inverted index](../../sql/indexes/inverted/index.md), and if you come from Elastic it covers what `significant_terms` gives you.

Nine articles fall into three categories (`science`, `sports` and `business`), each row carrying a tokenized `body`.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/significant-terms/setup" />

</details>

## Background frequency across the whole corpus

`ts_dict_agg(body)` returns every indexed term and `ts_dict_count(body)` returns how many documents contain each one. Run them over the full index to get the background rate. Common words like `data` sit at the top and tell you nothing about any one category.

<SqlLogicTest id="cookbook/search/significant-terms/example_001" />

## Foreground counts for one category

Filter the same aggregate to a single category and you get the per-term document counts inside that subset. On their own these counts are misleading: `data` and `quantum` both appear in all three science articles, so raw frequency cannot tell you which one is actually characteristic of science.

<SqlLogicTest id="cookbook/search/significant-terms/example_002" />

## Rank by lift

Join the foreground counts to the background counts on the term and score each by how far its foreground count beats what the background rate predicts: `fg_docs - bg_docs * fg_total / bg_total`. `quantum` tops the list because it fills every science article yet stays rare across the rest of the corpus. Right behind it sits a cluster of terms that each appear in a single science article and nowhere else.

<SqlLogicTest id="cookbook/search/significant-terms/example_003" />

## Require a minimum foreground count

The crude lift score rewards rarity, so a term that lands in one foreground document and no others (`breakthrough`, `computing`, `entanglement` and `experiment`) scores as high as anything with real support. A single stray document is enough to reach the top. Put a floor on `fg_docs` to make a term earn its place across several documents before it counts. At `fg_docs >= 2` only `quantum` stands out for science and the single-document noise is gone.

<SqlLogicTest id="cookbook/search/significant-terms/example_004" />

## The same query for another subset

Nothing in the query is tuned to science. Point the foreground filter at `business` and `market` comes out on top, concentrated in business articles and absent everywhere else. The same floor keeps the single-document terms out of the result.

<SqlLogicTest id="cookbook/search/significant-terms/example_005" />

## See also

- [Term Dictionary](../../sql/functions/search/term-dictionary.md): the `ts_dict_agg`, `ts_dict_count` and `ts_dict_freq` aggregates used here
- [Faceted Search](./faceted-search.md): count documents per label with the same aggregates
- [Trending Terms / Tag Cloud](./tag-cloud.md): rank terms by raw mentions instead of over-representation
