---
title: Tag Cloud
sidebar_position: 19
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Tag Cloud

A tag cloud or a trending words panel needs the vocabulary of a text column ranked by how often each word gets written. SereneDB keeps that tally in the [inverted index](../../sql/indexes/inverted/index.md) dictionary, so [`ts_dict_freq`](../../sql/functions/search/term-dictionary.md) ranks every term by total mentions without reading a single document.

The `posts` table below holds a handful of espresso brewing tips. Its `body` column runs through a `text` dictionary that sets `frequency = true` (what `ts_dict_freq` reads) and adds a small stopword list so filler like `the` never lands in the cloud.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/tag-cloud/setup" />

</details>

## Rank terms by mentions

`ts_dict_freq` is the total number of times a term occurs across the corpus, aligned with `ts_dict_agg`. Order by it and the cloud falls out, biggest word first.

<SqlLogicTest id="cookbook/search/tag-cloud/example_001" />

## Mentions is not documents

Watch the gap between [`ts_dict_count`](../../sql/functions/search/term-dictionary.md) and `ts_dict_freq`. Count is how many documents hold the term, frequency is how many times it shows up in total. `grind` and `shot` both land in all four posts, so their document counts tie, but `grind` gets written far more often and frequency pulls it clear ahead. `beans` and `fresh` go the other way with two mentions packed inside a single post.

<SqlLogicTest id="cookbook/search/tag-cloud/example_002" />

Reach for count when presence is the question (how many posts mention grinding) and frequency when volume is (how loud a word is across the corpus).

## Cap it at the top terms

A cloud only shows the headline terms, so `LIMIT` the ranked list to the size of your widget.

<SqlLogicTest id="cookbook/search/tag-cloud/example_003" />

## See also

- [Term Dictionary](../../sql/functions/search/term-dictionary.md): the full `ts_dict_*` reference, including how `ts_dict_freq` counts under deletes before compaction
- [Faceted Search](./faceted-search.md): count documents behind each value rather than rank the vocabulary
- [Autocomplete](./autocomplete.md): rank a prefix match by popularity from the same dictionary
