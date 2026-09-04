---
title: Counting Unique Results
sidebar_position: 21
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Counting Unique Results

`approx_count_distinct` tells you how many distinct values a search matched without counting them one at a time. It reads the [inverted index](../../sql/indexes/inverted/index.md) over a [full-text](../../sql/functions/search/full-text.md) filter and returns a HyperLogLog estimate on a fixed memory budget, so the cost holds steady whether the query matches ten documents or ten million. If you come from Elastic this is the cardinality aggregation.

A `products` table pairs a keyword `brand` with a tokenized `title`.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/result-cardinality/setup" />

</details>

## Hits and distinct values in one pass

The number you put next to a result set is usually two things at once: how many documents matched and how many distinct brands sit behind them. `count(*)` gives the first and `approx_count_distinct(brand)` gives the second, both riding the same `WHERE`.

<SqlLogicTest id="cookbook/search/result-cardinality/example_001" />

Seven titles mention "running" across five distinct brands.

## Check the estimate against the exact count

While the result set is small you can afford the exact `count(DISTINCT brand)` and hold the estimate up against it. At this size they land on the same number.

<SqlLogicTest id="cookbook/search/result-cardinality/example_002" />

The estimate trades a small margin of error for memory that does not grow with cardinality, so it is the one to reach for once you count distinct values across millions of hits. When you want a count per value rather than one total, [Faceted Search](./faceted-search.md) breaks the same result set down into a count for every brand.

## See also

- [Faceted Search](./faceted-search.md): exact per-value counts and distinct-value lists, counted in the term dictionary without opening a document
- [Search with joins and analytics](./search-with-joins.md): run these aggregates alongside joins and grouping
- [Full-text functions](../../sql/functions/search/full-text.md): the `@@` match operator used in the filter
