---
title: Faceted Search
sidebar_position: 18
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Faceted Search

Faceted navigation puts a count next to every filter so a shopper sees how many products sit behind each category or brand before they click. Every count comes out of the [inverted index](../../sql/indexes/inverted/index.md) term dictionary without opening a document, so there is no separate rollup table to keep in sync. This recipe faces a product catalog, counts facets over a search result, buckets a price range, keeps a drill-down honest and lists distinct values without touching the base table.

A `products` catalog backs every query here: `category`, `brand` and `price_band` are keyword columns the dictionary faces directly and `title` is tokenized for search.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/faceted-search/setup" />

</details>

## Count every category

The bread and butter of a facet sidebar: how many products in each category. On a keyword column the optimizer resolves a plain `GROUP BY` inside the dictionary itself, so it never opens a document. `EXPLAIN` shows `TsDict:` on the `IRESEARCH_SCAN` when the rewrite fires.

<SqlLogicTest id="cookbook/search/faceted-search/example_001" />

## Facet a search result

Real faceting reacts to what the user typed. Someone searches "running", you show the category breakdown of the matches so they can narrow down. A `@@` matcher filters documents and [`ts_dict_agg`](../../sql/functions/search/term-dictionary.md) with its aligned [`ts_dict_count`](../../sql/functions/search/term-dictionary.md) returns every category that survives, with a live count over that result set.

<SqlLogicTest id="cookbook/search/faceted-search/example_002" />

## Add another dimension

Each facet is its own `ts_dict_agg` over the same filter, so a second dimension is one more pair of lists. Here is the brand breakdown of the same "running" search.

<SqlLogicTest id="cookbook/search/faceted-search/example_003" />

## Count every dimension in one query

One query per dimension works, but a sidebar usually wants them all. `GROUP BY GROUPING SETS` counts several columns in a single pass: each set is one facet, so `((category), (brand), (price_band))` returns a `(facet, value, count)` row for every value across all three. `EXPLAIN` shows one `TsDict: category, brand, price_band` on the scan with no document lookup.

<SqlLogicTest id="cookbook/search/faceted-search/example_009" />

Every grouped column has to be `NOT NULL` for the rewrite to fire — a nullable facet drops the query to a document scan and collapses the `NULL` groups. Fill missing values with a sentinel such as `''` at write time and skip that row when you render.

This counts each dimension on its own, the marginal totals a sidebar shows. It is not `GROUP BY category, brand`, which asks for every category-and-brand *combination*: that needs the per-document pairing the separate dictionaries do not hold, so it always scans documents. When you do need the combination, precompute the pair as one keyword column (`category || '\x1f' || brand`, the same trick as the [price band](#bucket-a-numeric-field-into-range-facets) below) and group that.

## Bucket a numeric field into range facets

The dictionary faces terms, so a price or date range is just a keyword column you fill with the band a row lands in (`budget`, `mid`, `premium`) at write time. Facet it like any other dimension and you get a price ladder for free, no scan and no `CASE` over raw prices at query time.

<SqlLogicTest id="cookbook/search/faceted-search/example_006" />

Precompute the band on insert or in a generated column. The index never sees the raw number, only the bucket, so the facet stays a dictionary walk.

## Keep the active facet clickable

Once a shopper picks a category the brand list should narrow to that category, but the category list has to stay whole so they can switch. So each dimension counts with the *other* filters applied and its own left off. Here the brand facet is scoped to the "footwear" selection while the category facet above keeps showing every category.

<SqlLogicTest id="cookbook/search/faceted-search/example_008" />

## Every dimension from one index scan

You do not pay per facet. A single pass over the dictionary can emit every dimension at once, so the whole sidebar is one read of the index.

<SqlLogicTest id="cookbook/search/faceted-search/example_007" />

## List distinct values without a scan

Populating a filter dropdown, a "3 brands" badge or a validation check all want the distinct values of a column. `count(DISTINCT col)` and `array_agg`/`ts_dict_agg` over a keyword column come from the dictionary, so the cost is walking the terms rather than scanning rows.

<SqlLogicTest id="cookbook/search/faceted-search/example_004" />

<SqlLogicTest id="cookbook/search/faceted-search/example_005" />

## See also

- [Term Dictionary](../../sql/functions/search/term-dictionary.md): the full `ts_dict_*` reference, `min`/`max`, per-term frequency and the standard-SQL rewrites
- [Autocomplete](./autocomplete.md): prefix suggestions ranked by popularity from the same dictionary
- [Exact Value Matching](./exact-value-matching.md): filter the catalog down before or after faceting
