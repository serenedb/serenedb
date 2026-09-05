---
title: Search with Joins and Analytics
sidebar_position: 23
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Search with Joins and Analytics

A `@@` search on an [inverted index](../../sql/indexes/inverted/index.md) is just a predicate, so it composes with the rest of SQL. The rows a search matches are ordinary rows: you can join them to other tables and aggregate them in the same statement. There is no separate analytics store to feed and no export step to run. If you are coming from Elasticsearch this is the part that disappears, see [Migrating from Elasticsearch](../../sql/indexes/inverted/migrating-from-elasticsearch.md).

The queries below join two tables: `products` holds a full-text `title` and a keyword `category` while `orders` records `qty` and `amount` against each `product_id`.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/search-with-joins/setup" />

</details>

## Aggregate what matched

The search narrows the rows, `GROUP BY` rolls them up and `sum()` totals revenue and units, all in one query. The `@@` filter picks the products whose title matches "running", the join pulls their orders and the aggregate reports per product. One statement, one system.

<SqlLogicTest id="cookbook/search/search-with-joins/example_001" />

## Roll up by any dimension

Slice the same matched rows by `category` instead of by product and you get a revenue breakdown across the whole search result. No aggregation DSL and no second index: it is a `GROUP BY` on the column you want, the way a terms aggregation works if you come from Elastic.

<SqlLogicTest id="cookbook/search/search-with-joins/example_002" />

## Keep matches that never sold

An inner join drops matched products that have no orders, which hides part of the answer when you are looking at coverage or gaps. A `LEFT JOIN` keeps every matched product and `coalesce` turns the missing total into a zero, so a product that matched the search but never sold shows up with revenue 0 next to the ones that did.

<SqlLogicTest id="cookbook/search/search-with-joins/example_003" />

## See also

- [Faceted Search](./faceted-search.md): count and bucket the same search result without scanning the matched rows
- [Ranking](./ranking.md): order matched rows by relevance before you aggregate them
- [Migrating from Elasticsearch](../../sql/indexes/inverted/migrating-from-elasticsearch.md): how search and analytics collapse into one query engine
