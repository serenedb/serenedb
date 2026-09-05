---
title: Searching JSON
sidebar_position: 29
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Searching JSON

Semi-structured documents are first-class in SereneDB. You index each JSON field you want to search by listing its extraction expression in the [inverted index](../../sql/indexes/inverted/index.md) and casting it to a concrete type. The cast decides the behavior: a text cast is analyzed for full-text, a number cast gets range matching, a plain string is an exact keyword and an array casts to match any element. Your query repeats the same extraction expression to hit that indexed path. See [JSON indexing](../../sql/indexes/inverted/json.md) for the full extraction reference.

Every row below is one product stored whole in a `doc` VARIANT column. The index reaches into that JSON and lifts out the name, brand, tags and price as four paths you can search on their own.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/json-search/setup" />

</details>

## Full-text search a nested field

The product name lives under `doc['name']`. Casting that path to `VARCHAR` and attaching a text dictionary in the index analyzes it for full-text, so `@@ 'laptop'` matches any product whose name contains that word. Notice the query repeats the exact extraction expression you declared in the index.

<SqlLogicTest id="cookbook/search/json-search/example_001" />

## Exact match a nested field

A nested path cast to a plain `VARCHAR` with no dictionary is an exact keyword. `doc['attrs']['brand']` matches the stored value verbatim, which means the match is case sensitive: search `Globex`, not `globex`. Attach a text dictionary to that path if you want case folding.

<SqlLogicTest id="cookbook/search/json-search/example_002" />

## Match array elements

Cast an array path to `VARCHAR[]` and every element becomes its own indexed term. `@@ 'sale'` matches a row when any element of `doc['tags']` equals that term.

<SqlLogicTest id="cookbook/search/json-search/example_003" />

Wrap the terms in `ts_all` to require every one of them. Here a row matches only when its tags contain both `sale` and `new`.

<SqlLogicTest id="cookbook/search/json-search/example_004" />

## Range over a nested number

Cast a nested value to `INTEGER` and the path supports range matching. `ts_ge(40)` keeps every product priced at 40 or above and the other comparators work the same way.

<SqlLogicTest id="cookbook/search/json-search/example_005" />

## Combine fields in one query

Each indexed path is independent, so you `AND` them together in one `WHERE` clause. This finds laptops priced 50 or above by combining the full-text match on the name with the range on the price, both served from the same index.

<SqlLogicTest id="cookbook/search/json-search/example_006" />

## See also

- [JSON indexing](../../sql/indexes/inverted/json.md): the full reference for extraction expressions and casts
- [Faceted Search](./faceted-search.md): count and drill down over the values you extracted
- [Exact Value Matching](./exact-value-matching.md): keyword matching in more depth
