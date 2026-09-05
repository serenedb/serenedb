---
title: Computed Values
sidebar_position: 30
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Computed Values

You do not have to store a value to search it. Index an [expression over a column](../../sql/indexes/inverted/modeling.md) (a functional index) or a generated column the table computes for you, then query that computed value from the [inverted index](../../sql/indexes/inverted/index.md) like any other field. Reach for this when you want a case-folded key for exact lookups, a derived amount you filter by or a normalized form of messy input.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/computed-values/setup" />

</details>

## Index an expression

Wrap the expression in parentheses in the index definition and SereneDB indexes its result. Here `(lower(name))` folds case at index time, so one lowercase query matches every casing of the stored value. Query the same expression you indexed.

<SqlLogicTest id="cookbook/search/computed-values/example_001" />

## Index a generated column

A column declared `GENERATED ALWAYS AS (...) STORED` is computed by the table and kept in sync on every write. Index it by name like any stored column, then range over it. Here `price_with_tax` derives from `price` and you bound it with `ts_le` and `ts_ge`.

<SqlLogicTest id="cookbook/search/computed-values/example_002" />

<SqlLogicTest id="cookbook/search/computed-values/example_003" />

## Mix computed and stored

Put both in one `WHERE` and they filter together: the case-folded key narrows to a product and the generated amount bounds the price. The expression in the query must match the indexed expression exactly, so query `lower(name)`, not `name`.

<SqlLogicTest id="cookbook/search/computed-values/example_004" />

## Concatenate columns into a key

An expression can span several columns, so you can search a value the table never stores. Here `(first || ' ' || last)` builds a full-name key and one exact lookup finds the person, no `full_name` column required.

<SqlLogicTest id="cookbook/search/computed-values/example_005" />

## Bucket with CASE

A `CASE` expression labels a raw value at index time, so you can filter or facet on the bucket without a lookup table. Here orders index as `big` or `small` by amount and you query the label.

<SqlLogicTest id="cookbook/search/computed-values/example_006" />

## See also

- [What to Index](../../sql/indexes/inverted/modeling.md): choosing what to index and where expressions and generated columns fit
- [Range Queries](./range-queries.md): filtering indexed values by order with `ts_le`, `ts_ge` and friends
- [JSON Search](./json-search.md): indexing and querying values pulled out of JSON documents
