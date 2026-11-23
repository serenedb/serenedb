---
layout: docu
redirect_from:
- /docs/guides/performance/indexing
- /docs/preview/guides/performance/indexing
- /docs/stable/guides/performance/indexing
title: Indexing
split: page
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

This guide covers the indexes that affect analytical query performance: the automatic zonemaps and the indexes created with `CREATE INDEX`. For full-text, vector and geospatial search, see the [inverted index](../../sql/indexes/inverted/index.md).

## Zonemaps

SereneDB automatically creates [zonemaps](https://en.wikipedia.org/wiki/Block_Range_Index) (also known as min-max indexes) for the columns of all [general-purpose data types](../../sql/data_types/index.md#general-purpose-data-types).
Operations like predicate pushdown into scan operators and computing aggregations use zonemaps.
If a filter criterion (like `WHERE column1 = 123`) is in use, SereneDB can skip any row group whose min-max range does not contain that filter value (e.g., it can omit a block with a min-max range of 1000 to 2000 when comparing for `= 123` or `< 400`).

### The Effect of Ordering on Zonemaps

The more ordered the data within a column, the more valuable the zonemap indexes will be.
For example, a column could contain a random number on every row in the worst case.
Then, SereneDB will likely be unable to skip any row groups.
If you query specific columns with selective filters, it is best to pre-order data by those columns when inserting it.
Even an imperfect ordering will still be helpful.
The best case of ordered data commonly arises with `DATETIME` columns.

### Microbenchmark: The Effect of Ordering

For an example, let’s repeat the [timestamp comparison](./schema.md#using-temporal-types) with an ordered timestamp column using an ascending order vs. an unordered one.

| Column type | Ordered | Storage size | Query time |
|---|---|--:|--:|
| `DATETIME` | yes | 1.3 GB | 0.6 s |
| `DATETIME` | no  | 3.3 GB | 0.9 s |

The results show that simply keeping the column order allows for improved compression, yielding a 2.5× smaller storage size.
It also allows the computation to be 1.5× faster.

### Ordered Integers

Another practical way to exploit ordering is to use the `INTEGER` type with automatic increments rather than `UUID` for columns queried using selective filters.
In a scenario where a table contains out-of-order `UUID`s, SereneDB has to scan many row groups to find a specific `UUID` value.
An ordered `INTEGER` column allows skipping all row groups except those containing the value.

## Indexes

`PRIMARY KEY`, `FOREIGN KEY` and `UNIQUE` [constraints](./schema.md#constraints) are enforced by the table itself and need no index.

The [`CREATE INDEX`](../../sql/indexes/index.md) statement creates an inverted index on the target column(s):

<SqlLogicTest id="cookbook/performance/indexing/example_001" />

An index can cover several columns, and an expression:

<SqlLogicTest id="cookbook/performance/indexing/example_002" />

The tradeoffs of having an index are as follows:

1. Writes to an indexed table cost more, since every write also updates the index.
2. Selective conditions on the indexed columns, and full-text, vector and geospatial searches, read only the matching rows.

Use [`EXPLAIN ANALYZE`](./profiling.md#the-explain-analyze-statement) to see how a query reads the table.

<DocCallout type="bestPractice">

We recommend following these guidelines:

- Only use primary keys, foreign keys, or unique constraints, if these are necessary for enforcing constraints on your data.
- Index the columns that selective queries filter on, and leave the others unindexed.

</DocCallout>
