---
layout: docu
redirect_from:
- /docs/guides/performance/indexing
- /docs/preview/guides/performance/indexing
- /docs/stable/guides/performance/indexing
title: Indexing
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

This guide covers the indexes that affect analytical query performance: the automatic zonemaps and [ART indexes](../../sql/indexes/art.md). For full-text, vector and geospatial search, see the [inverted index](../../sql/indexes/inverted/index.md).

## Zonemaps

SereneDB automatically creates [zonemaps](https://en.wikipedia.org/wiki/Block_Range_Index) (also known as min-max indexes) for the columns of all [general-purpose data types](../../sql/data_types/overview.md#general-purpose-data-types).
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

## ART Indexes

SereneDB allows defining [Adaptive Radix Tree (ART) indexes](https://db.in.tum.de/~leis/papers/ART.pdf) in two ways.
First, such an index is created implicitly for columns with `PRIMARY KEY`, `FOREIGN KEY`, and `UNIQUE` [constraints](./schema.md#constraints).
Second, explicitly running the [`CREATE INDEX`](../../sql/indexes/index.md) statement creates an ART index on the target column(s).

The tradeoffs of having an ART index on a column are as follows:

1. ART indexes enable constraint checking during changes (inserts, updates, and deletes).
2. Changes on indexed tables perform worse than their non-indexed counterparts.
That is because of index maintenance for these operations.
3. For some use cases, _single-column ART indexes_ improve the performance of highly selective queries using the indexed column.

An ART index does not affect the performance of join, aggregation, and sorting queries.

### ART Index Scans

ART index scans probe a single-column ART index for the requested data instead of scanning a table sequentially.
Probing can improve the performance of some queries.
SereneDB will try to use an index scan for equality and `IN(...)` conditions.
It also pushes dynamic filters, e.g., from hash joins, into the scan, allowing dynamic index scans on these filters.

Indexes are only eligible for index scans if they index a single column without expressions.
E.g., the following index is eligible for index scans:

<SqlLogicTest id="cookbook/performance/indexing/example_001" />

A multi-column index is created but is **not** eligible for index scans. An index on an expression can also be created:

<SqlLogicTest id="cookbook/performance/indexing/example_002" />

The default threshold for index scans is `MAX(2048, 0.001 * table_cardinality)`.
You can configure this threshold via `index_scan_percentage` and `index_scan_max_count`, or disable them by setting these values to zero.
When in doubt, use [`EXPLAIN ANALYZE`](./profiling.md#the-explain-analyze-statement) to verify that your query plan uses the index scan.

### Indexes and Memory

SereneDB registers index memory through its buffer manager.
However, these index buffers are not yet buffer-managed.
That means SereneDB does not yet destroy any index buffers if it has to evict memory.
Thus, indexes can take up a significant portion of SereneDB's available memory, potentially affecting the performance of memory-intensive queries.
Re-attaching (`DETACH` + `ATTACH`) the database containing indexes can mitigate this effect, as we deserialize index memory lazily.
Disabling index scans and re-attaching after changes can further decrease the impact of indexes on SereneDB's available memory.

### Indexes and Opening Databases

Indexes are serialized to disk and deserialized lazily, i.e., when reopening the database.
Operations using the index will only load the required parts of the index.
Therefore, having an index will not cause any slowdowns when opening an existing database.

<DocCallout type="bestPractice">

We recommend following these guidelines:

- Only use primary keys, foreign keys, or unique constraints, if these are necessary for enforcing constraints on your data.
- Do not define explicit indexes unless you have highly selective queries and enough memory available.
- If you define an ART index, do so after bulk loading the data to the table. Adding an index prior to loading, either explicitly or via primary/foreign keys, is [detrimental to load performance](./schema.md#the-effect-of-primary-keys).

</DocCallout>
