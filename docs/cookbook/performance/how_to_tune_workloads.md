---
layout: docu
redirect_from:
- /docs/guides/performance/how-to-tune-workloads
- /docs/guides/performance/how_to_tune_workloads
- /docs/preview/guides/performance/how_to_tune_workloads
- /docs/stable/guides/performance/how_to_tune_workloads
title: Tuning Workloads
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## The `preserve_insertion_order` Option

When importing or exporting datasets (from/to the Parquet or CSV formats), which are much larger than the available memory, an out of memory error may occur:

```console
Out of Memory Error: failed to allocate data of size ... (.../... used)
```

In these cases, consider setting the [`preserve_insertion_order` configuration option](../../configuration/overview.md) to `false`:

<SqlLogicTest id="cookbook/performance/how_to_tune_workloads/example_001" />

This allows the system to re-order any results that do not contain `ORDER BY` clauses, potentially reducing memory usage.

## Parallelism (Multi-Core Processing)

### The Effect of Row Groups on Parallelism

SereneDB parallelizes the workload based on _row groups,_ i.e., groups of rows that are stored together at the storage level.
The default row group size in SereneDB's database format is 122,880 rows.
Parallelism starts at the level of row groups, therefore, for a query to run on _k_ threads, it needs to scan at least _k_ \* 122,880 rows.

The row group size can be specified as an option of the `ATTACH` statement: 

<SqlLogicTest id="cookbook/performance/how_to_tune_workloads/example_002" />

The [performance considerations when choosing `ROW_GROUP_SIZE` for Parquet files](../../data_import_and_export/parquet/tips.md#selecting-a-row_group_size) apply verbatim to SereneDB's own database format.

### Too Many Threads

Note that in certain cases SereneDB may launch _too many threads_ (e.g., due to HyperThreading), which can lead to slowdowns. In these cases, it’s worth manually limiting the number of threads using [`SET threads = X`](../../configuration/pragmas.md#threads).

## Larger-than-Memory Workloads (Out-of-Core Processing)

A key strength of SereneDB is support for larger-than-memory workloads, i.e., it is able to process datasets that are larger than the available system memory (also known as _out-of-core processing_).
It can also run queries where the intermediate results cannot fit into memory.
This section explains the prerequisites, scope, and known limitations of larger-than-memory processing in SereneDB.

### Spilling to Disk

Larger-than-memory workloads are supported by spilling to disk.
With the default configuration, SereneDB creates the `⟨database_file_name⟩.tmp`{:.language-sql .highlight} temporary directory (in persistent mode) or the `.tmp`{:.language-sql .highlight} directory (in in-memory mode). This directory can be changed using the [`temp_directory` configuration option](../../configuration/pragmas.md#temp-directory-for-spilling-data-to-disk), e.g.:

<SqlLogicTest id="cookbook/performance/how_to_tune_workloads/example_003" />

### Blocking Operators

Some operators cannot output a single row until the last row of their input has been seen.
These are called _blocking operators_ as they require their entire input to be buffered,
and are the most memory-intensive operators in relational database systems.
The main blocking operators are the following:

- _grouping:_ [`GROUP BY`](../../sql/query_syntax/groupby/index.md)
- _joining:_ [`JOIN`](../../sql/query_syntax/from_and_join/index.md#joins)
- _sorting:_ [`ORDER BY`](../../sql/query_syntax/orderby/index.md)
- _windowing:_ [`OVER ... (PARTITION BY ... ORDER BY ...)`](../../sql/functions/window_functions/index.md)

SereneDB supports larger-than-memory processing for all of these operators.

### Limitations

SereneDB strives to always complete workloads even if they are larger-than-memory.
That said, there are some limitations at the moment:

- If multiple blocking operators appear in the same query, SereneDB may still throw an out-of-memory exception due to the complex interplay of these operators.
- Some [aggregate functions](../../sql/functions/aggregates/index.md), such as `list()` and `string_agg()`, do not support offloading to disk.
- [Aggregate functions that use sorting](../../sql/functions/aggregates/index.md#order-by-clause-in-aggregate-functions) are holistic, i.e., they need all inputs before the aggregation can start. As SereneDB cannot yet offload some complex intermediate aggregate states to disk, these functions can cause an out-of-memory exception when run on large datasets.
- The [`PIVOT` operation](../../sql/statements/pivot/index.md) internally uses the `list()` function, therefore it is subject to the same limitation.

## Profiling

If your queries are not performing as well as expected, it’s worth studying their query plans:

- Use [`EXPLAIN`](../meta/explain.md) to print the physical query plan without running the query.
- Use [`EXPLAIN ANALYZE`](./profiling.md#the-explain-analyze-statement) to run and profile the query. This will show the CPU time that each step in the query takes. Note that due to multi-threading, adding up the individual times will be larger than the total query processing time.

Query plans can point to the root of performance issues. A few general directions:

- Avoid nested loop joins in favor of hash joins.
- A scan that does not include a filter pushdown for a filter condition that is later applied performs unnecessary IO. Try rewriting the query to apply a pushdown.
- Bad join orders where the cardinality of an operator explodes to billions of tuples should be avoided at all costs.

## Prepared Statements

[Prepared statements](../../sql/query_syntax/prepared_statements.md) can improve performance when running the same query many times, but with different parameters. When a statement is prepared, it completes several of the initial portions of the query execution process (parsing, planning, etc.) and caches their output. When it is executed, those steps can be skipped, improving performance. This is beneficial mostly for repeatedly running small queries (with a runtime of < 100ms) with different sets of parameters.

Note that it is not a primary design goal for SereneDB to quickly execute many small queries concurrently. Rather, it is optimized for running larger, less frequent queries.

## Querying Remote Files

SereneDB uses synchronous IO when reading remote files. This means that each SereneDB thread can make at most one HTTP request at a time. If a query must make many small requests over the network, increasing SereneDB's [`threads` setting](../../configuration/pragmas.md#threads) to larger than the total number of CPU cores (approx. 2-5 times CPU cores) can improve parallelism and performance.

### Avoid Reading Unnecessary Data

The main bottleneck in workloads reading remote files is likely to be the IO. This means that minimizing the unnecessarily read data can be highly beneficial.

Some basic SQL tricks can help with this:

- Avoid `SELECT *`. Instead, only select columns that are actually used. SereneDB will try to only download the data it actually needs.
- Apply filters on remote Parquet files when possible. SereneDB can use these filters to reduce the amount of data that is scanned.
- Either [sort](../../sql/query_syntax/orderby/index.md) or [partition](../../data_import_and_export/partitioning/partitioned_writes.md) data by columns that are regularly used for filters: this increases the effectiveness of the filters in reducing IO.

To inspect how much remote data is transferred for a query, [`EXPLAIN ANALYZE`](./profiling.md#the-explain-analyze-statement) can be used to print out the total number of requests and total data transferred for queries on remote files.

### Caching

Starting with version 1.3.0, SereneDB supports caching remote data. To inspect the content of the external file cache, run:

<SqlLogicTest id="cookbook/performance/how_to_tune_workloads/example_004" />

## Best Practices for Using Connections

SereneDB will perform best when reusing the same database connection many times. Disconnecting and reconnecting on every query will incur some overhead, which can reduce performance when running many small queries. SereneDB also caches some data and metadata in memory, and that cache is lost when the last open connection is closed. Frequently, a single connection will work best, but a connection pool may also be used.

Using multiple connections can parallelize some operations, although it is typically not necessary. SereneDB does attempt to parallelize as much as possible within each individual query, but it is not possible to parallelize in all cases. Making multiple connections can process more operations concurrently. This can be more helpful if SereneDB is not CPU limited, but instead bottlenecked by another resource like network transfer speed.

## Persistent vs. In-Memory Tables

SereneDB supports lightweight compression techniques. By default, compression is only applied on persistent (on-disk) databases and not on in-memory tables.

In some cases, this can result in counter-intuitive performance results where queries are faster on on-disk tables compared to in-memory ones. Let's take Q1 of the TPC-H workload for example on the SF30 dataset:

<SqlLogicTest id="cookbook/performance/how_to_tune_workloads/example_005" hideResult />

We run this script using three SereneDB prompts:

| Database setup              | SereneDB prompt                                                    | Execution time |
| --------------------------- | ------------------------------------------------------------------ | -------------: |
| In-memory DB (uncompressed) | `serened shell`                                                    |         4.22 s |
| In-memory DB (compressed)   | `serened shell -cmd "ATTACH ':memory:' AS db (COMPRESS); USE db;"` |         0.55 s |
| Persistent DB (compressed)  | `serened shell tpch-sf30.db`                                       |         0.56 s |

We can observe that the compressed databases are about 8× faster compared to the uncompressed in-memory database.
