---
title: Tips
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Below is a collection of tips to help when dealing with Parquet files.

## Tips for Reading Parquet Files

### Use `union_by_name` When Loading Files with Different Schemas

The `union_by_name` option can be used to unify the schema of files that have different or missing columns. For files that do not have certain columns, `NULL` values are filled in:

<SqlLogicTest id="data_import_and_export/parquet/tips/example_001" />

## Tips for Writing Parquet Files

Using a [glob pattern](../multiple_files/overview.md#glob-syntax) upon read or a [Hive partitioning](../partitioning/hive_partitioning.md) structure are good ways to transparently handle multiple files.

### Enabling `PER_THREAD_OUTPUT`

If the final number of Parquet files is not important, writing one file per thread can significantly improve performance:

<SqlLogicTest id="data_import_and_export/parquet/tips/example_002" />

### Selecting a `ROW_GROUP_SIZE`

The `ROW_GROUP_SIZE` parameter specifies the minimum number of rows in a Parquet row group, with a minimum value equal to SereneDB's vector size, 2,048, and a default of 122,880.
A Parquet row group is a partition of rows, consisting of a column chunk for each column in the dataset.

Compression algorithms are only applied per row group, so the larger the row group size, the more opportunities to compress the data.
On the other hand, larger row group sizes mean that each thread keeps more data in memory before flushing when streaming results.
Another argument for smaller row group sizes is that SereneDB can read Parquet row groups in parallel even within the same file and uses predicate pushdown to only scan the row groups whose metadata ranges match the `WHERE` clause of the query. However, there is some overhead associated with reading the metadata in each group.

A good rule of thumb is to ensure that the number of row groups per file is at least as large as the number of CPU threads used to query that file.
More row groups beyond the thread count would improve the speed of highly selective queries, but slow down queries that must scan the whole file like aggregations.

To write a query to a Parquet file with a different row group size, run:

<SqlLogicTest id="data_import_and_export/parquet/tips/example_003" />

### The `ROW_GROUPS_PER_FILE` Option

The `ROW_GROUPS_PER_FILE` parameter creates a new Parquet file if the current one has a specified number of row groups.

<SqlLogicTest id="data_import_and_export/parquet/tips/example_004" />

<DocCallout type="tip">
If multiple threads are active, the number of row groups in a file may slightly exceed the specified number of row groups to limit the amount of locking – similarly to the behavior of [`FILE_SIZE_BYTES`](../../sql/statements/copy#copy--to-options).
However, if `PER_THREAD_OUTPUT` is set, only one thread writes to each file, and it becomes accurate again.
</DocCallout>

See the [Performance Guide on “File Formats”](../../cookbook/performance/file_formats.md#parquet-file-sizes) for more tips.
