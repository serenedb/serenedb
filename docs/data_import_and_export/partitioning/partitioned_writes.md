---
title: Partitioned Writes
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

## Examples

Write a table to a Hive partitioned dataset of Parquet files:

<SqlLogicTest id="data_import_and_export/partitioning/partitioned_writes/example_001" />

Write a table to a Hive partitioned dataset of CSV files, allowing overwrites:

<SqlLogicTest id="data_import_and_export/partitioning/partitioned_writes/example_002" />

Write a table to a Hive partitioned dataset of GZIP-compressed CSV files, setting explicit data files' extension:

<SqlLogicTest id="data_import_and_export/partitioning/partitioned_writes/example_003" />

## Partitioned Writes

When the `PARTITION_BY` clause is specified for the [`COPY` statement](../../sql/statements/copy/index.md), the files are written in a [Hive partitioned](./hive_partitioning.md) folder hierarchy. The target is the name of the root directory (in the example above: `orders`). The files are written in-order in the file hierarchy. Currently, one file is written per thread to each directory.

```text
orders
├── year=2021
│    ├── month=1
│    │   ├── data_1.parquet
│    │   └── data_2.parquet
│    └── month=2
│        └── data_1.parquet
└── year=2022
     ├── month=11
     │   ├── data_1.parquet
     │   └── data_2.parquet
     └── month=12
         └── data_1.parquet
```

The values of the partitions are automatically extracted from the data. Note that it can be very expensive to write a larger number of partitions as many files will be created. The ideal partition count depends on how large your dataset is.

To limit the maximum number of files the system can keep open before flushing to disk when writing using `PARTITION_BY`, use the `partitioned_write_max_open_files` configuration option (default: 100):

```batch
SET partitioned_write_max_open_files = 10;
```

<DocCallout type="bestPractice">
    Writing data into many small partitions is expensive. It is generally recommended to have at least `100 MB` of data per partition.
</DocCallout>

### Filename Pattern

By default, files will be named `data_0.parquet` or `data_0.csv`. With the flag `FILENAME_PATTERN` a pattern with `{i}` or `{uuid}` can be defined to create specific filenames:

-   `{i}` will be replaced by an index.
-   `{uuid}` will be replaced by a 128 bits long UUID.

Write a table to a Hive partitioned dataset of .parquet files, with an index in the filename:

<SqlLogicTest id="data_import_and_export/partitioning/partitioned_writes/example_004" />

Write a table to a Hive partitioned dataset of .parquet files, with unique filenames:

<SqlLogicTest id="data_import_and_export/partitioning/partitioned_writes/example_005" />

### Overwriting

By default the partitioned write will not allow overwriting existing directories.
On a local file system, the `OVERWRITE` and `OVERWRITE_OR_IGNORE` options remove the existing directories.
On remote file systems, overwriting is not supported.

### Appending

To append to an existing Hive partitioned directory structure, use the `APPEND` option:

<SqlLogicTest id="data_import_and_export/partitioning/partitioned_writes/example_006" />

Using the `APPEND` option results in a behavior similar to the `OVERWRITE_OR_IGNORE, FILENAME_PATTERN '{uuid}'` options,
but SereneDB performs an extra check for whether the file already exists and then regenerates the UUID in the rare event that it does (to avoid clashes).

### Handling Slashes in Columns

To handle slashes in column names, use Percent-Encoding implemented by the [`url_encode` function](../../sql/functions/text.md#url_encodestring).
