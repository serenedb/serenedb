---
layout: docu
redirect_from:
- /docs/guides/performance/file-formats
- /docs/guides/performance/file_formats
- /docs/preview/guides/performance/file_formats
- /docs/stable/guides/performance/file_formats
title: File Formats
---

import DocCallout from "@site/src/components/DocCallout";
import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Handling Parquet Files

SereneDB has advanced support for Parquet files, which includes directly querying Parquet files.
When deciding on whether to query these files directly or to first load them to the database, you need to consider several factors.

### Reasons for Querying Parquet Files

**Availability of basic statistics:** Parquet files use a columnar storage format and contain basic statistics such as [zonemaps](./indexing.md#zonemaps). Thanks to these features, SereneDB can leverage optimizations such as projection and filter pushdown on Parquet files. Therefore, workloads that combine projection, filtering, and aggregation tend to perform quite well when run on Parquet files.

**Storage considerations:** Loading the data from Parquet files will require approximately the same amount of space for the SereneDB database file. Therefore, if the available disk space is constrained, it is worth running the queries directly on Parquet files.

### Reasons against Querying Parquet Files

**Lack of advanced statistics:** The SereneDB database format has the [hyperloglog statistics](https://en.wikipedia.org/wiki/HyperLogLog) that Parquet files do not have. These improve the accuracy of cardinality estimates, and are especially important if the queries contain a large number of join operators.

**Tip.** If you find that SereneDB produces a suboptimal join order on Parquet files, try loading the Parquet files to SereneDB tables. The improved statistics likely help obtain a better join order.

**Repeated queries:** If you plan to run multiple queries on the same dataset, it is worth loading the data into SereneDB. The queries will always be somewhat faster, which over time amortizes the initial load time.

**High decompression times:** Some Parquet files are compressed using heavyweight compression algorithms such as gzip. In these cases, querying the Parquet files will necessitate an expensive decompression time every time the file is accessed. Meanwhile, lightweight compression methods like Snappy, LZ4, and zstd, are faster to decompress. You may use the [`parquet_metadata` function](../../data_import_and_export/parquet/metadata.md#parquet-metadata) to find out the compression algorithm used.

#### Microbenchmark: Running TPC-H on a SereneDB Database vs. Parquet

The queries on the TPC-H benchmark run approximately 1.1-5.0× slower on Parquet files than on a SereneDB database.

<DocCallout type="bestPractice">

If you have the storage space available, and have a join-heavy workload and/or plan to run many queries on the same dataset, load the Parquet files into the database first. The compression algorithm and the row group sizes in the Parquet files have a large effect on performance: study these using the [`parquet_metadata` function](../../data_import_and_export/parquet/metadata.md#parquet-metadata).

</DocCallout>

### The Effect of Row Group Sizes

SereneDB works best on Parquet files with row groups of 100K-1M rows each. The reason for this is that SereneDB can only [parallelize over row groups](./how_to_tune_workloads.md#parallelism-multi-core-processing) – so if a Parquet file has a single giant row group it can only be processed by a single thread. You can use the [`parquet_metadata` function](../../data_import_and_export/parquet/metadata.md#parquet-metadata) to figure out how many row groups a Parquet file has. When writing Parquet files, use the [`row_group_size`](../../sql/statements/copy/index.md#parquet-options) option.

Very small row groups (below a few thousand rows) carry a large amount of per-row-group overhead and can make queries several times slower, while above roughly 100K rows the effect of further increasing the row group size is small.

### Parquet File Sizes

SereneDB can also parallelize across multiple Parquet files. It is advisable to have at least as many total row groups across all files as there are CPU threads. For example, with a machine having 10 threads, both 10 files with 1 row group or 1 file with 10 row groups will achieve full parallelism. It is also beneficial to keep the size of individual Parquet files moderate.

<DocCallout type="bestPractice">

The ideal range is between 100 MB and 10 GB per individual Parquet file.

</DocCallout>

### Hive Partitioning for Filter Pushdown

When querying many files with filter conditions, performance can be improved by using a [Hive-format folder structure](../../data_import_and_export/partitioning/hive_partitioning.md) to partition the data along the columns used in the filter condition. SereneDB will only need to read the folders and files that meet the filter criteria. This can be especially helpful when querying remote files.

### More Tips on Reading and Writing Parquet Files

For tips on reading and writing Parquet files, see the [Parquet Tips page](../../data_import_and_export/parquet/tips.md).

## Loading CSV Files

CSV files are often distributed in compressed format such as GZIP archives (`.csv.gz`). SereneDB can decompress these files on the fly. In fact, this is typically faster than decompressing the files first and loading them due to reduced IO.

| Schema | Load time |
|---|--:|
| Load from GZIP-compressed CSV files (`.csv.gz`) | 107.1 s |
| Decompressing (using parallel `gunzip`) and loading from decompressed CSV files | 121.3 s |

### Loading Many Small CSV Files

The [CSV reader](../../data_import_and_export/csv/overview.md) runs the CSV sniffer on all files. For many small files, this may cause an unnecessarily high overhead.
A potential optimization to speed this up is to turn the sniffer off. Assuming that all files have the same CSV dialect and column names/types, get the sniffer options as follows:

<SqlLogicTest id="cookbook/performance/file_formats/example_001" />

Then, you can adjust the `read_csv` command, by e.g., applying [filename expansion (globbing)](../../sql/functions/pattern_matching/index.md#globbing), and run with the rest of the options detected by the sniffer:

<SqlLogicTest id="cookbook/performance/file_formats/example_002" />
