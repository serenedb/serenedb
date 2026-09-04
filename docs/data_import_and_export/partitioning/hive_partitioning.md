---
title: Hive Partitioning
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Examples

Read data from a Hive partitioned dataset:

<SqlLogicTest id="data_import_and_export/partitioning/hive_partitioning/example_001" />

Write a table to a Hive partitioned dataset:

<SqlLogicTest id="data_import_and_export/partitioning/hive_partitioning/example_002" />

Note that the `PARTITION_BY` options cannot use expressions. You can produce columns on the fly using the following syntax:

<SqlLogicTest id="data_import_and_export/partitioning/hive_partitioning/example_003" />

When reading, the partition columns are read from the directory structure and
can be included or excluded depending on the `hive_partitioning` parameter.

<SqlLogicTest id="data_import_and_export/partitioning/hive_partitioning/example_004" />

## Hive Partitioning

Hive partitioning is a [partitioning strategy](https://en.wikipedia.org/wiki/Partition_%28database%29) that is used to split a table into multiple files based on **partition keys**. The files are organized into folders. Within each folder, the **partition key** has a value that is determined by the name of the folder.

Below is an example of a Hive partitioned file hierarchy. The files are partitioned on two keys (`year` and `month`).

```text
orders
├── year=2021
│    ├── month=1
│    │   ├── file1.parquet
│    │   └── file2.parquet
│    └── month=2
│        └── file3.parquet
└── year=2022
     ├── month=11
     │   ├── file4.parquet
     │   └── file5.parquet
     └── month=12
         └── file6.parquet
```

Files stored in this hierarchy can be read using the `hive_partitioning` flag.

<SqlLogicTest id="data_import_and_export/partitioning/hive_partitioning/example_005" />

When we specify the `hive_partitioning` flag, the values of the columns will be read from the directories.

### Filter Pushdown

Filters on the partition keys are automatically pushed down into the files. This way the system skips reading files that are not necessary to answer a query. For example, consider the following query on the above dataset:

<SqlLogicTest id="data_import_and_export/partitioning/hive_partitioning/example_006" />

When executing this query, only the following files will be read:

```text
orders
└── year=2022
     └── month=11
         ├── file4.parquet
         └── file5.parquet
```

### Auto-detection

By default the system tries to infer if the provided files are in a hive partitioned hierarchy. And if so, the `hive_partitioning` flag is enabled automatically. The auto-detection will look at the names of the folders and search for a `'key' = 'value'` pattern.

### Hive Types

`hive_types` is a way to specify the logical types of the hive partitions in a struct:

<SqlLogicTest id="data_import_and_export/partitioning/hive_partitioning/example_008" />

`hive_types` will be auto-detected for the following types: `DATE`, `TIMESTAMP` and `BIGINT`. To switch off the auto-detection, the flag `hive_types_autocast = 0` can be set.

### Writing Partitioned Files

See the [Partitioned Writes](./partitioned_writes.md) section.
