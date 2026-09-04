---
title: EXPORT / IMPORT DATABASE
unlisted: true
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `EXPORT DATABASE` command allows you to export the contents of the database to a specific directory. The `IMPORT DATABASE` command allows you to then read the contents again.

## Examples

Export the database to the target directory 'target_directory' as CSV files:

<SqlLogicTest id="sql/statements/export_and_import_database/index/example_001" />

Export to directory 'target_directory', using the given options for the CSV serialization:

<SqlLogicTest id="sql/statements/export_and_import_database/index/example_002" />

Export to directory 'target_directory', tables serialized as Parquet:

<SqlLogicTest id="sql/statements/export_and_import_database/index/example_003" />

Export to directory 'target_directory', tables serialized as Parquet, compressed with Zstd, with a row_group_size of 100,000:

<SqlLogicTest id="sql/statements/export_and_import_database/index/example_004" />

Reload the database again:

<SqlLogicTest id="sql/statements/export_and_import_database/index/example_005" />

Alternatively, use a `PRAGMA`:

<SqlLogicTest id="sql/statements/export_and_import_database/index/example_006" />

For details regarding the writing of Parquet files, see the [Parquet Files page in the Data Import section](../../../data_import_and_export/parquet/overview.md#writing-to-parquet-files) and the [`COPY` Statement page](../../statements/copy/index.md).

## `EXPORT DATABASE`

The `EXPORT DATABASE` command exports the full contents of the database – including schema information, tables, views and sequences – to a specific directory that can then be loaded again. The created directory will be structured as follows:

```text
target_directory/schema.sql
target_directory/load.sql
target_directory/t_1.csv
...
target_directory/t_n.csv
```

The `schema.sql` file contains the schema statements that are found in the database. It contains any `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VIEW` and `CREATE SEQUENCE` commands that are necessary to re-construct the database.

The `load.sql` file contains a set of `COPY` statements that can be used to read the data from the CSV files again. The file contains a single `COPY` statement for every table found in the schema.

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

## `IMPORT DATABASE`

The database can be reloaded by using the `IMPORT DATABASE` command again, or manually by running `schema.sql` followed by `load.sql` to re-load the data.

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />
