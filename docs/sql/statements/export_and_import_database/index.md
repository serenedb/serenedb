---
title: EXPORT / IMPORT DATABASE
unlisted: true
split: headings
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

The `schema.sql` file contains the schema statements that are found in the database. It contains the `CREATE SCHEMA`, `CREATE TYPE`, `CREATE SEQUENCE`, `CREATE TABLE`, `CREATE FUNCTION`, `CREATE VIEW`, [`CREATE TEXT SEARCH DICTIONARY`](../create_text_search_dictionary/index.md), [`CREATE INDEX`](../create_index/index.md) and [`CREATE JOB`](../create_job/index.md) commands, including inverted indexes, and the `COMMENT ON` commands for tables, columns, views, sequences, indexes, types and functions that are necessary to re-construct the database. Each sequence is written with its own `START` and, once it has been used, followed by a `SELECT setval(...)`, so it resumes where it was. A sequence owned by a table column, through a `SERIAL` column or [`OWNED BY`](../create_sequence/index.md), is attached to that column again with `ALTER SEQUENCE ... OWNED BY` after the table, so dropping the imported table or column still drops the sequence, and truncating the table with `RESTART IDENTITY` still restarts it. An attached DuckDB database is exported without sequence ownership: its sequences are imported without an owner. A search table's internal row-numbering sequence is not exported: the search table creates its own when it is imported. A column of a user-defined type, and a user-defined type used inside another type, a list, an array, a map or a struct, is written with the type's schema-qualified name, so it binds to the same type on import. The system catalogs (`pg_catalog` and `information_schema`) are not exported. Privileges are not exported either: roles belong to the server rather than to a database, so the imported objects are owned by the importing role, and `GRANT`s have to be issued again after the import. Jobs keep their schedule and stay suspended when they were; like every imported object they belong to the importing role, so they [run as that role](../create_job/index.md#security). Views and macros that read from another database keep its name, so that database must exist under the same name before the import; otherwise the import fails and leaves the database unchanged. Triggers, foreign servers and subscriptions are not exported; create them again after the import.

The database's default schema (`public`) is not written, because the new database the export is imported into already has it; every other schema is created like any other object, so importing into a database that already holds one of them fails. Each text search dictionary is written with its analyzer expression and its feature flags. The expression is stored as the dictionary was compiled: named arguments in place of positional ones, SQL stages as lambdas, and a dictionary used as a stage written out in full. A dictionary copied from another one therefore loads even when the source was dropped. Inverted indexes keep their column list, their per-column options such as `emb hnsw (m = 16, metric = 'cosine')` and their `WITH` options. A view-backed index is written with the interval of its [refresh job](../../indexes/inverted/views.md#automatic-refresh) as `reindex_interval`, or with `reindex_interval = 0` when it has none, so the importing session's setting does not apply. `ALTER JOB` statements after the index restore the job's schedule and, when they differ from what `reindex_interval` creates, its name and suspension. The import creates each refresh job under its index's name before renaming it, so it fails when another job already has that name at that point, for example after two refresh jobs swapped names; rename such a job before exporting. The index content itself is not exported. `schema.sql` creates each index before `load.sql` copies the rows in, so the rows are indexed as they load and become searchable after the next [refresh](../../indexes/inverted/maintenance.md), like any other insert.

The `load.sql` file contains a set of `COPY` statements that can be used to read the data from the CSV files again. The file contains a single `COPY` statement for every table found in the schema. Generated columns are not exported; their values are computed again on import. An empty string is written as a quoted empty field and `NULL` as an empty field, so both load back as they were. A search table exports the rows that queries see, that is, the rows as of its last refresh.

### Data formats

Use `FORMAT parquet`: it exports and imports far faster than the text formats and writes far smaller files. The schema round-trips in every format; the data files differ in which values they carry exactly. An export written in one format and imported with `IMPORT DATABASE` gives back:

| `FORMAT` | Data after import |
| :-- | :-- |
| `parquet` | The same values. The export fails with `Parquet files do not support negative intervals` when a table holds a negative `INTERVAL`, because the Parquet interval type has no sign. |
| `csv` (the default), `text`, `binary` | The same values, for every type. |
| `json` | The same values, except: a `NUMERIC` passes through a double, so digits beyond its precision change (`1234567890.0123456789` loads as `1234567890.0123457536`); a `BYTEA` loads as the bytes of its escaped text (`\x00\xFF`) rather than the bytes themselves; a `JSON` value loads without its insignificant whitespace, and a JSON `null` loads as SQL `NULL`. |

Fall back to `csv`, `text` or `binary` for a database that holds negative intervals.

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram1" />

## `IMPORT DATABASE`

The database can be reloaded by using the `IMPORT DATABASE` command again, or manually by running `schema.sql` followed by `load.sql` to re-load the data.

### Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram2" />
