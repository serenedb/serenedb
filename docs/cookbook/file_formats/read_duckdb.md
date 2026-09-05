---
layout: docu
redirect_from:
    - /docs/stable/guides/file_formats/read_duckdb
title: Directly Read DuckDB Databases
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB allows directly reading DuckDB files through the `read_duckdb` function:

<SqlLogicTest id="cookbook/file_formats/read_duckdb/example_001" />

Using this function is equivalent to performing the following steps:

-   Attaching to the database using a read-only connection.
-   Querying the table specified through the `table_name` argument.
-   Closing the connection to the database.

## Examples

### Reading a Specific Table

To read the `region` table from the TPC-H dataset, run:

<SqlLogicTest id="cookbook/file_formats/read_duckdb/example_002" />

### Reading from Multiple Databases

You can use globbing to read from multiple databases.
To illustrate this, let's create two tables:

```bash
serened shell my-1.duckdb \
    -c "CREATE TABLE numbers AS SELECT 42 AS x;" \
    -c "CREATE TABLE letters AS SELECT 'm' AS a;"

serened shell my-2.duckdb \
    -c "CREATE TABLE numbers AS SELECT 43 AS x;"
```

Then, in SereneDB, you can run:

<SqlLogicTest id="cookbook/file_formats/read_duckdb/example_003" />

### Reading from Databases with a Single Table

If all databases in `read_duckdb`'s argument have a single table, the `table_name` argument is optional:

<SqlLogicTest id="cookbook/file_formats/read_duckdb/example_004" />

If the extension is `.db` or `.duckdb`, you can also omit the `read_duckdb` call (similarly to how you can omit `read_csv` and `read_parquet`):

<SqlLogicTest id="cookbook/file_formats/read_duckdb/example_005" />

## Limitations

`read_duckdb` currently only supports reading from tables.
Reading from views is not yet supported.
