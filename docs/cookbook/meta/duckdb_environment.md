---
layout: docu
redirect_from:
- /docs/guides/meta/duckdb_environment
- /docs/preview/guides/meta/duckdb_environment
- /docs/stable/guides/meta/duckdb_environment
title: SereneDB Environment
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB provides a number of functions and `PRAGMA` options to retrieve information on the running SereneDB instance and its environment.

## Version

The `version()` function returns the version number of SereneDB.

<SqlLogicTest id="cookbook/meta/duckdb_environment/example_001" />

Using a `PRAGMA`:

<SqlLogicTest id="cookbook/meta/duckdb_environment/example_002" />

## Platform

The platform information consists of the operating system, system architecture, and, optionally, the compiler.
To retrieve the platform, use the following `PRAGMA`:

<SqlLogicTest id="cookbook/meta/duckdb_environment/example_003" />

The platform string combines the operating system and architecture — for example `osx_arm64` on macOS with Apple Silicon, `windows_amd64` on Windows on AMD64, or `linux_arm64` on Ubuntu Linux on ARM64.

## Meta Table Functions

SereneDB has the following built-in table functions to obtain metadata about available catalog objects:

* [`duckdb_columns()`](../../sql/functions/duckdb_table_functions.md#duckdb_columns): columns
* [`duckdb_constraints()`](../../sql/functions/duckdb_table_functions.md#duckdb_constraints): constraints
* [`duckdb_databases()`](../../sql/functions/duckdb_table_functions.md#duckdb_databases): lists the databases that are accessible from within the current SereneDB process
* [`duckdb_dependencies()`](../../sql/functions/duckdb_table_functions.md#duckdb_dependencies): dependencies between objects
* [`duckdb_extensions()`](../../sql/functions/duckdb_table_functions.md#duckdb_extensions): extensions
* [`duckdb_functions()`](../../sql/functions/duckdb_table_functions.md#duckdb_functions): functions
* [`duckdb_indexes()`](../../sql/functions/duckdb_table_functions.md#duckdb_indexes): secondary indexes
* [`duckdb_keywords()`](../../sql/functions/duckdb_table_functions.md#duckdb_keywords): SereneDB's keywords and reserved words
* [`duckdb_optimizers()`](../../sql/functions/duckdb_table_functions.md#duckdb_optimizers): the available optimization rules in the SereneDB instance
* [`duckdb_schemas()`](../../sql/functions/duckdb_table_functions.md#duckdb_schemas): schemas
* [`duckdb_sequences()`](../../sql/functions/duckdb_table_functions.md#duckdb_sequences): sequences
* [`duckdb_settings()`](../../sql/functions/duckdb_table_functions.md#duckdb_settings): settings
* [`duckdb_tables()`](../../sql/functions/duckdb_table_functions.md#duckdb_tables): base tables
* [`duckdb_temporary_files()`](../../sql/functions/duckdb_table_functions.md#duckdb_temporary_files): the temporary files SereneDB has written to disk, to offload data from memory
* [`duckdb_types()`](../../sql/functions/duckdb_table_functions.md#duckdb_types): data types
* [`duckdb_views()`](../../sql/functions/duckdb_table_functions.md#duckdb_views): views
