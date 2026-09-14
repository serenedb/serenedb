---
layout: docu
redirect_from:
- /docs/guides/meta/duckdb_environment
- /docs/preview/guides/meta/duckdb_environment
- /docs/stable/guides/meta/duckdb_environment
- /docs/cookbook/meta/duckdb_environment
title: SereneDB Environment
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB provides a number of functions and `PRAGMA` options to retrieve information on the running SereneDB instance and its environment.

## Version

The `version()` function returns the version number of SereneDB.

<SqlLogicTest id="cookbook/meta/environment/example_001" />

Using a `PRAGMA`:

<SqlLogicTest id="cookbook/meta/environment/example_002" />

## Platform

The platform information consists of the operating system, system architecture, and, optionally, the compiler.
To retrieve the platform, use the following `PRAGMA`:

<SqlLogicTest id="cookbook/meta/environment/example_003" />

The platform string combines the operating system and architecture — for example `osx_arm64` on macOS with Apple Silicon, `windows_amd64` on Windows on AMD64, or `linux_arm64` on Ubuntu Linux on ARM64.

## Meta Table Functions

SereneDB has the following built-in table functions to obtain metadata about available catalog objects:

* [`sdb_columns()`](../../sql/functions/metadata.md#sdb_columns): columns
* [`sdb_constraints()`](../../sql/functions/metadata.md#sdb_constraints): constraints
* [`sdb_databases()`](../../sql/functions/metadata.md#sdb_databases): lists the databases that are accessible from within the current SereneDB process
* [`sdb_dependencies()`](../../sql/functions/metadata.md#sdb_dependencies): dependencies between objects
* [`sdb_extensions()`](../../sql/functions/metadata.md#sdb_extensions): extensions
* [`sdb_functions()`](../../sql/functions/metadata.md#sdb_functions): functions
* [`sdb_indexes()`](../../sql/functions/metadata.md#sdb_indexes): secondary indexes
* [`sdb_keywords()`](../../sql/functions/metadata.md#sdb_keywords): SereneDB's keywords and reserved words
* [`sdb_optimizers()`](../../sql/functions/metadata.md#sdb_optimizers): the available optimization rules in the SereneDB instance
* [`sdb_schemas()`](../../sql/functions/metadata.md#sdb_schemas): schemas
* [`sdb_sequences()`](../../sql/functions/metadata.md#sdb_sequences): sequences
* [`sdb_settings()`](../../sql/functions/metadata.md#sdb_settings): settings
* [`sdb_tables()`](../../sql/functions/metadata.md#sdb_tables): base tables
* [`sdb_temporary_files()`](../../sql/functions/metadata.md#sdb_temporary_files): the temporary files SereneDB has written to disk, to offload data from memory
* [`sdb_types()`](../../sql/functions/metadata.md#sdb_types): data types
* [`sdb_views()`](../../sql/functions/metadata.md#sdb_views): views
