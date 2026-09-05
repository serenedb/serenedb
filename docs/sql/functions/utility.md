---
title: Utility Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

## Scalar Utility Functions

The functions below are difficult to categorize into specific function types and are broadly useful. The table-function examples share the tables created in [Setup](#setup).

| Name                                                                                                 | Description                                                                                                                                                                                                                                                                                                                                                                         |
| :--------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`alias(column)`](#aliascolumn)                                                                      | Return the name of the column.                                                                                                                                                                                                                                                                                                                                                      |
| [`can_cast_implicitly(source_value, target_value)`](#can_cast_implicitlysource_value-target_value)   | Whether or not we can implicitly cast from the types of the source value to the target value.                                                                                                                                                                                                                                                                                       |
| [`checkpoint(database)`](#checkpointdatabase)                                                        | Synchronize WAL with file for (optional) database without interrupting transactions.                                                                                                                                                                                                                                                                                                |
| [`coalesce(expr, ...)`](#coalesceexpr-)                                                              | Return the first expression that evaluates to a non-`NULL` value. Accepts 1 or more parameters. Each expression can be a column, literal value, function result, or many others.                                                                                                                                                                                                    |
| [`constant_or_null(arg1, arg2)`](#constant_or_nullarg1-arg2)                                         | If `arg2` is `NULL`, return `NULL`. Otherwise, return `arg1`.                                                                                                                                                                                                                                                                                                                       |
| [`count_if(x)`](#count_ifx)                                                                          | Aggregate function; counts the rows for which the `BOOLEAN` argument `x` is `true`.                                                                                                                                                                                                                                                                                                  |
| [`create_sort_key(parameters...)`](#create_sort_keyparameters)                                       | Constructs a binary-comparable sort key based on a set of input parameters and sort qualifiers.                                                                                                                                                                                                                                                                                     |
| [`current_catalog()`](#current_catalog)                                                              | Return the name of the currently active catalog. Default is postgres.                                                                                                                                                                                                                                                                                                               |
| [`current_database()`](#current_database)                                                            | Return the name of the currently active database.                                                                                                                                                                                                                                                                                                                                   |
| [`current_query()`](#current_query)                                                                  | Return the current query as a string.                                                                                                                                                                                                                                                                                                                                               |
| [`current_schema()`](#current_schema)                                                                | Return the name of the currently active schema. Default is public.                                                                                                                                                                                                                                                                                                                  |
| [`current_schemas(boolean)`](#current_schemasboolean)                                                | Return list of schemas. Pass a parameter of `true` to include implicit schemas.                                                                                                                                                                                                                                                                                                     |
| [`current_setting('setting_name')`](#current_settingsetting_name)                                    | Return the current value of the configuration setting.                                                                                                                                                                                                                                                                                                                              |
| [`currval('sequence_name')`](#currvalsequence_name)                                                  | Return the current value of the sequence. Note that `nextval` must be called at least once prior to calling `currval`.                                                                                                                                                                                                                                                              |
| [`error(message)`](#errormessage)                                                                    | Throws the given error `message`.                                                                                                                                                                                                                                                                                                                                                   |
| [`equi_width_bins(min, max, bincount, nice := false)`](#equi_width_binsmin-max-bincount-nice--false) | Returns the upper boundaries of a partition of the interval `[min, max]` into `bin_count` equal-sized subintervals (for use with, e.g., [`histogram`](../../sql/functions/aggregates/index.md#histogramarg-boundaries)). If `nice = true`, then `min`, `max` and `bincount` may be adjusted to produce more aesthetically pleasing results.                                          |
| [`force_checkpoint(database)`](#force_checkpointdatabase)                                            | Synchronize WAL with file for (optional) database interrupting transactions.                                                                                                                                                                                                                                                                                                        |
| [`gen_random_uuid()`](#gen_random_uuid)                                                              | Return a random UUID similar to this: `eeccb8c5-9943-b2bb-bb5e-222f4e14b687`.                                                                                                                                                                                                                                                                                                       |
| [`getenv(var)`](#getenvvar)                                                                          | Returns the value of the environment variable `var`. Only available in the command line client (`serened shell`).                                                                                                                                                                                                                                  |
| [`hash(value)`](#hashvalue)                                                                          | Returns a `UBIGINT` with a hash of `value`. The used hash function may change across SereneDB versions.                                                                                                                                                                                                                                                                             |
| [`icu_sort_key(string, collator)`](#icu_sort_keystring-collator)                                     | Surrogate [sort key](https://unicode-org.github.io/icu/userguide/collation/architecture.html#sort-keys) used to sort special characters according to the specific locale. Collator parameter is optional.                                                                                                                                                                             |
| [`if(a, b, c)`](#ifa-b-c)                                                                            | Ternary conditional operator.                                                                                                                                                                                                                                                                                                                                                       |
| [`ifnull(expr, other)`](#ifnullexpr-other)                                                           | A two-argument version of coalesce.                                                                                                                                                                                                                                                                                                                                                 |
| [`is_histogram_other_bin(arg)`](#is_histogram_other_binarg)                                          | Returns `true` when `arg` is the "catch-all element" of its datatype for the purpose of the [`histogram_exact`](../../sql/functions/aggregates/index.md#histogram_exactarg-elements) function, which is equal to the "right-most boundary" of its datatype for the purpose of the [`histogram`](../../sql/functions/aggregates/index.md#histogramarg-boundaries) function.            |
| [`md5(string)`](#md5string)                                                                          | Returns the MD5 hash of the `string` as a `VARCHAR`.                                                                                                                                                                                                                                                                                                                                |
| [`md5_number(string)`](#md5_numberstring)                                                            | Returns the MD5 hash of the `string` as a `UHUGEINT`.                                                                                                                                                                                                                                                                                                                               |
| [`md5_number_lower(string)`](#md5_number_lowerstring)                                                | Returns the lower 64-bit segment of the MD5 hash of the `string` as a `UBIGINT`.                                                                                                                                                                                                                                                                                                    |
| [`md5_number_upper(string)`](#md5_number_upperstring)                                                | Returns the upper 64-bit segment of the MD5 hash of the `string` as a `UBIGINT`.                                                                                                                                                                                                                                                                                                    |
| [`nextval('sequence_name')`](#nextvalsequence_name)                                                  | Return the following value of the sequence.                                                                                                                                                                                                                                                                                                                                         |
| [`nullif(a, b)`](#nullifa-b)                                                                         | Return `NULL` if `a = b`, else return `a`. Equivalent to `CASE WHEN a = b THEN NULL ELSE a END`.                                                                                                                                                                                                                                                                                    |
| [`parse_formatted_bytes(string)`](#parse_formatted_bytesstring)                                      | Parse a human-readable byte size string (e.g., `'16 KiB'`) into a `UBIGINT` number of bytes. Throws an error on invalid input.                                                                                                                                                                                                                                                      |
| [`pg_typeof(expression)`](#pg_typeofexpression)                                                      | Returns the lower case name of the data type of the result of the expression. For PostgreSQL compatibility.                                                                                                                                                                                                                                                                         |
| [`query(`_`query_string`_`)`](#queryquery_string)                                                    | Table function that parses and executes the query defined in _`query_string`_. Only constant strings are allowed. Warning: this function allows invoking arbitrary queries, potentially altering the database state.                                                                                                                                                                |
| [`query_table(`_`tbl_name`_`)`](#query_tabletbl_name)                                                | Table function that returns the table given in _`tbl_name`_.                                                                                                                                                                                                                                                                                                                        |
| [`query_table(`_`tbl_names`_`, [`_`by_name`_`])`](#query_tabletbl_names-by_name)                     | Table function that returns the union of tables given in _`tbl_names`_. If the optional _`by_name`_ parameter is set to `true`, it uses [`UNION ALL BY NAME`](../../sql/query_syntax/setops/index.md#union-all-by-name) semantics.                                                                                                                                                  |
| [`read_blob(source)`](#read_blobsource)                                                              | Returns the content from `source` (a filename, a list of filenames, or a glob pattern) as a `BLOB`. See the [`read_blob` guide](../../cookbook/file_formats/read_file.md#read_blob) for more details.                                                                                                                                                                               |
| [`read_text(source)`](#read_textsource)                                                              | Returns the content from `source` (a filename, a list of filenames, or a glob pattern) as a `VARCHAR`. The file content is first validated to be valid UTF-8. If `read_text` attempts to read a file with invalid UTF-8 an error is thrown suggesting to use `read_blob` instead. See the [`read_text` guide](../../cookbook/file_formats/read_file.md#read_text) for more details. |
| [`sha1(string)`](#sha1string)                                                                        | Returns a `VARCHAR` with the SHA-1 hash of the `string`.                                                                                                                                                                                                                                                                                                                            |
| [`sha256(string)`](#sha256string)                                                                    | Returns a `VARCHAR` with the SHA-256 hash of the `string`.                                                                                                                                                                                                                                                                                                                          |
| [`sleep_ms(milliseconds)`](#sleep_msmilliseconds)                                                    | Pause execution for the specified number of milliseconds. Returns `NULL`.                                                                                                                                                                                                                                                                                                           |
| [`stats(expression)`](#statsexpression)                                                              | Returns a string with statistics about the expression. Expression can be a column, constant, or SQL expression.                                                                                                                                                                                                                                                                     |
| [`txid_current()`](#txid_current)                                                                    | Returns the current transaction's identifier, a `BIGINT` value. It will assign a new one if the current transaction does not have one already.                                                                                                                                                                                                                                      |
| [`typeof(expression)`](#typeofexpression)                                                            | Returns the name of the data type of the result of the expression.                                                                                                                                                                                                                                                                                                                  |
| [`uuid()`](#uuid)                                                                                    | Return a random UUID (UUIDv4) similar to this: `eeccb8c5-9943-b2bb-bb5e-222f4e14b687`.                                                                                                                                                                                                                                                                                              |
| [`uuidv4()`](#uuidv4)                                                                                | Return a random UUID (UUIDv4) similar to this: `eeccb8c5-9943-b2bb-bb5e-222f4e14b687`.                                                                                                                                                                                                                                                                                              |
| [`uuidv7()`](#uuidv7)                                                                                | Return a random UUIDv7 similar to this: `81964ebe-00b1-7e1d-b0f9-43c29b6fb8f5`.                                                                                                                                                                                                                                                                                                     |
| [`uuid_extract_timestamp(uuidv7)`](#uuid_extract_timestampuuidv7)                                    | Extracts `TIMESTAMP WITH TIME ZONE` from a UUIDv7 value.                                                                                                                                                                                                                                                                                                                            |
| [`uuid_extract_version(uuid)`](#uuid_extract_versionuuid)                                            | Extracts UUID version (`4` or `7`).                                                                                                                                                                                                                                                                                                                                                 |
| [`version()`](#version)                                                                              | Return the currently active version of SereneDB in this format.                                                                                                                                                                                                                                                                                                                     |

### Setup {#setup}

<SqlLogicTest id="sql/functions/utility/setup" />

#### `alias(column)`

Return the name of the column.

<SqlLogicTest id="sql/functions/utility/alias" />

#### `can_cast_implicitly(source_value, target_value)`

Whether or not we can implicitly cast from the types of the source value to the target value.

<SqlLogicTest id="sql/functions/utility/can_cast_implicitly" />

#### `checkpoint(database)`

Synchronize the WAL with the file for the (optional) database without interrupting transactions.

<SqlLogicTest id="sql/functions/utility/checkpoint" />

#### `coalesce(expr, ...)`

Return the first expression that evaluates to a non-`NULL` value. Accepts 1 or more parameters. Each expression can be a column, literal value, function result, or many others.

<SqlLogicTest id="sql/functions/utility/coalesce" />

#### `constant_or_null(arg1, arg2)`

If `arg2` is `NULL`, return `NULL`. Otherwise, return `arg1`.

<SqlLogicTest id="sql/functions/utility/constant_or_null" />

#### `count_if(x)`

Aggregate function; counts the rows for which the `BOOLEAN` argument `x` is `true`.

<SqlLogicTest id="sql/functions/utility/count_if" />

#### `create_sort_key(parameters...)`

Constructs a binary-comparable sort key based on a set of input parameters and sort qualifiers.

<SqlLogicTest id="sql/functions/utility/create_sort_key" />

#### `current_catalog()`

Return the name of the currently active catalog. Default is `postgres`.

<SqlLogicTest id="sql/functions/utility/current_catalog" />

#### `current_database()`

Return the name of the currently active database.

<SqlLogicTest id="sql/functions/utility/current_database" />

#### `current_query()`

Return the current query as a string.

<SqlLogicTest id="sql/functions/utility/current_query" />

#### `current_schema()`

Return the name of the currently active schema. Default is public.

<SqlLogicTest id="sql/functions/utility/current_schema" />

#### `current_schemas(boolean)`

Return list of schemas. Pass a parameter of `true` to include implicit schemas.

<SqlLogicTest id="sql/functions/utility/current_schemas" />

#### `current_setting('setting_name')`

Return the current value of the configuration setting.

<SqlLogicTest id="sql/functions/utility/current_setting" />

#### `currval('sequence_name')`

Return the current value of the sequence. Note that `nextval` must be called at least once prior to calling `currval`.

<SqlLogicTest id="sql/functions/utility/currval" />

#### `error(message)`

Throws the given error `message`.

<SqlLogicTest id="sql/functions/utility/error" />

#### `equi_width_bins(min, max, bincount, nice := false)`

Returns the upper boundaries of a partition of the interval `[min, max]` into `bin_count` equal-sized subintervals (for use with, e.g., [`histogram`](../../sql/functions/aggregates/index.md#histogramarg-boundaries)). If `nice = true`, then `min`, `max` and `bincount` may be adjusted to produce more aesthetically pleasing results.

<SqlLogicTest id="sql/functions/utility/equi_width_bins" />

#### `force_checkpoint(database)`

Synchronize the WAL with the file for the (optional) database, interrupting transactions.

<SqlLogicTest id="sql/functions/utility/force_checkpoint" />

#### `gen_random_uuid()`

Return a random UUID (UUIDv4) similar to this: `eeccb8c5-9943-b2bb-bb5e-222f4e14b687`.

<SqlLogicTest id="sql/functions/utility/gen_random_uuid" />

#### `getenv(var)`

Returns the value of the environment variable `var`. Only available in the command line client ([`serened shell`](../../clients/serened-shell.md)).

<SqlLogicTest id="sql/functions/utility/getenv" />

#### `hash(value)`

Returns a `UBIGINT` with the hash of the `value`. The used hash function may change across SereneDB versions.

<SqlLogicTest id="sql/functions/utility/hash" />

#### `icu_sort_key(string, collator)`

Surrogate [sort key](https://unicode-org.github.io/icu/userguide/collation/architecture.html#sort-keys) used to sort special characters according to the specific locale. The collator parameter is optional.

<SqlLogicTest id="sql/functions/utility/icu_sort_key" />

#### `if(a, b, c)`

Ternary conditional operator; returns b if a, else returns c. Equivalent to `CASE WHEN a THEN b ELSE c END`.

<SqlLogicTest id="sql/functions/utility/if" />

#### `ifnull(expr, other)`

A two-argument version of coalesce.

<SqlLogicTest id="sql/functions/utility/ifnull" />

#### `is_histogram_other_bin(arg)`

Returns `true` when `arg` is the "catch-all element" of its datatype for the purpose of the [`histogram_exact`](../../sql/functions/aggregates/index.md#histogram_exactarg-elements) function, which is equal to the "right-most boundary" of its datatype for the purpose of the [`histogram`](../../sql/functions/aggregates/index.md#histogramarg-boundaries) function.

<SqlLogicTest id="sql/functions/utility/is_histogram_other_bin" />

#### `md5(string)`

Returns the MD5 hash of the `string` as a `VARCHAR`.

<SqlLogicTest id="sql/functions/utility/md5" />

#### `md5_number(string)`

Returns the MD5 hash of the `string` as a `UHUGEINT`.

<SqlLogicTest id="sql/functions/utility/md5_number" />

#### `md5_number_lower(string)`

Returns the lower 8 bytes of the MD5 hash of `string` as a `UBIGINT`.

<SqlLogicTest id="sql/functions/utility/md5_number_lower" />

#### `md5_number_upper(string)`

Returns the upper 8 bytes of the MD5 hash of `string` as a `UBIGINT`.

<SqlLogicTest id="sql/functions/utility/md5_number_upper" />

#### `nextval('sequence_name')`

Return the following value of the sequence.

<SqlLogicTest id="sql/functions/utility/nextval" />

#### `nullif(a, b)`

Return `NULL` if a = b, else return a. Equivalent to `CASE WHEN a = b THEN NULL ELSE a END`.

<SqlLogicTest id="sql/functions/utility/nullif" />

#### `parse_formatted_bytes(string)`

Parse a human-readable byte size string (e.g., `'16 KiB'`) into a `UBIGINT` number of bytes. Throws an error on invalid input.

<SqlLogicTest id="sql/functions/utility/parse_formatted_bytes" />

#### `pg_typeof(expression)`

Returns the lower case name of the data type of the result of the expression. For PostgreSQL compatibility.

<SqlLogicTest id="sql/functions/utility/pg_typeof" />

#### `query(query_string)`

Table function that parses and executes the query defined in `query_string`. Only constant strings are allowed. Warning: this function allows invoking arbitrary queries, potentially altering the database state.

<SqlLogicTest id="sql/functions/utility/query" />

#### `query_table(tbl_name)`

Table function that returns the table given in `tbl_name`.

<SqlLogicTest id="sql/functions/utility/query_table" />

#### `query_table(tbl_names, [by_name])`

Table function that returns the union of tables given in `tbl_names`. If the optional `by_name` parameter is set to `true`, it uses [`UNION ALL BY NAME`](../../sql/query_syntax/setops/index.md#union-all-by-name) semantics.

<SqlLogicTest id="sql/functions/utility/query_table_union" />

#### `read_blob(source)`

Returns the content from `source` (a filename, a list of filenames, or a glob pattern) as a `BLOB`. See the [`read_blob` guide](../../cookbook/file_formats/read_file.md#read_blob) for more details.

<SqlLogicTest id="sql/functions/utility/read_blob" />

#### `read_text(source)`

Returns the content from `source` (a filename, a list of filenames, or a glob pattern) as a `VARCHAR`. The file content is first validated to be valid UTF-8. If `read_text` attempts to read a file with invalid UTF-8 an error is thrown suggesting to use `read_blob` instead. See the [`read_text` guide](../../cookbook/file_formats/read_file.md#read_text) for more details.

<SqlLogicTest id="sql/functions/utility/read_text" />

#### `sha1(string)`

Returns a `VARCHAR` with the SHA-1 hash of the `string`.

<SqlLogicTest id="sql/functions/utility/sha1" />

#### `sha256(string)`

Returns a `VARCHAR` with the SHA-256 hash of the `string`.

<SqlLogicTest id="sql/functions/utility/sha256" />

#### `sleep_ms(milliseconds)`

Pause execution for the specified number of milliseconds. Returns `NULL`.

<SqlLogicTest id="sql/functions/utility/sleep_ms" />

#### `stats(expression)`

Returns a string with statistics about the expression. Expression can be a column, constant, or SQL expression.

<SqlLogicTest id="sql/functions/utility/stats" />

#### `txid_current()`

Returns the current transaction's identifier, a `BIGINT` value. It will assign a new one if the current transaction does not have one already.

<SqlLogicTest id="sql/functions/utility/txid_current" />

#### `typeof(expression)`

Returns the name of the data type of the result of the expression.

<SqlLogicTest id="sql/functions/utility/typeof" />

#### `uuid()`

Return a random UUID (UUIDv4) similar to this: `eeccb8c5-9943-b2bb-bb5e-222f4e14b687`.

<SqlLogicTest id="sql/functions/utility/uuid" />

#### `uuidv4()`

Return a random UUID (UUIDv4) similar to this: `eeccb8c5-9943-b2bb-bb5e-222f4e14b687`.

<SqlLogicTest id="sql/functions/utility/uuidv4" />

#### `uuidv7()`

Return a random UUIDv7 similar to this: `81964ebe-00b1-7e1d-b0f9-43c29b6fb8f5`.

<SqlLogicTest id="sql/functions/utility/uuidv7" />

#### `uuid_extract_timestamp(uuidv7)`

Extracts a `TIMESTAMP WITH TIME ZONE` from a UUIDv7 value.

<SqlLogicTest id="sql/functions/utility/uuid_extract_timestamp" />

#### `uuid_extract_version(uuid)`

Extracts the UUID version (`4` or `7`).

<SqlLogicTest id="sql/functions/utility/uuid_extract_version" />

#### `version()`

Return the currently active version of SereneDB.

<SqlLogicTest id="sql/functions/utility/version" />

## Utility Table Functions

A [table function](../../sql/query_syntax/from_and_join/index.md#table-functions) is used in place of a table in a `FROM` clause.

| Name                                                           | Description                                                                                                                                                                                                         |
| :------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`glob(search_path)`](#globsearch_path)                        | Return filenames found at the location indicated by the _search_path_ in a single column named `file`. The _search_path_ may contain [glob pattern matching syntax](../../sql/functions/pattern_matching/index.md). |
| [`repeat_row(varargs, num_rows)`](#repeat_rowvarargs-num_rows) | Returns a table with `num_rows` rows, each containing the fields defined in `varargs`.                                                                                                                              |

#### `glob(search_path)`

Return filenames found at the location indicated by the _search_path_ in a single column named `file`. The _search_path_ may contain [glob pattern matching syntax](../../sql/functions/pattern_matching/index.md).

<SqlLogicTest id="sql/functions/utility/glob" />

#### `repeat_row(varargs, num_rows)`

Returns a table with `num_rows` rows, each containing the fields defined in `varargs`.

<SqlLogicTest id="sql/functions/utility/repeat_row" />
