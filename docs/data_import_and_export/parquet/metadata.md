---
title: Metadata
sidebar_position: 2
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Parquet Metadata

The `parquet_metadata` function can be used to query the metadata contained within a Parquet file, which reveals various internal details of the Parquet file such as the statistics of the different columns. This can be useful for figuring out what kind of skipping is possible in Parquet files, or even to obtain a quick overview of what the different columns contain. The function supports glob patterns to query metadata across multiple files in parallel:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_001" />

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_002" />

The columns returned by `parquet_metadata` are:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_009" />

## Parquet Schema

The `parquet_schema` function can be used to query the internal schema contained within a Parquet file. Note that this is the schema as it is contained within the metadata of the Parquet file. If you want to figure out the column names and types contained within a Parquet file it is easier to use `DESCRIBE`.

Fetch the column names and column types:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_003" />

Fetch the internal schema of a Parquet file:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_004" />

The columns returned by `parquet_schema` are:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_010" />

## Parquet File Metadata

The `parquet_file_metadata` function can be used to query file-level metadata such as the format version and the encryption algorithm used:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_005" />

The columns returned by `parquet_file_metadata` are:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_011" />

## Parquet Key-Value Metadata

The `parquet_kv_metadata` function can be used to query custom metadata defined as key-value pairs. Its `key` and `value` columns are returned as `BLOB`, so they display in hexadecimal:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_006" />

The columns returned by `parquet_kv_metadata` are:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_012" />

## Full Metadata

The `parquet_full_metadata` function returns all metadata for a Parquet file in a single row, combining the results of `parquet_file_metadata`, `parquet_metadata`, `parquet_schema` and `parquet_kv_metadata` as nested struct arrays. The length of each array reflects the file's structure — here three column chunks and four schema elements:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_007" />

Each struct array contains the same columns as the corresponding standalone function; run `SELECT * FROM parquet_full_metadata('test.parquet')` to retrieve them all.

## Bloom Filters

SereneDB supports Bloom filters for pruning the row groups that need to be read to answer highly selective queries.
Currently, Bloom filters are supported for the following types:

-   Integer types: `TINYINT`, `UTINYINT`, `SMALLINT`, `USMALLINT`, `INTEGER`, `UINTEGER`, `BIGINT`, `UBIGINT`
-   Floating point types: `FLOAT`, `DOUBLE`
-   `VARCHAR`
-   `BLOB`

The `parquet_bloom_probe(filename, column_name, value)` function shows which row groups can be excluded when filtering for a given value of a given column using the Bloom filter.
For example:

<SqlLogicTest id="data_import_and_export/parquet/metadata/example_008" />
