---
title: Variant
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `VARIANT` type stores typed, binary data where each row is self-contained with its own type information. This differs from the [JSON type](../../data_import_and_export/json/json_type.md), which is physically stored as text. Because type metadata is embedded per-value, `VARIANT` provides better compression and query performance than JSON for semi-structured data.

The `VARIANT` type is inspired by [Snowflake's semi-structured `VARIANT` data type](https://docs.snowflake.com/en/sql-reference/data-types-semistructured). It is available [in Parquet since 2025](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md) and also supported by SereneDB's [Parquet reader](#parquet-support).

## Examples

### Storing Different Types in the Same Column

A `VARIANT` column can hold values of different types across rows:

<SqlLogicTest id="sql/data_types/variant/example_001" />

### Checking the Type of a Value

Use `variant_typeof` to inspect the underlying type of each row:

<SqlLogicTest id="sql/data_types/variant/example_002" />

### Extracting Fields from Nested Variants

Fields can be extracted from nested `VARIANT` values using dot notation or the `variant_extract` function:

<SqlLogicTest id="sql/data_types/variant/example_003" />

<SqlLogicTest id="sql/data_types/variant/example_004" />

## Parquet Support

SereneDB supports reading and writing `VARIANT` types from [Parquet files](../../data_import_and_export/parquet/overview.md), including _shredding,_ a technique that stores nested data as flat values for more efficient access.

### Writing VARIANT to Parquet

When writing `VARIANT` columns to Parquet, SereneDB can automatically shred (decompose) the variant data into typed columns based on the structure of the first row group. This auto-shredding improves read performance by enabling predicate pushdown and efficient column access.

To explicitly provide a schema for shredding, use the `SHREDDING` copy option:

<SqlLogicTest id="sql/data_types/variant/example_005" />

### Reading Snowflake VARIANT from Parquet

SereneDB can read shredded `VARIANT` Parquet files produced by Snowflake, automatically reconstructing the variant values from the shredded columns.
