---
title: Data Types
slug: /sql/data_types
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## General-Purpose Data Types

The table below shows all the built-in general-purpose data types. The alternatives listed in the aliases column can be used to refer to these types as well, however, note that the aliases are not part of the SQL standard and hence might not be accepted by other database engines.

| Name                       | Aliases                            | Description                                                                                                |
| :------------------------- | :--------------------------------- | :--------------------------------------------------------------------------------------------------------- |
| `BIGINT`                   | `INT8`, `LONG`                     | Signed eight-byte integer                                                                                  |
| `BIT`                      | `BITSTRING`                        | String of 1s and 0s                                                                                        |
| `BLOB`                     | `BYTEA`, `BINARY`, `VARBINARY`     | Variable-length binary data                                                                                |
| `BIGNUM`                   |                                    | Variable-length integer                                                                                    |
| `BOOLEAN`                  | `BOOL`, `LOGICAL`                  | Logical Boolean (`true` / `false`)                                                                         |
| `DATE`                     |                                    | Calendar date (year, month, day)                                                                           |
| `DECIMAL(prec, scale)`     | `NUMERIC(prec, scale)`             | Fixed-precision number with the given width (precision) and scale, defaults to `prec = 18` and `scale = 3` |
| `DOUBLE`                   | `FLOAT8`                           | Double precision floating-point number (8 bytes)                                                           |
| `FLOAT`                    | `FLOAT4`, `REAL`                   | Single precision floating-point number (4 bytes)                                                           |
| `HUGEINT`                  |                                    | Signed sixteen-byte integer                                                                                |
| `INTEGER`                  | `INT4`, `INT`, `SIGNED`            | Signed four-byte integer                                                                                   |
| `INTERVAL`                 |                                    | Date / time delta                                                                                          |
| `JSON`                     |                                    | [JSON object](../../data_import_and_export/json/overview.md)                                               |
| `SMALLINT`                 | `INT2`, `SHORT`                    | Signed two-byte integer                                                                                    |
| `TIME`                     |                                    | Time of day (no time zone)                                                                                 |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMPTZ`                      | Combination of time and date that uses the current time zone                                               |
| `TIMESTAMP`                | `DATETIME`                         | Combination of time and date                                                                               |
| `TINYINT`                  | `INT1`                             | Signed one-byte integer                                                                                    |
| `UBIGINT`                  |                                    | Unsigned eight-byte integer                                                                                |
| `UHUGEINT`                 |                                    | Unsigned sixteen-byte integer                                                                              |
| `UINTEGER`                 |                                    | Unsigned four-byte integer                                                                                 |
| `USMALLINT`                |                                    | Unsigned two-byte integer                                                                                  |
| `UTINYINT`                 |                                    | Unsigned one-byte integer                                                                                  |
| `UUID`                     |                                    | [UUID data type](../../sql/data_types/numeric.md#universally-unique-identifiers-uuids)                     |
| `VARCHAR`                  | `CHAR`, `BPCHAR`, `TEXT`, `STRING` | Variable-length character string                                                                           |

Implicit and explicit typecasting is possible between numerous types, see the [Typecasting](../../sql/data_types/typecasting.md) page for details.

## Nested / Composite Types

SereneDB supports five nested data types: `ARRAY`, `LIST`, `MAP`, `STRUCT` and `UNION`. Each supports different use cases and has a different structure.

| Name                                         | Description                                                                                                                                                                                     | Rules when used in a column                                                                                    | Build from values         | Define in DDL/CREATE               |
| :------------------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------- | :------------------------ | :--------------------------------- |
| [`ARRAY`](../../sql/data_types/array.md)     | An ordered, fixed-length sequence of data values of the same type.                                                                                                                              | Each row must have the same data type within each instance of the `ARRAY` and the same number of elements.     | `[1, 2, 3]`               | `INTEGER[3]`                       |
| [`LIST`](../../sql/data_types/list.md)       | An ordered sequence of data values of the same type.                                                                                                                                            | Each row must have the same data type within each instance of the `LIST`, but can have any number of elements. | `[1, 2, 3]`               | `INTEGER[]`                        |
| [`MAP`](../../sql/data_types/map.md)         | A dictionary of multiple named values, each key having the same type and each value having the same type. Keys and values can be any type and can be different types from one another.          | Rows may have different keys.                                                                                  | `map([1, 2], ['a', 'b'])` | `MAP(INTEGER, VARCHAR)`            |
| [`STRUCT`](../../sql/data_types/struct.md)   | A dictionary of multiple named values, where each key is a string, but the value can be a different type for each key.                                                                          | Each row must have the same keys.                                                                              | `{'i': 42, 'j': 'a'}`     | `STRUCT(i INTEGER, j VARCHAR)`     |
| [`UNION`](../../sql/data_types/union.md)     | A union of multiple alternative data types, storing one of them in each value at a time. A union also contains a discriminator “tag” value to inspect and access the currently set member type. | Rows may be set to different member types of the union.                                                        | `union_value(num := 2)`   | `UNION(num INTEGER, text VARCHAR)` |
| [`VARIANT`](../../sql/data_types/variant.md) | A semi-structured type where each value is self-contained with its own type information.                                                                                                        | Each row may hold a value of a different type.                                                                 | `42::VARIANT`             | `VARIANT`                          |

### Rules for Case Sensitivity

The keys of `MAP`s are case-sensitive, while keys of `UNION`s and `STRUCT`s are case-insensitive.
For examples, see the [Rules for Case Sensitivity section](../../compatibility/keywords_and_identifiers.md#case-sensitivity-of-keys-in-nested-data-structures).

### Updating Values of Nested Types

When performing _updates_ on values of nested types, SereneDB performs a _delete_ operation followed by an _insert_ operation.
When used in a table with ART indexes (either via explicit indexes or primary keys/unique constraints), this can lead to [unexpected constraint violations](../../sql/indexes/art.md#constraint-checking-in-update-statements).

## Nesting

`ARRAY`, `LIST`, `MAP`, `STRUCT` and `UNION` types can be arbitrarily nested to any depth, so long as the type rules are observed.

Struct with `LIST`s:

<SqlLogicTest id="sql/data_types/overview/example_001" />

`MAP` with `LIST` values:

<SqlLogicTest id="sql/data_types/overview/example_002" />

A list of `STRUCT`s:

<SqlLogicTest id="sql/data_types/overview/example_003" />

## Performance Implications

The choice of data types can have a strong effect on performance. Please consult the [Performance Guide](../../cookbook/performance/schema.md) for details.
