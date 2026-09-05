---
title: Typecasting
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Typecasting is an operation that converts a value in one particular data type to the closest corresponding value in another data type.
Like other SQL engines, SereneDB supports both implicit and explicit typecasting.

## Explicit Casting

Explicit typecasting is performed by using a `CAST` expression. For example, `CAST(col AS VARCHAR)` or `col::VARCHAR` explicitly cast the column `col` to `VARCHAR`. See the cast page for more information.

## Implicit Casting

In many situations, the system will add casts by itself. This is called _implicit_ casting and happens, for example, when a function is called with an argument that does not match the type of the function but can be cast to the required type.

Implicit casts can only be added for a number of type combinations, and is generally only possible when the cast cannot fail. For example, an implicit cast can be added from `INTEGER` to `DOUBLE` – but not from `DOUBLE` to `INTEGER`.

Consider the function `sin(DOUBLE)`. This function takes as input argument a column of type `DOUBLE`, however, it can be called with an integer as well: `sin(1)`. The integer is converted into a double before being passed to the `sin` function.

<DocCallout type="tip">
To check whether a type can be implicitly cast to another type, use the [`can_cast_implicitly` function](../../sql/functions/utility.md#can_cast_implicitlysource_value-target_value).
</DocCallout>

### Combination Casting

When values of different types need to be combined to an unspecified joint parent type, the system will perform implicit casts to an automatically selected parent type. For example, `list_value(1::INT64, 1::UINT64)` creates a list of type `INT128[]`. The implicit casts performed in this situation are sometimes more lenient than regular implicit casts. For example, a `BOOL` value may be cast to `INT` (with `true` mapping to `1` and `false` to `0`) even though this is not possible for regular implicit casts.

This _combination casting_ occurs for comparisons (`=` / `<` / `>`), set operations (`UNION` / `EXCEPT` / `INTERSECT`), and nested type constructors (`list_value` / `[...]` / `MAP`).

## Casting Operations Matrix

Values of a particular data type cannot always be cast to any arbitrary target data type. The only exception is the `NULL` value – which can always be converted between types.
The following matrix describes which conversions are supported.
When implicit casting is allowed, it implies that explicit casting is also possible.

<img alt="Typecasting matrix" src="/images/typecasting-matrix.png"/>

Even though a casting operation is supported based on the source and target data type, it does not necessarily mean the cast operation will succeed at runtime.

### Lossy Casts

Casting operations that result in loss of precision are allowed. For example, it is possible to explicitly cast a numeric type with fractional digits – such as `DECIMAL`, `FLOAT` or `DOUBLE` – to an integral type like `INTEGER` or `BIGINT`. The number will be rounded.

<SqlLogicTest id="sql/data_types/typecasting/example_001" />

### Overflows

Casting operations that would result in a value overflow throw an error. For example, the value `999` is too large to be represented by the `TINYINT` data type. Therefore, an attempt to cast that value to that type results in a runtime error:

<SqlLogicTest id="sql/data_types/typecasting/example_002" />

So even though the cast operation from `INTEGER` to `TINYINT` is supported, it is not possible for this particular value. `TRY_CAST` can be used to convert the value into `NULL` instead of throwing an error.

### Varchar

The [`VARCHAR`](./text.md) type acts as a universal target: any arbitrary value of any arbitrary type can always be cast to the `VARCHAR` type. This type is also used for displaying values in the shell.

<SqlLogicTest id="sql/data_types/typecasting/example_003" />

Casting from `VARCHAR` to another data type is supported, but can raise an error at runtime if SereneDB cannot parse and convert the provided text to the target data type.

<SqlLogicTest id="sql/data_types/typecasting/example_004" />

In general, casting to `VARCHAR` is a lossless operation and most scalar types can be cast back to the original type after being converted into text. Nested types are an exception: a list is rendered in text as `[1, 2, 3]`, but casting text back to a list requires the PostgreSQL array form `{1, 2, 3}`. As a result, the textual representation of a list cannot be cast directly back to a list.

<SqlLogicTest id="sql/data_types/typecasting/example_005" />

### Literal Types

Integer literals (such as `42`) and string literals (such as `'string'`) have special implicit casting rules. See the [literal types page](./literal_types.md) for more information.

### Lists / Arrays

Lists can be explicitly cast to other lists using the same casting rules. The cast is applied to the children of the list. For example, if we convert an `INTEGER[]` list to a `VARCHAR[]` list, the child `INTEGER` elements are individually cast to `VARCHAR` and a new list is constructed.

<SqlLogicTest id="sql/data_types/typecasting/example_006" />

### Arrays

Arrays follow the same casting rules as lists. In addition, arrays can be implicitly cast to lists of the same type. For example, an `INTEGER[3]` array can be implicitly cast to an `INTEGER[]` list.

### Structs

Structs can be cast to other structs as long as they share at least one field.

<DocCallout type="tip">
The rationale behind this requirement is to help avoid unintended errors. If two structs do not have any fields in common, then the cast was likely not intended.
</DocCallout>

<SqlLogicTest id="sql/data_types/typecasting/example_007" />

Fields that exist in the target struct, but that do not exist in the source struct, default to `NULL`.

<SqlLogicTest id="sql/data_types/typecasting/example_008" />

Fields that only exist in the source struct are ignored.

<SqlLogicTest id="sql/data_types/typecasting/example_009" />

The names of the struct can also be in a different order. The fields of the struct will be reshuffled based on the names of the structs.

<SqlLogicTest id="sql/data_types/typecasting/example_010" />

For [combination casting](./typecasting.md#combination-casting), the fields of the resulting struct are the superset of all fields of the input structs.
This logic also applies recursively to potentially nested structs.

<SqlLogicTest id="sql/data_types/typecasting/example_011" />

<SqlLogicTest id="sql/data_types/typecasting/example_012" />

### Unions

Union casting rules can be found on the [`UNION type page`](./union.md#casting-to-unions).
