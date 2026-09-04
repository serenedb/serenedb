---
title: Enum Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<!-- markdownlint-disable MD001 -->

This section describes functions and operators for examining and manipulating [`ENUM` values](../../sql/data_types/enum.md).
The examples assume an enum type created as:

<SqlLogicTest id="sql/functions/enum/example_001" />

These functions can take `NULL` or a specific value of the type as argument(s).
With the exception of `enum_range_boundary`, the result depends only on the type of the argument and not on its value.

| Name                                                               | Description                                                      |
| :----------------------------------------------------------------- | :--------------------------------------------------------------- |
| [`enum_code(enum_value)`](#enum_codeenum_value)                    | Returns the numeric value backing the given enum value.          |
| [`enum_first(enum)`](#enum_firstenum)                              | Returns the first value of the input enum type.                  |
| [`enum_last(enum)`](#enum_lastenum)                                | Returns the last value of the input enum type.                   |
| [`enum_range(enum)`](#enum_rangeenum)                              | Returns all values of the input enum type as an array.           |
| [`enum_range_boundary(enum, enum)`](#enum_range_boundaryenum-enum) | Returns the range between the two given enum values as an array. |

#### `enum_code(enum_value)`

Returns the numeric value backing the given enum value.

<SqlLogicTest id="sql/functions/enum/enum_code" />

#### `enum_first(enum)`

Returns the first value of the input enum type.

<SqlLogicTest id="sql/functions/enum/enum_first" />

#### `enum_last(enum)`

Returns the last value of the input enum type.

<SqlLogicTest id="sql/functions/enum/enum_last" />

#### `enum_range(enum)`

Returns all values of the input enum type as an array.

<SqlLogicTest id="sql/functions/enum/enum_range" />

#### `enum_range_boundary(enum, enum)`

Returns the range between the two given enum values as an array. The values must be of the same enum type. When the first parameter is `NULL`, the result starts with the first value of the enum type. When the second parameter is `NULL`, the result ends with the last value of the enum type.

<SqlLogicTest id="sql/functions/enum/enum_range_boundary" />
