---
title: Text
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

In SereneDB, strings can be stored in the `VARCHAR` field.
The field allows storage of Unicode characters. Internally, the data is encoded as UTF-8.

| Name         | Aliases                                        | Description                                                                                                   |
| :----------- | :--------------------------------------------- | :------------------------------------------------------------------------------------------------------------ |
| `VARCHAR`    | `CHAR`, `BPCHAR`, `STRING`, `TEXT`             | Variable-length character string                                                                              |
| `VARCHAR(n)` | `CHAR(n)`, `BPCHAR(n)`, `STRING(n)`, `TEXT(n)` | Variable-length character string. The maximum length `n` has no effect and is only provided for compatibility |

## Specifying a Length Limit

Specifying the length for the `VARCHAR`, `STRING` and `TEXT` types is not required and has no effect on the system. Specifying the length will not improve performance or reduce storage space of the strings in the database. These variants are supported for compatibility with other systems that do require a length to be specified for strings.

If you wish to restrict the number of characters in a `VARCHAR` column for data integrity reasons the `CHECK` constraint should be used, for example:

<SqlLogicTest id="sql/data_types/text/example_001" />

The `VARCHAR` field allows storage of Unicode characters. Internally, the data is encoded as UTF-8.

## Specifying a Compression Type

You can specify a compression type for a string with the `USING COMPRESSION` clause.
For example, to apply zstd compression, run:

<SqlLogicTest id="sql/data_types/text/example_002" />

## Text Type Values

Values of the text type are character strings, also known as string values or simply strings. At runtime, string values are constructed in one of the following ways:

-   referencing columns whose declared or implied type is the text data type
-   [string literals](../../sql/data_types/literal_types.md#string-literals)
-   [casting](../../sql/expressions/cast/index.md#explicit-casting) expressions to a text type
-   applying a [string operator](../../sql/functions/text.md#text-functions-and-operators), or invoking a function that returns a text type value

## Strings with Special Characters

To use special characters in a string, use [escape string literals](../../sql/data_types/literal_types.md#escape-string-literals) or [dollar-quoted string literals](../../sql/data_types/literal_types.md#dollar-quoted-string-literals). Alternatively, you can use concatenation and the [`chr` character function](../../sql/functions/text.md):

<SqlLogicTest id="sql/data_types/text/example_003" />

## Functions

See [Text Functions](../../sql/functions/text.md) and [Pattern Matching](../../sql/functions/pattern_matching/index.md).
