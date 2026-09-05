---
title: Enum
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

| Name   | Description                                                    |
| :----- | :------------------------------------------------------------- |
| `ENUM` | Dictionary representing all possible string values of a column |

The enum type represents a dictionary data structure with all possible unique values of a column. For example, a column storing the days of the week can be an enum holding all possible days. Enums are particularly interesting for string columns with low cardinality (i.e., fewer distinct values). This is because the column only stores a numerical reference to the string in the enum dictionary, resulting in immense savings in disk storage and faster query performance.

## Creating Enums

You can create an enum using hardcoded values:

<SqlLogicTest id="sql/data_types/enum/example_001" />

You can create enums in a specific schema:

<SqlLogicTest id="sql/data_types/enum/example_002" />

Anonymous enums can be created on the fly during [casting](../../sql/expressions/cast/index.md):

<SqlLogicTest id="sql/data_types/enum/example_003" />

You can also create an enum using a `SELECT` statement that returns a single column of `VARCHAR`s.
The set of values from the select statement will be deduplicated automatically,
and `NULL` values will be ignored:

<SqlLogicTest id="sql/data_types/enum/example_004" />

If you are importing data from a file, you can create an enum for a `VARCHAR` column before importing:

<SqlLogicTest id="sql/data_types/enum/example_005" />

## Using Enums

Enum values are case-sensitive, so 'maltese' and 'Maltese' are considered different values:

<SqlLogicTest id="sql/data_types/enum/example_006" />

After an enum has been created, it can be used anywhere a standard built-in type is used.
For example, we can create a table with a column that references the enum.

<SqlLogicTest id="sql/data_types/enum/example_007" />

The following query will fail since the mood type does not have a `quackity-quack` value.

<SqlLogicTest id="sql/data_types/enum/example_008" />

## Enums vs. Strings

SereneDB enums are automatically cast to `VARCHAR` types whenever necessary.
This characteristic allows for comparisons between different enums, or an enum and a `VARCHAR` column.

It also allows for an enum to be used in any `VARCHAR` function. For example:

<SqlLogicTest id="sql/data_types/enum/example_009" />

When comparing two different enum types, SereneDB will cast both to strings and perform a string comparison:

<SqlLogicTest id="sql/data_types/enum/example_010" />

When comparing an enum to a `VARCHAR`, SereneDB will cast the enum to `VARCHAR` and perform a string comparison:

<SqlLogicTest id="sql/data_types/enum/example_011" />

When comparing against a constant string, SereneDB will perform an optimization
and `try_cast(⟨constant string⟩, enum_type)`{:.language-sql .highlight} so that physically
we are doing an integer comparison instead of a string comparison
(but logically it is still a string comparison):

<SqlLogicTest id="sql/data_types/enum/example_012" />

> Warning This means that comparing against a random (non-equivalent) string always results in `false` (and does not error):

<SqlLogicTest id="sql/data_types/enum/example_013" />

If you want to enforce type-safety, cast to the enum explicitly:

<SqlLogicTest id="sql/data_types/enum/example_014" />

## Ordering of Enums

Enum values are ordered according to their order in the enum's definition. For example:

<SqlLogicTest id="sql/data_types/enum/example_015" />

<SqlLogicTest id="sql/data_types/enum/example_016" />

<DocCallout type="attention">
If you compare an enum to a non-enum (e.g., a `VARCHAR` or a different enum type),
the enum will first be cast to a string (as described in the previous section),
and the comparison will be done lexicographically as with strings:
</DocCallout>

<SqlLogicTest id="sql/data_types/enum/example_017" />

So, if you want to e.g. "get all priorities at or above `medium`" then explicitly cast to the enum type:

<SqlLogicTest id="sql/data_types/enum/example_018" />

## Functions

See [Enum Functions](../../sql/functions/enum.md).

For example, show the available values in the `mood` enum using the `enum_range` function:

<SqlLogicTest id="sql/data_types/enum/example_019" />

## Enum Removal

Enum types are stored in the catalog, and a catalog dependency is added to each table that uses them. It is possible to drop an enum from the catalog using the following command:

<SqlLogicTest id="sql/data_types/enum/example_020" hideResult />

Currently, it is possible to drop enums that are used in tables without affecting the tables.

> Warning This behavior of the enum removal feature is subject to change. In future releases, it is expected that any dependent columns must be removed before dropping the enum, or the enum must be dropped with the additional `CASCADE` parameter.
