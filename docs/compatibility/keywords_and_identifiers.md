---
title: Keywords and Identifiers
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Identifiers

Similarly to other SQL dialects and programming languages, identifiers in SereneDB's SQL are subject to several rules.

-   Unquoted identifiers need to conform to a number of rules:
    -   They must not be a reserved keyword (see [`duckdb_keywords()`](../sql/functions/duckdb_table_functions.md#duckdb_keywords)), e.g., `SELECT 123 AS SELECT` will fail.
    -   They must not start with a number or special character, e.g., `SELECT 123 AS 1col` is invalid.
    -   They cannot contain whitespaces (including tabs and newline characters).
-   Identifiers can be quoted using double-quote characters (`"`). Quoted identifiers can use any keyword, whitespace or special character, e.g., `"SELECT"` and `" § 🌊 ¶ "` are valid identifiers.
-   Double quotes can be escaped by repeating the quote character, e.g., to create an identifier named `IDENTIFIER "X"`, use `"IDENTIFIER ""X"""`.

### Duplicate Identifiers

In some cases, duplicate identifiers can occur, e.g., column names may conflict when unnesting a nested data structure.
Following PostgreSQL, SereneDB allows duplicate column names in a result and preserves them as-is — they are not renamed or deduplicated.

For example, recursively unnesting a struct whose nested fields repeat a name yields a result with repeated column names:

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_001" />

## Database Names

Database names are subject to the rules for [identifiers](#identifiers).

Additionally, it is best practice to avoid SereneDB's two internal [database schema names](../sql/functions/duckdb_table_functions.md#duckdb_databases), `system` and `temp`.
By default, persistent databases are named after their filename without the extension.
Therefore, the filenames `system.db` and `temp.db` (as well as `system.duckdb` and `temp.duckdb`) result in the database names `system` and `temp`, respectively.
If you need to attach to a database that has one of these names, use an alias, e.g.:

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_002" />

## Rules for Case-Sensitivity

### Keywords and Function Names

SQL keywords and function names are case-insensitive in SereneDB.

For example, the following two queries are equivalent:

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_003" />

### Case-Sensitivity of Identifiers

Identifiers in SereneDB are always case-insensitive, similarly to PostgreSQL.
However, unlike PostgreSQL (and some other major SQL implementations), SereneDB also treats quoted identifiers as case-insensitive.

**Comparison of identifiers:**
Case-insensitivity is implemented using an ASCII-based comparison:
`col_A` and `col_a` are equal but `col_á` is not equal to them.

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_004" />

**Preserving cases:**
While SereneDB treats identifiers in a case-insensitive manner, it preserves the cases of these identifiers.
That is, each character's case (uppercase/lowercase) is maintained as originally specified by the user even if a query uses different cases when referring to the identifier.
For example:

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/preserve_identifier_case/example_005" />

To change this behavior, set the `preserve_identifier_case` [configuration option](../configuration/overview.md#configuration-reference) to `false`.

### Case-Sensitivity of Keys in Nested Data Structures

The keys of `MAP`s are case-sensitive (looking up `A` when the key is `a` finds nothing):

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_006" />

The keys of `UNION`s and `STRUCT`s are case-insensitive:

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_007" />

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_008" />

#### Handling Conflicts

When the same identifier is spelt with different cases within a nested structure, SereneDB raises an error rather than silently picking one. For example:

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/example_009" />

#### Disabling Preserving Cases

With the `preserve_identifier_case` [configuration option](../configuration/overview.md#configuration-reference) set to `false`, all identifiers are turned into lowercase:

<SqlLogicTest id="sql/dialect/keywords_and_identifiers/lowercase_identifier_case/example_010" />
