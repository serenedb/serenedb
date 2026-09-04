---
title: ORDER BY
sidebar_position: 7
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

`ORDER BY` is an output modifier. Logically it is applied near the very end of the query (just prior to [`LIMIT`](../../query_syntax/limit/index.md) or [`OFFSET`](../../query_syntax/limit/index.md), if present).
The `ORDER BY` clause sorts the rows on the sorting criteria in either ascending or descending order.
In addition, every order clause can specify whether `NULL` values should be moved to the beginning or to the end.

The `ORDER BY` clause may contain one or more expressions, separated by commas.
An error will be thrown if no expressions are included, since the `ORDER BY` clause should be removed in that situation.
The expressions may begin with either an arbitrary scalar expression (which could be a column name), a column position number (where the indexing starts from 1), or the keyword `ALL`.
Each expression can optionally be followed by an order modifier (`ASC` or `DESC`, default is `ASC`), and/or a `NULL` order modifier (`NULLS FIRST` or `NULLS LAST`, default is `NULLS LAST`).

## `ORDER BY ALL`

The `ALL` keyword indicates that the output should be sorted by every column in order from left to right.
The direction of this sort may be modified using either `ORDER BY ALL ASC` or `ORDER BY ALL DESC` and/or `NULLS FIRST` or `NULLS LAST`.
Note that `ALL` may not be used in combination with other expressions in the `ORDER BY` clause – it must be by itself.
See examples below.

## `NULL` Order Modifier

By default, SereneDB sorts `ASC` and `NULLS LAST`, i.e., the values are sorted in ascending order and `NULL` values are placed last.
This is identical to the default sort order of PostgreSQL.
The default sort order can be changed with the following configuration options.

Use the `default_null_order` option to change the default `NULL` sorting order to either `NULLS_FIRST`, `NULLS_LAST`, `NULLS_FIRST_ON_ASC_LAST_ON_DESC` or `NULLS_LAST_ON_ASC_FIRST_ON_DESC`:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_001" />

Use the `default_order` option to change the direction of the default sorting order to either `DESC` or `ASC`:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_002" />

## Collations

Text is sorted using the binary comparison collation by default, which means values are sorted on their binary UTF-8 values.
While this works well for ASCII text (e.g., for English language data), the sorting order can be incorrect for other languages.
For this purpose, SereneDB provides collations.
For more information on collations, see the [Collation page](../../expressions/collations/index.md).

## Examples

All examples use this example table:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_003" />

Select the planets, ordered by kind using the default `NULL` order and default order:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_004" />

Select the planets, ordered by kind in descending order with nulls at the end:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_005" />

Order by kind and then by planet, both using the default orderings:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_006" />

Region- and language-specific (ICU) collations, such as German, are available in this build of SereneDB, so ordering by one such collation works. For example, German orders `ä` next to `a` rather than at the end. For more information, see the [Collation page](../../expressions/collations/index.md):

<SqlLogicTest id="sql/query_syntax/orderby/index/example_007" />

### `ORDER BY ALL` Examples

Order from left to right (by planet, then by kind, then by moons) in ascending order:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_008" />

Order from left to right (by planet, then by kind, then by moons) in descending order:

<SqlLogicTest id="sql/query_syntax/orderby/index/example_009" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
