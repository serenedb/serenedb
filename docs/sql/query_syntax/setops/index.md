---
title: Set Operations
sidebar_position: 15
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

Set operations allow queries to be combined according to [set operation semantics](https://en.wikipedia.org/wiki/Set_%28mathematics%29#Basic_operations). Set operations refer to the [`UNION [ALL]`](#union), [`INTERSECT [ALL]`](#intersect) and [`EXCEPT [ALL]`](#except) clauses. The vanilla variants use set semantics, i.e., they eliminate duplicates, while the variants with `ALL` use bag semantics.

Traditional set operations unify queries **by column position**, and require the to-be-combined queries to have the same number of input columns. If the columns are not of the same type, casts may be added. The result will use the column names from the first query.

SereneDB also supports [`UNION [ALL] BY NAME`](#union-all-by-name), which joins columns by name instead of by position. `UNION BY NAME` does not require the inputs to have the same number of columns. `NULL` values will be added in case of missing columns.

## `UNION`

The `UNION` clause can be used to combine rows from multiple queries. The queries are required to return the same number of columns. [Implicit casting](../../data_types/typecasting.md#implicit-casting) to one of the returned types is performed to combine columns of different types where necessary. If this is not possible, the `UNION` clause throws an error.

### Vanilla `UNION` (Set Semantics)

The vanilla `UNION` clause follows set semantics, therefore it performs duplicate elimination, i.e., only unique rows will be included in the result.

<SqlLogicTest id="sql/query_syntax/setops/index/example_001" />

### `UNION ALL` (Bag Semantics)

`UNION ALL` returns all rows of both queries following bag semantics, i.e., _without_ duplicate elimination.

<SqlLogicTest id="sql/query_syntax/setops/index/example_002" />

### `UNION [ALL] BY NAME`

The `UNION [ALL] BY NAME` clause can be used to combine rows from different tables by name, instead of by position. `UNION BY NAME` does not require both queries to have the same number of columns. Any columns that are only found in one of the queries are filled with `NULL` values for the other query.

Take the following tables for example:

<SqlLogicTest id="sql/query_syntax/setops/index/example_003" />

<SqlLogicTest id="sql/query_syntax/setops/index/example_004" />

`UNION BY NAME` follows set semantics (therefore it performs duplicate elimination), whereas `UNION ALL BY NAME` follows bag semantics.

## `INTERSECT`

The `INTERSECT` clause can be used to select all rows that occur in the result of **both** queries.

### Vanilla `INTERSECT` (Set Semantics)

Vanilla `INTERSECT` performs duplicate elimination, so only unique rows are returned.

<SqlLogicTest id="sql/query_syntax/setops/index/example_005" />

### `INTERSECT ALL` (Bag Semantics)

`INTERSECT ALL` follows bag semantics, so duplicates are returned.

<SqlLogicTest id="sql/query_syntax/setops/index/example_006" />

## `EXCEPT`

The `EXCEPT` clause can be used to select all rows that **only** occur in the left query.

### Vanilla `EXCEPT` (Set Semantics)

Vanilla `EXCEPT` follows set semantics, therefore, it performs duplicate elimination, so only unique rows are returned.

<SqlLogicTest id="sql/query_syntax/setops/index/example_007" />

### `EXCEPT ALL` (Bag Semantics)

`EXCEPT ALL` uses bag semantics:

<SqlLogicTest id="sql/query_syntax/setops/index/example_008" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
