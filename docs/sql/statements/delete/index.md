---
title: DELETE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `DELETE` statement removes rows from the table identified by the table-name.
If the `WHERE` clause is not present, all records in the table are deleted.
If a `WHERE` clause is supplied, then only those rows for which the `WHERE` clause results in true are deleted. Rows for which the expression is false or `NULL` are retained.

## Examples

Remove the rows matching the condition `i = 2` from the database:

<SqlLogicTest id="sql/statements/delete/index/example_001" />

Delete all rows in the table `tbl`:

<SqlLogicTest id="sql/statements/delete/index/example_002" />

### `USING` Clause

The `USING` clause allows deleting based on the content of other tables or subqueries.

### `RETURNING` Clause

The `RETURNING` clause allows returning the deleted values. It uses the same syntax as the `SELECT` clause except the `DISTINCT` modifier is not supported.

<SqlLogicTest id="sql/statements/delete/index/example_003" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

## The `TRUNCATE` Statement

The `TRUNCATE` statement removes all rows from a table, acting as an alias for `DELETE FROM` without a `WHERE` clause:

<SqlLogicTest id="sql/statements/delete/index/example_004" />

## Limitations on Reclaiming Memory and Disk Space

Running `DELETE` does not mean space is reclaimed. In general, rows are only marked as deleted. SereneDB reclaims space when performing a `CHECKPOINT`. [`VACUUM`](../../statements/vacuum/index.md) currently does not reclaim space.
