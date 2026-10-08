---
title: DELETE
split: headings
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

`TRUNCATE` can name several tables; they are emptied in one transaction, so either all of them are emptied or, if any of them fails, none is. As in PostgreSQL, a table that another table references with a foreign key can only be truncated together with the referencing table. By default (`RESTRICT`) the statement is refused otherwise, even when the referencing table holds no rows. `CASCADE` also truncates every table that references a truncated table, and the tables that reference those:

<SqlLogicTest id="sql/statements/delete/index/example_005" />

`RESTART IDENTITY` also restarts the sequences the truncated tables own, those behind their `SERIAL` columns and those attached with [`OWNED BY`](../create_sequence/index.md), so new rows are numbered from the start again. `CONTINUE IDENTITY`, the default, leaves them where they are. The restart belongs to the transaction: if it rolls back, the sequences continue where they were:

<SqlLogicTest id="sql/statements/delete/index/example_006" />

PostgreSQL makes `TRUNCATE` wait for the transactions that are writing to the table. SereneDB does not wait: `TRUNCATE` fails with a serialization error (`40001`) when another transaction has added rows to the table and not committed yet, or committed them after the truncating transaction started, and the transaction can be retried. On a search table, any open write to the table refuses `TRUNCATE` the same way, and while a transaction that truncated a search table is open, writes to that table from other transactions fail with `40001`.

## Limitations on Reclaiming Memory and Disk Space

Running `DELETE` does not mean space is reclaimed. In general, rows are only marked as deleted. SereneDB reclaims space when performing a `CHECKPOINT`. [`VACUUM`](../../statements/vacuum/index.md) currently does not reclaim space.
