---
title: UPDATE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `UPDATE` statement modifies the values of rows in a table.

## Examples

For every row where `i` is `NULL`, set the value to 0 instead:

<SqlLogicTest id="sql/statements/update/index/example_001" />

Set all values of `i` to 1 and all values of `j` to 2:

<SqlLogicTest id="sql/statements/update/index/example_002" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

`UPDATE` changes the values of the specified columns in all rows that satisfy the condition. Only the columns to be modified need be mentioned in the `SET` clause; columns not explicitly modified retain their previous values.

## Update from Other Table

A table can be updated based upon values from another table. This can be done by specifying a table in a `FROM` clause, or using a sub-select statement. Both approaches have the benefit of completing the `UPDATE` operation in bulk for increased performance.

<SqlLogicTest id="sql/statements/update/index/example_003" />

<SqlLogicTest id="sql/statements/update/index/example_004" />

Or:

<SqlLogicTest id="sql/statements/update/index/example_005" />

<SqlLogicTest id="sql/statements/update/index/example_006" />

## Update from Same Table

The only difference between this case and the above is that a different table alias must be specified on both the target table and the source table.
In this example `AS true_original` and `AS new` are both required.

<SqlLogicTest id="sql/statements/update/index/example_007" />

## Update Using Joins

To select the rows to update, `UPDATE` statements can use the `FROM` clause and express joins via the `WHERE` clause. For example:

<SqlLogicTest id="sql/statements/update/index/example_008" />

To increase the revenue of all cities in France, join the `city` and the `country` tables, and filter on the latter:

<SqlLogicTest id="sql/statements/update/index/example_009" />

<SqlLogicTest id="sql/statements/update/index/example_010" />

## Upsert (Insert or Update)

See the [Insert documentation](../../statements/insert/index.md#on-conflict-clause) for details.
