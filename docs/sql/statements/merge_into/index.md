---
title: MERGE INTO
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `MERGE INTO` statement is an alternative to `INSERT INTO ... ON CONFLICT` that doesn't need a primary key since it allows for a custom match condition. This is a very useful alternative for upserting use cases (`INSERT` + `UPDATE`) when the destination table does not have a primary key constraint.

## Examples

First, let's create a simple table.

<SqlLogicTest id="sql/statements/merge_into/index/example_001" />

The simplest upsert would be to use a whole row in the `USING` clause.
This way, if there is a match,
the row can be updated to the new row without further instructions
(`WHEN MATCHED THEN UPDATE`), and when there is no match,
the row can be trivially inserted into the table
(`WHEN NOT MATCHED THEN INSERT`).

<SqlLogicTest id="sql/statements/merge_into/index/example_002" />

In the previous example we are updating the whole row if `id` matches. However, it is also a common pattern to receive a _change set_ with some keys and the changed value. This is a good use for `SET`. If the match condition uses a column that has the same name in the source and destination, the keyword `USING` can be used in the match condition.

<SqlLogicTest id="sql/statements/merge_into/index/example_003" />

Another common pattern is to receive a _delete set_ of rows, which may only contain ids of rows to be deleted.

<SqlLogicTest id="sql/statements/merge_into/index/example_004" />

`MERGE INTO` also supports more complex conditions, for example, for a given _delete set_ we can decide to only remove rows that contain a `salary` bigger or equal than a certain amount.

<SqlLogicTest id="sql/statements/merge_into/index/example_005" />

If needed, SereneDB also supports multiple `UPDATE` and `DELETE` conditions. The `RETURNING` clause can be used to indicate which rows were affected by the `MERGE` statement.

<SqlLogicTest id="sql/statements/merge_into/index/example_006" />

In some cases, you may want to perform a different action specifically if the source doesn't meet a condition. For example, if we expect that data that is not present on the source shouldn't be present in the target:

<SqlLogicTest id="sql/statements/merge_into/index/example_007" />

There is also the possibility of specifying `WHEN NOT MATCHED BY TARGET`. However, the behavior is, as you may expect, the same as `WHEN NOT MATCHED` since by default when specifying conditions, we look at the target.

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
