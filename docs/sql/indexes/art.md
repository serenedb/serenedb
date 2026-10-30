---
title: Adaptive Radix Tree (ART)
sidebar_position: 2
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

An [Adaptive Radix Tree (ART)](https://db.in.tum.de/~leis/papers/ART.pdf) is SereneDB's default secondary index — the index type created by [`CREATE INDEX`](../statements/create_index/index.md) when no `USING` clause is given. It is mainly used to enforce primary key constraints and to speed up point and very highly selective (i.e., < 0.1%) queries. ART indexes are created automatically for columns with a `UNIQUE` or `PRIMARY KEY` constraint, and can be created manually with `CREATE INDEX`.

<DocCallout type="attention">

An ART index must fit in memory while it is created. Avoid creating an ART index that does not.

</DocCallout>

## Persistence

ART indexes are persisted on disk.

## Limitations

An ART index keeps a second copy of the data it indexes, so the following limitations apply when modifying that data.

### Constraint Checking in `UPDATE` Statements

`UPDATE` statements on indexed columns and columns that cannot be updated in place are transformed into a `DELETE` of the original row followed by an `INSERT` of the updated row. This rewrite has performance implications, particularly for wide tables, as entire rows are rewritten instead of only the affected columns. A row whose indexed values the `UPDATE` leaves as they are is updated in place instead, unless the statement sets only indexed columns, sets a column that cannot be updated in place, or sets a floating-point, `INTERVAL` or `TIME WITH TIME ZONE` indexed column. A `CHECK` constraint over several columns makes every `UPDATE` of the table set all of them, each to itself when the statement doesn't set it. So when a `CHECK` spans an indexed and a non-indexed column, every `UPDATE` of the table deletes and re-inserts each row it touches, even one that sets only columns outside the constraint; a `CHECK` over indexed columns only keeps the in-place path. The same applies to the `UPDATE` of `MERGE INTO`, `INSERT ... ON CONFLICT DO UPDATE` and `INSERT OR REPLACE`.

Additionally, it causes the following constraint-checking limitation of `UPDATE` statements. The same limitation exists in other DBMSs, like PostgreSQL.

In the example below, the table has more than 2048 rows, the number of rows an `UPDATE` processes at a time. The `UPDATE` is rewritten into a `DELETE` followed by an `INSERT`, 2048 rows at a time: when it sets `i = 2047` to `2048`, the row that holds `2048` comes in the next 2048 rows and has not been changed to `2049` yet, so the `UPDATE` fails with a constraint violation.

<SqlLogicTest id="sql/indexes/art/example_001" />

A workaround is to split the `UPDATE` into a `DELETE` followed by an `INSERT`, in one transaction: copy the rows to a temporary table, delete them from the table, insert them back with their new values, and drop the temporary table.

<SqlLogicTest id="sql/indexes/art/example_002" />

A client can do the same without a temporary table: run `DELETE ... RETURNING *` and insert the rows it returns, with their new values, in the same transaction.

### Over-Eager Constraint Checking in Foreign Keys

This limitation occurs if you meet the following conditions:

-   A table has a `FOREIGN KEY` constraint.
-   An `UPDATE` of the corresponding `PRIMARY KEY` table deletes and re-inserts the rows it touches, as described [above](#constraint-checking-in-update-statements). One that sets a payload column of type `LIST`, `ARRAY`, `MAP`, `UNION`, `VARIANT` or `GEOMETRY`, or a `STRUCT` that contains one, always does.
-   The to-be-deleted row exists in the foreign key table.

If these hold, you'll encounter an unexpected constraint violation:

<SqlLogicTest id="sql/indexes/art/example_003" />

SereneDB does not look ahead: when the `DELETE` removes the row, it does not know that the `INSERT` of the same `UPDATE` puts the key back.

## See also

- [Inverted Index](./inverted/index.md) — for full-text, vector and geospatial search
- [CREATE INDEX](../statements/create_index/index.md)
