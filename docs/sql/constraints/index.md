---
title: Constraints
sidebar_position: 7
split: headings
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

In SQL, constraints can be specified for tables. Constraints enforce certain properties over data that is inserted into a table. Constraints can be specified along with the schema of the table as part of the [`CREATE TABLE` statement](../../sql/statements/create_table/index.md). Constraints can also be added to an existing table with the [`ALTER TABLE` statement](../../sql/statements/alter_table/index.md): `ADD CONSTRAINT` for `CHECK`, `UNIQUE`, `PRIMARY KEY` and `FOREIGN KEY` constraints, and `ALTER COLUMN ... SET NOT NULL`.

<DocCallout type="attention">

`PRIMARY KEY`, `UNIQUE` and `FOREIGN KEY` constraints guarantee integrity and cost time on every load and update; most analytical tables are better off without them. See the [Performance Guide](../../cookbook/performance/schema.md#constraints) for when to declare them.

</DocCallout>

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

## Check Constraint

Check constraints allow you to specify an arbitrary Boolean expression. Any columns that _do not_ satisfy this expression violate the constraint. For example, we could enforce that the `name` column does not contain spaces using the following `CHECK` constraint.

<SqlLogicTest id="sql/constraints/index/example_001" />

## Not Null Constraint

A not-null constraint specifies that the column cannot contain any `NULL` values. By default, all columns in tables are nullable. Adding `NOT NULL` to a column definition enforces that a column cannot contain `NULL` values.

<SqlLogicTest id="sql/constraints/index/example_002" />

## Primary Key and Unique Constraint

Primary key or unique constraints define a column, or set of columns, that are a unique identifier for a row in the table. The constraint enforces that the specified columns are _unique_ within a table, i.e., that at most one row contains the given values for the set of columns.

<SqlLogicTest id="sql/constraints/index/example_003" />

<SqlLogicTest id="sql/constraints/index/example_004" />

Primary key constraints and unique constraints are identical except for two points:

-   A table can only have one primary key constraint defined, but many unique constraints
-   A primary key constraint also enforces the keys to not be `NULL`.

<SqlLogicTest id="sql/constraints/index/example_005" />

<SqlLogicTest id="sql/constraints/index/example_006" />

You can also define a uniqueness constraint on multiple columns:

<SqlLogicTest id="sql/constraints/index/example_007" />

### Deferred Checking

A primary key or unique constraint is checked by every statement that changes the table. Write `DEFERRED` after `PRIMARY KEY` or `UNIQUE` to check it when the transaction commits instead: in between, the transaction may hold duplicate keys, and the commit fails if any are left. `IMMEDIATE`, the default, can be written out the same way. The PostgreSQL forms `DEFERRABLE`, `INITIALLY DEFERRED` and `SET CONSTRAINTS` are not supported.

<SqlLogicTest id="sql/constraints/index/example_009" />

The `constraint_check_mode` column of [`sdb_constraints()`](../../sql/functions/metadata.md#sdb_constraints) shows which of the two a constraint was declared with, and `DEFAULT` when it names neither:

<SqlLogicTest id="sql/constraints/index/example_010" />

## Foreign Keys

Foreign keys define a column, or set of columns, that refer to a primary key or unique constraint from _another_ table. The constraint enforces that the key exists in the other table.

<SqlLogicTest id="sql/constraints/index/example_008" />

Deleting or updating a row whose key is still referenced fails with `update or delete on table "⟨table⟩" violates foreign key constraint "⟨name⟩" on table "⟨referencing table⟩"`. Referential actions such as `ON DELETE CASCADE` are not supported; see [Foreign Key Constraints](../statements/create_table/index.md#foreign-key-constraints).
