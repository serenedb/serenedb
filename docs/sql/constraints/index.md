---
title: Constraints
sidebar_position: 7
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

In SQL, constraints can be specified for tables. Constraints enforce certain properties over data that is inserted into a table. Constraints can be specified along with the schema of the table as part of the [`CREATE TABLE` statement](../../sql/statements/create_table/index.md). In certain cases, constraints can also be added to a table using the [`ALTER TABLE` statement](../../sql/statements/alter_table/index.md), but this is not currently supported for all constraints.

<DocCallout type="attention">

Constraints have a strong impact on performance: they slow down loading and updates but speed up certain queries. Please consult the [Performance Guide](../../cookbook/performance/schema.md#constraints) for details.

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

To enforce this property efficiently, an [ART index is automatically created](../../sql/indexes/index.md) for every primary key or unique constraint that is defined in the table.

Primary key constraints and unique constraints are identical except for two points:

-   A table can only have one primary key constraint defined, but many unique constraints
-   A primary key constraint also enforces the keys to not be `NULL`.

<SqlLogicTest id="sql/constraints/index/example_005" />

<SqlLogicTest id="sql/constraints/index/example_006" />

<DocCallout type="attention">

Indexes have certain limitations that might result in constraints being evaluated too eagerly, leading to constraint errors such as `violates primary key constraint` and `violates unique constraint`. See the [ART index limitations](../../sql/indexes/art.md#limitations) for more details.

</DocCallout>

You can also define a uniqueness constraint on multiple columns:

<SqlLogicTest id="sql/constraints/index/example_007" />

## Foreign Keys

Foreign keys define a column, or set of columns, that refer to a primary key or unique constraint from _another_ table. The constraint enforces that the key exists in the other table.

<SqlLogicTest id="sql/constraints/index/example_008" />

To enforce this property efficiently, an [ART index is automatically created](../../sql/indexes/index.md) for every foreign key constraint that is defined in the table.

<DocCallout type="attention">

Indexes have certain limitations that might result in constraints being evaluated too eagerly, leading to constraint errors such as `violates primary key constraint` and `violates unique constraint`. See the [ART index limitations](../../sql/indexes/art.md#limitations) for more details.

</DocCallout>
