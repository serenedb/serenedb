---
title: CREATE SEQUENCE
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

The `CREATE SEQUENCE` statement creates a new sequence number generator.

## Examples

Generate an ascending sequence starting from 1:

<SqlLogicTest id="sql/statements/create_sequence/index/example_001" />

Generate sequence from a given start number:

<SqlLogicTest id="sql/statements/create_sequence/start_value/example_002" />

Generate odd numbers using `INCREMENT BY`:

<SqlLogicTest id="sql/statements/create_sequence/increment_by/example_003" />

Descending sequences are not yet supported. A negative `INCREMENT BY` is rejected:

<SqlLogicTest id="sql/statements/create_sequence/index/example_004" />

By default, cycles are not allowed. Once an ascending sequence reaches its `MAXVALUE`, the next call to `nextval` returns an error:

<SqlLogicTest id="sql/statements/create_sequence/maxvalue/example_005" />

`CYCLE` allows cycling through the same sequence repeatedly. Once the limit is reached, the next value wraps back to the start:

<SqlLogicTest id="sql/statements/create_sequence/cycle/example_006" />

### Creating and Dropping Sequences

Sequences can be created and dropped similarly to other catalog items.

Overwrite an existing sequence:

<SqlLogicTest id="sql/statements/create_sequence/or_replace/example_007" />

Only create sequence if no such sequence exists yet:

<SqlLogicTest id="sql/statements/create_sequence/index/example_008" />

Remove sequence:

<SqlLogicTest id="sql/statements/create_sequence/index/example_009" />

Remove sequence if exists:

<SqlLogicTest id="sql/statements/create_sequence/index/example_010" />

### Using Sequences for Primary Keys

Sequences can be used as `DEFAULT` values in [`CREATE TABLE` statements](../../statements/create_table/index.md).

The example below uses a sequence to create an integer [primary key](../../constraints/index.md#primary-key-and-unique-constraint):

<SqlLogicTest id="sql/statements/create_sequence/primary_key_default/example_011" />

You can also add a sequence-backed column to an existing table with `ALTER TABLE ... ADD COLUMN ... DEFAULT nextval(...)`. Existing rows are backfilled from the sequence and subsequent inserts continue from it:

<SqlLogicTest id="sql/statements/create_sequence/alter_table_default/example_012" />

### Selecting the Next Value

To select the next number from a sequence, use `nextval`:

<SqlLogicTest id="sql/statements/create_sequence/nextval_usage/example_013" />

Using this sequence in an `INSERT` command:

<SqlLogicTest id="sql/statements/create_sequence/insert_nextval/example_014" />

### Selecting the Current Value

You may also view the current number from the sequence. Note that the `nextval` function must have already been called before calling `currval`, otherwise a Serialization Error (`sequence is not yet defined in this session`) will be thrown.

<SqlLogicTest id="sql/statements/create_sequence/currval/example_015" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

`CREATE SEQUENCE` creates a new sequence number generator.

If a schema name is given then the sequence is created in the specified schema. Otherwise it is created in the current schema. Temporary sequences exist in a special schema, so a schema name may not be given when creating a temporary sequence. The sequence name must be distinct from the name of any other sequence in the same schema.

After a sequence is created, you use the function `nextval` to operate on the sequence.

## Parameters

| Name                  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| :-------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CYCLE` or `NO CYCLE` | The `CYCLE` option allows the sequence to wrap around when the `maxvalue` or `minvalue` has been reached by an ascending or descending sequence respectively. If the limit is reached, the next number generated will be the `minvalue` or `maxvalue`, respectively. If `NO CYCLE` is specified, any calls to `nextval` after the sequence has reached its maximum value will return an error. If neither `CYCLE` nor `NO CYCLE` are specified, `NO CYCLE` is the default. |
| `increment`           | The optional clause `INCREMENT BY increment` specifies which value is added to the current sequence value to create a new value. A positive value will make an ascending sequence, a negative one a descending sequence. The default value is 1.                                                                                                                                                                                                                           |
| `maxvalue`            | The optional clause `MAXVALUE maxvalue` determines the maximum value for the sequence. If this clause is not supplied or `NO MAXVALUE` is specified, then default values will be used. The defaults are 2^63 - 1 and -1 for ascending and descending sequences, respectively.                                                                                                                                                                                              |
| `minvalue`            | The optional clause `MINVALUE minvalue` determines the minimum value a sequence can generate. If this clause is not supplied or `NO MINVALUE` is specified, then defaults will be used. The defaults are 1 and -(2^63 - 1) for ascending and descending sequences, respectively.                                                                                                                                                                                           |
| `name`                | The name (optionally schema-qualified) of the sequence to be created.                                                                                                                                                                                                                                                                                                                                                                                                      |
| `start`               | The optional clause `START WITH start` allows the sequence to begin anywhere. The default starting value is `minvalue` for ascending sequences and `maxvalue` for descending ones.                                                                                                                                                                                                                                                                                         |
| `TEMPORARY` or `TEMP` | If specified, the sequence object is created only for this session, and is automatically dropped on session exit. Existing permanent sequences with the same name are not visible (in this session) while the temporary sequence exists, unless they are referenced with schema-qualified names.                                                                                                                                                                           |

<DocCallout type="tip">
Sequences are based on `BIGINT` arithmetic, so the range cannot exceed the range of an eight-byte integer (-9223372036854775808 to 9223372036854775807).
</DocCallout>

## Limitations

When a table column uses a sequence as its `DEFAULT`, the column keeps a dependency on that sequence. The default can be changed with `ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` — here it is reset to `NULL`, so subsequent rows no longer draw from the sequence:

<SqlLogicTest id="sql/statements/create_sequence/drop_dependency/example_016" />

While a table column still depends on the sequence through a `DEFAULT nextval(...)` expression, attempting to drop the sequence results in an error:

<SqlLogicTest id="sql/statements/create_sequence/drop_dependency_restrict/example_017" />

As the error message suggests, you can force dropping by adding `CASCADE`. This removes the dependency and the column default, but keeps the table and its existing data:

<SqlLogicTest id="sql/statements/create_sequence/drop_dependency_cascade/example_018" />
