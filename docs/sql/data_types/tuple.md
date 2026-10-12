---
title: Tuple
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

| Name    | Description                                  |
| :------ | :------------------------------------------- |
| `TUPLE` | Fixed sequence of values without field names |

A `TUPLE` is a [`STRUCT`](./struct.md) whose fields have no names: each field is addressed by its position, starting at 1, and can have its own type. `TUPLE(INTEGER, VARCHAR)` holds an `INTEGER` and a `VARCHAR`. It is the counterpart of a PostgreSQL anonymous record.

## Creating Tuples

The `row` function returns a tuple of its arguments, and so does a parenthesized list of two or more values:

<SqlLogicTest id="sql/data_types/tuple/example_001" />

## Tuple Columns

Declare a column as `TUPLE(type, ...)` and fill it with `row(...)`. A field of a tuple can be `NULL` while the tuple itself is not:

<SqlLogicTest id="sql/data_types/tuple/example_002" />

Search tables (`CREATE TABLE … WITH (storage = 'search')`) take tuple columns too.

[`CREATE TYPE`](../statements/create_type/index.md) gives a tuple type a name:

<SqlLogicTest id="sql/data_types/tuple/example_003" />

## Accessing Fields

`t[i]` returns the field at position `i`, counting from 1, and is the same as `struct_extract(t, i)`. The position must be a constant, and a position outside the tuple is an error. A tuple has no field names, so dot notation and the struct functions that work with names, such as `struct_insert` and `struct_update`, don't apply to it.

<SqlLogicTest id="sql/data_types/tuple/example_004" />

## Casting

A tuple casts to a `STRUCT` with the same number of fields, and a `STRUCT` casts to a tuple, field by field in order: the names play no part. This is how `row(...)` fills a struct column, see [Creating Structs with the `row` Function](./struct.md#creating-structs-with-the-row-function). A cast between different numbers of fields fails.

<SqlLogicTest id="sql/data_types/tuple/example_005" />

## Comparison and Ordering

Tuples compare like structs, field by field in order; see [Comparison and Ordering](./struct.md#comparison-and-ordering).

## PostgreSQL Clients

Clients receive a tuple as PostgreSQL's `record` type, in the text form PostgreSQL gives a record: `(1,a)`, with a `NULL` field left empty. A cast to `VARCHAR` writes `(1, a)` instead. A tuple type made with `CREATE TYPE` reaches clients as that type: it is a composite type in `pg_type`, and `pg_attribute` names its fields `element1`, `element2` and so on.

<SqlLogicTest id="sql/data_types/tuple/example_006" />
