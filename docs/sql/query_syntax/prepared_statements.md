---
title: Prepared Statements
sidebar_position: 16
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports prepared statements where parameters are substituted when the query is executed.
This can improve readability and is useful for preventing [SQL injections](https://en.wikipedia.org/wiki/SQL_injection).

## Syntax

There are three syntaxes for denoting parameters in prepared statements:
auto-incremented (`?`),
positional (`$1`),
and named (`$param`).
Note that not all clients support all of these syntaxes, e.g., the [JDBC client](../../clients/java.md) only supports auto-incremented parameters in prepared statements.

### Example Dataset

In the following, we introduce the three different syntaxes and illustrate them with examples using the following table.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_001" />


In our example query, we'll look for people whose name starts with a `B` and are at least 40 years old.
This will return a single row `<'Bob', 41>`.

### Auto-Incremented Parameters: `?`

SereneDB supports using prepared statements with auto-incremented indexing,
i.e., the position of the parameters in the query corresponds to their position in the execution statement.
For example:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_002" />


Using the CLI client, the statement is executed as follows.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_003" />


### Positional Parameters: `$1`

Prepared statements can use positional parameters, where parameters are denoted with an integer (`$1`, `$2`).
For example:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_004" />


Using the CLI client, the statement is executed as follows.
Note that the first parameter corresponds to `$1`, the second to `$2`, and so on.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_005" />


### Named Parameters: `$parameter`

SereneDB also supports named parameters where parameters are denoted with `$parameter_name`.
For example:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_006" />


Using the CLI client, the statement is executed as follows.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_007" />


### Declared Parameter Types

`PREPARE name(type, ...)` declares the types of the positional parameters `$1`, `$2`, and so on. `EXECUTE` converts each argument to the declared type and fails when an argument cannot be converted. Declared parameters count toward the arguments `EXECUTE` expects, even when the query does not use them, and `EXECUTE` with a different number of arguments fails. Parameters without a declared type get their type from the query.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_011" />


## Plans

An execution of a prepared statement skips parsing. A statement that reads only [inverted indexes](../indexes/inverted/index.md) and search tables (`storage = 'search'`) also keeps its plan between executions: it is planned again only by the first execution after an index it reads has changed, for example after [a refresh](../indexes/inverted/maintenance.md#visibility-and-the-refresh-model) made new rows searchable, and that execution's plan is kept in turn. A statement that reads a regular table is planned on every execution.

The values the index takes over are read at execution, so one plan serves them all: the query vector, a full-text query, the conditions the index answers, `LIMIT` and `OFFSET`. A parameter in a condition the index cannot answer, one that is itself an operand of `AND` or `OR`, and any parameter of a statement that calls `ts_dict_agg` or `ts_offsets` is planned with its value, on every execution.

An `EXECUTE` whose values do not have the parameters' types plans the statement again for that execution: `[1, 1]` is an `INTEGER[]` and `2` an `INTEGER`. Declare the parameters with those types or cast the values; client drivers that send typed parameters are not affected.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_010" />


When a change to a table a statement reads changes the statement's result columns, for example a column added to the table of a `SELECT *`, `EXECUTE` fails with `cached plan must not change result type` (SQLSTATE `0A000`); prepare the statement again. A statement whose parameter types are only known from the arguments of each `EXECUTE` is not affected.

## Lifetime

A prepared statement belongs to the session that prepared it and lives until it is dropped or the session ends. `PREPARE` and `DEALLOCATE` take effect immediately: `ROLLBACK` does not undo them. A name can be prepared only once; preparing it again fails until the statement is dropped:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_012" />


Names follow the rules for identifiers: an unquoted name is folded to lower case and a quoted name keeps its case, so `foo`, `FOO` and `"foo"` name one statement and `"Foo"` another.

Statements a client prepares through the extended query protocol share these names, exactly as the client sends them: `EXECUTE` runs them, `DEALLOCATE` drops them, and `PREPARE` cannot reuse their names. `EXECUTE` fails with SQLSTATE `0A000` for such a statement when it is a `COPY ... FROM STDIN`, a `COPY ... TO STDOUT`, an empty query, or an `ALTER TABLE ... ADD COLUMN` whose default is volatile.

## Listing Prepared Statements

The `pg_catalog.pg_prepared_statements` view lists the prepared statements of the session: the name, the text that prepared it, when it was prepared, the parameter and result types, and `from_sql`, which is `true` for statements prepared with `PREPARE` and `false` for named statements prepared through the extended query protocol. `generic_plans` and `custom_plans` are always `NULL`.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_013" />


## Dropping Prepared Statements: `DEALLOCATE`

To drop a prepared statement, use the `DEALLOCATE` statement:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_008" />


Alternatively, use:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_009" />


`DEALLOCATE ALL` drops every prepared statement of the session, including the named statements prepared through the extended query protocol. [`DISCARD ALL`](../statements/discard.md) drops them too.

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_014" />

