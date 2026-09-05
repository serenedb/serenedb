---
title: Prepared Statements
sidebar_position: 16
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


## Dropping Prepared Statements: `DEALLOCATE`

To drop a prepared statement, use the `DEALLOCATE` statement:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_008" />


Alternatively, use:

<SqlLogicTest id="sql/query_syntax/prepared_statements/example_009" />

