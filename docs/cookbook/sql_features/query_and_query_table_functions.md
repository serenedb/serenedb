---
layout: docu
redirect_from:
    - /docs/guides/sql_features/query_and_query_table_functions
    - /docs/preview/guides/sql_features/query_and_query_table_functions
    - /docs/stable/guides/sql_features/query_and_query_table_functions
title: query and query_table Functions
sidebar_label: Query and Query Table Functions
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The [`query_table`](../../sql/functions/utility.md#query_tabletbl_name)
and [`query`](../../sql/functions/utility.md#queryquery_string)
functions enable powerful and more dynamic SQL.

The `query_table` function returns the table whose name is specified by its string argument; the `query` function returns the table obtained by executing the query specified by its string argument.

Both functions only accept constant strings. For example, they allow passing in a table name as a prepared statement parameter:

<SqlLogicTest id="cookbook/sql_features/query_and_query_table_functions/example_001" />

When combined with the [`COLUMNS` expression](../../sql/expressions/star/index.md#columns-expression), we can write very generic SQL-only macros. For example, below is a custom version of `SUMMARIZE` that computes the `min` and `max` of every column in a table:

<SqlLogicTest id="cookbook/sql_features/query_and_query_table_functions/example_002" />

The `query` function allows for even more flexibility. For example, users who prefer pandas' `stack` syntax over SQL's `UNPIVOT` syntax, may use:

<SqlLogicTest id="cookbook/sql_features/query_and_query_table_functions/example_003" />
