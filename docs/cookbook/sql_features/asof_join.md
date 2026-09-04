---
layout: docu
redirect_from:
- /docs/guides/sql_features/asof_join
- /docs/preview/guides/sql_features/asof_join
- /docs/stable/guides/sql_features/asof_join
title: AsOf Join
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## What is an AsOf Join?

Time series data is not always perfectly aligned.
Clocks may be slightly off, or there may be a delay between cause and effect.
This can make connecting two sets of ordered data challenging.
AsOf joins are a tool for solving this and other similar problems.

One of the problems that AsOf joins are used to solve is
finding the value of a varying property at a specific point in time.
This use case is so common that it is where the name came from:

_Give me the value of the property **as of this time**_.

More generally, however, AsOf joins embody some common temporal analytic semantics,
which can be cumbersome and slow to implement in standard SQL.

## Portfolio Example Dataset

Let's start with a concrete example.
Suppose we have a table of stock `prices` with timestamps:

| ticker | when | price |
| :----- | :--- | ----: |
| APPL   | 2001-01-01 00:00:00 | 1 |
| APPL   | 2001-01-01 00:01:00 | 2 |
| APPL   | 2001-01-01 00:02:00 | 3 |
| MSFT   | 2001-01-01 00:00:00 | 1 |
| MSFT   | 2001-01-01 00:01:00 | 2 |
| MSFT   | 2001-01-01 00:02:00 | 3 |
| GOOG   | 2001-01-01 00:00:00 | 1 |
| GOOG   | 2001-01-01 00:01:00 | 2 |
| GOOG   | 2001-01-01 00:02:00 | 3 |

We have another table containing portfolio `holdings` at various points in time:

| ticker | when | shares |
| :----- | :--- | -----: |
| APPL   | 2000-12-31 23:59:30 | 5.16   |
| APPL   | 2001-01-01 00:00:30 | 2.94   |
| APPL   | 2001-01-01 00:01:30 | 24.13  |
| GOOG   | 2000-12-31 23:59:30 | 9.33   |
| GOOG   | 2001-01-01 00:00:30 | 23.45  |
| GOOG   | 2001-01-01 00:01:30 | 10.58  |
| DATA   | 2000-12-31 23:59:30 | 6.65   |
| DATA   | 2001-01-01 00:00:30 | 17.95  |
| DATA   | 2001-01-01 00:01:30 | 18.37  |

To load these tables to SereneDB, run:

<SqlLogicTest id="cookbook/sql_features/asof_join/example_001" />

## Inner AsOf Joins

We can compute the value of each holding at that point in time by finding
the most recent price before the holding's timestamp by using an AsOf Join:

<SqlLogicTest id="cookbook/sql_features/asof_join/example_002" />

This attaches the value of the holding at that time to each row.

It essentially executes a function defined by looking up nearby values in the `prices` table.
Note also that missing `ticker` values do not have a match and don't appear in the output.

## Outer AsOf Joins

Because AsOf produces at most one match from the right hand side,
the left side table will not grow as a result of the join,
but it could shrink if there are missing times on the right.
To handle this situation, you can use an *outer* AsOf Join:

<SqlLogicTest id="cookbook/sql_features/asof_join/example_003" />

As you might expect, this will produce `NULL` prices and values instead of dropping left side rows
when there is no ticker or the time is before the prices begin.

## AsOf Joins with the `USING` Keyword

So far we have been explicit about specifying the conditions for AsOf,
but SQL also has a simplified join condition syntax
for the common case where the column names are the same in both tables.
This syntax uses the `USING` keyword to list the fields that should be compared for equality.
AsOf also supports this syntax, but with two restrictions:

* The last field is the inequality
* The inequality is `>=` (the most common case)

Our first query can then be written as:

<SqlLogicTest id="cookbook/sql_features/asof_join/example_004" />

### Clarification on Column Selection with `USING` in ASOF Joins

When you use the `USING` keyword in a join, the columns specified in the `USING` clause are merged in the result set. This means that if you run:

<SqlLogicTest id="cookbook/sql_features/asof_join/example_005" />

You will get back only the columns `h.ticker, h.when, h.shares, p.price`. The columns `ticker` and `when` will appear only once, with `ticker`
and `when` coming from the left table (holdings).

This behavior is fine for the `ticker` column because the value is the same in both tables. However, for the `when` column, the values might
differ between the two tables due to the `>=` condition used in the AsOf join. The AsOf join is designed to match each row in the left
table (`holdings`) with the nearest preceding row in the right table (`prices`) based on the `when` column.

If you want to retrieve the `when` column from both tables to see both timestamps, you need to list the columns explicitly rather than
relying on `*`, like so:

<SqlLogicTest id="cookbook/sql_features/asof_join/example_006" />

This ensures that you get the complete information from both tables, avoiding any potential confusion caused by the default behavior of
the `USING` keyword.
