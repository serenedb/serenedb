---
layout: docu
redirect_from:
- /docs/guides/meta/summarize
- /docs/preview/guides/meta/summarize
- /docs/stable/guides/meta/summarize
title: Summarize
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `SUMMARIZE` command can be used to easily compute a number of aggregates over a table or a query.
The `SUMMARIZE` command launches a query that computes a number of aggregates over all columns (`min`, `max`, `approx_unique`, `avg`, `std`, `q25`, `q50`, `q75`, `count`), and return these along the column name, column type, and the percentage of `NULL` values in the column.
Note that the quantiles and percentiles are **approximate values**.

## Usage

To summarize the contents of a table, use `SUMMARIZE` followed by the table name.

<SqlLogicTest id="cookbook/meta/summarize/example_001" />

To summarize a query, prepend `SUMMARIZE` to a query.

<SqlLogicTest id="cookbook/meta/summarize/example_002" />

## Example

Below is an example of `SUMMARIZE` on a sample `lineitem` table modeled on the TPC-H schema.

<SqlLogicTest id="cookbook/meta/summarize/example_003" />

<SqlLogicTest id="cookbook/meta/summarize/example_004" />

## Using `SUMMARIZE` in a Subquery

`SUMMARIZE` can be used as a subquery. This allows creating a table from the summary, for example:

<SqlLogicTest id="cookbook/meta/summarize/example_005" />

## Summarizing Remote Tables

It is possible to summarize remote tables over HTTP(S) and S3 using the `SUMMARIZE TABLE` statement. For example:

<SqlLogicTest id="cookbook/meta/summarize/example_006" />
