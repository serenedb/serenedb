---
layout: docu
redirect_from:
    - /docs/guides/meta/describe
    - /docs/preview/guides/meta/describe
    - /docs/stable/guides/meta/describe
title: Describe
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## Describing a Table

To view the schema of a table, use the `DESCRIBE` statement (or its aliases `DESC` and `SHOW`) followed by the table name.

<SqlLogicTest id="cookbook/meta/describe/table_description/example_001" />

## Describing a Query

To view the schema of the result of a query, prepend `DESCRIBE` to a query.

<SqlLogicTest id="cookbook/meta/describe/query_description/example_002" />

## Using `DESCRIBE` in a Subquery

`DESCRIBE` can be used as a subquery. This allows creating a table from the description, for example:

<SqlLogicTest id="cookbook/meta/describe/description_subquery/example_003" />

## Describing Remote Tables

It is possible to describe remote tables over HTTP(S) and S3 using the `DESCRIBE TABLE` statement. For example:

<SqlLogicTest id="cookbook/meta/describe/example_004" />
