---
title: DESCRIBE
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `DESCRIBE` statement shows the schema of a table, view or query.

## Usage

<SqlLogicTest id="sql/statements/describe/example_001" />

To describe a query, prepend `DESCRIBE` to a query.

<SqlLogicTest id="sql/statements/describe/example_002" />

## Alias

`SHOW TABLE <name>` returns the same as `DESCRIBE TABLE <name>`. A name after `SHOW` without the `TABLE` keyword is read as a setting instead: see [`SHOW`](show.md).

## See Also

For more examples, see the [guide on `DESCRIBE`](../../cookbook/meta/describe.md).
