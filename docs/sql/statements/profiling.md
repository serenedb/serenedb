---
title: Profiling Queries
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB supports profiling queries via the `EXPLAIN` and `EXPLAIN ANALYZE` statements.

## `EXPLAIN`

To see the query plan of a query without executing it, run:

<SqlLogicTest id="sql/statements/profiling/example_001" hideResult />

The output of `EXPLAIN` contains the estimated cardinalities for each operator.

## `EXPLAIN ANALYZE`

To profile a query, run:

<SqlLogicTest id="sql/statements/profiling/example_002" hideResult />

The `EXPLAIN ANALYZE` statement runs the query, and shows the actual cardinalities for each operator,
as well as the cumulative wall-clock time spent in each operator.
