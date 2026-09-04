---
layout: docu
redirect_from:
- /docs/guides/performance/join_operations
- /docs/preview/guides/performance/join_operations
- /docs/stable/guides/performance/join_operations
title: Join Operations
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

## How to Force a Join Order

SereneDB has a cost-based query optimizer, which uses statistics in the base tables (stored in a SereneDB database or Parquet files) to estimate the cardinality of operations.

### Turn off the Join Order Optimizer

To turn off the join order optimizer, set the following [`PRAGMA`s](../../configuration/pragmas.md):

<SqlLogicTest id="cookbook/performance/join_operations/example_001" />

This disables both the join order optimizer and left/right swapping for joins.
This way, SereneDB builds a left-deep join tree following the order of `JOIN` clauses.

<SqlLogicTest id="cookbook/performance/join_operations/example_002" />

Once the query in question has been executed, turn back the optimizers with the following command:

<SqlLogicTest id="cookbook/performance/join_operations/example_003" />

### Create Temporary Tables

To force a particular join order, you can break up the query into multiple queries, with each creating a temporary table:

<SqlLogicTest id="cookbook/performance/join_operations/example_004" />

To clean up, drop the interim tables:

<SqlLogicTest id="cookbook/performance/join_operations/example_005" />
