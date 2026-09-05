---
title: WITH
sidebar_position: 11
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `WITH` clause allows you to specify common table expressions (CTEs).
Regular (non-recursive) common-table-expressions are essentially views that are limited in scope to a particular query.
CTEs can reference each other and can be nested. [Recursive CTEs](#recursive-ctes) can reference themselves.

## Basic CTE Examples

Create a CTE called `cte` and use it in the main query:

<SqlLogicTest id="sql/query_syntax/with/index/example_001" />

Create two CTEs `cte1` and `cte2`, where the second CTE references the first CTE:

<SqlLogicTest id="sql/query_syntax/with/index/example_002" />

You can specify column names for CTEs:

<SqlLogicTest id="sql/query_syntax/with/index/example_003" />

## CTE Materialization

SereneDB handles CTEs as _materialized_ by default, meaning that the CTE is evaluated
once and the result is stored in a temporary table. However, under certain conditions,
SereneDB can _inline_ the CTE into the main query, which means that the CTE is not
materialized and its definition is duplicated in each place it is referenced.
Inlining is done using the following heuristics:

-   The CTE is not referenced more than once.
-   The CTE does not contain a `VOLATILE` function.
-   The CTE is using `AS NOT MATERIALIZED` and does not use `AS MATERIALIZED`.
-   The CTE does not perform a grouped aggregation.

Materialization can be explicitly activated by defining the CTE using `AS MATERIALIZED` and disabled by using `AS NOT MATERIALIZED`. Note that inlining is not always possible, even if the heuristics are met. For example, if the CTE contains a `read_csv` function, it cannot be inlined.

Take the following query for example, which invokes the same CTE three times:

<SqlLogicTest id="sql/query_syntax/with/index/example_004" hideResult />

Inlining duplicates the definition of `t` for each reference which results in the following query:

<SqlLogicTest id="sql/query_syntax/with/index/example_005" hideResult />

If the CTE body is expensive, materializing it with the `MATERIALIZED` keyword can improve performance. In this case, the CTE body is evaluated only once.

<SqlLogicTest id="sql/query_syntax/with/index/example_006" hideResult />

If one wants to disable materialization, use `NOT MATERIALIZED`:

<SqlLogicTest id="sql/query_syntax/with/index/example_007" hideResult />

Generally, it is not recommended to use explicit materialization hints, as SereneDB's query optimizer is capable of deciding when to materialize or inline a CTE based on the query structure and the heuristics mentioned above. However, in some cases, it may be beneficial to use `MATERIALIZED` or `NOT MATERIALIZED` to control the behavior explicitly.

## Recursive CTEs

`WITH RECURSIVE` allows the definition of CTEs which can refer to themselves. Note that the query must be formulated in a way that ensures termination, otherwise, it may run into an infinite loop.

### Example: Fibonacci Sequence

`WITH RECURSIVE` can be used to make recursive calculations. For example, here is how `WITH RECURSIVE` could be used to calculate the first ten Fibonacci numbers:

<SqlLogicTest id="sql/query_syntax/with/index/example_008" />

### Example: Tree Traversal

`WITH RECURSIVE` can be used to traverse trees. For example, take a hierarchy of tags:

<img src="/images/examples/with-recursive-tree-example-light.svg" alt="Example graph" style={{width: "700px", textAlign: "center"}} class="lightmode-img"/>
<img src="/images/examples/with-recursive-tree-example-dark.svg" alt="Example graph" style={{width: "700px", textAlign: "center"}} class="darkmode-img"/>

<SqlLogicTest id="sql/query_syntax/with/index/example_009" />

The following query returns the path from the node `Oasis` to the root of the tree (`Art`).

<SqlLogicTest id="sql/query_syntax/with/index/example_010" />

### Graph Traversal

The `WITH RECURSIVE` clause can be used to express graph traversal on arbitrary graphs. However, if the graph has cycles, the query must perform cycle detection to prevent infinite loops.
One way to achieve this is to store the path of a traversal in a [list](../../data_types/list.md) and, before extending the path with a new edge, check whether its endpoint has been visited before (see the example later).

Take the following directed graph from the [LDBC Graphalytics benchmark](https://arxiv.org/pdf/2011.15028.pdf):

<img src="/images/examples/with-recursive-graph-example-light.svg" alt="Example graph" style={{width: "700px", textAlign: "center"}} class="lightmode-img"/>
<img src="/images/examples/with-recursive-graph-example-dark.svg" alt="Example graph" style={{width: "700px", textAlign: "center"}} class="darkmode-img"/>

<SqlLogicTest id="sql/query_syntax/with/index/example_011" />

Note that the graph contains directed cycles, e.g., between nodes 1, 5 and 8.

#### Enumerate All Paths from a Node

The following query returns **all paths** starting in node 1:

<SqlLogicTest id="sql/query_syntax/with/index/example_012" />

Note that the result of this query is not restricted to shortest paths, e.g., for node 5, the results include paths `[1, 5]` and `[1, 3, 5]`.

#### Enumerate Unweighted Shortest Paths from a Node

In most cases, enumerating all paths is not practical or feasible. Instead, only the **(unweighted) shortest paths** are of interest. To find these, the second half of the `WITH RECURSIVE` query should be adjusted such that it only includes a node if it has not yet been visited. This is implemented by using a subquery that checks if any of the previous paths includes the node:

<SqlLogicTest id="sql/query_syntax/with/index/example_013" />

#### Enumerate Unweighted Shortest Paths between Two Nodes

`WITH RECURSIVE` can also be used to find **all (unweighted) shortest paths between two nodes**. To ensure that the recursive query is stopped as soon as we reach the end node, we use a [window function](../../functions/window_functions/index.md) which checks whether the end node is among the newly added nodes.

The following query returns all unweighted shortest paths between nodes 1 (start node) and 8 (end node):

<SqlLogicTest id="sql/query_syntax/with/index/example_014" />

### Accessing the Union Table with `recurring`

Within the recursive term of a `WITH RECURSIVE` CTE, the CTE name (e.g., `counter`) refers to the rows produced by the _last iteration_. To access _all rows accumulated so far_ (the union table), use the `recurring` schema prefix:

<SqlLogicTest id="sql/query_syntax/with/index/example_015" />

Here, `recurring.counter` gives access to all rows accumulated across all previous iterations, while `counter` in the `FROM` clause only contains the rows from the most recent iteration. This is useful when termination conditions or calculations depend on the full accumulated result rather than just the previous iteration.

## Recursive CTEs with `USING KEY`

`USING KEY` alters the behavior of a regular recursive CTE.

In each iteration, a regular recursive CTE appends result rows to the union table, which ultimately defines the overall result of the CTE. In contrast, a CTE with `USING KEY` has the ability to update rows that have been placed in the union table in an earlier iteration: if the current iteration produces a row with key `k`, it replaces a row with the same key `k` in the union table (like a dictionary). If no such row exists in the union table yet, the new row is appended to the union table as usual.

This allows a CTE to exercise fine-grained control over the union table contents. Avoiding the append-only behavior can lead to significantly smaller union table sizes. This helps query runtime, memory consumption, and makes it feasible to access the union table while the iteration is still ongoing. In a CTE `WITH RECURSIVE T(...) USING KEY ...`, table `T` denotes the rows added by the last iteration (as is usual for recursive CTEs), while table `recurring.T` denotes the [union table built so far](#accessing-the-union-table-with-recurring). References to `recurring.T` allow for the elegant and idiomatic translation of rather complex algorithms into readable SQL code.

### Example: `USING KEY`

This is a recursive CTE where `USING KEY` has a key column (`a`) and a payload column (`b`).
The payload columns correspond to the columns to be overwritten.
In the first iteration we have two different keys, `1` and `2`.
These two keys will generate two new rows, `(1, 3)` and `(2, 4)`.
In the next iteration we produce a new key, `3`, which generates a new row.
We also generate the row `(2, 3)`, where `2` is a key that already exists from the previous iteration.
This will overwrite the old payload `4` with the new payload `3`.

<SqlLogicTest id="sql/query_syntax/with/index/example_017" />

## Using `VALUES`

You can use the `VALUES` clause for the initial (anchor) part of the CTE:

<SqlLogicTest id="sql/query_syntax/with/index/example_018" />

### Example: `USING KEY` References Union Table

As well as using the union table as a dictionary, we can now reference it in queries. This allows you to use results from not just the previous iteration, but also earlier ones. This new feature makes certain algorithms easier to implement.

One example is the connected components algorithm. For each node, the algorithm determines the node with the lowest ID to which it is connected. To achieve this, we use the entries in the union table to track the lowest ID found for a node. If a new incoming row contains a lower ID, we update this value.

<img src="/images/examples/using-key-graph-example-light.svg" alt="Example graph" style={{width: "700px", textAlign: "center"}} class="lightmode-img"/>
<img src="/images/examples/using-key-graph-example-dark.svg" alt="Example graph" style={{width: "700px", textAlign: "center"}} class="darkmode-img"/>

<SqlLogicTest id="sql/query_syntax/with/index/example_019" />

<SqlLogicTest id="sql/query_syntax/with/index/example_020" />

## Limitations

SereneDB does not support mutually recursive CTEs.

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
