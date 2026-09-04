---
layout: docu
redirect_from:
    - /dev/profiling
    - /docs/dev/profiling
    - /docs/preview/dev/profiling
    - /docs/stable/dev/profiling
    - /docs/contribution/profiling
    - /docs/guides/meta/explain_analyze
    - /docs/preview/guides/meta/explain_analyze
    - /docs/stable/guides/meta/explain_analyze
    - /docs/cookbook/meta/explain_analyze
    - /docs/cookbook/performance/explain_analyze
title: Profiling
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

Profiling is essential to help understand why certain queries exhibit specific performance characteristics.
SereneDB contains several built-in features to enable query profiling, which this page covers.
For a high-level example of using `EXPLAIN`, see the [“Inspect Query Plans” page](../meta/explain.md).

## Statements

### The `EXPLAIN` Statement

The first step to profiling a query can include examining the query plan.
The [`EXPLAIN`](../meta/explain.md) statement shows the query plan and describes what is going on under the hood.

### The `EXPLAIN ANALYZE` Statement

The query plan helps developers understand the performance characteristics of the query.
However, it is often also necessary to examine the performance numbers of individual operators and the cardinalities that pass through them.
Prepending a query with `EXPLAIN ANALYZE` both pretty-prints the query plan and executes it, providing run-time performance numbers for every operator, as well as the actual row counts flowing through each operator.

<SqlLogicTest id="cookbook/performance/explain_analyze/example_001" />

Note that the **cumulative** wall-clock time that is spent on every operator is shown. When multiple threads are processing the query in parallel, the total processing time of the query may be lower than the sum of all the times spent on the individual operators.

For brevity, the samples on this page omit the optimizer and planner timing breakdown that `EXPLAIN ANALYZE` prints between the total time and the operator tree.

For multi-file reads (e.g., reading multiple Parquet files), the output includes the file names being read.

Below is an example of running `EXPLAIN ANALYZE` on a join query. The output is a profiling tree showing the `Total Time`, the per-operator wall-clock timings and the actual row counts flowing through each operator (the exact timings vary between runs):

<SqlLogicTest id="cookbook/performance/explain_analyze/example_002" />

### The `FORMAT` Option

The `EXPLAIN [ANALYZE]` statement allows exporting to several formats:

-   `text` – default ASCII-art style output
-   `graphviz` – produces a DOT output, which can be rendered with [Graphviz](https://graphviz.org/)
-   `html` – produces an HTML output, which can be rendered with [treeflex](https://dumptyd.github.io/treeflex/)
-   `json` – produces a JSON output
-   `mermaid` – produces a [Mermaid](https://mermaid.js.org/) flowchart

To specify a format, use the `FORMAT` tag:

<SqlLogicTest id="cookbook/performance/profiling/example_001" />

## Pragmas

SereneDB supports several pragmas for turning profiling on and off and controlling the level of detail in the profiling output.

The following pragmas are available and can be set using either `PRAGMA` or `SET`.
They can also be reset using `RESET`, followed by the setting name.
For more information, see the [“Profiling”](../../configuration/pragmas.md#profiling) section of the pragmas page.

| Setting                                                                                                                                                                            | Description                                     | Default                                                  | Options                                                                                                                                                            |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| [`enable_profiling`](../../configuration/pragmas.md#enable-profiling), [`enable_profile`](../../configuration/pragmas.md#enable-profiling)     | Turn on profiling                               | `query_tree`                                             | `query_tree`, `json`, `query_tree_optimizer`, `no_output`                                                                                                          |
| [`profiling_coverage`](../../configuration/pragmas.md#profiling-coverage)                                                                                        | Set the operators to profile                    | `SELECT`                                                 | `SELECT`, `ALL`                                                                                                                                                    |
| [`profiling_output`](../../configuration/pragmas.md#profiling-output)                                                                                            | Set a profiling output file                     | Console                                                  | A filepath                                                                                                                                                         |
| [`profiling_mode`](../../configuration/pragmas.md#profiling-format)                                                                                                | Toggle additional optimizer and planner metrics | `standard`                                               | `standard`, `detailed`, `all`                                                                                                                                      |
| [`configure_profiling`](../../configuration/pragmas.md#profiling-format)                                                                                 | Enable or disable specific metrics              | All metrics except those activated by detailed profiling | A JSON object that matches the following: `{"METRIC_NAME": "boolean", ...}`. (List of all available metrics) |
| [`disable_profiling`](../../configuration/pragmas.md#disable-profiling), [`disable_profile`](../../configuration/pragmas.md#disable-profiling) | Turn off profiling                              |                                                          |                                                                                                                                                                    |

## Table Functions

SereneDB provides table functions to enable and disable profiling, consolidating multiple settings into a single call.

### `enable_profiling()`

The `enable_profiling()` function configures profiling with the specified options.

<SqlLogicTest id="cookbook/performance/profiling/example_002" />

| Parameter       | Type                      | Description                                                                      |
| --------------- | ------------------------- | -------------------------------------------------------------------------------- |
| `metrics`       | `LIST`, `STRUCT`, or JSON | Specifies which metrics to enable                                                |
| `mode`          | `VARCHAR`                 | Profiling level: `'standard'` or `'detailed'`                                    |
| `save_location` | `VARCHAR`                 | File path for profiling output                                                   |
| `coverage`      | `VARCHAR`                 | Query coverage: `'select'` or `'all'`                                            |
| `format`        | `VARCHAR`                 | Output format: `'query_tree'`, `'json'`, `'query_tree_optimizer'`, `'no_output'` |

All parameters are optional and named. You can also pass metrics as an unnamed parameter:

<SqlLogicTest id="cookbook/performance/profiling/example_003" />

### `disable_profiling()`

The `disable_profiling()` function turns off profiling.

<SqlLogicTest id="cookbook/performance/profiling/example_004" />

## Metrics

SereneDB supports a wide range of metrics that can be enabled or disabled independently.

## Detailed Profiling

When the `profiling_mode` is set to `detailed`, an extra set of metrics are enabled, which are only available in the `QUERY_ROOT` node.
These include all the metrics in the Phase timing metric group.
It is possible to toggle each of these additional metrics individually.

## Notation in Query Plans

In query plans, the [hash join](https://en.wikipedia.org/wiki/Hash_join) operators adhere to the following convention:
the _probe side_ of the join is the left operand, while the _build side_ is the right operand.

Join operators in the query plan show the join type used:

-   Inner joins are denoted as `INNER`.
-   Left outer joins and right outer joins are denoted as `LEFT` and `RIGHT`, respectively.
-   Full outer joins are denoted as `FULL`.
