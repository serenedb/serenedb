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
split: page
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
-   `html` – produces an HTML output, which can be rendered with [treeflex](https://www.jsdelivr.com/package/npm/treeflex)
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
| [`tracked_metrics`](../../configuration/overview.md#logging-metrics-and-profiling) | Enable specific metrics | `[*]` | A list of metric glob patterns, e.g. `['query.*', 'optimizer.*']` |
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

## Inverted Index Scans

In `EXPLAIN ANALYZE` and in the profiling output, a scan over an inverted index has two boxes below it: `INDEX_PREPARE` and `INDEX_EXECUTE`. Like every box, each shows its own time: the scan's box keeps the time the scan spends outside the index, and the boxes below it the time the index spends on the query.

<SqlLogicTest id="cookbook/performance/profiling/example_005" />

`INDEX_PREPARE` is the time the index spends getting ready for the query, summed over threads:

| Line | Meaning |
|---|---|
| `Total` | The whole preparation, including the boxes below it |
| `Snapshot` | Opening the index for the query |
| `Segments` | How many index segments the query reads, of how many the index has: a segment none of whose rows can pass the scan's column filters is skipped |
| `Stats` | Gathering the statistics a ranking function needs |

Below it, one box per filter of the scan's `Index Filter` that the index used shows the time spent preparing that filter, such as looking up its terms or expanding a pattern. Each box shows how many posting lists it opened (`Posting Lists`), the index's estimate of the documents it matches (`Estimated Matches`) and, as its rows, how many documents the posting lists it opened hold. Filters over n-grams, geo shapes or vectors, and filters that match every row, leave `Posting Lists` out. A box's time is its own; `Total` adds the boxes below it.

`INDEX_EXECUTE` is the time spent choosing and running the index's execution plans (`Total`); its own time is choosing them. Below it, one box per kind of execution plan that ran shows how many times the scan chose it (`Plans`) and the time that took (`Plan`), how many times the scan called it (`Calls`) and how many documents it produced; its own time is running it. Both counts depend on how the scan's threads split the work. In a query that orders by a ranking function with a `LIMIT`, those are the documents it offered to the top results. The box names identify the plans the index chose and can change between releases.

A query over the term dictionary, such as `ts_dict_agg`, has one plan per segment: the one that finds the documents its `WHERE` matches, as the walk over the dictionary asks for them. Its box counts those documents as rows, and walking the dictionary is the scan's own time. Without a `WHERE`, and with no deleted rows, the counts come from the dictionary itself and `INDEX_EXECUTE` is left out.

The scan's own box adds `Filtered`, how many of the documents the index produced the scan's other filters dropped, and the time spent reading columns and computing the positions `ts_offsets` and `ts_highlight` return (`Fetch`) and finding the rows in the indexed table or view (`Lookup Time`).

`SET sdb_index_profile_nodes = true` adds boxes for the nested parts of each execution plan. Each shows the filter it runs (`Filter`), how many times its plan called it (`Calls`) and, as its rows, the documents it matched. A part that marks a window of documents at a time counts the documents it added to the window, or removed from it when it excludes them, so in an `OR` a part counts only the documents the parts before it had not marked. Their times are estimated from the calls they timed (`Timed`), about one in 32, and the query runs slower while the setting is on, so they compare the parts of a plan rather than measure them exactly. Parts that are called very often for very little work each time can add up to more than the time of the plan they belong to; the plan's box then shows no time of its own.

In the JSON output, each of these boxes is an operator whose `type` is the box's name.

Removing `operator.index_profile` from [`tracked_metrics`](../../configuration/overview.md#logging-metrics-and-profiling) leaves the index boxes out.

## Notation in Query Plans

In query plans, the [hash join](https://en.wikipedia.org/wiki/Hash_join) operators adhere to the following convention:
the _probe side_ of the join is the left operand, while the _build side_ is the right operand.

Join operators in the query plan show the join type used:

-   Inner joins are denoted as `INNER`.
-   Left outer joins and right outer joins are denoted as `LEFT` and `RIGHT`, respectively.
-   Full outer joins are denoted as `FULL`.

A `LOOKUP_JOIN` reads columns of rows its left input picked — after a `TOP_N`, a `LIMIT` or a `SAMPLE` — from the scan on its right. The right scan is not run as a scan: it reads only the rows the left input produces, and its row count in `EXPLAIN ANALYZE` is the number of rows it read. Its `Key` names the row ids it finds the rows by:

| Scan on the right | `Key` |
|---|---|
| Parquet files | `file_index, file_row_number` |
| DuckDB files (`read_duckdb`) | `file_index, rowid` |
| CSV and JSON files | `file_index, file_offset`, the byte offset of the row in its file |
| A PostgreSQL table | `ctid` |
| A table, or an inverted index on one | `rowid` |
| An inverted index | `iresearch_pk`, the row's address in the index |
| The source of an inverted index (`INDEX_SOURCE`) | the [row identity](../../sql/indexes/inverted/views.md#row-identity) the index stores, named as its source names it: `rowid` for a table, `ctid` for a PostgreSQL view, `file_index, file_row_number` for a Parquet glob |

A row that is no longer in the source produces no output row. It is used when the `TOP_N`, `LIMIT` or `SAMPLE` keeps at most [`late_materialization_lookup_max_rows`](../../configuration/overview.md#query-execution-and-the-optimizer) rows; `SET late_materialization_lookup_max_rows = 0` turns it off, and the plan then joins the scan with itself on the row ids (a `HASH_JOIN`), as long as the limit is at most `late_materialization_max_rows`. Scans of CSV and JSON files and of PostgreSQL tables use it only when the file or the [server](../../sql/statements/attach/postgres.md#remote-servers) is remote; a local one is read in one pass. Over an inverted index, two `LOOKUP_JOIN`s can sit one above the other: one reads the columns the index stores, the other reads the remaining columns from the indexed table or view (an `INDEX_SOURCE` on its right, keyed by the row identity the index stores).
