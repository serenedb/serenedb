---
title: QUALIFY
sidebar_position: 12
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `QUALIFY` clause is used to filter the results of [`WINDOW` functions](../../functions/window_functions/index.md). This filtering of results is similar to how a [`HAVING` clause](../../query_syntax/having/index.md) filters the results of aggregate functions applied based on the [`GROUP BY` clause](../../query_syntax/groupby/index.md).

The `QUALIFY` clause avoids the need for a subquery or [`WITH` clause](../../query_syntax/with/index.md) to perform this filtering (much like `HAVING` avoids a subquery). An example using a `WITH` clause instead of `QUALIFY` is included below the `QUALIFY` examples.

Note that this is filtering based on [`WINDOW` functions](../../functions/window_functions/index.md), not necessarily based on the [`WINDOW` clause](../../query_syntax/window/index.md). The `WINDOW` clause is optional and can be used to simplify the creation of multiple `WINDOW` function expressions.

The position of where to specify a `QUALIFY` clause is following the [`WINDOW` clause](../../query_syntax/window/index.md) in a `SELECT` statement (`WINDOW` does not need to be specified), and before the [`ORDER BY`](../../query_syntax/orderby/index.md).

## Examples

Each of the following examples produces the same output.

Filter based on a window function defined in the `QUALIFY` clause:

<SqlLogicTest id="sql/query_syntax/qualify/index/example_001" />


Filter based on a window function defined in the `SELECT` clause:

<SqlLogicTest id="sql/query_syntax/qualify/index/example_002" />


Filter based on a window function defined in the `QUALIFY` clause, but using the `WINDOW` clause:

<SqlLogicTest id="sql/query_syntax/qualify/index/example_003" />


Filter based on a window function defined in the `SELECT` clause, but using the `WINDOW` clause:

<SqlLogicTest id="sql/query_syntax/qualify/index/example_004" />


Equivalent query based on a `WITH` clause (without a `QUALIFY` clause):

<SqlLogicTest id="sql/query_syntax/qualify/index/example_005" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
