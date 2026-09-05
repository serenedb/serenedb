---
title: CASE Expression
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

The `CASE` expression performs a switch based on a condition. The basic form is identical to the ternary condition used in many programming languages (`CASE WHEN cond THEN a ELSE b END` is equivalent to `cond ? a : b`). With a single condition this can be expressed with `IF(cond, a, b)`.

<SqlLogicTest id="sql/expressions/case/index/example_001" />

This is equivalent to:

<SqlLogicTest id="sql/expressions/case/index/example_002" />

The `WHEN cond THEN expr` part of the `CASE` expression can be chained, whenever any of the conditions returns true for a single tuple, the corresponding expression is evaluated and returned.

<SqlLogicTest id="sql/expressions/case/index/example_003" />

The `ELSE` clause of the `CASE` expression is optional. If no `ELSE` clause is provided and none of the conditions match, the `CASE` expression will return `NULL`.

<SqlLogicTest id="sql/expressions/case/index/example_004" />

It is also possible to provide an individual expression after the `CASE` but before the `WHEN`. When this is done, the `CASE` expression is effectively transformed into a `switch` statement.

<SqlLogicTest id="sql/expressions/case/index/example_005" />

This is equivalent to:

<SqlLogicTest id="sql/expressions/case/index/example_006" />

## `SWITCH` Expression

The `SWITCH` expression is syntactic sugar for the `CASE` expression. It takes an expression, a [`MAP`](../../../sql/data_types/map.md) of values to results, and an optional default value.

<SqlLogicTest id="sql/expressions/case/index/example_007" />

A default value can be provided as the third argument, which is returned when none of the map keys match:

<SqlLogicTest id="sql/expressions/case/index/example_008" />
