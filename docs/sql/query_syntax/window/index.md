---
title: WINDOW
sidebar_position: 12
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';
import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `WINDOW` clause allows you to specify named windows that can be used within [window functions](../../functions/window_functions/index.md). These are useful when you have multiple window functions, as they allow you to avoid repeating the same window clause.

## Examples

The `WINDOW` clause defines a named window (`w`) once and reuses it across several window functions — here both `rank()` and `dense_rank()` share the same partitioning and ordering instead of repeating the `OVER (...)` clause:

<SqlLogicTest id="sql/query_syntax/window/index/example_001" />

## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
