---
title: Logical Operators
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';
import SqlLogicTest from "@site/src/components/SqlLogicTest";

<RailroadDiagram source={RailroadSource} production="rrdiagram" />

The following logical operators are available: `AND`, `OR` and `NOT`. SQL uses a three-valued logic system with `true`, `false` and `NULL`. Note that logical operators involving `NULL` do not always evaluate to `NULL`. For example, `NULL AND false` will evaluate to `false`, and `NULL OR true` will evaluate to `true`. Below are the complete truth tables.

## Binary Operators: `AND` and `OR`

<SqlLogicTest id="sql/expressions/logical_operators/index/example_001" />

## Unary Operator: `NOT`

<SqlLogicTest id="sql/expressions/logical_operators/index/example_002" />

The operators `AND` and `OR` are commutative, that is, you can switch the left and right operand without affecting the result.
