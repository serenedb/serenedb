---
title: WHERE
sidebar_position: 3
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `WHERE` clause specifies any filters to apply to the data. This allows you to select only a subset of the data in which you are interested. Logically the `WHERE` clause is applied immediately after the `FROM` clause.

## Examples

Select all rows where the `id` is equal to 3:

<SqlLogicTest id="sql/query_syntax/where/index/example_001" />


Select all rows that match the given **case-sensitive** `LIKE` expression:

<SqlLogicTest id="sql/query_syntax/where/index/example_002" />


Select all rows that match the given **case-insensitive** expression formulated with the `ILIKE` operator:

<SqlLogicTest id="sql/query_syntax/where/index/example_003" />


Select all rows that match the given composite expression:

<SqlLogicTest id="sql/query_syntax/where/index/example_004" />


## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
