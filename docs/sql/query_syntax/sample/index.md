---
title: SAMPLE
sidebar_position: 9
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `SAMPLE` clause allows you to run the query on a sample from the base table. This can significantly speed up processing of queries, at the expense of accuracy in the result. Samples can also be used to quickly see a snapshot of the data when exploring a dataset. The sample clause is applied right after anything in the `FROM` clause (i.e., after any joins, but before the `WHERE` clause or any aggregates). See the [`SAMPLE`](../../samples/index.md) page for more information.

## Examples

Select a sample of 1% of the addresses table using default (system) sampling:

<SqlLogicTest id="sql/query_syntax/sample/index/example_001" />


Select a sample of 1% of the addresses table using bernoulli sampling:

<SqlLogicTest id="sql/query_syntax/sample/index/example_002" />


Select a sample of 10 rows from the subquery:

<SqlLogicTest id="sql/query_syntax/sample/index/example_003" />


## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
