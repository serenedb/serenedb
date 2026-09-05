---
title: LIMIT / OFFSET
sidebar_position: 8
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

`LIMIT` is an output modifier. Logically it is applied at the very end of the query. The `LIMIT` clause restricts the amount of rows fetched. The `OFFSET` clause indicates at which position to start reading the values, i.e., the first `OFFSET` values are ignored.

Note that while `LIMIT` can be used without an `ORDER BY` clause, the results might not be deterministic without the `ORDER BY` clause. This can still be useful, however, for example when you want to inspect a quick snapshot of the data.

## Examples

Select the first 5 rows from the addresses table:

<SqlLogicTest id="sql/query_syntax/limit/index/example_001" />


Select the 5 rows from the addresses table, starting at position 5 (i.e., ignoring the first 5 rows):

<SqlLogicTest id="sql/query_syntax/limit/index/example_002" />


Select the top 5 cities with the highest population:

<SqlLogicTest id="sql/query_syntax/limit/index/example_003" />


Select 10% of the rows from the addresses table:

<SqlLogicTest id="sql/query_syntax/limit/index/example_004" />


## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
