---
title: HAVING
sidebar_position: 6
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `HAVING` clause can be used after the `GROUP BY` clause to provide filter criteria _after_ the grouping has been completed. In terms of syntax the `HAVING` clause is identical to the `WHERE` clause, but while the `WHERE` clause occurs before the grouping, the `HAVING` clause occurs after the grouping.

## Examples

Count the number of entries in the `addresses` table that belong to each different `city`, filtering out cities with a count below 50:

<SqlLogicTest id="sql/query_syntax/having/index/example_001" />


Compute the average income per city per `street_name`, filtering out cities with an average `income` bigger than twice the median `income`:

<SqlLogicTest id="sql/query_syntax/having/index/example_002" />


## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
