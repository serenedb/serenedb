---
title: VALUES
sidebar_position: 13
---

import RailroadDiagram from '@site/src/components/RailroadDiagram';
import RailroadSource from './diagram.js';

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `VALUES` clause is used to specify a fixed number of rows. The `VALUES` clause can be used as a stand-alone statement, as part of the `FROM` clause, or as input to an `INSERT INTO` statement.

## Examples

Generate two rows and directly return them:

<SqlLogicTest id="sql/query_syntax/values/index/example_001" />


Generate two rows as part of a `FROM` clause, and rename the columns:

<SqlLogicTest id="sql/query_syntax/values/index/example_002" />


Generate two rows and insert them into a table:

<SqlLogicTest id="sql/query_syntax/values/index/example_003" />


Create a table directly from a `VALUES` clause:

<SqlLogicTest id="sql/query_syntax/values/index/example_004" />


## Syntax

<RailroadDiagram source={RailroadSource} production="rrdiagram" />
