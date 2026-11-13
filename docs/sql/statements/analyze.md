---
title: ANALYZE
sidebar_position: 2
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `ANALYZE` statement recomputes the statistics of a table in an attached DuckDB database file. The statistics of SereneDB tables and their indexes follow their rows as they are written and compacted, so on them `ANALYZE` does no work and is accepted for PostgreSQL compatibility.

## Usage

`ANALYZE table_name` on a table of an attached DuckDB database recomputes the statistics used for join order optimization. Recompute them after large inserts or deletes to such a table for better join orders.

`ANALYZE` without a table name is accepted and does no work:

<SqlLogicTest id="sql/statements/analyze/example_001" />
