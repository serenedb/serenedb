---
title: ANALYZE
sidebar_position: 2
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The `ANALYZE` statement recomputes the statistics on SereneDB's tables.

## Usage

The statistics recomputed by the `ANALYZE` statement are only used for join order optimization. It is therefore recommended to recompute these statistics for improved join orders, especially after performing large updates (inserts and/or deletes).

To recompute the statistics, run:

<SqlLogicTest id="sql/statements/analyze/example_001" />

## Privileges

Analyzing a table needs the `MAINTAIN` privilege on it or ownership of it; reading the table is not required. For a table the role may not maintain, `ANALYZE` emits the warning `permission denied to analyze "<table>", skipping it` and changes nothing. `VACUUM` follows the same rule.
