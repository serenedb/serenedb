---
title: Range Queries
sidebar_position: 2
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Range Queries

Match terms that are above, below, or between reference values using lexicographic comparison. Useful for filtering categories, tags, or any indexed string values by order.

See [Setup](./index.md#setup) for the shared dataset used in all examples.

## Greater than

Find genres that come after `'drama'` lexicographically:

<SqlLogicTest id="cookbook/search/range-queries/example_001" />

## Less than or equal

Find genres up to and including `'comedy'`:

<SqlLogicTest id="cookbook/search/range-queries/example_002" />

## Combining range conditions

Use `AND` to define a range:

<SqlLogicTest id="cookbook/search/range-queries/example_003" />

## Negation with range

Find everything outside a range:

<SqlLogicTest id="cookbook/search/range-queries/example_004" />

## Combine with phrase search

Range query on genre plus full-text search on description:

<SqlLogicTest id="cookbook/search/range-queries/example_005" />

## See also

- [Exact Value Matching](./exact-value-matching.md) — equality and IN queries
- [Wildcard Search](./wildcard-search.md) — pattern matching
