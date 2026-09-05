---
title: Exact Value Matching
sidebar_position: 1
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Exact Value Matching

Match precise token values in indexed columns. This is the simplest form of search — find documents that contain an exact term.

See [Setup](./index.md#setup) for the shared dataset used in all examples.

## Match a single value

Match a single exact token with the `@@` operator and a bare term:

<SqlLogicTest id="cookbook/search/exact-value-matching/example_001" />

## Match multiple alternatives

Use `ts_any` to match any of several values — more efficient than chaining `OR`:

<SqlLogicTest id="cookbook/search/exact-value-matching/example_002" />

## Negation

Combine `NOT` with term operations to exclude matches:

<SqlLogicTest id="cookbook/search/exact-value-matching/example_003" />

## Combine with phrase search

Search for an exact genre and a phrase in the description:

<SqlLogicTest id="cookbook/search/exact-value-matching/example_004" />

## Combine with analytics

Search and aggregate in the same query:

<SqlLogicTest id="cookbook/search/exact-value-matching/example_005" />

## See also

- [Range Queries](./range-queries.md) — compare terms with `>`, `<`, `>=`, `<=`
- [Inverted index query functions](../../sql/statements/create_index/index.md) — `@@`, `ts_any` and more
