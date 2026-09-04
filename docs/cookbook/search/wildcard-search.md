---
title: Wildcard Search
sidebar_position: 4
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Wildcard Search

Search for partial matches using pattern wildcards. Useful when you need prefix, infix, suffix, or complex pattern matching on indexed terms.

See [Setup](./index.md#setup) for the shared dataset used in all examples.

## Wildcard characters

| Character | Meaning |
|---|---|
| `_` | Match any single character |
| `%` | Match zero or more characters |
| `\_` | Literal underscore |
| `\%` | Literal percent sign |

## Prefix matching

Find genres starting with `'sci'`:

<SqlLogicTest id="cookbook/search/wildcard-search/example_001" />

## Suffix matching

Find genres ending with `'dy'`:

<SqlLogicTest id="cookbook/search/wildcard-search/example_002" />

## Infix matching

Find genres containing `'vent'`:

<SqlLogicTest id="cookbook/search/wildcard-search/example_003" />

## Single character wildcard

Match genres with exactly one character between `'co'` and `'edy'`:

<SqlLogicTest id="cookbook/search/wildcard-search/example_004" />

## Complex patterns

Combine wildcards for more specific matching. Find words matching `'h_____'` (h + exactly 5 characters):

<SqlLogicTest id="cookbook/search/wildcard-search/example_005" />

## Wildcard on text fields

Search for tokens matching a pattern within full-text columns:

<SqlLogicTest id="cookbook/search/wildcard-search/example_006" />

## Combine with other search functions

<SqlLogicTest id="cookbook/search/wildcard-search/example_007" />

## See also

- [Exact Value Matching](./exact-value-matching.md) — exact term matching with the `@@` operator
- [Phrase and Proximity Search](./phrase-and-proximity-search.md) — ordered token matching
