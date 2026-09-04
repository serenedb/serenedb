---
title: One Search Box
sidebar_position: 11
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# One Search Box

A single input that searches everything is the most common search UI there is, and it does not need one `@@` predicate per column. Declare a catch-all column as `GENERATED ALWAYS AS (concat_ws(...)) STORED`, index it, and point the search box at that one field. The table keeps the column in sync on every write, so the index never drifts from the source columns — unlike a concatenation you materialize by hand at load time.

`concat_ws` skips NULLs, so optional columns cost nothing to include.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/one-search-box/setup" />

</details>

## Search everything with one predicate

A bare multi-token string matches any of its tokens (`OR` semantics) and [BM25](../../sql/functions/search/scoring.md) sums the per-term contributions, so rows matching more of the query rank first: the machine that matches both `wouter` and `fendt` beats the rows matching one term each.

<SqlLogicTest id="cookbook/search/one-search-box/example_001" />

## Keep a boosted field for precision

Index an important column separately alongside the catch-all and [boost](boosting.md) it, so matches on the record's own name outrank matches buried in the long field.

<SqlLogicTest id="cookbook/search/one-search-box/example_002" />

## Updates stay searchable

Because the catch-all is a stored generated column, an `UPDATE` to any source column recomputes it and the index follows — no reindex step, no drift.

<SqlLogicTest id="cookbook/search/one-search-box/example_003" />

## Related

- [Computed Values](computed-values.md) — expression indexes and generated columns in general.
- [Boosting](boosting.md) — weighting one field over another.
- [Ranking](ranking.md) — scoring and ordering the merged result.
