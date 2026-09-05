---
title: Collapsing and Grouping Results
sidebar_position: 22
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Collapsing and Grouping Results

Rank matches inside a partition with a window function then keep the rows you want: you get one best hit per group or the top N per group from a single ranked search, with no second round trip and no per bucket sub search. Field collapsing and `top_hits` per bucket, if you come from Elasticsearch, are both this one pattern.

Eight products spread across three categories fill the catalog below. Each row pairs a plain keyword `category` with a tokenized `title` so [BM25](../../sql/functions/search/scoring.md) can score every match.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/grouping-results/setup" />

</details>

## Collapse to the best hit per category

Rank every match inside its `category` with `ROW_NUMBER()`, ordering by `BM25` so the strongest hit lands at row 1, then keep only `rn = 1`. The `id` tiebreaker holds the ranking steady when two rows tie on score. `BM25` runs directly inside the window over the index scan, so the whole collapse is one query.

<SqlLogicTest id="cookbook/search/grouping-results/example_001" />

## Top N hits per category

Change the filter to `rn <= 2` and you get `top_hits` per bucket: the two best matches in every category, still ranked by relevance. Raise the bound for more hits per group.

<SqlLogicTest id="cookbook/search/grouping-results/example_002" />

## See also

- [Ranking and Relevance](./ranking.md): tune the BM25 scores these examples rank on
- [Pagination](./pagination.md): page through ranked results with stable ordering
- [Faceted Search](./faceted-search.md): count matches per category instead of collapsing to one
- [Scoring functions](../../sql/functions/search/scoring.md): BM25 and the other relevance scorers
