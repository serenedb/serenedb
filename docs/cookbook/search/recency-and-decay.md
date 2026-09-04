---
title: Recency and Decay
sidebar_position: 13
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Recency and Decay

Fresh content and popular content both deserve a say in the final order, but neither should steamroll relevance. Because [BM25](../../sql/functions/search/scoring.md) is a plain number you can do arithmetic on, you express both as ordinary SQL in the `ORDER BY`: a time-decay factor that fades a document as it ages and a saturating factor that rewards popularity with diminishing returns. The `gauss`/`exp` decay and `rank_feature` saturation you would reach for in Elastic are both plain arithmetic here, no DSL.

Four articles cover the same topic, each tagged with an `age_days` and a `popularity`. The queries pull back `id` and `title` alone, so a signal earns its place by moving a row up the page, not by printing a score beside it.

<details>
<summary>Schema and sample data</summary>

<SqlLogicTest id="cookbook/search/recency-and-decay/setup" />

</details>

## Baseline relevance

Start with pure relevance so you can see what the signals change. Every article mentions "kubernetes" and the one that mentions it most, in the shortest field, ranks first.

<SqlLogicTest id="cookbook/search/recency-and-decay/example_001" />

## Fade older documents with a decay factor

Multiply the score by `1.0 / (1 + age_days)` and a document loses ground as it ages. The storage guide is a touch less relevant than the networking guide but it is one day old against ninety, so it climbs past it to the top. The stale-but-relevant article slips to second without falling off the page. Swap in `exp(-age_days / 30.0)` for a gentler curve with a half-life instead of a hard hyperbola.

<SqlLogicTest id="cookbook/search/recency-and-decay/example_002" />

## Reward popularity with a saturating curve

A linear popularity multiply lets one runaway hit dominate everything. Multiply by `popularity / (popularity + 10)` instead and the factor rises steeply for the first few points then flattens toward 1, so extra popularity beyond the knee barely moves the needle. Here the year-end retro is the least relevant article but it is the most read, so it overtakes the low-traffic platform notes. Notice that neither low-relevance article reaches the top pair: because the multiplier is capped at 1, popularity refines the order inside a relevance tier rather than buying a way past it. The `10` sets the pivot where the boost hits half strength, the knob Elastic exposes as `k` on a `saturation` feature.

<SqlLogicTest id="cookbook/search/recency-and-decay/example_003" />

## See also

- [Relevance Tuning](./boosting.md): the boost operator and folding a business signal into the score
- [BM25/TFIDF Ranking](./ranking.md): scoring functions and their `k1` and `b` parameters
- [Scoring reference](../../sql/functions/search/scoring.md): the `BM25` and `TFIDF` signatures
