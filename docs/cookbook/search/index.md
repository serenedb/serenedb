---
title: Search
sidebar_position: 2
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

# Search

Examples demonstrating SereneDB's full-text search capabilities using inverted indexes.

| Example | Description |
|---|---|
| [Exact Value Matching](./exact-value-matching.md) | Match precise values, multiple alternatives and negation |
| [Range Queries](./range-queries.md) | Compare and filter indexed terms by lexicographic order |
| [Case-Sensitivity and Diacritics](./case-sensitivity-and-diacritics.md) | Normalize case and accents for flexible or strict matching |
| [Wildcard Search](./wildcard-search.md) | Pattern matching with `_` and `%` wildcards |
| [Phrase and Proximity Search](./phrase-and-proximity-search.md) | Match tokens in sequence, combine with analytics |
| [Stemming and Stopwords](./stemming.md) | Fold word forms together and drop noise words |
| [Synonyms](./synonyms.md) | Match the words a shopper types to the words your catalog uses |
| [Fuzzy Search](./fuzzy-search.md) | Typo-tolerant matching with Levenshtein distance and n-gram similarity |
| [Spell Correction](./spell-correction.md) | "Did you mean" corrections from the indexed vocabulary via fuzzy matching |
| [Autocomplete](./autocomplete.md) | Prefix type-ahead suggestions ranked by popularity |
| [BM25/TFIDF Ranking](./ranking.md) | Relevance scoring and result ordering |
| [Relevance Tuning](./boosting.md) | Boost fields and blend business signals into the score |
| [Recency and Decay](./recency-and-decay.md) | Blend relevance with freshness decay and popularity saturation |
| [Pinned Results](./pinned-results.md) | Promote chosen results to the top of an organic ranking |
| [Reciprocal Rank Fusion](./reciprocal-rank-fusion.md) | Combine results from multiple ranked queries into one |
| [Semantic and Hybrid Search](./hybrid-search.md) | Rank by vector meaning and fuse it with keyword search |
| [Finding Similar Documents](./similar-documents.md) | Find near matches by vector distance or MinHash signatures |
| [Faceted Search](./faceted-search.md) | Count how many results sit behind each category or brand from the index |
| [Tag Cloud](./tag-cloud.md) | Rank a text column's vocabulary by how often each term is written |
| [Significant Terms](./significant-terms.md) | Find terms over-represented in a subset versus the whole corpus |
| [Counting Unique Results](./result-cardinality.md) | Count hits and unique values over a search, exact or approximate |
| [Collapsing and Grouping Results](./grouping-results.md) | Collapse to one result per group or return the top N per group |
| [Search with Joins and Analytics](./search-with-joins.md) | Join matched rows to other tables and roll them up |
| [Highlighting](./highlighting.md) | Wrap matched terms in snippets and pull raw offsets |
| [Pagination](./pagination.md) | Page through ranked results with LIMIT/OFFSET or keyset |
| [Match Several Terms](./terms-set.md) | Match rows that contain at least N of several terms |
| [Saved Searches](./saved-searches.md) | Match a document against stored queries for reverse search and alerting |
| [Geospatial Search](./geospatial-search.md) | Filter by distance, mix location with keyword search and roll points into a heatmap grid |
| [Searching JSON](./json-search.md) | Full-text and range queries over nested JSON fields |
| [Computed Values](./computed-values.md) | Index expressions and generated columns, then query them |
| [Indexing Views](./indexing-views.md) | Build the index on a view to search projections, joins and files |
| [Indexing External Data](./indexing-external-data.md) | Full-text search over Parquet/CSV files on S3 or local disk |

These are task-oriented recipes. For the authoritative reference see [Inverted Index](../../sql/indexes/inverted/index.md), [Full-Text Search](../../sql/indexes/inverted/full-text-search.md) and [Full-Text Search Functions](../../sql/functions/search/full-text.md).

Each recipe sets up its own small dataset. The shared [setup](#setup) below backs the earlier matching and ranking examples.

## Setup {#setup}

<SqlLogicTest id="cookbook/search/index/example_001" />
