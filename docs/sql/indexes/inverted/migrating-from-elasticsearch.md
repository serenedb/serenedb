---
title: Migrating from Elasticsearch
sidebar_position: 13
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";
import DocCallout from "@site/src/components/DocCallout";

If you are coming from Elasticsearch or OpenSearch, most search features map onto SereneDB's [inverted index](./index.md) and plain SQL. This page maps the concepts side by side; each Elasticsearch feature links to its reference. The biggest shift is that **search and analytics are both just SQL** — you filter with `@@` and aggregate with `GROUP BY` in the same query, against the same database that holds your relational data.

## Key differences

| Aspect | Elasticsearch | SereneDB |
|---|---|---|
| Query language | Query DSL (JSON) | SQL — `@@`, `ORDER BY`, `GROUP BY` |
| Data model | Documents in indices | Rows in tables, with an inverted index beside the columnar data |
| Schema & types | Dynamic field mappings, JSON types | Typed SQL columns; per-column [operator classes](./index.md#operator-classes-and-fields) pick how each is indexed |
| Search types | Separate field types — `text`, `dense_vector`, `geo_*` | One inverted index spans full-text, [vector](./vector-search.md) and [geospatial](./geospatial-search.md) |
| Analyzer config | Analyzers, tokenizers, token filters | [Text search dictionaries](./text-analysis.md) — templates + `pipeline` |
| Relevance | BM25 by default | BM25 by default, plus other [scorers](./ranking.md) |
| Aggregations + hits | One request, two result trees | One SQL query — filter + `GROUP BY` / window |
| Transactions | None across documents | Full ACID |
| Joins | Limited (nested / parent-child) | Native SQL joins |
| Deployment | Distributed JVM cluster | Single binary, PostgreSQL wire protocol |

## Migration tips

- Model each Elasticsearch index as a table plus an inverted index; map field mappings to [operator classes](./index.md#operator-classes-and-fields).
- Replace analyzer definitions with [text search dictionaries](./text-analysis.md).
- Replace the Query DSL with `WHERE … @@ …` for matching and `ORDER BY <scorer>` for relevance; see [Full-Text Search](./full-text-search.md) and [Ranking](./ranking.md).

## Query capabilities

| Elasticsearch | SereneDB | Notes |
|---|:---:|---|
| [`match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html) | ✅ | `col @@ 'terms'` ([Full-Text Search](./full-text-search.md)) |
| [`match_phrase`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html) / proximity | ✅ | [`ts_phrase`](../../functions/search/full-text.md#ts_phrase), [`##` operator](../../functions/search/full-text.md#a--b-phrase) |
| [`prefix`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html) / [`wildcard`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html) / [`regexp`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html) | ✅ | [`ts_starts_with`](../../functions/search/full-text.md#ts_starts_with), [`ts_like`](../../functions/search/full-text.md#ts_like), [`ts_regexp`](../../functions/search/full-text.md#ts_regexp) |
| [`fuzzy`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html) | ✅ | [`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein); plus [`ts_ngram`](../../functions/search/full-text.md#ts_ngram) n-gram similarity (no ES equivalent) |
| [`bool`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) | ✅ | [`&&`](../../functions/search/full-text.md#a--b-and) [`\|\|`](../../functions/search/full-text.md#a--b-or) [`!!`](../../functions/search/full-text.md#-a-not), [`ts_compound`](../../functions/search/full-text.md#ts_compound) |
| [`term`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html) / [`terms`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html) | ✅ | verbatim columns; [`ts_any`](../../functions/search/full-text.md#ts_any) / [`ts_all`](../../functions/search/full-text.md#ts_all) |
| [`range`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html) | ✅ | [`ts_between`](../../functions/search/full-text.md#ts_between), [`ts_lt`](../../functions/search/full-text.md#ts_lt)/[`le`](../../functions/search/full-text.md#ts_le)/[`gt`](../../functions/search/full-text.md#ts_gt)/[`ge`](../../functions/search/full-text.md#ts_ge) |
| [`query_string`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html) | ✅ | [`to_tsquery`](../../functions/search/full-text.md#to_tsquery), [`websearch_to_tsquery`](../../functions/search/full-text.md#websearch_to_tsquery) |
| [`more_like_this`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html) | ⚠️ | No direct function; use [`minhash`](../../statements/create_text_search_dictionary/minhash/index.md) or [vector](./vector-search.md) similarity |

## Function mapping

The detailed mapping from each Elasticsearch query to the specific SereneDB function — the left column links to the Elasticsearch reference, the right to the SereneDB function reference:

| Elasticsearch query | SereneDB function |
|---|---|
| [`match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html) | a bare string, or [`ts_tokenize`](../../functions/search/full-text.md#ts_tokenize) |
| [`match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html) (`operator: and`) | [`plainto_tsquery`](../../functions/search/full-text.md#plainto_tsquery) |
| [`match_phrase`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html) | [`ts_phrase`](../../functions/search/full-text.md#ts_phrase) / [`phraseto_tsquery`](../../functions/search/full-text.md#phraseto_tsquery) |
| [`match_phrase`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html) (`slop`) | [`ts_phrase`](../../functions/search/full-text.md#ts_phrase) with `slop := N` or `::slop(N)` |
| [`prefix`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html) | [`ts_starts_with`](../../functions/search/full-text.md#ts_starts_with) |
| [`wildcard`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html) | [`ts_like`](../../functions/search/full-text.md#ts_like) |
| [`regexp`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html) | [`ts_regexp`](../../functions/search/full-text.md#ts_regexp) |
| [`fuzzy`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html) | [`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein) |
| _(no ES equivalent)_ | [`ts_ngram`](../../functions/search/full-text.md#ts_ngram) — n-gram similarity |
| [`term`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html) / [`terms`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html) | a verbatim token, [`ts_any`](../../functions/search/full-text.md#ts_any) / [`ts_all`](../../functions/search/full-text.md#ts_all) |
| [`terms_set`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-set-query.html) | [`ts_any`](../../functions/search/full-text.md#ts_any) with `min_match` |
| [`range`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html) | [`ts_between`](../../functions/search/full-text.md#ts_between), [`ts_lt`](../../functions/search/full-text.md#ts_lt) / [`ts_le`](../../functions/search/full-text.md#ts_le) / [`ts_gt`](../../functions/search/full-text.md#ts_gt) / [`ts_ge`](../../functions/search/full-text.md#ts_ge) |
| [`exists`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html) | Plain SQL [`IS NOT NULL` / `IS NULL`](../../functions/search/full-text.md#is-null) — the index claims both on indexed columns. |
| [`bool`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html) | [`ts_compound`](../../functions/search/full-text.md#ts_compound), or [`&&`](../../functions/search/full-text.md#a--b-and) / [`\|\|`](../../functions/search/full-text.md#a--b-or) / [`!!`](../../functions/search/full-text.md#-a-not) |
| [`query_string`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html) | [`to_tsquery`](../../functions/search/full-text.md#to_tsquery) |
| [`simple_query_string`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-simple-query-string-query.html) | [`websearch_to_tsquery`](../../functions/search/full-text.md#websearch_to_tsquery) |
| [boost](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html) (`^`) | [`^`](../../functions/search/full-text.md#a--factor-boost) operator |
| [`_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html) (BM25) | [`BM25`](../../functions/search/scoring.md) and other [scorers](../../functions/search/scoring.md) |
| [highlighting](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html) | [`ts_highlight`](../../functions/search/highlighting.md), [`ts_offsets`](../../functions/search/highlighting.md) |
| [kNN](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html) | [`<->`](../../functions/vector.md) / [`<=>`](../../functions/vector.md) / [`<#>`](../../functions/vector.md) + `ORDER BY … LIMIT` |
| [geo queries](https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-queries.html) | [`ST_Intersects`](../../functions/search/geo.md), [`ST_Contains`](../../functions/search/geo.md), [`ST_Distance_*`](../../functions/search/geo.md) |
| [analyzer test](https://www.elastic.co/guide/en/elasticsearch/reference/current/test-analyzer.html) | [`ts_lexize`](../../functions/search/full-text.md#ts_lexize) |

## Text analysis

| Elasticsearch | SereneDB | Notes |
|---|:---:|---|
| [Tokenizers](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-tokenizers.html) | ✅ | [`text`](../../statements/create_text_search_dictionary/text.md), [`ngram`](../../statements/create_text_search_dictionary/ngram.md), [`delimiter`](../../statements/create_text_search_dictionary/delimiter.md), [`segmentation`](../../statements/create_text_search_dictionary/segmentation.md), … [templates](./text-analysis.md) |
| [Token filters](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-tokenfilters.html) (lowercase / stemming / stopwords) | ✅ | [`text`](../../statements/create_text_search_dictionary/text.md) template options + [`stem`](../../statements/create_text_search_dictionary/stem.md) / [`stopwords`](../../statements/create_text_search_dictionary/stopwords.md) templates |
| [Accent folding](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-asciifolding-tokenfilter.html) | ✅ | [`accent = false`](../../statements/create_text_search_dictionary/text.md) |
| [n-gram](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-ngram-tokenizer.html) / [edge n-gram](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-edgengram-tokenizer.html) | ✅ | [`ngram`](../../statements/create_text_search_dictionary/ngram.md), [`sparse_ngram`](../../statements/create_text_search_dictionary/sparse-ngram.md) |
| [Synonyms](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-tokenfilter.html) | ✅ | [`solr_synonyms`](../../statements/create_text_search_dictionary/solr-synonyms.md), [`wordnet_synonyms`](../../statements/create_text_search_dictionary/wordnet-synonyms.md) |
| [Custom analyzers](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-custom-analyzer.html) | ✅ | compose templates with [`pipeline`](../../statements/create_text_search_dictionary/pipeline/index.md) |
| [Separate search analyzer](https://www.elastic.co/guide/en/elasticsearch/reference/current/specify-analyzer.html) | ✅ | Symmetric by default; override per query with [`ts_tokenize(text, 'dict')`](../../functions/search/full-text.md#ts_tokenize) or `'text'::tokenize('dict')` |

## Scoring and relevance

| Elasticsearch | SereneDB | Notes |
|---|:---:|---|
| [BM25](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html) | ✅ | [`BM25(idx.tableoid)`](../../functions/search/scoring.md#bm25) ([Ranking](./ranking.md)) |
| Other scorers | ✅ | [`TFIDF`](../../functions/search/scoring.md#tfidf), [`lm_jm`](../../functions/search/scoring.md#lm_jm), [`lm_dirichlet`](../../functions/search/scoring.md#lm_dirichlet), [`dfi`](../../functions/search/scoring.md#dfi), [more](../../functions/search/scoring.md#scorers) |
| [`boosting`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html) / boost | ✅ | [`^` operator](../../functions/search/full-text.md#a--factor-boost) ([Relevance Tuning](../../../cookbook/search/boosting.md)) |
| [`function_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html) | ✅ | compose the scorer in SQL arithmetic, e.g. [`BM25(...) * 2`](../../functions/search/scoring.md#bm25) |
| Top-K acceleration | ✅ | [`optimize_top_k`](./ranking.md#top-k-queries-and-wand-pruning) (WAND) |
| [Reciprocal Rank Fusion](https://www.elastic.co/guide/en/elasticsearch/reference/current/rrf.html) | ✅ | [Hybrid Search](./hybrid-search.md) + [RRF](../../../cookbook/search/reciprocal-rank-fusion.md) |

## Highlighting

| Elasticsearch | SereneDB | Notes |
|---|:---:|---|
| [Snippets / fragments](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html) | ✅ | [`ts_highlight`](../../functions/search/highlighting.md) |
| [Custom tags](https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html#specify-highlight-tags) | ✅ | [`StartSel` / `StopSel`](../../functions/search/highlighting.md) |
| Match offsets | ✅ | [`ts_offsets`](../../functions/search/highlighting.md) |

## Suggesters

Elasticsearch suggesters read a field's terms to complete and correct what a user types. SereneDB serves the same from the inverted index dictionary with the [`ts_dict_*`](../../functions/search/term-dictionary.md) aggregates, so there is no separate suggester structure to build or keep in sync. You query the dictionary directly and rank however you like.

| Elasticsearch | SereneDB | Notes |
|---|:---:|---|
| [`completion` suggester](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/search-suggesters#completion-suggester) | ✅ | prefix match over the dictionary, ranked by document frequency ([Autocomplete](../../../cookbook/search/autocomplete.md)) |
| [`term` suggester](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/search-suggesters#term-suggester) | ✅ | [`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein) over the dictionary, ranked by [`ts_dict_score`](../../functions/search/term-dictionary.md) ([Spell Correction](../../../cookbook/search/spell-correction.md)) |
| [`phrase` suggester](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/search-suggesters#phrase-suggester) | ⚠️ | correct each term with [`ts_levenshtein`](../../functions/search/full-text.md#ts_levenshtein); no whole-phrase language model ([Spell Correction](../../../cookbook/search/spell-correction.md)) |
| [context suggester](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/search-suggesters#context-suggester) | ✅ | autocomplete with a `WHERE` filter on another indexed column |

## Vector and geospatial

| Elasticsearch | SereneDB | Notes |
|---|:---:|---|
| [Dense vector / kNN (IVF)](https://www.elastic.co/guide/en/elasticsearch/reference/current/knn-search.html) | ✅ | [Vector Search](./vector-search.md) |
| [Geo queries](https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-queries.html) | ✅ | [Geospatial Search](./geospatial-search.md) (`ST_*`) |

## Aggregations

This is where SereneDB's SQL model shines: every Elasticsearch aggregation maps to a SQL construct, run in the **same query** as the search filter. Elasticsearch's three families map as follows.

### Bucket aggregations

Bucket aggregations group documents into buckets — SereneDB's [`GROUP BY`](../../query_syntax/groupby/index.md) clause:

| Elasticsearch | SereneDB |
| :--- | :--- |
| [`terms`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html) / [`multi_terms`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-multi-terms-aggregation.html) | [`GROUP BY`](../../query_syntax/groupby/index.md) `col` (one or more columns) |
| [`histogram`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html) | [`GROUP BY`](../../query_syntax/groupby/index.md) `width_bucket(col, …)` |
| [`date_histogram`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html) | [`GROUP BY`](../../query_syntax/groupby/index.md) [`date_trunc('month', col)`](../../functions/timestamp.md) |
| [`range`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html) / [`date_range`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html) / [`ip_range`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-iprange-aggregation.html) | [`GROUP BY`](../../query_syntax/groupby/index.md) [`CASE …`](../../expressions/case/index.md) |
| [`filter`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filter-aggregation.html) | [`WHERE …`](../../query_syntax/where/index.md) |
| [`filters`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filters-aggregation.html) | `count(*)` with a [`FILTER` clause](../../query_syntax/filter/index.md) per branch |
| [`missing`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-missing-aggregation.html) | [`WHERE`](../../query_syntax/where/index.md) `col IS NULL` |
| [`rare_terms`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-rare-terms-aggregation.html) | [`GROUP BY`](../../query_syntax/groupby/index.md) `col` with [`HAVING count(*) <= n`](../../query_syntax/having/index.md) |
| [`nested`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-nested-aggregation.html) | [`UNNEST`](../../query_syntax/unnest.md) / [`LATERAL`](../../query_syntax/from_and_join/index.md) |

### Metric aggregations

Metric aggregations compute a value over each bucket — SereneDB's [aggregate functions](../../functions/aggregates/index.md):

| Elasticsearch | SereneDB |
| :--- | :--- |
| [`avg`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html) / [`sum`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html) / [`min`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html) / [`max`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html) / [`value_count`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-valuecount-aggregation.html) | [`avg` / `sum` / `min` / `max` / `count`](../../functions/aggregates/index.md) |
| [`stats`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html) / [`extended_stats`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html) | those together (+ [`stddev` / `variance`](../../functions/aggregates/index.md)) |
| [`cardinality`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html) | [`approx_count_distinct(col)`](../../functions/aggregates/index.md) (or `count(DISTINCT col)`) |
| [`percentiles`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html) | [`quantile_cont(col, p)`](../../functions/aggregates/index.md) |
| [`percentile_ranks`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html) | `count(*)` [`FILTER (WHERE col <= v)`](../../query_syntax/filter/index.md) `/ count(*)` |
| [`weighted_avg`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-weight-avg-aggregation.html) | [`sum(w*x) / sum(w)`](../../functions/aggregates/index.md) |
| [`top_hits`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-hits-aggregation.html) | [`DISTINCT ON`](../../query_syntax/select/index.md) / windowed [`row_number()`](../../functions/window_functions/index.md) |

### Pipeline aggregations

Pipeline aggregations post-process the output of other aggregations — SereneDB's [window functions](../../query_syntax/window/index.md):

| Elasticsearch | SereneDB |
| :--- | :--- |
| [`cumulative_sum`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-cumulative-sum-aggregation.html) | [`sum(x) OVER (ORDER BY …)`](../../functions/window_functions/index.md) |
| [`derivative`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-derivative-aggregation.html) / [`serial_diff`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-serialdiff-aggregation.html) | `x - ` [`lag(x) OVER (…)`](../../functions/window_functions/index.md) |
| [`moving_fn`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-movfn-aggregation.html) | [`avg(x) OVER (… ROWS BETWEEN …)`](../../functions/window_functions/index.md) |
| [`bucket_script`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-bucket-script-aggregation.html) | arithmetic over aggregated columns |
| [`bucket_selector`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-bucket-selector-aggregation.html) | [`HAVING`](../../query_syntax/having/index.md) |
| [`bucket_sort`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-bucket-sort-aggregation.html) | [`ORDER BY`](../../query_syntax/orderby/index.md) `… ` [`LIMIT`](../../query_syntax/limit/index.md) |

Aggregates run **over the inverted index itself** — `GROUP BY` and aggregate functions over indexed (and [`INCLUDE`d](./modeling.md#indexed-vs-included-columns)) columns are answered without materializing the base table. Add columns you frequently aggregate to the index. On a keyword column a `terms` aggregation goes further and reads the [term dictionary](../../functions/search/term-dictionary.md) directly, so facet counts cost a dictionary walk rather than a scan; see [Faceted Search](../../../cookbook/search/faceted-search.md). The query below filters with `@@` and buckets the matches by category in one statement — the Elasticsearch equivalent of a `terms` aggregation inside a query:

<SqlLogicTest id="sql/indexes/inverted/migrating-from-elasticsearch/example_001" />

## Index management and operations

| Elasticsearch | SereneDB | Notes |
|---|:---:|---|
| [Create / delete index](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html) | ✅ | [`CREATE INDEX … USING inverted`](../../statements/create_index/inverted.md) / [`DROP INDEX`](../../statements/drop/index.md) |
| [Reindex](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-reindex.html) | ✅ | [`DROP INDEX`](../../statements/drop/index.md) + [`CREATE INDEX`](../../statements/create_index/inverted.md) |
| [Refresh](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html) | ✅ | [`VACUUM (REFRESH_TABLE)`](../../statements/vacuum/index.md) ([Maintenance](./maintenance.md)) |
| [Force merge](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-forcemerge.html) | ✅ | [`VACUUM (COMPACT_TABLE)`](../../statements/vacuum/index.md) |
| [Aliases](https://www.elastic.co/guide/en/elasticsearch/reference/current/aliases.html) | ✅ | Use a [view](../../statements/create_view/index.md) |
| [Pagination](https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html) (`from`/`size`, `search_after`) | ✅ | [`LIMIT` / `OFFSET`](../../query_syntax/limit/index.md), keyset pagination |

## See also

- [Inverted Index](./index.md) · [Text Analysis](./text-analysis.md) · [Full-Text Search](./full-text-search.md) · [Ranking](./ranking.md)
- [Vector Search](./vector-search.md) · [Hybrid Search](./hybrid-search.md) · [Geospatial Search](./geospatial-search.md)
- [Search cookbook](../../../cookbook/search/index.md)
