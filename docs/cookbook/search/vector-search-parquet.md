---
title: Vector Search over Parquet Files with SQL
sidebar_label: Vector Search over Parquet
sidebar_position: 34
description: Run nearest-neighbor and hybrid BM25 + vector search over embeddings in Parquet files with plain SQL. A view, an ivf or hnsw index, and an ORDER BY on distance.
split: page
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

SereneDB runs nearest-neighbor and hybrid search over embeddings stored in Parquet files without loading the rows into a table. You expose the files through a view over `read_parquet`, index the view with an [inverted index](../../sql/indexes/inverted/external-data.md) that holds a vector column, and order by the distance to a query vector. Every query runs from `psql` or any Postgres driver.

Tested with SereneDB 26.11.2 on 2026-11-20.

The examples read and write files under `/data`, the folder mounted into the server's container in [How to Read a Parquet File with SQL](../file_formats/read-parquet-file.md#start-serenedb-with-your-files-mounted).

## Expose the embeddings through a view

A Parquet file of embeddings usually has an id, the text and the embedding as a list of floats. This sample file has five support tickets with three-dimensional embeddings written by hand, so the distances stay easy to follow: the first axis means roughly "search", the second "performance", the third "billing". Real embeddings come from a model and have hundreds of dimensions.

<SqlLogicTest id="cookbook/search/vector-search-parquet/make_sample" />

Parquet stores the embedding as a variable-length list, so SereneDB reads it as `FLOAT[]`:

<SqlLogicTest id="cookbook/search/vector-search-parquet/describe" />

A vector index needs a fixed-size `FLOAT[N]` column ([Column types](../../sql/indexes/inverted/vector-search.md#column-types)). Cast the column in the view, with `N` set to the model's dimension:

<SqlLogicTest id="cookbook/search/vector-search-parquet/view" />

## Build a vector index over the view

SereneDB indexes the view like a table. This index holds the text for BM25 with a stemming dictionary, and the embedding in an `ivf` vector index with the cosine metric. The rows stay in the Parquet file.

<SqlLogicTest id="cookbook/search/vector-search-parquet/dictionary" />

<SqlLogicTest id="cookbook/search/vector-search-parquet/index" />

The metric decides which distance operator the index serves:

| Metric | Operator | Distance |
|---|---|---|
| `cosine` | `<=>` | cosine distance |
| `l2` | `<->` | Euclidean distance |
| `ip` | `<#>` | negative inner product |
| `l1` | `<+>` | Manhattan distance |

Use the metric your embedding model was trained for; most text embedding models are compared with cosine. [Vector Search](../../sql/indexes/inverted/vector-search.md) covers quantization and the `nprobe` setting that trades recall for speed.

`hnsw` builds a graph index instead of `ivf` clusters, with the same metrics and operators:

<SqlLogicTest id="cookbook/search/vector-search-parquet/hnsw" />

## Find the nearest neighbors

A kNN query orders by the distance to the query vector and takes the first `k` rows. Query the index by its name. The query vector here means "slow and search-related":

<SqlLogicTest id="cookbook/search/vector-search-parquet/knn" />

Ticket 4 ranks second although it shares no word with "slow search": its embedding sits close to the query on the "performance" axis.

## Turn a question into a query vector

SereneDB can compute embeddings in SQL with [`ai_embed`](../../sql/functions/ai.md#ai_embed), which calls an embedding model through an OpenAI-compatible endpoint. These examples use a local [Ollama](https://ollama.com/) server with the `all-minilm` model (384 dimensions); [Providers](../../sql/functions/ai.md#providers) covers hosted providers and other endpoints.

<SqlLogicTest id="cookbook/search/vector-search-parquet/embed_secret" />

`base_url` must be an address the server can reach. When SereneDB runs in Docker, `localhost` is the container itself, so point it at the Ollama host instead and allow the plain-HTTP endpoint with `SET sdb_ai_allow_insecure_endpoint = true` ([Providers](../../sql/functions/ai.md#providers)).

Embed the text of the tickets once, write the result as Parquet, and index it through a view that casts to `FLOAT[384]`:

<SqlLogicTest id="cookbook/search/vector-search-parquet/embed_files" />

At query time, embed the question in the same `ORDER BY`:

<SqlLogicTest id="cookbook/search/vector-search-parquet/embed_query" />

The query vector must come from the same model as the stored embeddings. If your application already computes embeddings, send the vector from any Postgres driver instead of calling `ai_embed`.

## Combine BM25 and vector search

Hybrid search uses both signals: BM25 matches the words of the query, and the vector distance matches its meaning. SereneDB keeps both in one index, so one query can use them together.

### Filter by keyword, rank by meaning

A keyword match in `WHERE` keeps only tickets about search; the vector orders them by how close they are to "slow":

<SqlLogicTest id="cookbook/search/vector-search-parquet/filter_rank" />

### Fuse the two rankings

BM25 alone returns only the tickets that contain the words:

<SqlLogicTest id="cookbook/search/vector-search-parquet/bm25_only" />

[Reciprocal rank fusion](./reciprocal-rank-fusion.md) merges the BM25 ranking and the vector ranking into one list. BM25 scores are better when higher and distances when lower, so the BM25 branch ranks descending and the vector branch ascending:

<SqlLogicTest id="cookbook/search/vector-search-parquet/rrf" />

Ticket 4 now appears in the results: the vector branch brings it in, although it contains none of the query words. [Hybrid Search](./hybrid-search.md) shows more ways to weigh the two signals.

## Folders of files and S3

A view can read a glob of Parquet files, on local disk or in S3, and the index covers all of them. With an [S3 secret](../../configuration/secrets_manager.md) for the bucket:

<SqlLogicTest id="cookbook/search/vector-search-parquet/glob_view" />

The index holds a snapshot of the files. When files are added, changed or removed behind the glob, refresh it in one pass; behind a glob, `REINDEX INDEX` re-reads only the files that changed ([Refreshing the index](../../sql/indexes/inverted/views.md#refreshing-the-index)):

<SqlLogicTest id="cookbook/search/vector-search-parquet/reindex" />

## FAQ

### Do I need a vector database to search embeddings in Parquet files?

No. SereneDB builds a vector index over a view of the Parquet files and answers kNN queries in SQL, while the rows stay in the files. The same index can also serve BM25 full-text search over the text columns.

### How should I store embeddings in Parquet?

Store each embedding as a list of 32-bit floats in one column, next to an id and the text it was computed from. SereneDB reads that column as `FLOAT[]`; cast it to `FLOAT[N]` in the view, with `N` equal to the model's dimension.

### Which distance metric should I use?

Use the metric the embedding model was built for. Most text embedding models, including `all-minilm`, compare vectors with cosine similarity, so `metric = 'cosine'` with the `<=>` operator is the usual choice.

### What is the difference between hybrid search and vector search?

Vector search ranks rows by how close their embeddings are to the query's embedding, which captures meaning. Hybrid search combines that with keyword search such as BM25, which captures exact words, names and codes. In SereneDB both run over the same index, and [reciprocal rank fusion](./reciprocal-rank-fusion.md) merges the two rankings.

### Does SereneDB copy my Parquet data?

The rows stay in the Parquet files, and SereneDB reads them through the view. The index stores the search postings and the vector column it indexes, so queries can rank rows without rescanning every file.

## Related pages

- [How to Read a Parquet File with SQL](../file_formats/read-parquet-file.md)
- [Indexing External Data](./indexing-external-data.md)
- [Hybrid Search](./hybrid-search.md)
- [Reciprocal Rank Fusion](./reciprocal-rank-fusion.md)
- [Vector Search reference](../../sql/indexes/inverted/vector-search.md)
- [External data reference](../../sql/indexes/inverted/external-data.md)
- [AI Functions](../../sql/functions/ai.md)
