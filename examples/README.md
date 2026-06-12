<div>

<picture align=left>
    <source media="(prefers-color-scheme: dark)" srcset="../resources/images/sdb-examples-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="../resources/images/sdb-examples-light.svg">
    <img alt="The SereneDB Examples logo." src="../resources/images/sdb-examples-light.svg">
</picture>

[![Star Us](https://img.shields.io/badge/⭐-Star%20Us-9865e8?style=for-the-badge)](https://github.com/serenedb/serenedb)
[![Apache License 2.0](https://img.shields.io/badge/License-Apache%202.0-a2b9f4?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)
[![Website](https://img.shields.io/website?up_message=VISIT&down_message=FIXING&color=fbe5f5&url=https%3A%2F%2Fwww.serenedb.com&style=for-the-badge)](https://www.serenedb.com)

</div>

End-to-end demos that show what SereneDB does, against real data, in scripts you can run in under a minute.

## Demos

| | What it shows | Dataset |
|---|---|---|
| [demo0](demo0/) | Zero ETL search over **remote** Parquet files | IMDb reviews on Hugging Face (100k rows, 3 shards) |
| [demo1](demo1/) | Same demo over **local** Parquet -- same SQL, two orders of magnitude faster | IMDb reviews staged in `/tmp` |
| [demo2](demo2/) | Same demo over a **native table** -- production shape, durable, sub-millisecond queries | IMDb reviews ingested into a SereneDB table |
| [demo3](demo3/) | **Elastic-grade FTS tour** -- phrase + slop, boolean, prefix / wildcard / regex / fuzzy, ngram, four scoring models, byte-range highlights, plus a complex composite query and a `##`-chained mixed-part phrase | IMDb reviews on Hugging Face (~100k reviews, two analyzers in one index) |
| [demo4](demo4/) | **Hybrid vector + FTS** -- one index with BM25 and HNSW cosine ANN; vector top-K, range search, text+vector hybrid | Qdrant dbpedia-entities (~100k abstracts, 1536-dim OpenAI embeddings) |
| [demo5](demo5/) | **Self-embedding semantic search** -- `ai_embed(...)` at ingest and query time against any OpenAI-compatible endpoint | neuralwork/arxiver (arXiv ML abstracts, Gemini 3072-dim embeddings) |
| [demo6](demo6/) | **Code search with sparse ngrams** -- GitHub-style substring + fuzzy code search via the `sparse_ngram` tokenizer, BM25 over statements/editorials, hybrid RRF of both retrievers | Codeforces tasks (open-r1, ~9.5k problems) + 30k accepted Python solutions (MatrixStudio) |

## Running a demo

```bash
psql -h <host> -p <port> -U serenedb -d postgres -f <demo>/demo.sql
```

>
> **One-time bootstrap.** Some demos ship a `bootstrap.sql` that stages the data locally (downloads parquet shards, or ingests them into a native table). Run it once before `demo.sql`:
>
> ```bash
> psql -h <host> -p <port> -U postgres -d postgres -f <demo>/bootstrap.sql
> ```
>

If you don't have a SereneDB server running yet, check the [downloads page](https://serenedb.com/download).
