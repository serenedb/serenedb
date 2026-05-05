# Demo 0 -- Full-text search over remote Parquet

Indexes the [IMDb sentiment dataset](https://huggingface.co/datasets/stanfordnlp/imdb) (100,000 movie reviews, three Parquet shards) directly from Hugging Face -- no download, no `COPY`, no ETL. SereneDB streams the remote Parquet, builds a BM25 inverted index and runs ranked search and SQL analytics against it, all in roughly the time it takes to read this paragraph.

## What it shows

- **Zero-copy indexing.** Point at a `hf://` URL, get a real inverted index. The data never leaves Hugging Face's CDN.
- **Native search expressions.** Phrase (`'plot' ## 'twist'`), boolean composition (`||`, `&&`, `!!`), score boost (`^ N`) -- all first-class operators inside `WHERE`. No DSL, no client library.
- **Search composes with SQL.** Full-text predicates feed `count`, `avg`, `GROUP BY`, and `JOIN` like any other expression. One round-trip computes "top-K BM25 matches" *and* "matches by sentiment, with average relevance per group" in a single query.
