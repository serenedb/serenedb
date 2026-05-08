# Demo 3 -- Elastic-grade full-text search in plain SQL

A breadth tour of SereneDB's FTS surface against the IMDb reviews dataset (~100k movie reviews) -- streamed live from Hugging Face. Phrase with slop, boolean composition (native operators *and* Lucene-style), prefix / wildcard / regex / fuzzy, ngram substring matching, four scoring models side-by-side, byte-range highlights, and a composite query that wires several of these together at once. All in one SQL file.

## What it shows

- **At least as much as Elasticsearch.** Phrase with slop, boolean queries, prefix / wildcard / regex / fuzzy, ngram, multi-analyzer (english + 3-gram in one index), byte-range highlights. If you can write it as an Elastic query, you can write it here.
- **Where SereneDB goes wider.** Four scoring models (BM25, TFIDF, LM-Dirichlet, DFI) you can swap by changing one function name -- Elastic ships BM25 only by default. Composability through SQL operators (`||`, `&&`, `!!`, `^`, `##`) instead of nested JSON. JOINs over the search result set (see [demo2](../demo2/)).
- **No DSL, no schema mapping, no client library.** One `psql` invocation, one URL, every feature.
