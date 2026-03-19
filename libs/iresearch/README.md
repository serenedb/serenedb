# IResearch

IResearch is an open-source (Apache 2.0) C++ information retrieval library designed to be embedded into transactional database systems. It provides a feature set comparable to Lucene — inverted indexes with BM25 scoring, columnar storage, vector search, S2 geospatial indexing and semi-structured filtering — while integrating natively with the host database's write-ahead log and transaction protocol. There is no separate process, no JVM dependency and no consistency gap between the search index and the primary store.

## Performance

Benchmarked against Lucene and Tantivy on the [search-benchmark-game](https://github.com/serenedb/search-benchmark-game) — a standard benchmark built by the Tantivy team using the English Wikipedia corpus and the AOL query dataset.
Results, methodology and per-query breakdowns are available in the [benchmark overview post](https://serenedb.com/blog/search-benchmark-game-overview).
[Detailed benchmark results](https://serenedb.com/search-benchmark-game).

## Features

**Full-text search.** Inverted indexes with BM25 and TFIDF scoring. Phrase queries, boolean conjunctions and disjunctions, required/optional, negation, wildcard and fuzzy matching.

**Vectorized scoring.** Scoring is restructured as a block-at-a-time SIMD pipeline over posting list data rather than a document-at-a-time scalar loop. This delivers x2-5 throughput improvement depending on query type and posting list density.

**Lazy evaluation.** The `LazySeek` iterator contract allows non-lead iterators in conjunctions, exclusions and two-phase queries to defer work until strictly necessary. Position intersection uses frequency-sorted zigzag traversal, bounding cost to the rarest term in the query and document.

**Pluggable scoring.** The scoring function is a composable component configurable at query time. BM25 parameters are tunable without recompilation. Custom scorers can be registered and receive the same block buffers — frequencies, norms, filter boosts and columnar signals — as the built-in implementation.

**Columnar storage.** Document field values are stored alongside the inverted index. Numeric attributes, timestamps and other per-document signals are first-class inputs to the scoring pipeline, not afterthoughts.

**Vector search.** Approximate nearest-neighbor search for embedding vectors, integrated with the same transaction and WAL protocol as the inverted index.

**Geospatial indexing.** S2-based indexing for intersects and contains queries.

**NLP pipeline.** Analyzers covering many languages, with support for custom analysis components. A lot of builtin tokenizers, token/char filters and analyzers, similar to Lucene.

**Transactional integration.** IResearch integrates with the host database's WAL and transaction manager. Search indexes participate in the host's commit protocol, providing eventual snapshot-isolation.

## Architecture

The library exposes two primary interfaces: a **IndexWriter** that accepts document batches and indexes field values per-transaction and a **IndexReader** that evaluates query trees against a consistent snapshot of the index. Concurrent readers and writers are supported without external locking.

Internally, the index is organized into segments. Each segment contains an inverted index, columnar storage and optional skip structures. Segments are immutable once written; updates and deletions are tracked as new revisions, allowing readers to hold a consistent view while writers continue to ingest data.

Query trees are constructed directly from API building blocks — terms, phrases, booleans, ranges, geospatial filters — and evaluated by a pipeline of iterator objects that merge posting lists and feed candidates into the scoring pipeline.

## Production history

IResearch has been in continuous production use since 2018, first as the search engine behind [ArangoSearch](https://arangodb.com/docs/stable/arangosearch.html) in ArangoDB. Since 2024 it is the search foundation of [SereneDB](https://github.com/serenedb/serenedb), a search-OLAP database. Both deployments have stress-tested the embedding design across architecturally distinct host databases.

## License

Apache 2.0. See [LICENSE.md](LICENSE.md).

Copyright (c) 2024–2026 SereneDB
Copyright (c) 2017–2023 ArangoDB GmbH
Copyright (c) 2016–2017 EMC Corporation

Licensing information for third-party components is in [LICENSES.md](../../third_party/LICENSES.md).
