<div>

<picture align=left>
    <source media="(prefers-color-scheme: dark)" srcset="https://github.com/user-attachments/assets/cf4f0e3b-d515-422a-8047-8375544bb10e">
    <source media="(prefers-color-scheme: light)" srcset="https://github.com/user-attachments/assets/8bd10baa-3144-4ae9-84e8-6de783321f47">
    <img alt="The SereneDB IResearch logo." src="https://github.com/user-attachments/assets/cf4f0e3b-d515-422a-8047-8375544bb10e">
</picture>

[![Star Us](https://img.shields.io/badge/⭐-Star%20Us-9865e8?style=for-the-badge)](https://github.com/serenedb/serenedb)
[![Apache License 2.0](https://img.shields.io/badge/License-Apache%202.0-a2b9f4?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)
[![Website](https://img.shields.io/website?up_message=VISIT&down_message=FIXING&color=fbe5f5&url=https%3A%2F%2Fwww.serenedb.com&style=for-the-badge)](https://www.serenedb.com/search-benchmark-game)

</div>

IResearch is a high-performance C++ search engine library. It's up to [5x faster](#performance) than Lucene and Tantivy, runs without a JVM and has powered [production](#production-history) search since 2018.

## Quickstart

```bash
git clone --recursive https://github.com/serenedb/serenedb
cd serenedb
cmake --preset lldb
cmake --build build --target iresearch-example-basic
./build/iresearch/examples/iresearch-example-basic
```

To depend on iresearch from your own CMake project, vendor SereneDB as a submodule and link against the `iresearch-static` target:

```cmake
add_subdirectory(third_party/serenedb)
target_link_libraries(my_app PRIVATE iresearch-static)
```

## Features

- **Full-text search.** Phrase, boolean, prefix, wildcard, fuzzy (Levenshtein), n-gram, regex, range.
- **Pluggable scoring.** BM25, TFIDF, LM-Dirichlet, DFI built-in; custom scorers supported.
- **Vectorized scoring.** Block-at-a-time SIMD pipeline over posting lists; up to 5x throughput vs scalar loops.
- **Lazy evaluation.** Non-lead iterators in conjunctions, exclusions and two-phase queries defer work.
- **Columnar storage.** Modern column store with adaptive compression.
- **Vector search.** Approximate nearest-neighbor (HNSW).
- **Geospatial.** S2-based intersects/contains.
- **NLP pipeline.** Tokenizers, stemmers, stopwords, synonyms (Solr/WordNet), language-aware analysis, pluggable custom analyzers.

## Performance

IResearch is **up to 5x faster than Lucene and Tantivy** on the Tantivy team's [Search Benchmark, The Game](https://tantivy-search.github.io/bench/). The Tantivy maintainers validated and merged iresearch's [results](https://github.com/quickwit-oss/search-benchmark-game/commit/f4149454e8de6d97a0b5c1b4253db1a6e2c82622) themselves.

* [Detailed per-query breakdown](https://serenedb.com/search-benchmark-game)
* [Benchmark overview](https://blog.serenedb.com/search-benchmark-game-overview)

### Where the speed comes from

The win is a result of specific optimizations. We wrote them up as a five-post technical retrospective called `Search Optimization Journey`:
* [Collecting top-K candidates](https://blog.serenedb.com/search-optimization-1)
* [Block scoring](https://blog.serenedb.com/search-optimization-2)
* [Norm gathering](https://blog.serenedb.com/norm-gathering)
* [Lazy two-phase queries](https://blog.serenedb.com/search-optimization-4)
* [Adaptive posting list format](https://blog.serenedb.com/search-optimization-5)

## Examples

See the [examples](examples/) directory for complete programs covering the public API, all built and exercised in CI on every PR so they stay in sync:

- [basic.cpp](examples/basic.cpp) -- index documents, run term / phrase / boolean / prefix / fuzzy / top-K BM25 queries, read stored fields, delete documents, compact.
- [text_filters.cpp](examples/text_filters.cpp) -- phrase search, n-gram similarity matching, regular expressions, SQL-style wildcard patterns (`%` and `_`) and fuzzy term matching with a configurable edit distance, all shown side-by-side against the same small corpus.
- [geo.cpp](examples/geo.cpp) -- geospatial search over a GeoJSON-indexed point corpus: find everything within a radius of a center point, everything inside an annulus and everything that falls inside an arbitrary polygon.

## Production history

IResearch has been in continuous production use since 2018, first as the search engine behind ArangoSearch. Since 2024 it is the search foundation of search-OLAP database [SereneDB](https://github.com/serenedb/serenedb).

## License

Apache 2.0. See [LICENSE.md](LICENSE.md).

Copyright (c) 2024-2026 SereneDB<br>
Copyright (c) 2017-2023 ArangoDB GmbH<br>
Copyright (c) 2016-2017 EMC Corporation<br>

Licensing information for third-party components is in [LICENSES.md](../../third_party/LICENSES.md).
