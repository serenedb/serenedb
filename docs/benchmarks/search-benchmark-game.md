---
title: Search Benchmark Game
---

# Search Benchmark Game

The [Search Benchmark Game](https://github.com/quickwit-oss/search-benchmark-game) is a standardized benchmark originally developed by the Tantivy team and maintained by Quickwit. It uses the English Wikipedia corpus with queries derived from the AOL query dataset, and has become the industry yardstick for real-world search performance — adopted by both Tantivy and Lucene.

**IResearch wins across every query type and collection mode.**

[View interactive results →](https://serenedb.com/search-benchmark-game)

## Engines compared

| Engine | Language | Version |
|---|---|---|
| **IResearch** (SereneDB) | C++ | main |
| Lucene | Java | 10.3.0 |
| Tantivy | Rust | 0.25 |

## Query types

| Type | Description |
|---|---|
| **Term** | Single high-frequency stop word (`the`). Stress test for raw posting list traversal. |
| **Intersection** | All terms required (`+griffith +observatory`). Tests conjunctive traversal and skipping. |
| **Union** | All terms optional (`griffith observatory`). Most expensive — merges many posting lists. |
| **Phrase** | Consecutive terms in order (`"griffith observatory"`). Requires positional verification. |
| **Required/Optional** | Some terms mandatory, others boost ranking (`+climate policy`). Default Lucene query mode. |
| **Negation** | Required term with exclusions (`+python -snake -monty`). Tests exclusion efficiency. |
| **Two-phase** | Phrase + required term (`+"the who" +uk`). Tests two-phase evaluation. |

## Collection modes

| Mode | Description |
|---|---|
| **COUNT** | Total match count only. No scoring. Isolates raw traversal speed. |
| **TOP 100** | 100 highest-scoring documents by BM25. Tests dynamic pruning (WAND/MaxScore). |
| **TOP 100 COUNT** | Top 100 + exact count. Disables pruning — measures raw scoring throughput. |

## Methodology

- **Corpus**: English Wikipedia (~5 GB), fits entirely in RAM
- **Warmup**: 60 seconds before timing (ensures warm cache, JIT compilation)
- **Execution**: 10 runs per query, shuffled with fixed seed. **Median** latency reported.
- **Mode**: Single-threaded, in-process (no network)
- **Reproducible**: `git clone && make bench`

## Why IResearch is fast

Key architectural decisions behind the results:

- **Block-at-a-time vectorized scoring** — processes documents in cache-friendly blocks instead of one at a time
- **Redesigned top-K collection pipeline** — more efficient candidate management
- **Improved phrase and conjunction queries** — better skipping and position checking

For technical deep-dives, see the blog series:

- [Search optimization 1: Collecting top-K candidates](https://blog.serenedb.com/search-optimization-1)
- [Search optimization 2: Block scoring](https://blog.serenedb.com/search-optimization-2)

## Running the benchmark yourself

```bash
git clone https://github.com/serenedb/search-benchmark-game
cd search-benchmark-game
make bench
```

Results are generated as a static site you can view locally.

## See also

- [Interactive benchmark results](https://serenedb.com/search-benchmark-game)
- [Full blog post: The C++ search engine that won the game](https://blog.serenedb.com/search-benchmark-game-overview)
- [IResearch roadmap](https://github.com/serenedb/serenedb/issues/364)
