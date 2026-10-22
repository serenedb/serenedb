# Index reads per query

Measures which byte ranges of the index files each search query reads, on the Search Benchmark Game corpus (English Wikipedia, 5.03M documents, its 962 queries), and replays them through a remote block cache. It answers how many requests a query would send to object storage for a given block size, merge gap and cache size, on one segment or many.

## Method

Each query runs in a fresh `search-benchmark-game-query` process after the index is opened. The pages it touched are the present bits of the index mappings in `/proc/<pid>/pagemap` after the query minus those after opening. The kernel maps 64 KiB around a fault, so the touched set is exact at 64 KiB and coarser. `model.py` then forms requests the way the block cache does: blocks of a given size, missing runs merged across gaps under 1 MiB, at most 16 MiB per request, cold and through an LRU cache fed every query 20 times in random order.

## Prerequisites

- `search-benchmark-game-build` and `search-benchmark-game-query` from the `perf` preset: `ninja -C build_perf serenedb-bench-micro`.
- `curl`, `python3`, about 20 GB in the data directory.

## Commands

```bash
scripts/perf/index_reads/run.sh all                          # download, build three layouts, measure them
scripts/perf/index_reads/run.sh build tiered tiered          # one layout
scripts/perf/index_reads/run.sh measure tiered
```

Layouts: `single` compacts everything into one segment; `segment_docs=N` never merges, so segments are cut by the writer's memory limit and every N documents (`all` builds 24 segments with `100000000` and 73 with `78627`); `tiered` refreshes and compacts every second as in production, then lets the tier policy settle.

Each layout directory gets `reads.jsonl` (one record per query: touched page runs per file), `analysis.txt` (ranges, blocks and requests per query, per file kind, pages touched by opening the index, working set of all queries) and `model.txt` (requests and bytes per query by block size and merge gap, cold and warm).

| Variable | Default | Meaning |
|---|---|---|
| `SERENEDB_INDEX_READS_DATA` | `~/.cache/serenedb-index-reads` | corpus, queries, indexes, results |
| `BUILD_DIR` | `build_perf` | build with the benchmark binaries |
| `ROW_GROUP` | 122880 | row group size, the production default |
| `WORKERS` | half the cores | indexing threads and parallel queries |
