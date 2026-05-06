# Demo 2 -- Full-text search over a native table

Data ingested into SereneDB's native storage. The inverted index lives next to a regular table; queries read pre-tokenised posting lists and a rocksdb point-lookup per result row.

## What it shows

- **Sub-millisecond query latency.** Top-K, aggregates, and JOINs are all sub-millisecond to a few milliseconds regardless of `K` or query shape.
- **Production storage.** Durable across restarts, supports `INSERT` / `UPDATE` / `DELETE` (the index updates transactionally with the table), recoverable via WAL -- the inverted index is just another secondary index.
