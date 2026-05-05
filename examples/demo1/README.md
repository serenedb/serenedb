# Demo 1 -- Full-text search over local Parquet

The local-data sibling of [demo0](../demo0/). Same dataset, same queries, but the Parquet files live on disk and the network leaves the bottleneck.

## What it shows

- **Same SQL, two orders of magnitude faster.** Top-K queries that took ~1.7 s remote finish in ~10 ms over local Parquet. The script is unchanged.
- **No ingest required.** A `CREATE VIEW` over `read_parquet('/tmp/...')` is a first-class index target -- same shape as the remote case, just pointing at disk.
