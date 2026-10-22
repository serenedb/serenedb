---
title: Index Storage
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The files of inverted indexes and search tables live in the data directory by default, next to the databases they belong to. The `--index_location` flag of `serened` puts them elsewhere: on another local disk, or in an object store.

## Choosing the location

`--index_location` takes a local directory or an object store URL:

| Value | Index files live in |
| ----- | ------------------- |
| empty (the default) | the data directory |
| `/mnt/fast/indexes` | that directory; a relative path is resolved against the data directory |
| `s3://bucket/prefix` | an S3 bucket, or a bucket of an S3-compatible store, under that prefix |

```bash
serened /var/lib/serenedb --index_location=s3://search-indexes/production
```

A cluster keeps the location it was first started with. Starting it with another location, or without the location it was created with, is refused at startup, and the error names the location the cluster uses. A cluster created without `--index_location` keeps its index files in the data directory.

Local storage is the fastest: index files are read in place. On an object store, every read that misses the cache is a request to the store.

## Object store credentials

The server reads and writes index files with the credentials of a persistent secret whose scope covers the location. Create it before the first index, with a scope that matches the location's bucket and prefix:

<SqlLogicTest id="configuration/index_storage/example_secret" />

The secret has to be persistent: the server opens indexes at startup and maintains them in the background, outside any session. See [AWS Credentials](./aws_credentials.md) for the keys to use and for S3-compatible endpoints.

## Caching

Index files on an object store are read through the external file cache, in blocks of 256 KiB. A refresh or a merge keeps the files it writes in the cache, so the first queries over new data read memory. A query that touches a part of an index that is not cached reads it from the store and caches it; later queries read it from memory until the cache evicts it. A read that goes through a file in order fetches the blocks ahead of it, in requests of up to 16 MiB, and a large read fetches all of its blocks with one request.

<SqlLogicTest id="configuration/index_storage/example_cache_settings" />

- `external_file_cache_size` is the memory the cache may use, a quarter of `memory_limit` by default. The cache gives memory back when queries need it.
- `external_file_cache_spill_path` and `external_file_cache_spill_size` keep evicted blocks in a file on local disk, so reading them again costs a disk read instead of a request. Spilling is off by default.

The `sdb_metrics` system table reports how the cache does, in rows with a `NULL` `relation_id`:

| Metric | Meaning |
| :--- | :--- |
| `block_cache_capacity_bytes` | Memory the cache may use, in bytes |
| `block_cache_used_bytes` | Memory the cache uses, in bytes |
| `block_cache_hits` | Block reads served from the cache since startup |
| `block_cache_misses` | Block reads that went to the store since startup |
| `block_cache_fetches` | Requests that read blocks from the store since startup |
| `block_cache_fetched_bytes` | Bytes read from the store into the cache since startup |
| `block_cache_evictions` | Blocks evicted from memory since startup |
| `block_cache_spilled_blocks` | Evicted blocks kept in the spill file since startup |
| `block_cache_spill_reads` | Blocks read back from the spill file since startup |

## Requests

What costs requests to the object store:

| Operation | Requests |
| --------- | -------- |
| A query | reads of the blocks that are not cached |
| A refresh or a merge | the upload of each new index file: one request for a file up to 5 MiB, parts for a larger one |
| Each commit of an index | a copy, two size checks and a delete of one small file |
| Opening an index at startup | a listing of its files, then one read of its commit file and one of each segment's metadata; the footers of the index files come with the metadata, and the segments open in parallel |
| Cleanup | one request for up to 1000 files that no index reader uses anymore |
| Startup | listings of the location, to remove files that belong to no index |

The parts of a large file upload at the same time, up to `s3_uploader_parts_in_flight` of them, 20 by default; setting it to `1` uploads one part at a time.

## Limits

Only S3 and S3-compatible stores can hold index files.
