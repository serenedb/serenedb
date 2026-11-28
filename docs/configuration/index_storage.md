---
title: Index Storage
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The files of tables and inverted indexes live in the [tablespace](../sql/statements/create_tablespace/index.md) they are created in: the server directory by default, another local disk, or an object store.

## Choosing the location

A tablespace's location is a local directory or an object store URL:

| Location | Index files live in |
| -------- | ------------------- |
| `pg_default`, the default tablespace | the server directory |
| `/mnt/fast/indexes` | that directory |
| `s3://bucket/prefix` | an S3 bucket, or a bucket of an S3-compatible store, under that prefix |
| `az://container/prefix` | an Azure Blob Storage container, under that prefix |

A table or an inverted index on a view goes to the tablespace its `CREATE` names, else to the one `default_tablespace` names, else to the tablespace of its database; a database created with `CREATE DATABASE ... TABLESPACE` keeps its tables and indexes there by default. An index on a table is stored with the table. See [CREATE TABLESPACE](../sql/statements/create_tablespace/index.md) for how to create a tablespace and place objects in it.

Local storage is the fastest: index files are read in place. On an object store, every read that misses the cache is a request to the store.

## Object store credentials

The server reads and writes index files with the credentials of a persistent secret whose scope covers the tablespace's location. Create it before the tablespace, with a scope that matches the location's bucket and prefix:

<SqlLogicTest id="configuration/index_storage/example_secret" />

The secret has to be persistent: the server opens tables and indexes at startup and maintains them in the background, outside any session. See [AWS Credentials](./aws_credentials.md) for the keys to use and for S3-compatible endpoints. For Azure, create a persistent secret of `TYPE azure` whose scope covers the container and prefix, for example with a `CONNECTION_STRING`; the container has to exist.

Rotate credentials that expire, such as an S3 `SESSION_TOKEN` or an Azure shared access signature, by replacing the secret with `CREATE OR REPLACE PERSISTENT SECRET` before they expire. A request the store refuses with the old credentials is sent again with the replaced secret, for files that are already open and for background flushes and merges as well. A flush that cannot write its files, for example because the credentials expired before the secret was replaced, fails: its rows stay readable and in the write-ahead log (WAL), and the next flush writes them. A server starts with the secret as it was last replaced.

## Caching

Index files on an object store are read through the external file cache, in blocks of 128 KiB; the last block of a file takes the memory of the bytes it holds. A flush or a merge keeps the files it writes in the cache, so the first queries over new data read memory. A query that touches a part of an index that is not cached reads it from the store and caches it; later queries read it from memory until the cache evicts it. A read that goes through a file in order fetches up to 8 MiB of blocks ahead of it, never past the end of the list or column it reads, and a large read fetches all of its blocks at once, in concurrent requests. A query reads each block of a request once the block has arrived, without waiting for the rest of the request.

- `external_file_cache_remote_min_block_size` sets the block size, 128 KiB by default: a power of two from 32 KiB to 256 KiB. A file gets the block size in effect when the cache starts holding it; blocks cached before a change keep theirs. Smaller blocks fetch fewer bytes a query does not need, larger ones take fewer requests.
- `external_file_cache_remote_max_block_size` sets the size of the concurrent requests of a large read, 4 MiB by default, a power of two. The server keeps at most 64 MiB of requests to object stores in flight: a smaller size sends more requests at once, each finishing sooner, a larger one fewer requests for the same bytes. `external_file_cache_local_max_block_size` does the same for local files the cache holds when `cache_local_files` is on.

When a server opens an index on an object store, it loads the term dictionaries of the index into the cache in the background, so the first searches after a start do not wait for them; they take cache memory like any other cached part of the index. A query over uncached data asks the store for what it needs in a few rounds rather than piece by piece: the term dictionaries of all segments together, then, for a ranked query, the start of every term's postings in every segment together, and at the end the columns of all its results together, also when a `LIMIT` query reads the remaining columns of its rows after sorting them. A scan of whole columns keeps asking ahead of itself for the columns it filters on, or for the columns it reads when it filters on none: the rows each thread is about to read, the rows the next free thread will take and the next segments, so the store is asked for them while earlier rows are processed. Rows whose stored value ranges rule out the filter are not asked for. The columns a filtered scan reads besides the ones it filters on are asked for together once rows pass the filter, and for the rows ahead of every thread while rows keep passing. A cold ranked query over many segments therefore waits about as long as one over a single segment.

<SqlLogicTest id="configuration/index_storage/example_cache_settings" />

- The cache shares `memory_limit` with queries: it uses memory that queries leave free, and blocks it holds are evicted when queries need the memory. Blocks of files on an object store keep `external_file_cache_min_memory`, 5% of `memory_limit` by default: below it, queries spill to the temporary directory first, so a large join or aggregate does not empty the cache of the blocks every search reads. The cache goes below it only when queries cannot spill.
- A query reads data in place from a cached block. Data it needs in one piece that spans two blocks, such as a block of a column, is copied out of them once, and later queries read the same copy: the cache counts it in `external_file_cache_used_bytes` and evicts it, before the blocks, when queries need the memory.
- An open index also keeps part of the term dictionaries of each segment in memory, and more of them for an index on an object store or in memory. This memory counts against `memory_limit` too: opening an index at the limit evicts other memory or fails with an out-of-memory error.
- A block of a file on an object store keeps a copy in the temporary directory (`temp_directory`) once it is evicted, so reading it again costs a disk read instead of a request. The copy is written once and stays while the file version is cached. `external_file_cache_spill_size` is the disk space the copies may take, half of `max_temp_directory_size` by default; past it, evicted blocks are dropped and fetched again, and `0` drops them all, as `external_file_cache_spill = false` does. The last block of a file, when it is shorter than the others, may keep no copy and is then fetched again. Blocks of index files on local disk are dropped, and blocks of in-memory indexes are always kept.
- `enable_external_file_cache = false` drops the cached blocks of files on object stores and the copies made from them, and flushes and merges stop keeping the files they write in the cache; index files are still read through it.

The `sdb_metrics` system table reports how the cache and the uploads do, in rows with a `NULL` `relation_id`:

| Metric | Meaning |
| :--- | :--- |
| `external_file_cache_capacity_bytes` | Memory the cache may use, in bytes: `memory_limit`, which it shares with queries |
| `external_file_cache_used_bytes` | Memory the cache uses, in bytes |
| `external_file_cache_misses` | Block reads that went to the store since startup |
| `external_file_cache_fetches` | Requests that read blocks from the store since startup |
| `external_file_cache_fetched_bytes` | Bytes read from the store into the cache since startup |
| `external_file_cache_evictions` | Blocks evicted from memory since startup |
| `external_file_cache_spilled_blocks` | Evicted blocks written to the temporary directory since startup |
| `external_file_cache_spill_reads` | Blocks read back from the temporary directory since startup |
| `index_uploads_pending` | Index files being written to the object store whose upload has not completed |
| `index_upload_pending_bytes` | Bytes written to those files so far |

## Requests

What costs requests to the object store:

| Operation | Requests |
| --------- | -------- |
| A query | reads of the blocks that are not cached |
| A flush or a merge | the upload of each new index file: one request for a file up to 5 MiB, parts for a larger one; with the external file cache on, the table or index opens the files it wrote without a request |
| A checkpoint | none |
| Opening a table or an index at startup | one check that the `store` directory of its tablespace exists, once for each tablespace, then for each index file a check of its size and a read of its footer. Tables, indexes and their segments open in parallel, so startup waits for the slowest of them rather than for all of them in turn |
| Startup after a crash | also a listing of the `store` directory of each tablespace, to remove the files an interrupted write left behind; a listing returns only the entries one level down |
| Cleanup | one request for up to 1000 files (256 on Azure) that no query uses anymore |

The parts of a large file upload at the same time, up to `s3_uploader_parts_in_flight` of them on S3 and `azure_write_blocks_in_flight` on Azure, 20 by default; setting it to `1` uploads one part at a time.

The requests themselves, those of queries and those of background flushes, merges and cleanup alike, are in the `HTTP` [log](./logging/index.md), so they can be counted by kind:

<SqlLogicTest id="configuration/index_storage_requests/example_001" hideResult />

Reads and listings are `GET`; the check that the `store` directory of a tablespace exists and the size check of each index file a table or an index opens, other than one it wrote with the external file cache on, `HEAD`; uploads and their parts `PUT`; batched deletes, and on S3 the start and completion of a large upload, `POST`.

## Integrity

Every upload to S3 carries the CRC32C checksum of the whole file, which the store checks against the bytes it received before it keeps the object; an upload that does not match fails, and the flush or merge that wrote it fails with it. `SET s3_upload_checksum = false` turns the checksums off for S3-compatible stores that refuse them.

Every read of an index file asks for the version of the object the index found when it opened the file. A file that was replaced in the bucket after that fails the query with an error saying the file changed after it was opened, instead of returning other bytes.

A read from S3 fails when it takes longer than `http_timeout` plus one second for every MiB it asks for, and `http_retries` retries it, so a store that answers too slowly fails the query instead of holding it. A connection that is not established within `http_connect_timeout_ms`, two seconds by default, is given up and the request retried on a new one; a request that never reached the store is retried whatever its kind, uploads included.

## Limits

S3, S3-compatible stores and Azure Blob Storage can hold index files. Other object stores cannot.
