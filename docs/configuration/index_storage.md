---
title: Index Storage
split: headings
---

import SqlLogicTest from "@site/src/components/SqlLogicTest";

The files of inverted indexes and search tables live in the [tablespace](../sql/statements/create_tablespace/index.md) they are created in: the data directory by default, another local disk, or an object store.

## Choosing the location

A tablespace's location is a local directory or an object store URL:

| Location | Index files live in |
| -------- | ------------------- |
| `pg_default`, the default tablespace | the data directory |
| `/mnt/fast/indexes` | that directory |
| `s3://bucket/prefix` | an S3 bucket, or a bucket of an S3-compatible store, under that prefix |
| `az://container/prefix` | an Azure Blob Storage container, under that prefix |

A search table or an inverted index goes to the tablespace its `CREATE` names, else to the one `default_tablespace` names, else to the tablespace of its database; a database created with `CREATE DATABASE ... TABLESPACE` keeps its search tables and indexes there by default. See [CREATE TABLESPACE](../sql/statements/create_tablespace/index.md) for how to create a tablespace and place objects in it.

Local storage is the fastest: index files are read in place. On an object store, every read that misses the cache is a request to the store.

## Object store credentials

The server reads and writes index files with the credentials of a persistent secret whose scope covers the tablespace's location. Create it before the tablespace, with a scope that matches the location's bucket and prefix:

<SqlLogicTest id="configuration/index_storage/example_secret" />

The secret has to be persistent: the server opens indexes at startup and maintains them in the background, outside any session. See [AWS Credentials](./aws_credentials.md) for the keys to use and for S3-compatible endpoints. For Azure, create a persistent secret of `TYPE azure` whose scope covers the container and prefix, for example with a `CONNECTION_STRING`; the container has to exist.

Rotate credentials that expire, such as an S3 `SESSION_TOKEN` or an Azure shared access signature, by replacing the secret with `CREATE OR REPLACE PERSISTENT SECRET` before they expire. A request the store refuses with the old credentials is sent again with the replaced secret, for files that are already open and for background refreshes and merges as well. If a refresh cannot write its files, for example because the credentials expired before the secret was replaced, the index stops publishing new rows and its database stops checkpointing until the server restarts; the index then catches up from the write-ahead log.

## Caching

Index files on an object store are read through the external file cache, in blocks of 256 KiB. A refresh or a merge keeps the files it writes in the cache, so the first queries over new data read memory. A query that touches a part of an index that is not cached reads it from the store and caches it; later queries read it from memory until the cache evicts it. A read that goes through a file in order fetches the blocks ahead of it, in requests of up to 16 MiB, and a large read fetches all of its blocks with one request.

A query over uncached data asks the store for what it needs in a few rounds rather than piece by piece: the term dictionaries of all segments together, then, for a ranked query, the start of every term's postings in every segment together, and at the end the columns of all its results together. A cold ranked query over many segments therefore waits about as long as one over a single segment.

<SqlLogicTest id="configuration/index_storage/example_cache_settings" />

- `external_file_cache_size` is the memory the cache may use, a quarter of `memory_limit` by default. The cache gives memory back when queries need it.
- `external_file_cache_spill_path` and `external_file_cache_spill_size` keep evicted blocks in a file on local disk, so reading them again costs a disk read instead of a request. Spilling is off by default.

The `sdb_metrics` system table reports how the cache and the uploads do, in rows with a `NULL` `relation_id`:

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
| `index_uploads_pending` | Index files being written to the object store whose upload has not completed |
| `index_upload_pending_bytes` | Bytes written to those files so far |

## Requests

What costs requests to the object store:

| Operation | Requests |
| --------- | -------- |
| A query | reads of the blocks that are not cached |
| A refresh or a merge | the upload of each new index file: one request for a file up to 5 MiB, parts for a larger one |
| Each commit of an index | a copy and a delete of one small file |
| Opening an index at startup | one listing of its files, then one read of its commit file and one of each segment's metadata; the footers of the index files come with the metadata, and the segments open in parallel. While the index stays open it lists its files again only after a failed upload, delete or rename |
| Cleanup | one request for up to 1000 files (256 on Azure) that no index reader uses anymore; the first cleanup after an index opens also removes the files an interrupted write left behind, which the listing of the open found |
| Startup | a listing of each tablespace location and of each database directory in it, to remove files that belong to no index; a listing returns only the entries one level down, so the files of the indexes cost no listing requests here |

The parts of a large file upload at the same time, up to `s3_uploader_parts_in_flight` of them on S3 and `azure_write_blocks_in_flight` on Azure, 20 by default; setting it to `1` uploads one part at a time.

The requests themselves, those of queries and those of background refreshes, merges and cleanup alike, are in the `HTTP` [log](./logging/index.md), so they can be counted by kind:

<SqlLogicTest id="configuration/index_storage_requests/example_001" hideResult />

Reads and listings are `GET`; uploads, their parts and copies `PUT`; batched deletes, and on S3 the start and completion of a large upload, `POST`.

## Integrity

Every upload to S3 carries the CRC32C checksum of the whole file, which the store checks against the bytes it received before it keeps the object; an upload that does not match fails, and the refresh or merge that wrote it fails with it. `SET s3_upload_checksum = false` turns the checksums off for S3-compatible stores that refuse them.

An index remembers the ETag the store gave each of its files, and every read asks for that version of the object. A file that was replaced in the bucket after the index wrote it fails the query with an error saying the file changed after it was opened, instead of returning other bytes.

A read from S3 fails when it takes longer than `http_timeout` plus one second for every MiB it asks for, and `http_retries` retries it, so a store that answers too slowly fails the query instead of holding it.

## Limits

S3, S3-compatible stores and Azure Blob Storage can hold index files. Other object stores cannot.
