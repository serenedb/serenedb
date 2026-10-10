---
paths:
  - "iresearch/formats/**"
  - "iresearch/utils/serializer.hpp"
  - "server/catalog/boot.*"
  - "server/catalog/database_directory.*"
  - "server/catalog/persistence/**"
  - "server/connector/duckdb_storage_extension.*"
  - "server/connector/file_manifest.*"
  - "server/search/search_db_wal.*"
  - "third_party/duckdb/src/common/serializer/**"
  - "third_party/duckdb/src/include/duckdb/common/serializer/**"
  - "third_party/duckdb/src/include/duckdb/storage/**"
  - "third_party/duckdb/src/storage/**"
---

# Storage compatibility

These rules cover everything SereneDB writes: database files and their write-ahead logs, the search-table WAL and search index directories.

Files are not reproducible byte for byte, and making them so is not a goal. The same data and statements can write different bytes: hash tables iterate in a different order in every process, and parallel builds, checkpoints, refreshes and merges run in a different order every time. Compatibility is about what a reader gets back, so compatibility tests compare contents, never the bytes of a file.

A file never holds stale memory, though: a compression method writes every byte of the segment size it reports, padding and alignment gaps included, so no file carries bytes left in a buffer by another table or database. `StorageVersionTest.CheckpointWritesNoStaleBufferBytes` writes the same data with each compression method twice, from buffers filled with zeros and with ones, and requires identical data blocks. Legacy FSST is left out: it samples its input at random.

Only two places record a storage version, a `serenedb_vN` value of DuckDB's `StorageVersion`:

- The headers of each database file (`engine_v1/<oid>/data.db`). The file's write-ahead log and the database's search-table WAL follow it.
- `segments_N` of each search index directory. The directory's other files are only reached through it.

SereneDB always writes `SERENEDB_LATEST`, and only into its own databases (`CREATE DATABASE`): an `ATTACH` of a DuckDB database refuses a SereneDB storage version, and nothing attaches a SereneDB database by path. A reader opens the versions from `SERENEDB_VERSION_LOWER` to `SERENEDB_VERSION_UPPER` and refuses the rest: a higher one as written by a newer release, a lower one as older than it reads (`duckdb::StorageVersionError`; the constants are in `third_party/duckdb/src/include/duckdb/storage/storage_info.hpp`).

Most changes need no new version:

- **New field or option.** Give it a default that keeps today's behaviour, write it only when it differs from the default, and read the default when it is missing (`WritePropertyWithDefault` with `ReadPropertyWithDefault` or `ReadPropertyWithExplicitDefault`, or a struct member with a default member initializer). Newer releases read older data as the default, and older releases keep reading data that leaves it at the default. They refuse data that uses it, because every reader checks the end of each object. A new `generate_ngrams` option is added this way.
- **Removed field.** Read it with `ReadDeletedProperty`; a struct member becomes `irs::utils::Deleted<T>` of its old type.
- **New value** of an enum or a variant, or a new codec: append it. An older release refuses data that uses a value it does not know. A new compression method is added this way: older releases refuse the `.col` block or the column segment that uses it.
- **Never** reuse a field id, reorder struct members, or change a default, a type or what a field means.

Add a `serenedb_vN` only for a change that older releases would misread instead of refusing, or to stop reading old data. Add it to `third_party/duckdb/src/storage/version_map.json` and run `scripts/generate_storage_info.py` there; `SERENEDB_LATEST` and `SERENEDB_VERSION_UPPER` follow it, and older releases refuse everything the new one writes. To stop reading the data of earlier releases, also point `SERENEDB_VERSION_LOWER` at the new value; data below it has to be upgraded through an earlier release first. Keeping older database files readable (`SERENEDB_VERSION_LOWER` below `SERENEDB_LATEST`) takes one more change, which a `static_assert` in `RequestSereneDBStorageVersion` asks for: attach raises such a file only in memory, so it has to be checkpointed before anything writes to its logs. Make a version change in its own PR and list it in the release notes. A value is never reused.

## Search index files

Every file of an iresearch segment (`segments_N`, `.sm`, `.doc`, `.pos`, `.pay`, `.idx`, `.col`) is the file's data from offset 0, then a footer (a `BinarySerializer` object holding `data_crc32c` and the file's own fields in a `meta` object), then 8 bytes with the footer's CRC32C and its length. `format_utils::WriteFooter` writes it; `format_utils::ReadFooter` checks the checksum and reads the footer. Without a callback, neither side has a `meta` object, and a reader without a callback refuses a footer that has one. `segments_N` stores the storage version as its first field.

- **Field ids:** every object numbers its fields from 0. Name them with `kField...` constants next to the file's writer (`index_meta::kFieldPayload`, `segment_meta::kFieldFiles`, ...), and read them through the same constants.
- **Empty lists** are not written, and are read as optional (`ReadOptionalList`, `ReadOptionalObject`), unless the presence of the list itself means something (the file list of a `.sm`).
- **Callbacks:** footer, list and payload callbacks take `duckdb::BinarySerializer&` and `duckdb::BinaryDeserializer&` (`BinarySerializer::List&` and `BinaryDeserializer::List&` for list elements), never the `Serializer` or `Deserializer` base or `auto&`, so every call into the serializer is direct.
- **New data layout** (block encoding, term dictionary, ...): select it with a new field, and keep reading the old layout while it is supported.
- **Every field is read:** `segments_N` is read with its payload reader. `DirectoryReader` and `DirectoryReader::Reopen` take one, and an index with a payload but no reader is refused.
- The footer trailer and the leading `storage_version` field of `segments_N` never change.

## Database files

Database files are DuckDB database files. Their checkpoint and write-ahead log entries (`.wal`, and the `.wal.checkpoint` and `.wal.recovery` files beside it) are `BinarySerializer` objects with the field ids of `third_party/duckdb/src/include/duckdb/storage/serialization/*.json`: a new field is a json member with a new id and a `default`, and a removed one is marked deleted. A log entry that matches its checksum but cannot be read stops the database from opening instead of being dropped like a torn tail. The log header never gains a field; a framing change bumps `WAL_VERSION_NUMBER`. A file with a SereneDB storage version opens only at a SereneDB storage version, and a file with a DuckDB storage version only at a DuckDB one or with none given. SereneDB opens its own files at `SERENEDB_LATEST` (`RequestSereneDBStorageVersion`), so it refuses a plain DuckDB file, and a plain `ATTACH` refuses a SereneDB file, before anything is read from it.

## DuckDB database files

A database file with a DuckDB storage version (a plain `ATTACH`, `serened shell`) must stay readable by the DuckDB release of that version, and SereneDB reads what that release writes. Field ids and enum values outside SereneDB's ranges belong to upstream:

- **Fields.** A SereneDB field of an upstream class takes 16384 plus the id upstream's numbering would give it: 16484 in a class whose fields start at 100, 16584 at 200. A field from 16384 (`SERENEDB_FIELD_ID_BASE`) up is refused when written to a DuckDB file. With `"version": "serenedb_v1"` on its json member it is skipped there instead: use that for state DuckDB drops as well, such as object ids, constraint names and sequence ownership. A class SereneDB added is reached only through a SereneDB enum value, so its fields keep the usual ids.
- **Enum values.** A SereneDB value of a stored upstream enum starts at 200 (`SERENEDB_ENUM_VALUE_BASE`), and the enum is listed in `IsSereneDBEnumValue`. Writing such a value to a DuckDB file is refused.
- **Data layouts.** A new compression method or block layout (the dict_fsst plus modes, FOR-packed RLE) is chosen only when `IsSereneDBStorageVersion` holds for the storage version being written.
- **Log entries.** Where SereneDB logs an operation in another shape than DuckDB, a DuckDB file keeps DuckDB's: `CREATE SCHEMA` logs the name, and table and view renames log `RenameTableInfo` and `RenameViewInfo`. `ALTER TABLE ADD UNIQUE`, which DuckDB cannot replay, is refused.
- **Catalog.** Objects in a DuckDB file get no owner or privileges (`StoresPermissions` in `server/auth/enforce.cpp`).
- **In-memory databases** have a SereneDB storage version, so they take every SereneDB feature.

`tests/duckdb/run.sh --suite interop` checks both directions against the official `duckdb/duckdb` image; see [tests/duckdb/README.md](../../tests/duckdb/README.md).

## Data directory

```
engine_v1/
  catalog.wal          the catalog log: the definitions of every database
  <database oid>/      one database
    data.db            its DuckDB file, with data.db.wal beside it
    search.wal.<tick>  its search-table WAL
    <object oid>/      a search table, or an inverted index on a table or view
```

- Every directory has one owner, and only the owner creates or removes it. `catalog::DatabaseDirectory` owns `<database oid>/`. The database's catalog entry, its attachment (until DuckDB has closed the files, through `AttachedDatabase::HoldUntilClosed`), every storage of the database and every pending removal hold it, so after a drop it removes the directory last, once all of them let go. A storage removes its `<object oid>/` after its own drop or a rolled-back create; `DROP DATABASE` marks only the database.
- A directory is created, and its parent fsynced, before the statement that creates its object commits. Paths are oids, so a rename moves nothing.
- Boot removes what no live object owns: each `<database oid>/` that names no database right after the catalog log replays, before bootstrap may create the default database again, and each `<object oid>/` that names no storage inside an attached database once its objects are loaded. That covers a crash between a create and its commit and one between a drop and the removal. A missing catalog log beside database directories that hold anything stops the boot.

## Serialized structs

Blobs stored in catalog entries (tokenizer configs, the inverted index payload), the view-backed index manifest and the segment references of the search-table WAL are written with `irs::utils::WriteTuple` and read with `ReadTuple`. An aggregate is a `BinarySerializer` object whose field ids are the positions of its members, and a member equal to its value in a value-initialized aggregate is not written. A struct boost::pfr cannot reflect (one holding a `std::vector<std::unique_ptr<T>>`) declares `SerdeFields(value)` returning `std::tie` of its members, in declaration order.

## Search-table WAL

The WAL of a database's search tables is a series of segments in the database's directory, `search.wal.<first tick>` with the tick as 16 hex digits. Each frame is `[u64 size][u64 checksum][record]`. The record is a `BinarySerializer` object holding `tick` and then its sections and their ops, each with their own field ids. It records no storage version: the WAL belongs to one database and follows that database's file. The frame and the leading `tick` field never change.
