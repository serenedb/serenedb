---
paths:
  - "iresearch/formats/**"
  - "iresearch/index/directory_reader*"
  - "iresearch/index/index_writer*"
  - "iresearch/utils/serialization.hpp"
  - "iresearch/utils/serializer.hpp"
  - "server/catalog/boot.*"
  - "server/catalog/cluster.*"
  - "server/catalog/database_directory.*"
  - "server/catalog/persistence/**"
  - "server/connector/duckdb_storage_extension.*"
  - "server/connector/file_manifest.*"
  - "server/connector/index_expression.*"
  - "server/search/inverted_index_storage.*"
  - "server/search/search_db_wal.*"
  - "server/search/search_table.*"
  - "third_party/duckdb/src/common/serializer/**"
  - "third_party/duckdb/src/include/duckdb/common/serializer/**"
  - "third_party/duckdb/src/include/duckdb/storage/**"
  - "third_party/duckdb/src/storage/**"
---

# Storage compatibility

These rules cover everything SereneDB writes.

- Compatibility tests compare what a reader gets back, never the bytes of a file: the same data can be written differently on every run.
- A file never carries stale memory: a compression method writes every byte of the segment size it reports, padding and alignment gaps included. `StorageVersionTest.CheckpointWritesNoStaleBufferBytes` checks every method except legacy FSST.

## Storage versions

Older releases refuse data with a field or a value they don't know, so these changes need no new version:

- **New field or option.** Give it a default that keeps today's behaviour, write it only when it differs from the default, and read the default when it is missing (`WritePropertyWithDefault` with `ReadPropertyWithDefault` or `ReadPropertyWithExplicitDefault`, or a struct member with a default member initializer).
- **Removed field.** Read it with `ReadDeletedProperty`; a struct member becomes `irs::utils::Deleted<T>` of its old type.
- **New value** of an enum or a variant, or a new codec: append it.
- **Never** reuse a field id or reorder struct members. Changing a default, a type or what a field means takes a new field, or a new version.

Add a `serenedb_vN` only for a change that older releases would misread instead of refusing, or to stop reading old data. Add it to `third_party/duckdb/src/storage/version_map.json` and run `scripts/duckdb_family.sh regen`; `SERENEDB_LATEST` and `SERENEDB_VERSION_UPPER` follow it. To stop reading the data of earlier releases, also point `SERENEDB_VERSION_LOWER` (`third_party/duckdb/src/include/duckdb/storage/storage_info.hpp`) at the new value. Keeping older database files readable (`SERENEDB_VERSION_LOWER` below `SERENEDB_LATEST`) takes one more change, which a `static_assert` in `RequestSereneDBStorageVersion` asks for: attach raises such a file only in memory, so it has to be checkpointed before anything writes to its logs. Make a version change in its own PR and list it in the release notes. A value is never reused.

Test a format change the way a release meets old data: a fault point makes the writer produce the old shape, then the test restarts and reads it back (`tests/sqllogic/recovery/tokenizer_option_compat.test`).

## Search index files

- Every file of a segment ends with a footer that `format_utils::WriteFooter` writes and `format_utils::ReadFooter` checks; the footer's trailer and the leading `storage_version` field of `segments_N` never change. A file's own fields go into the footer's `meta` object through a callback.
- **Field ids:** every object numbers its fields from 0. Name them with `kField...` constants next to the file's writer (`index_meta::kFieldPayload`, `segment_meta::kFieldFiles`, ...), and read them through the same constants.
- **Empty lists** are not written, and are read as optional (`ReadOptionalList`, `ReadOptionalObject`), unless the presence of the list itself means something (the file list of a `.sm`).
- **Callbacks:** footer, list and payload callbacks take `duckdb::BinarySerializer&` and `duckdb::BinaryDeserializer&` (`BinarySerializer::List&` and `BinaryDeserializer::List&` for list elements), never the `Serializer` or `Deserializer` base or `auto&`.
- **New data layout** (block encoding, term dictionary, ...): select it with a new field, and keep reading the old layout while it is supported.
- **Every field is read:** `segments_N` is read with its payload reader. `DirectoryReader` and `DirectoryReader::Reopen` take one, and an index with a payload but no reader is refused.

## Database files

- Checkpoint and write-ahead log entries take the field ids of `third_party/duckdb/src/include/duckdb/storage/serialization/*.json`: a new field is a json member with a new id and a `default`, and a removed one is marked deleted.
- The log header never gains a field; a framing change bumps `WAL_VERSION_NUMBER`.
- Replay never drops an entry that matches its checksum but can't be read: the database refuses to open instead. Only a torn tail is dropped.
- A file opens only at the kind of storage version it was written with: SereneDB opens its own files at `SERENEDB_LATEST` (`RequestSereneDBStorageVersion`).

## DuckDB database files

A database file with a DuckDB storage version (a plain `ATTACH`, `serened shell`) must stay readable by the DuckDB release of that version, and SereneDB reads what that release writes. Field ids and enum values outside SereneDB's ranges belong to upstream:

- **Fields.** A SereneDB field of an upstream class takes 16384 plus the id upstream's numbering would give it: 16484 in a class whose fields start at 100, 16584 at 200. A field from 16384 (`SERENEDB_FIELD_ID_BASE`) up is refused when written to a DuckDB file. With `"version": "serenedb_v1"` on its json member it is skipped there instead: use that for state DuckDB drops as well, such as object ids, constraint names and sequence ownership. The fields of a class SereneDB added keep the usual ids.
- **Enum values.** A SereneDB value of a stored upstream enum starts at 200 (`SERENEDB_ENUM_VALUE_BASE`), and the enum is listed in `IsSereneDBEnumValue`.
- **Data layouts.** A new compression method or block layout is chosen only when `IsSereneDBStorageVersion` holds for the storage version being written.
- **Log entries.** Where SereneDB logs an operation in another shape than DuckDB, a DuckDB file keeps DuckDB's.
- **Catalog.** Objects in a DuckDB file get no owner or privileges (`StoresPermissions` in `server/auth/enforce.cpp`).
- **In-memory databases** have a SereneDB storage version, so they take every SereneDB feature.

`tests/duckdb/run.sh --suite interop` checks both directions against the official `duckdb/duckdb` image. A change to what a DuckDB file holds also follows "Changing the DuckDB on-disk format" in [tests/duckdb/README.md](../../tests/duckdb/README.md).

## Data directory

- Every directory has one owner, and only the owner creates or removes it.
- Create a directory, and fsync its parent, before the statement that creates its object commits.
- Leave crash leftovers to boot.

## Serialized structs

`irs::utils::WriteTuple` and `ReadTuple` store catalog-entry blobs (tokenizer configs, the inverted index payload), the view-backed index manifest and the search-table WAL's segment references. They number fields by member position, so the struct rules above apply member by member, and a member left at its value-initialized value isn't written. A struct boost::pfr can't reflect (one holding a `std::vector<std::unique_ptr<T>>`) declares `SerdeFields(value)` returning `std::tie` of its members, in declaration order.

## Search-table WAL

The frame header never changes, and the record follows the rules above. A record change that needs a new version bumps the database's.
