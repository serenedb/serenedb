---
paths:
  - "iresearch/formats/**"
  - "iresearch/index/directory_reader*"
  - "iresearch/index/index_writer*"
  - "iresearch/utils/serialization.hpp"
  - "iresearch/utils/serializer.hpp"
  - "server/catalog/boot.*"
  - "server/catalog/cluster.*"
  - "server/catalog/persistence/**"
  - "server/connector/duckdb_storage_extension.*"
  - "server/connector/file_manifest.*"
  - "server/connector/index_expression.*"
  - "server/search/inverted_index_storage.*"
  - "server/search/index_durability.*"
  - "server/search/search_table.*"
  - "server/search/search_table_transaction.*"
  - "server/search/wal_recovery.*"
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

## Search tables in the write-ahead log

A commit logs each search table it changed into its database's write-ahead log, inside the same commit as the rest of the transaction: `USE_TABLE` with the table's oid, then `TRUNCATE_TABLE` if the transaction emptied the table, then `ADOPT_SEGMENTS` naming the meta files of segments flushed and fsynced before the commit, then the rows still buffered and the removals, in the order they were made. `INSERT_TUPLE` carries buffered rows and, in its `row_start` field (id 16486), the generated key of its first row; `DELETE_TUPLE` carries the generated keys of rows a `DELETE` removed or an `UPDATE` replaced, one entry per batch a statement processed. The segments can go ahead of the removals because a removal only names rows committed before the transaction, and a `TRUNCATE` drops whatever the transaction wrote before it, so nothing precedes `TRUNCATE_TABLE`.

A transaction buffers a table's rows until they reach the database's `checkpoint_threshold`; then they go into the table's exclusive iresearch transaction, whose segments are written when one fills up and when the statement ends, so the commit only fsyncs them and `ADOPT_SEGMENTS` names them. Rows still buffered at commit are logged inline. The entries after `USE_TABLE` are serialized before the commit takes DuckDB's commit lock, straight into one buffer (`WALEntryBuffer`); under the lock the commit only writes `USE_TABLE` and appends that buffer (`WriteAheadLog::WriteEntries`).

Search tables and inverted indexes share one durability model, `search::IndexDurability`:

- Under the commit lock, after the commit's WAL write, a transaction registers its index transactions with their writers and takes their ticks (`TransactionPreCheckpoint`); once the WAL is synced it commits them at the commit's WAL position (`TransactionDurable`). Registration makes a refresh wait for them, so a refresh saves a prefix of the log.
- Each saved index meta records the highest WAL position it holds, as the checkpoint iteration and the offset. An inverted index's replay skips the entries below that offset when the iteration is still the database's, and replays the whole log otherwise; a search table's replay skips the entries before that position, whichever log holds them.
- Boot finishes the replays once every database has loaded (`search::RecoverSearchStorages`) and syncs each write-ahead log first, so the end-of-log position a replay saves is never past what the log durably holds.
- A checkpoint saves every inverted index in two phases: before the new database header is written, as a pending meta tagged with the checkpoint's iteration; after the header, by committing it. A refresh whose meta holds commits made during a running checkpoint saves it the same way, tagged with that checkpoint; the checkpoint commits it at its end, or, for a search table busy at that moment, the table's next refresh does, so a checkpoint never waits for a search table. An index that opens with a pending meta commits it when the database file reached that iteration and discards it otherwise.
- A checkpoint never saves a search table. It keeps the log it retires as `.wal.retained.<iteration>` while a table has a commit that is not in a meta its directory sync made durable (`IndexDurability::NeededWAL`), and deletes the kept logs below the oldest iteration a table still needs. Kept logs stay contiguous: while there are any, a checkpoint whose log is empty still retires it, and a checkpoint request with nothing to checkpoint only deletes.
- Boot replays the kept logs oldest first, search-table entries only, before the main log. A main log whose checkpoint wrote the header but did not retire it becomes a kept log when the database has search tables, and a missing main log next to a checkpoint log means the crash came between retiring the one and moving the other over it, so the checkpoint log becomes the main log. Once the tables are refreshed, boot deletes the kept logs they no longer need.
- A prepared commit's decision stays in the catalog log while a log holding its batch can still be replayed (`StorageManager::GetOldestWALGeneration`).

An autocommit `TRUNCATE` that clears the index holds the database's shared checkpoint lock from before its entry is written until the index is cleared. The clear writes its meta directly instead of through a refresh, and must not meet the pending meta of a refresh that ran during a checkpoint.

## DuckDB database files

A database file with a DuckDB storage version (a plain `ATTACH`, `serened shell`) must stay readable by the DuckDB release of that version, and SereneDB reads what that release writes. Field ids and enum values outside SereneDB's ranges belong to upstream:

- **Fields.** A SereneDB field of an upstream class takes 16384 plus the id upstream's numbering would give it: 16484 in a class whose fields start at 100, 16584 at 200. A field from 16384 (`SERENEDB_FIELD_ID_BASE`) up is refused when written to a DuckDB file. With `"version": "serenedb_v1"` on its json member it is skipped there instead: use that for state DuckDB drops as well, such as object ids, constraint names and sequence ownership. The fields of a class SereneDB added keep the usual ids.
- **Enum values.** A SereneDB value of a stored upstream enum starts at 200 (`SERENEDB_ENUM_VALUE_BASE`), and the enum is listed in `IsSereneDBEnumValue`.
- **Data layouts.** A new compression method or block layout is chosen only when `IsSereneDBStorageVersion` holds for the storage version being written.
- **Log entries.** Where SereneDB logs an operation in another shape than DuckDB, a DuckDB file keeps DuckDB's.
- **Catalog.** Objects in a DuckDB file get no owner or privileges (`StoresPermissions` in `server/auth/enforce.cpp`).
- **In-memory databases** have a SereneDB storage version, so they take every SereneDB feature.

`tests/duckdb/run.sh --suite interop` checks both directions against the official `duckdb/duckdb` image. A change to what a DuckDB file holds also follows "Changing the DuckDB on-disk format" in [tests/duckdb/README.md](../../tests/duckdb/README.md).

## Serialized structs

`irs::utils::WriteTuple` and `ReadTuple` store catalog-entry blobs (tokenizer configs, the inverted index payload) and the view-backed index manifest. They number fields by member position, so the struct rules above apply member by member, and a member left at its value-initialized value isn't written. A struct boost::pfr can't reflect (one holding a `std::vector<std::unique_ptr<T>>`) declares `SerdeFields(value)` returning `std::tie` of its members, in declaration order.
