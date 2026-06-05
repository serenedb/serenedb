////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/directory.hpp>
#include <memory>

#include "catalog/identifiers/object_id.h"
#include "search/search_db_wal.h"
#include "storage_engine/table_shard.h"

namespace sdb::search {

// Table shard backed by an iresearch columnstore (StorageKind::kSearch).
// Created by MakeTableShard(kSearch, ...) and torn down via the static
// TableShard::DropArtifacts(kSearch, ...) helper. The directory layout is
// derived from (db_id, schema_id, table_id, shard_id); see GetPath.
//
// PR 2.2 scope: lifecycle only -- the iresearch writer is created (and an
// empty commit is forced on first-create) but no rows are ever written
// through this class in M2. INSERT / SELECT / UPDATE / DELETE / TRUNCATE
// land in M3-M6 as parallel physical operators.
class SearchTableShard final : public TableShard {
 public:
  // CREATE TABLE path: creates the iresearch directory and an empty writer.
  SearchTableShard(ObjectId db_id, ObjectId schema_id, ObjectId table_id,
                   const catalog::TableStats& stats);

  // Recovery / restore path: opens an existing iresearch directory.
  SearchTableShard(ObjectId db_id, ObjectId schema_id, ObjectId table_id,
                   ObjectId shard_id, const catalog::TableStats& stats);

  // Lets the iresearch writer flush/close cleanly. The shared_ptr's normal
  // destruction order would do this anyway, but binding it to the dtor here
  // documents the lifecycle contract: drop_task's DropArtifacts assumes
  // the on-disk state is consistent before it wipes the directory.
  ~SearchTableShard() override;

  // Directory layout: <search persisted_path for db>/<schema_id>/<table_id>/.
  // No shard_id component: the current arch is single-shard-per-table, and
  // putting shard_id in the path would split create-time and recovery-time
  // paths (the new-shard ctor runs before RegisterObject has stamped an id,
  // so GetId() == 0 then but non-zero on recovery -- a divergence the drop
  // path can't detect after the shard is gone).
  static std::filesystem::path GetPath(ObjectId db_id, ObjectId schema_id,
                                       ObjectId table_id);

  // The DATABASE's search WAL directory (WAL_DESIGN.md §4.0): one `wal/` tree
  // under the per-db engine root, shared by all the db's search shards (the
  // central commit log lives here; chunks under chunks/<schema>/<table>/).
  // iresearch never opens it as a Directory.
  static std::filesystem::path GetWalPath(ObjectId db_id);

  // This shard's bulk chunk subtree:
  // GetWalPath(db)/chunks/<schema_id>/<table_id>/. Wiped on drop (the shared
  // central log is NOT); see DropArtifacts.
  static std::filesystem::path GetChunkDir(ObjectId db_id, ObjectId schema_id,
                                           ObjectId table_id);

  // Returns a fresh iresearch IndexWriter::Transaction tied to this shard's
  // writer. Used by SereneDBSearchInsert (M3) to stash one trx per shard
  // in query::Transaction::_search_transactions; the per-sdb-txn lifetime
  // is then managed by Transaction::Commit/Rollback exactly like
  // InvertedIndexShard's own search trxs.
  irs::IndexWriter::Transaction GetTransaction() noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetBatch();
  }

  // Captures the writer's current segment state as a DirectoryReader.
  // Used by SereneDBSearchScan (M4) -- the result is pinned per-sdb-txn
  // in query::Transaction::EnsureSearchTableReader so every scan inside
  // the same transaction sees the same committed view (mirrors the
  // per-txn pinning InvertedIndexShard's GetInvertedIndexSnapshot
  // provides for inverted-index scans).
  //
  // NOTE: returns a snapshot over whatever segments the writer has
  // already produced; iresearch trxs committed via _trx.Commit() but
  // not yet flushed into a segment are invisible. To force the flush,
  // run VACUUM on the table (or the schema) -- the update_indexes
  // path in duckdb_vacuum_function.cpp calls Commit() below.
  irs::DirectoryReader GetDirectoryReader() noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetSnapshot();
  }

  // Forces an iresearch IndexWriter::Commit so any trxs committed up to
  // this moment land in a real segment visible to GetDirectoryReader.
  // VACUUM <table> / VACUUM (UPDATE_INDEXES) <table> hooks here -- the
  // search-backed table counterpart of InvertedIndexShard::CommitWait.
  // Once SearchTableShard grows a background commit thread (parity with
  // InvertedIndexShard's TasksSettings), explicit VACUUM becomes
  // optional; until then it's the only way to make in-flight inserts
  // visible to subsequent scans.
  void Commit() {
    SDB_ASSERT(_writer && _wal);
    _writer->RefreshCommit();
    // RefreshCommit invoked the meta_payload_provider, advancing
    // _last_committed_tick to the now-durable iresearch tick. Publish it to the
    // db WAL's flush-subscription + run a GC sweep (WAL_DESIGN.md §10.3).
    _wal->OnShardCommit(GetTableId().id(), _last_committed_tick);
  }

  // The database's shared search WAL (WAL_DESIGN.md). Valid after OpenWriter.
  SearchDbWal& Wal() noexcept {
    SDB_ASSERT(_wal);
    return *_wal;
  }

  // Open a bulk chunk-file writer for this shard (delegates to the db WAL with
  // this shard's schema/table). Used by the parallel INSERT sink.
  SearchDbWal::ChunkWriter NewChunkWriter() {
    SDB_ASSERT(_wal);
    return _wal->NewChunkWriter(_schema_id.id(), GetTableId().id());
  }

  ObjectId GetSchemaId() const noexcept { return _schema_id; }

  // The shard's last durable iresearch commit tick (the recovery skip / WAL GC
  // watermark). Recovery (WAL_DESIGN.md §11) replays only records with
  // tick > this. Restored from the iresearch commit-meta payload at OpenWriter.
  uint64_t CommittedTick() const noexcept { return _last_committed_tick; }

  // After WAL recovery replayed + RefreshCommit'd this shard, set num_rows from
  // iresearch's live doc count (WAL_DESIGN.md §12 -- no baseline/delta
  // tracking).
  void SyncNumRowsFromIndex() {
    SDB_ASSERT(_writer);
    auto reader = _writer->GetSnapshot();
    auto live = static_cast<int64_t>(reader.live_docs_count());
    UpdateNumRows(live - GetTableStats().num_rows);
  }

 private:
  void OpenWriter();

  ObjectId _db_id;
  ObjectId _schema_id;
  bool _is_new;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // The database's shared search WAL (WAL_DESIGN.md), owned by the search
  // engine and borrowed here (set in OpenWriter). Outlives the shard.
  SearchDbWal* _wal = nullptr;
  // The iresearch commit tick made durable so far, stored in / restored from
  // the iresearch commit meta payload (meta_payload_provider in OpenWriter,
  // mirroring InvertedIndexShard). Advanced at each RefreshCommit; the WAL
  // GC + recovery skip use it. uint64_t (not atomic): updated only under
  // iresearch's commit lock / single-threaded shard open.
  uint64_t _last_committed_tick = 0;
};

}  // namespace sdb::search
