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
#include "search/search_shard_wal.h"
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

  // WAL directory for this shard (WAL_DESIGN.md §4.0): a sibling tree under the
  // same per-db engine root, keyed like GetPath but under `wal/` so iresearch
  // never opens it as a directory (its unreferenced-file GC would delete our
  // files). Wiped alongside GetPath on drop.
  static std::filesystem::path GetWalPath(ObjectId db_id, ObjectId schema_id,
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
    SDB_ASSERT(_writer);
    _writer->RefreshCommit();
  }

  // The shard's self-contained WAL (WAL_DESIGN.md). Valid after construction.
  SearchShardWal& Wal() noexcept {
    SDB_ASSERT(_wal);
    return *_wal;
  }

 private:
  void OpenWriter();

  ObjectId _db_id;
  ObjectId _schema_id;
  bool _is_new;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // Per-shard self-contained WAL (WAL_DESIGN.md). Constructed in OpenWriter;
  // borrows the DuckDB engine's FileSystem, which outlives the shard.
  std::unique_ptr<SearchShardWal> _wal;
};

}  // namespace sdb::search
