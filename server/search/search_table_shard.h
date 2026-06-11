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
// The directory layout is derived from (db_id, table_id); see GetPath.
class SearchTableShard final : public TableShard {
 public:
  SearchTableShard(ObjectId db_id, ObjectId table_id,
                   const catalog::TableStats& stats);

  SearchTableShard(ObjectId db_id, ObjectId table_id, ObjectId shard_id,
                   const catalog::TableStats& stats);

  ~SearchTableShard() override;

  static std::filesystem::path GetPath(ObjectId db_id, ObjectId table_id);
  static std::filesystem::path GetWalPath(ObjectId db_id);
  static std::filesystem::path GetChunkDir(ObjectId db_id, ObjectId table_id);
  static Result DropArtifacts(ObjectId db_id, ObjectId table_id);

  irs::IndexWriter::Transaction GetTransaction() noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetBatch();
  }

  irs::DirectoryReader GetDirectoryReader() noexcept {
    SDB_ASSERT(_writer);
    return _writer->GetSnapshot();
  }

  void Commit() {
    SDB_ASSERT(_writer && _wal);
    _writer->RefreshCommit();
    _wal->OnShardCommit(GetTableId(), _last_committed_tick);
  }

  SearchDbWal& Wal() noexcept {
    SDB_ASSERT(_wal);
    return *_wal;
  }

  SearchDbWal::ChunkWriter NewChunkWriter() {
    SDB_ASSERT(_wal);
    return _wal->NewChunkWriter(GetTableId());
  }

  uint64_t CommittedTick() const noexcept { return _last_committed_tick; }

  void SyncNumRowsFromIndex() {
    SDB_ASSERT(_writer);
    auto reader = _writer->GetSnapshot();
    auto live = static_cast<int64_t>(reader.live_docs_count());
    UpdateNumRows(live - GetTableStats().num_rows);
  }

 private:
  void OpenWriter();

  ObjectId _db_id;
  bool _is_new;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // Borrowed from the search engine (set in OpenWriter). Outlives the shard.
  SearchDbWal* _wal = nullptr;
  uint64_t _last_committed_tick = 0;
};

}  // namespace sdb::search
