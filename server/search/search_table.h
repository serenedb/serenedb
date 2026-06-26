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

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/directory.hpp>
#include <memory>
#include <shared_mutex>

#include "basics/assert.h"
#include "basics/result.h"
#include "catalog/identifiers/object_id.h"
#include "search/search_db_wal.h"

namespace sdb::search {

// Per-table iresearch columnstore store for a TableEngine::Search table -- the
// Search-engine sibling of InvertedIndexStorage. Attached to the
// catalog::Table via GetData()/SetData(); the directory layout is derived from
// (db_id, table_id), see GetPath.
class SearchTable {
 public:
  // `is_new` opens a fresh index (and publishes an empty commit); otherwise the
  // durable index is reopened and its committed tick restored.
  SearchTable(ObjectId db_id, ObjectId table_id, bool is_new);
  ~SearchTable();

  SearchTable(const SearchTable&) = delete;
  SearchTable& operator=(const SearchTable&) = delete;

  // Opens (or reopens, when !is_new) this table's on-disk iresearch store and
  // binds the database WAL; the returned handle is attached to the catalog
  // Table via Table::SetData. Mirror of InvertedIndexStorage::Create.
  static std::shared_ptr<SearchTable> Create(ObjectId db_id, ObjectId table_id,
                                             bool is_new);

  ObjectId GetTableId() const noexcept { return _table_id; }
  auto& GetTableLock() noexcept { return _table_lock; }

  void UpdateNumRows(int64_t delta) noexcept {
    _num_rows.fetch_add(delta, std::memory_order_relaxed);
  }
  int64_t NumRows() const noexcept {
    return _num_rows.load(std::memory_order_relaxed);
  }

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

  void Clear(uint64_t tick) {
    SDB_ASSERT(_writer);
    _writer->Clear(tick);
    if (tick > _last_committed_tick) {
      _last_committed_tick = tick;
    }
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
    UpdateNumRows(live - NumRows());
  }

 private:
  void OpenWriter();

  ObjectId _table_id;
  ObjectId _db_id;
  bool _is_new;
  std::atomic<int64_t> _num_rows{0};
  std::shared_mutex _table_lock;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
  // Borrowed from the search engine (set in OpenWriter). Outlives this object.
  SearchDbWal* _wal = nullptr;
  uint64_t _last_committed_tick = 0;
};

}  // namespace sdb::search
