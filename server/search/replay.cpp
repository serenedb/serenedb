////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "search/replay.h"

#include <algorithm>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <iresearch/utils/log.hpp>
#include <utility>

#include "query/config_variable_names.h"
#include "search/row_position_wal.h"

namespace sdb::search {
namespace {

constexpr uint64_t kReplayBytesPerSlot = uint64_t{1} << 20;

size_t ReplaySlots(duckdb::AttachedDatabase& db,
                   const irs::SourcePosition& durable) {
  auto& storage = db.GetStorageManager();
  const uint64_t durable_offset =
    durable.generation == storage.GetBlockManager().GetCheckpointIteration()
      ? durable.offset
      : 0;
  auto& instance = db.GetDatabase();
  auto& fs = instance.GetInternalFileSystem();
  const auto handle = fs.OpenFile(
    storage.GetWALPath(), duckdb::FileFlags::FILE_FLAGS_READ |
                            duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
  if (!handle) {
    return 1;
  }
  const uint64_t size = handle->GetFileSize();
  const auto pending = size > durable_offset ? size - durable_offset : 0;
  return std::clamp<size_t>(
    pending / kReplayBytesPerSlot, 1,
    std::max<size_t>(
      1, duckdb::TaskScheduler::GetScheduler(instance).NumberOfThreads()));
}

size_t ReplayDepth(duckdb::DatabaseInstance& db) {
  auto& config = duckdb::DBConfig::GetConfig(db);
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  const auto index = config.TryGetSettingIndex(
    duckdb::Identifier{kRecoveryReplayDepthSetting}, option);
  duckdb::Value value;
  size_t depth = 0;
  if (index.IsValid() &&
      config.user_settings.TryGetSetting(index.GetIndex(), value) &&
      !value.IsNull()) {
    depth = value.GetValue<uint32_t>();
  }
  if (depth == 0) {
    depth = 4 * std::max<size_t>(
                  1, duckdb::TaskScheduler::GetScheduler(db).NumberOfThreads());
  }
  return std::clamp<size_t>(depth, 1, 1024);
}

}  // namespace

Replay::Replay(duckdb::AttachedDatabase& db, Store& store)
  : _db{db},
    _store{store},
    _head{store.GetTransaction()},
    _tail{store.GetTransaction()} {
  _slots.push_back(store.GetTransaction());
}

void Replay::FeedInParallel(
  absl::FunctionRef<Feeder::Feed(std::span<irs::IndexWriter::Transaction>)>
    make_feed) {
  const auto slots = ReplaySlots(_db, _store.PersistedPosition());
  while (_slots.size() < slots) {
    _slots.push_back(_store.GetTransaction());
  }
  auto& instance = _db.GetDatabase();
  _feeder.emplace(instance, slots,
                  std::max<size_t>(1, ReplayDepth(instance) / slots),
                  make_feed(_slots));
}

void Replay::Truncate() {
  SDB_ASSERT(!_feeder);
  for (auto& slot : _slots) {
    slot.Abort();
  }
  _head.Abort();
  _removals.Clear();
  _head.RemoveAll();
}

void Replay::Adopt(std::span<const std::string> segments) {
  for (const auto& segment : segments) {
    try {
      _head.Adopt(segment);
    } catch (const std::exception& e) {
      SDB_FATAL(SEARCH, "search WAL recovery: failed to adopt segment '",
                segment, "' for store ", _store.GetId(), ": ",
                duckdb::ErrorData{e}.RawMessage());
    }
  }
}

void Replay::Finish() {
  if (_feeder) {
    _feeder->Finish();
  }
  if (_removals.HasKeys()) {
    _tail.Remove(irs::KeyRemoval{_removals.TakeKeys()});
  }
  if (auto positions =
        std::move(_removals).Resolve(_store.GetDirectoryReader())) {
    _tail.Remove(std::move(positions));
  }
  auto& storage = _db.GetStorageManager();
  const irs::SourcePosition end{
    storage.GetBlockManager().GetCheckpointIteration(), storage.GetWALSize()};
  std::vector<irs::IndexWriter::Transaction*> trxs;
  trxs.reserve(_slots.size() + 2);
  trxs.push_back(&_head);
  for (auto& slot : _slots) {
    trxs.push_back(&slot);
  }
  trxs.push_back(&_tail);
  irs::IndexWriter::Transaction::Stage(trxs);
  for (auto* trx : trxs) {
    SDB_FATAL_IF(SEARCH, !trx->Commit(end),
                 "search WAL recovery: iresearch commit failed for store ",
                 _store.GetId());
  }
  _store.MarkStaged(end);
}

}  // namespace sdb::search
