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

#include "catalog/drop_task.h"

#include <rocksdb/options.h>

#include <yaclib/async/join.hpp>
#include <yaclib/async/make.hpp>
#include <yaclib/async/when_all.hpp>

#include "basics/assert.h"
#include "basics/errors.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/types.h"
#include "connector/key_utils.hpp"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::catalog {

namespace {
bool CheckResult(const Result& result) {
  return result == ERROR_OK || result == ERROR_SERVER_DATA_SOURCE_NOT_FOUND ||
         result == ERROR_SERVER_DATABASE_NOT_FOUND ||
         result == ERROR_SERVER_DOCUMENT_NOT_FOUND ||
         result == ERROR_SERVER_INDEX_NOT_FOUND;
}
}  // namespace

AsyncResult TableShardDrop::operator()() {
  auto& server = GetServerEngine();
  auto [start, end] = connector::key_utils::CreateTableRange(parent_id);
  // Drop table data
  auto r = server.DropRange(start, end,
                            RocksDBColumnFamilyManager::get(
                              RocksDBColumnFamilyManager::Family::Default));
  if (!CheckResult(r)) {
    return QueueDropTask(shared_from_this());
  }
  rocksdb::Slice start_slice{start}, end_slice{end};
  r = rocksutils::ConvertStatus(server.db()->CompactRange(
    rocksdb::CompactRangeOptions{}, &start_slice, &end_slice));
  if (!CheckResult(r)) {
    return QueueDropTask(shared_from_this());
  }
  r = server.DropDefinition(parent_id, RocksDBEntryType::TableShard, id);
  if (!CheckResult(r)) {
    return QueueDropTask(shared_from_this());
  }
  return yaclib::MakeFuture<Result>();
}

AsyncResult IndexShardDrop::operator()() {
  auto& server = GetServerEngine();
  // TODO(codeworse): in case of table/schema/database drop delete the entire
  // folder at once
  if (type == IndexType::Inverted) {
    auto path = search::InvertedIndexShard::GetPath(db_id, schema_id, id);
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
      return QueueDropTask(shared_from_this());
    }
  }
  if (is_root) {
    auto r = server.DropDefinition(parent_id, RocksDBEntryType::IndexShard, id);
    if (!CheckResult(r)) {
      return QueueDropTask(shared_from_this());
    }
  }
  return yaclib::MakeFuture<Result>();
}

Result IndexDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::IndexShard);
  if (!CheckResult(r)) {
    return r;
  }
  r = server.DropDefinition(parent_id, RocksDBEntryType::Index, id);
  if (!CheckResult(r)) {
    return r;
  }
  if (is_root) {
    return server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
  }
  return {};
}

AsyncResult IndexDrop::operator()() {
  auto shard_task = std::make_shared<IndexShardDrop>(
    DropTask{.parent_id = id, .id = shard_id, .is_root = false}, db_id,
    schema_id, type);
  auto r = co_await QueueDropTask(std::move(shard_task));
  if (!CheckResult(r) || !CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

Result TableDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::TableShard);
  if (!CheckResult(r)) {
    return r;
  }
  r = server.DropDefinition(parent_id, RocksDBEntryType::Table, id);
  if (!CheckResult(r)) {
    return r;
  }
  if (is_root) {
    return server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
  }
  return {};
}

AsyncResult TableDrop::operator()() {
  std::vector<AsyncResult> async_results;
  async_results.reserve(indexes.size());
  for (auto& index : indexes) {
    async_results.push_back(QueueDropTask(index));
  }
  if (!async_results.empty()) {
    co_await yaclib::Join(async_results.begin(), async_results.end());
  }
  auto shard_task = std::make_shared<TableShardDrop>(
    DropTask{.parent_id = id, .id = shard_id, .is_root = false});
  auto r = co_await QueueDropTask(std::move(shard_task));
  if (!CheckResult(r) || !CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

Result SchemaDrop::Finalize() {
  auto& server = GetServerEngine();
  for (auto entry_type : {RocksDBEntryType::Table, RocksDBEntryType::View,
                          RocksDBEntryType::Function}) {
    auto r = server.DropEntry(id, entry_type);
    if (!CheckResult(r)) {
      return r;
    }
  }
  auto r = server.DropDefinition(parent_id, RocksDBEntryType::Schema, id);
  if (!CheckResult(r)) {
    return r;
  }
  if (is_root) {
    r = server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
    if (!CheckResult(r)) {
      return r;
    }
  }
  return {};
}

AsyncResult SchemaDrop::operator()() {
  std::vector<AsyncResult> async_results;
  async_results.reserve(tables.size());
  for (auto& table : tables) {
    async_results.push_back(QueueDropTask(table));
  }
  if (!async_results.empty()) {
    co_await yaclib::Join(async_results.begin(), async_results.end());
  }
  if (!CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

Result DatabaseDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::Schema);
  if (!CheckResult(r)) {
    return r;
  }
  r = server.DropDefinition(id::kInstance, RocksDBEntryType::Database, id);
  if (!CheckResult(r)) {
    return r;
  }
  return server.DropDefinition(id::kInstance, RocksDBEntryType::Tombstone, id);
}

AsyncResult DatabaseDrop::operator()() {
  SDB_ASSERT(is_root);
  std::vector<AsyncResult> async_results;
  async_results.reserve(schemas.size());
  for (auto& schema : schemas) {
    async_results.push_back(QueueDropTask(schema));
  }
  if (!async_results.empty()) {
    co_await yaclib::Join(async_results.begin(), async_results.end());
  }
  if (!CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

}  // namespace sdb::catalog
