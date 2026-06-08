////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "table_shard.h"

#include <absl/strings/str_cat.h>

#include <atomic>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <tuple>
#include <yaclib/async/make.hpp>

#include "basics/errors.h"
#include "basics/serializer.h"
#include "catalog/object.h"
#include "catalog/table.h"
#include "connector/key_utils.hpp"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/search_table_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb {

TableShard::TableShard(ObjectId id, ObjectId table_id,
                       const catalog::TableStats& stats)
  : catalog::Object{table_id, id, "", catalog::ObjectType::TableShard},
    _num_rows{stats.num_rows} {}

TableShard::TableShard(ObjectId table_id, const catalog::TableStats& stats)
  : catalog::Object{table_id, ObjectId{0}, "", catalog::ObjectType::TableShard},
    _num_rows{stats.num_rows} {}

// Persists (StorageKind, stats) so the catalog reconstructs the right subclass
// on load (search vs rocksdb). DeserializeStorageKind reads the leading kind
// for that dispatch; DeserializeStats reads the stats.
void TableShard::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink,
                     std::forward_as_tuple(GetStorage(), GetTableStats()));
}

namespace {

std::tuple<catalog::StorageKind, catalog::TableStats> DeserializeShard(
  std::string_view bytes) {
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_t*>(
      reinterpret_cast<const duckdb::data_t*>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  std::tuple<catalog::StorageKind, catalog::TableStats> out;
  basics::ReadTuple(deserializer, out);
  return out;
}

}  // namespace

catalog::TableStats TableShard::DeserializeStats(std::string_view bytes) {
  return std::get<1>(DeserializeShard(bytes));
}

catalog::StorageKind TableShard::DeserializeStorageKind(
  std::string_view bytes) {
  return std::get<0>(DeserializeShard(bytes));
}

Result TableShard::DropArtifacts(catalog::StorageKind kind, ObjectId db_id,
                                 ObjectId schema_id, ObjectId table_id,
                                 ObjectId shard_id, uint64_t size) {
  switch (kind) {
    case catalog::StorageKind::kRocksDB: {
      auto& server = GetServerEngine();
      auto [start, end] = connector::key_utils::CreateTableRange(table_id);
      auto* cf = RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::Default);
      // TODO(codeworse): add some parameter for large range(not just >= 1000)
      return rocksutils::RemoveLargeRange(server.db(), rocksdb::Slice{start},
                                          rocksdb::Slice{end}, cf, true,
                                          (size >= 1000));
    }
    case catalog::StorageKind::kSearch:
      // shard_id is unused for search (single-shard-per-table, path keyed on
      // db/schema/table); the cleanup lives on the search shard.
      (void)shard_id;
      return search::SearchTableShard::DropArtifacts(db_id, schema_id,
                                                     table_id);
  }
  SDB_UNREACHABLE();
}

ResultOr<std::shared_ptr<TableShard>> MakeTableShard(
  catalog::StorageKind kind, ObjectId db_id, ObjectId schema_id,
  ObjectId table_id, const catalog::TableStats& stats) {
  switch (kind) {
    case catalog::StorageKind::kRocksDB:
      return std::make_shared<TableShard>(table_id, stats);
    case catalog::StorageKind::kSearch:
      return std::make_shared<search::SearchTableShard>(db_id, schema_id,
                                                        table_id, stats);
  }
  SDB_UNREACHABLE();
}

}  // namespace sdb
