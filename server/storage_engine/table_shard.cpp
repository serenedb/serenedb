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
#include <filesystem>
#include <system_error>
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
#include "storage_engine/search_engine.h"

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
    case catalog::StorageKind::kSearch: {
      // remove_all returns 0 and leaves ec clear when the path doesn't
      // exist -- treat that as success (idempotent drop, also covers the
      // create-then-rollback path where the dir may never have been made).
      // shard_id is unused: single-shard-per-table arch, path is keyed on
      // (db_id, schema_id, table_id) only -- see SearchTableShard::GetPath.
      (void)shard_id;
      auto path = search::SearchTableShard::GetPath(db_id, schema_id, table_id);
      std::error_code ec;
      std::filesystem::remove_all(path, ec);
      if (ec) {
        return Result{ERROR_INTERNAL,
                      "Failed to remove search table shard directory '" +
                        path.string() + "': " + ec.message()};
      }
      // Wipe THIS shard's bulk chunk subtree (WAL_DESIGN.md §4.0). The central
      // commit log is per-DATABASE (shared), so it is NOT removed here -- the
      // dropped shard's central sections become orphans (skipped on recovery
      // via the catalog, GC'd with their segment).
      auto chunk_dir =
        search::SearchTableShard::GetChunkDir(db_id, schema_id, table_id);
      std::filesystem::remove_all(chunk_dir, ec);
      if (ec) {
        return Result{ERROR_INTERNAL,
                      "Failed to remove search table chunk directory '" +
                        chunk_dir.string() + "': " + ec.message()};
      }
      // Deregister from the db WAL's flush-subscription so the dropped shard's
      // frozen committed tick can't pin GC (WAL_DESIGN.md §10.3).
      search::GetSearchEngine().GetDbWal(db_id).DeregisterShard(table_id.id());
      return {};
    }
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
