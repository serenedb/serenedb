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
#include <vpack/builder.h>
#include <vpack/collection.h>
#include <vpack/iterator.h>
#include <vpack/slice.h>

#include <atomic>
#include <yaclib/async/make.hpp>

#include "basics/errors.h"
#include "catalog/object.h"
#include "catalog/table.h"
#include "connector/key_utils.hpp"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb {

TableShard::TableShard(ObjectId id, ObjectId table_id,
                       const catalog::TableStats& stats)
  : catalog::Object{ObjectId{0}, id, "", catalog::ObjectType::TableShard},
    _table_id{table_id},
    _num_rows{stats.num_rows} {}

TableShard::TableShard(ObjectId table_id, const catalog::TableStats& stats)
  : catalog::Object{ObjectId{0}, ObjectId{0}, "",
                    catalog::ObjectType::TableShard},
    _table_id{table_id},
    _num_rows{stats.num_rows} {}

Result TableShard::DropArtifacts(StorageKind kind, ObjectId table_id,
                                 ObjectId /*shard_id*/, uint64_t size) {
  switch (kind) {
    case StorageKind::kRocksDB: {
      auto& server = GetServerEngine();
      auto [start, end] = connector::key_utils::CreateTableRange(table_id);
      auto* cf = RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::Default);
      // TODO(codeworse): add some parameter for large range(not just >= 1000)
      return rocksutils::RemoveLargeRange(server.db(), rocksdb::Slice{start},
                                          rocksdb::Slice{end}, cf, true,
                                          (size >= 1000));
    }
    case StorageKind::kSearch:
      // SearchTableShard is not implemented yet (lands in M2). When it
      // does, this case computes the iresearch directory path from
      // (table_id, shard_id) and removes it. Until then, reaching this
      // case means corruption -- a kSearch shard payload exists on disk
      // but the code that handles it doesn't.
      SDB_ASSERT(false, "DropArtifacts(kSearch) not yet implemented");
      return Result{ERROR_NOT_IMPLEMENTED};
  }
  SDB_UNREACHABLE();
}

}  // namespace sdb
