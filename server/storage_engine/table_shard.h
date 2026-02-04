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

#pragma once

#include <rocksdb/types.h>

#include <atomic>
#include <functional>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/read_write_lock.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/index_id.h"
#include "catalog/identifiers/revision_id.h"
#include "catalog/identifiers/transaction_id.h"
#include "catalog/table_options.h"
#include "storage_engine/replication_iterator.h"
// consider just forward declaration
#include "utils/coro_helper.h"
#include "utils/merkle_tree.h"
#include "utils/operation_result.h"

#ifdef SDB_CLUSTER
#include "cluster/follower_info.h"
#include "indexes/index.h"
#include "indexes/index_iterator.h"
#endif

namespace vpack {
class Builder;
class Slice;
}  // namespace vpack
namespace sdb {
namespace transaction {
class Methods;
}

class Index;
class IndexesSnapshot;
class FollowerInfo;
class DocumentIterator;

struct OperationOptions;
class Result;

catalog::TableMeta MakeTableMeta(const catalog::Table& c);

class TableShard {
 public:
  static constexpr double kDefaultLockTimeout = 10.0 * 60.0;

  virtual ~TableShard() = default;

  auto& GetMeta() const noexcept { return _collection_meta; }
  auto GetId() const noexcept { return _collection_meta.id; }

  void UpdateNumRows(int64_t delta) {
    _num_rows.fetch_add(delta, std::memory_order_relaxed);
  }

  catalog::TableStats GetTableStats() const {
    return {.num_rows = _num_rows.load(std::memory_order_relaxed)};
  }

  void GetTableStatsVPack(vpack::Builder& builder) const {
    vpack::WriteTuple(builder, GetTableStats());
  }

  auto GetIndexes() const { return _indexes; }

  void AddIndex(ObjectId index_id) {
    auto [_, is_new] = _indexes.emplace(index_id);
    SDB_ENSURE(is_new, ERROR_INTERNAL, "Index already exists in TableShard");
  }

  void RemoveIndex(ObjectId index_id) {
    auto num_erased = _indexes.erase(index_id);
    SDB_ENSURE(num_erased == 1, ERROR_INTERNAL,
               "Index does not exist in TableShard");
  }

  explicit TableShard(catalog::TableMeta collection,
                      const catalog::TableStats& stats);

 protected:
  /// Inject figures that are specific to StorageEngine
  virtual void figuresSpecific(bool details, vpack::Builder&) {}

  catalog::TableMeta _collection_meta;

  containers::FlatHashSet<ObjectId> _indexes;

  // TODO(codeworse): this probably won't work in case of distributed setup
  std::atomic_uint64_t _num_rows{0};
};

}  // namespace sdb
