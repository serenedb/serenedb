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

#include "catalog/fwd.h"
#include "catalog/table_options.h"

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

  void UpdateNumRows(int64_t delta) noexcept {
    _num_rows.fetch_add(delta, std::memory_order_relaxed);
  }

  catalog::TableStats GetTableStats() const {
    return {.num_rows = _num_rows.load(std::memory_order_relaxed)};
  }

  void GetTableStatsVPack(vpack::Builder& builder) const {
    vpack::WriteTuple(builder, GetTableStats());
  }

  explicit TableShard(catalog::TableMeta collection,
                      const catalog::TableStats& stats);

 protected:
  /// Inject figures that are specific to StorageEngine
  virtual void figuresSpecific(bool details, vpack::Builder&) {}

  catalog::TableMeta _collection_meta;

  // TODO(codeworse): this probably won't work in case of distributed setup
  std::atomic_uint64_t _num_rows{0};
};

}  // namespace sdb
