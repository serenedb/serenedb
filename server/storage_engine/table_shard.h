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

#include <atomic>

#include "catalog/fwd.h"
#include "catalog/object.h"
#include "catalog/table_options.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb {

class TableShard : public catalog::Object {
 public:
  virtual ~TableShard() = default;
  std::shared_ptr<Object> Clone() const final { return nullptr; }

  ObjectId GetTableId() const noexcept { return GetParentId(); }

  void UpdateNumRows(int64_t delta) noexcept {
    _num_rows.fetch_add(delta, std::memory_order_relaxed);
  }

  catalog::TableStats GetTableStats() const {
    return {.num_rows = _num_rows.load(std::memory_order_relaxed)};
  }

  void Serialize(duckdb::Serializer& sink) const final;
  static catalog::TableStats DeserializeStats(std::string_view bytes);

  // New table shard ctor
  explicit TableShard(ObjectId table_id, const catalog::TableStats& stats);

  // existed table shard ctor
  explicit TableShard(ObjectId id, ObjectId table_id,
                      const catalog::TableStats& stats);

 protected:
  // TODO(codeworse): this probably won't work in case of distributed setup
  std::atomic_uint64_t _num_rows{0};
};

}  // namespace sdb
