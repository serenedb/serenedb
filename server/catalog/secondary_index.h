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

#include "basics/down_cast.h"
#include "catalog/index.h"
#include "storage_engine/secondary_index_shard.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

class SecondaryIndex : public Index {
 public:
  SecondaryIndex(ObjectId database_id, ObjectId schema_id, ObjectId id,
                 ObjectId relation_id, std::string name,
                 std::vector<Column::Id> column_ids, bool unique);

  static std::shared_ptr<SecondaryIndex> Deserialize(duckdb::Deserializer& src,
                                                     ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;
  bool IsUnique() const noexcept { return _unique; }

  ResultOr<std::shared_ptr<IndexShard>> CreateIndexShard(
    bool is_new, ObjectId id) const final {
    if (is_new) {
      return std::make_shared<SecondaryIndexShard>(GetId());
    }
    return std::make_shared<SecondaryIndexShard>(id, GetId());
  }

  std::vector<Column::Id> GetReferencedColumnIds() const final {
    const auto ids = GetColumnIds();
    return {ids.begin(), ids.end()};
  }

 private:
  bool _unique;
};

}  // namespace sdb::catalog
