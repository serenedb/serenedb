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
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include "basics/serializer.h"

#include <atomic>
#include <yaclib/async/make.hpp>

#include "catalog/object.h"
#include "catalog/table.h"

namespace sdb {

TableShard::TableShard(ObjectId id, ObjectId table_id,
                       const catalog::TableStats& stats)
  : catalog::Object{table_id, id, "", catalog::ObjectType::TableShard},
    _num_rows{stats.num_rows} {}

TableShard::TableShard(ObjectId table_id, const catalog::TableStats& stats)
  : catalog::Object{table_id, ObjectId{0}, "", catalog::ObjectType::TableShard},
    _num_rows{stats.num_rows} {}

void TableShard::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, GetTableStats());
}

catalog::TableStats TableShard::DeserializeStats(std::string_view bytes) {
  duckdb::MemoryStream stream{
    const_cast<duckdb::data_t*>(
      reinterpret_cast<const duckdb::data_t*>(bytes.data())),
    bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  catalog::TableStats stats;
  basics::ReadTuple(deserializer, stats);
  return stats;
}

}  // namespace sdb
