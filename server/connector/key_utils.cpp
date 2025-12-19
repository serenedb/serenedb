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

#include "key_utils.hpp"

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "rocksdb_engine_catalog/concat.h"

namespace sdb::connector::key_utils {

std::string PrepareTableKey(ObjectId id) {
  SDB_ASSERT(id.isSet());
  std::string key;
  rocksutils::Concat(key, id);
  return key;
}

std::string PrepareColumnKey(ObjectId id, catalog::Column::Id column_oid) {
  SDB_ASSERT(id.isSet());
  std::string key;
  rocksutils::Concat(key, id, column_oid);
  return key;
}

void AppendColumnKey(std::string& key, catalog::Column::Id column_oid) {
  SDB_ASSERT(!key.empty());
  rocksutils::Append(key, column_oid);
}

void ReserveBuffer(std::string& buf) {
  basics::StrResize(buf, sizeof(catalog::Column::Id) + sizeof(ObjectId));
}

void PrepareBufferForLockKey(std::string& buf, std::string_view table_key) {
  SDB_ASSERT(table_key.size() == sizeof(ObjectId));
  std::memcpy(buf.data() + sizeof(catalog::Column::Id), table_key.data(),
              sizeof(ObjectId));
}

std::string_view GetLockKey(std::string& buf) {
  return {buf.begin() + sizeof(catalog::Column::Id), buf.end()};
}

void PrepareBufferForCellKey(std::string& buf, std::string_view table_key) {
  SDB_ASSERT(table_key.size() == sizeof(ObjectId));
  std::memcpy(buf.data(), table_key.data(), sizeof(ObjectId));
}

void SetupColumnForCellKey(std::string& buf, catalog::Column::Id column_id) {
  absl::big_endian::Store32(buf.data() + sizeof(ObjectId), column_id);
}

std::pair<std::string, std::string> CreateTableRange(ObjectId id) {
  SDB_ASSERT(id.isSet());
  if (id.id() != std::numeric_limits<decltype(id.id())>::max()) {
    return {PrepareTableKey(id), PrepareTableKey(ObjectId{id.id() + 1})};
  }
  return {
    PrepareTableKey(id),
    PrepareColumnKey(id, std::numeric_limits<catalog::Column::Id>::max())};
}

std::pair<std::string, std::string> CreateTableColumnRange(
  ObjectId id, catalog::Column::Id column_oid) {
  SDB_ASSERT(column_oid != std::numeric_limits<catalog::Column::Id>::max());
  return {PrepareColumnKey(id, column_oid),
          PrepareColumnKey(id, column_oid + 1)};
}

}  // namespace sdb::connector::key_utils
