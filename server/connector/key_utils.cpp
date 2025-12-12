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

namespace sdb::connector::key_utils {

std::string PrepareTableKey(ObjectId id) {
  SDB_ASSERT(id.isSet());
  std::string key;
  rocksutils::Concat(key, id, kKeySeparator);
  return key;
}

std::string PrepareColumnKey(ObjectId id, ColumnId column_oid) {
  SDB_ASSERT(id.isSet());
  std::string key;
  rocksutils::Concat(key, id, kKeySeparator, column_oid, kKeySeparator);
  return key;
}

void AppendColumnKey(std::string& key, ColumnId column_oid) {
  SDB_ASSERT(!key.empty());
  SDB_ASSERT(key.ends_with(kKeySeparator),
             "Key must end with column key separator");
  rocksutils::Concat(key, column_oid, kKeySeparator);
}

void AppendCellKey(std::string& key, ColumnId column_oid,
                   std::string_view primary_key) {
  SDB_ASSERT(!primary_key.empty());
  SDB_ASSERT(!key.empty());
  SDB_ASSERT(key.ends_with(kKeySeparator),
             "Key must end with column key separator");
  absl::StrAppend(&key, column_oid, kKeySeparator, primary_key);
}

void AppendPrimaryKey(std::string& key, std::string_view primary_key) {
  SDB_ASSERT(!primary_key.empty());
  SDB_ASSERT(!key.empty());
  SDB_ASSERT(key.ends_with(kKeySeparator),
             "Key must end with column key separator");
  absl::StrAppend(&key, primary_key);
}

std::pair<std::string, std::string> CreateTableRange(ObjectId id) {
  SDB_ASSERT(id.isSet());
  SDB_ASSERT(id.id() != std::numeric_limits<decltype(id.id())>::max());
  std::string start_key;
  rocksutils::Concat(start_key, id);

  std::string end_key;
  rocksutils::Concat(end_key, id.id() + 1);

  return {start_key, end_key};
}

std::pair<std::string, std::string> CreateTableColumnRange(
  ObjectId id, ColumnId column_oid) {
  SDB_ASSERT(id.isSet());
  SDB_ASSERT(column_oid != std::numeric_limits<ColumnId>::max());
  std::string start_key;
  rocksutils::Concat(start_key, id, kKeySeparator, column_oid);

  std::string end_key;
  rocksutils::Concat(end_key, id, kKeySeparator, column_oid + 1);
  return {start_key, end_key};
}

}  // namespace sdb::connector::key_utils
