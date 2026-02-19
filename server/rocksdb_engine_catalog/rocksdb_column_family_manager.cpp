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

#include "rocksdb_column_family_manager.h"

namespace sdb {

std::array<const char*,
           sdb::RocksDBColumnFamilyManager::kNumberOfColumnFamilies>
  RocksDBColumnFamilyManager::gNames = {
    "definitions",
    "default",
};

std::array<rocksdb::ColumnFamilyHandle*,
           RocksDBColumnFamilyManager::kNumberOfColumnFamilies>
  RocksDBColumnFamilyManager::gHandles = {
    nullptr,
    nullptr,
};

rocksdb::ColumnFamilyHandle* RocksDBColumnFamilyManager::get(Family family) {
  SDB_VERIFY(family != Family::Invalid);
  const auto index = std::to_underlying(family);
  SDB_ASSERT(index < gHandles.size());
  return gHandles[index];
}

void RocksDBColumnFamilyManager::set(Family family,
                                     rocksdb::ColumnFamilyHandle* handle) {
  SDB_VERIFY(family != Family::Invalid);
  const auto index = std::to_underlying(family);
  SDB_ASSERT(index < gHandles.size());
  gHandles[index] = handle;
}

const char* RocksDBColumnFamilyManager::name(Family family) {
  SDB_VERIFY(family != Family::Invalid);
  const auto index = std::to_underlying(family);
  SDB_ASSERT(index < gNames.size());
  return gNames[index];
}

const char* RocksDBColumnFamilyManager::name(
  rocksdb::ColumnFamilyHandle* handle) {
  for (size_t i = 0; i < gHandles.size(); ++i) {
    if (gHandles[i] == handle) {
      return gNames[i];
    }
  }
  SDB_VERIFY(false);
}

const std::array<rocksdb::ColumnFamilyHandle*,
                 RocksDBColumnFamilyManager::kNumberOfColumnFamilies>&
RocksDBColumnFamilyManager::allHandles() {
  return gHandles;
}

}  // namespace sdb
