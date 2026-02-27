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

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "rocksdb_engine_catalog/rocksdb_common.h"

namespace sdb {

/// Globally defined column families. If you do change the number of
/// column-families consider if there
/// is a need for an upgrade script. Added column families
/// can be created automatically by rocksdb.
/// Do check the RocksDB WAL tailing code and the
/// counter manager. Maybe the the number of families in the shouldHandle method
/// needs to be changed
struct RocksDBColumnFamilyManager {
  enum class Family : size_t {
    Default = 0,      // serenedb connector data
    Definitions = 1,  // serenedb catalog data
    Invalid = std::numeric_limits<size_t>::max(),
  };

  static constexpr size_t kMinNumberOfColumnFamilies = 2;
  static constexpr size_t kNumberOfColumnFamilies = 2;

  static rocksdb::ColumnFamilyHandle* get(Family family);
  static void set(Family family, rocksdb::ColumnFamilyHandle* handle);

  static const char* name(Family family);
  static const char* name(rocksdb::ColumnFamilyHandle* handle);

  static const std::array<rocksdb::ColumnFamilyHandle*,
                          kNumberOfColumnFamilies>&
  allHandles();

 private:
  static std::array<const char*, kNumberOfColumnFamilies> gNames;
  static std::array<rocksdb::ColumnFamilyHandle*, kNumberOfColumnFamilies>
    gHandles;
};

}  // namespace sdb
