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

#include "rocksdb_types.h"

#include "catalog/identifiers/object_id.h"
#include "magic_enum/magic_enum.hpp"

namespace sdb {
namespace {

template<RocksDBEntryType Type>
const rocksdb::Slice MakeSlice() {
  static constexpr auto kData = [] {
    std::array<char, sizeof(ObjectId::BaseType) + 1> buf{};
    auto root = id::kRoot.id();
    for (size_t i = 0; i < sizeof(root); ++i) {
      buf[i] = static_cast<char>((root >> ((sizeof(root) - 1 - i) * 8)) & 0xFF);
    }
    buf[sizeof(root)] = static_cast<char>(Type);
    return buf;
  }();
  return rocksdb::Slice{kData.data(), kData.size()};
}

const auto kPlaceholder = MakeSlice<RocksDBEntryType::Placeholder>();

const auto kSettings = MakeSlice<RocksDBEntryType::SettingsValue>();

const auto kRole = MakeSlice<RocksDBEntryType::Role>();

const auto kDatabase = MakeSlice<RocksDBEntryType::Database>();

const auto kSchema = MakeSlice<RocksDBEntryType::Schema>();

const auto kFunction = MakeSlice<RocksDBEntryType::Function>();

const auto kView = MakeSlice<RocksDBEntryType::View>();

const auto kTable = MakeSlice<RocksDBEntryType::Table>();

const auto kIndex = MakeSlice<RocksDBEntryType::Index>();

const auto kScopeTombstone = MakeSlice<RocksDBEntryType::ScopeTombstone>();

const auto kTableTombstone = MakeSlice<RocksDBEntryType::TableTombstone>();

const auto kIndexTombstone = MakeSlice<RocksDBEntryType::IndexTombstone>();

const auto kTableShard = MakeSlice<RocksDBEntryType::TableShard>();

const auto kIndexShard = MakeSlice<RocksDBEntryType::IndexShard>();

const auto kIndexShardTombstone =
  MakeSlice<RocksDBEntryType::IndexShardTombstone>();

const auto kTablePhysical = MakeSlice<RocksDBEntryType::TablePhysical>();

}  // namespace

char RocksDbFormatVersion() { return '1'; }

const rocksdb::Slice& RocksDbSlice(const RocksDBEntryType& type) {
  switch (type) {
    case RocksDBEntryType::Placeholder:
      return kPlaceholder;
    case RocksDBEntryType::SettingsValue:
      return kSettings;
    case RocksDBEntryType::Role:
      return kRole;
    case RocksDBEntryType::Database:
      return kDatabase;
    case RocksDBEntryType::Schema:
      return kSchema;
    case RocksDBEntryType::Function:
      return kFunction;
    case RocksDBEntryType::View:
      return kView;
    case RocksDBEntryType::Table:
      return kTable;
    case RocksDBEntryType::Index:
      return kIndex;
    case RocksDBEntryType::ScopeTombstone:
      return kScopeTombstone;
    case RocksDBEntryType::TableTombstone:
      return kTableTombstone;
    case RocksDBEntryType::IndexTombstone:
      return kIndexTombstone;
    case RocksDBEntryType::TableShard:
      return kTableShard;
    case sdb::RocksDBEntryType::IndexShard:
      return kIndexShard;
  }

  return kPlaceholder;  // avoids warning - errorslice instead ?!
}

}  // namespace sdb
