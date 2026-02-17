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

const auto kDatabase = MakeSlice<RocksDBEntryType::Database>();

const auto kFunction = MakeSlice<RocksDBEntryType::Function>();

const auto kCollection = MakeSlice<RocksDBEntryType::Collection>();

const auto kCounter = MakeSlice<RocksDBEntryType::CounterValue>();

const auto kDocument = MakeSlice<RocksDBEntryType::Document>();

const auto kPrimaryIndex = MakeSlice<RocksDBEntryType::PrimaryIndexValue>();

const auto kEdgeIndex = MakeSlice<RocksDBEntryType::EdgeIndexValue>();

const auto kVPackIndex = MakeSlice<RocksDBEntryType::VPackIndexValue>();

const auto kUniqueVPackIndex =
  MakeSlice<RocksDBEntryType::UniqueVPackIndexValue>();

const auto kView = MakeSlice<RocksDBEntryType::View>();

const auto kRole = MakeSlice<RocksDBEntryType::Role>();

const auto kSchema = MakeSlice<RocksDBEntryType::Schema>();

const auto kSettings = MakeSlice<RocksDBEntryType::SettingsValue>();

const auto kReplicationApplierConfig =
  MakeSlice<RocksDBEntryType::ReplicationApplierConfig>();

const auto kIndexEstimate = MakeSlice<RocksDBEntryType::IndexEstimateValue>();

const auto kKeyGenerator = MakeSlice<RocksDBEntryType::KeyGeneratorValue>();

const auto kRevisionTree = MakeSlice<RocksDBEntryType::RevisionTreeValue>();

const auto kTableTombstone = MakeSlice<RocksDBEntryType::TableTombstone>();

const auto kScopeTombstone = MakeSlice<RocksDBEntryType::ScopeTombstone>();

const auto kIndexTombstone = MakeSlice<RocksDBEntryType::IndexTombstone>();

const auto kIndex = MakeSlice<RocksDBEntryType::Index>();

const auto kStats = MakeSlice<RocksDBEntryType::Stats>();

const auto kIndexPhysical = MakeSlice<RocksDBEntryType::IndexPhysical>();

const auto kIndexShardTombstone =
  MakeSlice<RocksDBEntryType::IndexShardTombstone>();

const auto kTablePhysical = MakeSlice<RocksDBEntryType::TablePhysical>();

}  // namespace

char RocksDbFormatVersion() { return '1'; }

const rocksdb::Slice& RocksDbSlice(const RocksDBEntryType& type) {
  switch (type) {
    case RocksDBEntryType::Placeholder:
      return kPlaceholder;
    case RocksDBEntryType::Database:
      return kDatabase;
    case RocksDBEntryType::Function:
      return kFunction;
    case RocksDBEntryType::Collection:
      return kCollection;
    case RocksDBEntryType::CounterValue:
      return kCounter;
    case RocksDBEntryType::Document:
      return kDocument;
    case RocksDBEntryType::PrimaryIndexValue:
      return kPrimaryIndex;
    case RocksDBEntryType::EdgeIndexValue:
      return kEdgeIndex;
    case RocksDBEntryType::VPackIndexValue:
      return kVPackIndex;
    case RocksDBEntryType::UniqueVPackIndexValue:
      return kUniqueVPackIndex;
    case RocksDBEntryType::View:
      return kView;
    case RocksDBEntryType::Role:
      return kRole;
    case RocksDBEntryType::Schema:
      return kSchema;
    case RocksDBEntryType::SettingsValue:
      return kSettings;
    case RocksDBEntryType::ReplicationApplierConfig:
      return kReplicationApplierConfig;
    case RocksDBEntryType::IndexEstimateValue:
      return kIndexEstimate;
    case RocksDBEntryType::KeyGeneratorValue:
      return kKeyGenerator;
    case RocksDBEntryType::RevisionTreeValue:
      return kRevisionTree;
    case RocksDBEntryType::TableTombstone:
      return kTableTombstone;
    case RocksDBEntryType::ScopeTombstone:
      return kScopeTombstone;
    case RocksDBEntryType::Index:
      return kIndex;
    case RocksDBEntryType::Stats:
      return kStats;
    case sdb::RocksDBEntryType::IndexPhysical:
      return kIndexPhysical;
    case RocksDBEntryType::IndexTombstone:
      return kIndexTombstone;
    case RocksDBEntryType::IndexShardTombstone:
      return kIndexShardTombstone;
    case sdb::RocksDBEntryType::TablePhysical:
      return kTablePhysical;
  }

  return kPlaceholder;  // avoids warning - errorslice instead ?!
}

}  // namespace sdb
