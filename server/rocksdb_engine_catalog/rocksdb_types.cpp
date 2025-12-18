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

#include "magic_enum/magic_enum.hpp"

namespace sdb {
namespace {

// TODO(mbkkt) add constexpr ctor for rocksdb::Slice
template<RocksDBEntryType Type>
const rocksdb::Slice MakeSlice() {
  static constexpr auto kData = Type;
  return {
    reinterpret_cast<const std::underlying_type_t<RocksDBEntryType>*>(&kData),
    1};
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
    case RocksDBEntryType::IndexTombstone:
      return kIndexTombstone;
  }

  return kPlaceholder;  // avoids warning - errorslice instead ?!
}

}  // namespace sdb
