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

#include "rocksdb_key.h"

#include <absl/base/internal/endian.h>
#include <absl/strings/internal/resize_uninitialized.h>

#include <limits>

#include "basics/exceptions.h"
#include "catalog/identifiers/object_id.h"
#include "rocksdb_engine_catalog/concat.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace sdb {

using namespace rocksutils;

ObjectId DefinitionKey::GetParentId() const {
  SDB_ASSERT(_key.size() ==
             sizeof(ObjectId) + sizeof(RocksDBEntryType) + sizeof(ObjectId));
  return ObjectId{rocksutils::Uint64FromPersistent(_key.data())};
}

RocksDBEntryType DefinitionKey::GetEntryType() const {
  SDB_ASSERT(_key.size() ==
             sizeof(ObjectId) + sizeof(RocksDBEntryType) + sizeof(ObjectId));
  return static_cast<RocksDBEntryType>(_key.data()[sizeof(ObjectId)]);
}

ObjectId DefinitionKey::GetObjectId() const {
  SDB_ASSERT(_key.size() ==
             sizeof(ObjectId) + sizeof(RocksDBEntryType) + sizeof(ObjectId));
  return ObjectId{rocksutils::Uint64FromPersistent(
    _key.data() + sizeof(ObjectId) + sizeof(RocksDBEntryType))};
}

std::string DefinitionKey::Create(ObjectId parent_id, RocksDBEntryType entry,
                                  ObjectId id) {
  std::string key;
  key.reserve(sizeof(ObjectId) + sizeof(RocksDBEntryType) + sizeof(ObjectId));
  Uint64ToPersistent(key, parent_id.id());
  key.push_back(static_cast<char>(entry));
  Uint64ToPersistent(key, id.id());
  return key;
}

std::pair<std::string, std::string> DefinitionKey::CreateInterval(
  ObjectId parent_id) {
  std::string start, end;
  Uint64ToPersistent(start, parent_id.id());
  start.push_back(0);
  Uint64ToPersistent(start, 0ULL);

  Uint64ToPersistent(end, parent_id.id());
  end.push_back(std::numeric_limits<uint8_t>::max());
  Uint64ToPersistent(end, std::numeric_limits<unsigned long long>::max());
  return {start, end};
}

std::pair<std::string, std::string> DefinitionKey::CreateInterval(
  ObjectId parent_id, RocksDBEntryType type) {
  std::string start, end;
  Uint64ToPersistent(start, parent_id.id());
  start.push_back(static_cast<char>(type));
  Uint64ToPersistent(start, 0ULL);

  Uint64ToPersistent(end, parent_id.id());
  end.push_back(static_cast<char>(type));
  Uint64ToPersistent(end, std::numeric_limits<unsigned long long>::max());
  return {start, end};
}

std::string SettingsKey::Create(RocksDBSettingsType settings_type) {
  std::string key;
  rocksutils::Concat(key, RocksDBEntryType::SettingsValue, settings_type);
  return key;
}

RocksDBSettingsType SettingsKey::GetSettingsType() const {
  SDB_ASSERT(_key.size() ==
             sizeof(RocksDBEntryType) + sizeof(RocksDBSettingsType));
  return static_cast<RocksDBSettingsType>(
    _key.data()[sizeof(RocksDBEntryType)]);
}

RocksDBEntryType SettingsKey::GetEntryType() const {
  SDB_ASSERT(_key.size() ==
             sizeof(RocksDBEntryType) + sizeof(RocksDBSettingsType));
  return static_cast<RocksDBEntryType>(*_key.data());
}

}  // namespace sdb
