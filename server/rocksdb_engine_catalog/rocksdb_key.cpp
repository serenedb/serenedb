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

#include "basics/exceptions.h"
#include "catalog/identifiers/object_id.h"
#include "rocksdb_engine_catalog/concat.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace sdb {

using namespace rocksutils;

void RocksDBKey::constructDatabase(ObjectId database_id) {
  SDB_ASSERT(database_id.isSet());
  _type = RocksDBEntryType::Database;
  rocksutils::Concat(*_buffer, _type, database_id);
}

void RocksDBKey::constructDatabaseObject(RocksDBEntryType type,
                                         ObjectId database_id, ObjectId id) {
  SDB_ASSERT(database_id.isSet());
  SDB_ASSERT(id.isSet());
  _type = type;
  rocksutils::Concat(*_buffer, _type, database_id, id.id());
}

void RocksDBKey::constructSchemaObject(RocksDBEntryType type,
                                       ObjectId database_id, ObjectId schema_id,
                                       ObjectId id) {
  SDB_ASSERT(database_id.isSet());
  _type = type;
  rocksutils::Concat(*_buffer, type, database_id, schema_id.id(), id.id());
}
void RocksDBKey::constructSettingsValue(RocksDBSettingsType st) {
  SDB_ASSERT(st != RocksDBSettingsType::Invalid);
  _type = RocksDBEntryType::SettingsValue;
  rocksutils::Concat(*_buffer, _type, st);
}

RocksDBEntryType RocksDBKey::type(const RocksDBKey& key) {
  return type(key._buffer->data(), key._buffer->size());
}

Tick RocksDBKey::GetDatabaseId(const char* data, size_t size) {
  SDB_ASSERT(data);
  SDB_ASSERT(size >= sizeof(char) + sizeof(uint64_t));
  return Uint64FromPersistent(data + sizeof(char));
}

ObjectId RocksDBKey::GetSchemaId(const char* data, size_t size) {
  SDB_ASSERT(data);
  SDB_ASSERT(size >= sizeof(char) + (2 * sizeof(uint64_t)));
  return ObjectId{Uint64FromPersistent(data + sizeof(char) + sizeof(uint64_t))};
}

ObjectId RocksDBKey::GetObjectId(const char* data, size_t size) {
  SDB_ASSERT(data);
  SDB_ASSERT(size >= sizeof(char) + (3 * sizeof(uint64_t)));
  return ObjectId{
    Uint64FromPersistent(data + sizeof(char) + 2 * sizeof(uint64_t))};
}

}  // namespace sdb
