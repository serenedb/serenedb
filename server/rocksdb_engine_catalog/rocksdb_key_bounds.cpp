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

#include "rocksdb_key_bounds.h"

#include "basics/exceptions.h"
#include "rocksdb_engine_catalog/concat.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace sdb {

static_assert(sizeof(RocksDBEntryType) == 1);

using namespace rocksutils;

RocksDBKeyBounds RocksDBKeyBounds::Empty() { return RocksDBKeyBounds{}; }

RocksDBKeyBounds RocksDBKeyBounds::Databases() {
  RocksDBKeyBounds bounds;
  rocksutils::Concat(bounds.internals().buffer(), RocksDBEntryType::Database,
                     RocksDBEntryType::Database, static_cast<char>(0xFFU));
  bounds.internals().separate(sizeof(RocksDBEntryType::Database));
  return bounds;
}

RocksDBKeyBounds RocksDBKeyBounds::DatabaseObjects(RocksDBEntryType entry,
                                                   ObjectId database_id) {
  return {entry, database_id.id()};
}

RocksDBKeyBounds RocksDBKeyBounds::SchemaObjects(RocksDBEntryType entry,
                                                 ObjectId database_id,
                                                 ObjectId schema_id) {
  // Key: 1 + 8-byte database ID + 8-byte schema ID + 8-byte object ID
  RocksDBKeyBounds bounds;
  rocksutils::Concat(bounds.internals().buffer(), std::to_underlying(entry),
                     database_id.id(), schema_id.id(),
                     std::to_underlying(entry), database_id.id(),
                     schema_id.id(), UINT64_MAX);
  bounds.internals().separate(sizeof(entry) + sizeof(database_id) +
                              sizeof(schema_id.id()));
  return bounds;
}

RocksDBKeyBounds RocksDBKeyBounds::DefinitionObjects(ObjectId parent_id,
                                                     RocksDBEntryType type) {
  // Key: [parent_id(8) | type(1) | object_id(8)]
  // Range: [parent_id | type | 0] to [parent_id | type | UINT64_MAX]
  RocksDBKeyBounds bounds;
  bounds._type = type;
  auto& buf = bounds.internals().buffer();
  // start: parent_id | type | 0 (implicit since ObjectId{0})
  Uint64ToPersistent(buf, parent_id.id());
  buf.push_back(static_cast<char>(type));
  Uint64ToPersistent(buf, 0ULL);
  bounds.internals().separate();
  // end: parent_id | type | MAX
  Uint64ToPersistent(buf, parent_id.id());
  buf.push_back(static_cast<char>(type));
  Uint64ToPersistent(buf, UINT64_MAX);
  return bounds;
}

RocksDBKeyBounds RocksDBKeyBounds::ChildDefinitions(ObjectId parent_id) {
  // Range: [parent_id | 0x00 | 0] to [parent_id | 0xFF | UINT64_MAX]
  RocksDBKeyBounds bounds;
  bounds._type = RocksDBEntryType::Database;  // marker for definitions CF
  auto& buf = bounds.internals().buffer();
  // start
  Uint64ToPersistent(buf, parent_id.id());
  buf.push_back('\0');
  Uint64ToPersistent(buf, 0ULL);
  bounds.internals().separate();
  // end
  Uint64ToPersistent(buf, parent_id.id());
  buf.push_back(static_cast<char>(0xFF));
  Uint64ToPersistent(buf, UINT64_MAX);
  return bounds;
}

rocksdb::ColumnFamilyHandle* RocksDBKeyBounds::columnFamily() const {
  switch (_type) {
    case RocksDBEntryType::SettingsValue:
    case RocksDBEntryType::Role:
    case RocksDBEntryType::View:
    case RocksDBEntryType::Function:
    case RocksDBEntryType::Database:
    case RocksDBEntryType::Schema:
    case RocksDBEntryType::Table:
    case RocksDBEntryType::Index:
    case RocksDBEntryType::TableTombstone:
    case RocksDBEntryType::ScopeTombstone:
    case RocksDBEntryType::IndexTombstone:
    case RocksDBEntryType::TableShard:
    case RocksDBEntryType::IndexShard:
      return RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::Definitions);
    default:
      SDB_THROW(ERROR_TYPE_ERROR);
  }
}

RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t first)
  : _type(type) {
  switch (_type) {
    case RocksDBEntryType::TableTombstone:
    case RocksDBEntryType::ScopeTombstone:
    case RocksDBEntryType::IndexTombstone:
    case RocksDBEntryType::Table:
    case RocksDBEntryType::Schema:
    case RocksDBEntryType::Role: {
      // Key: 1 + 8-byte SereneDB database ID + 8-byte SereneDB collection ID
      _internals.reserve(2 * sizeof(char) + 3 * sizeof(uint64_t));
      _internals.push_back(static_cast<char>(_type));
      Uint64ToPersistent(_internals.buffer(), first);
      _internals.separate();
      _internals.push_back(static_cast<char>(_type));
      Uint64ToPersistent(_internals.buffer(), first);
      Uint64ToPersistent(_internals.buffer(), UINT64_MAX);
      break;
    }
    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

}  // namespace sdb
