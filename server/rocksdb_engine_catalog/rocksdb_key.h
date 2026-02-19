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

#include <rocksdb/slice.h>
#include <vpack/slice.h>

#include <iosfwd>
#include <string>
#include <string_view>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/identifiers/revision_id.h"
#include "catalog/types.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace sdb {

class RocksDBKey {
 public:
  explicit RocksDBKey(std::string* buf) : _buffer{buf} { SDB_ASSERT(_buffer); }

  // construct a RocksDB key from another, already filled buffer
  void constructFromBuffer(std::string_view buffer);

  // New flat definition key: [parent_id(8) | type(1) | object_id(8)] = 17 bytes
  void constructDefinition(ObjectId parent_id, RocksDBEntryType type,
                           ObjectId object_id);

  // Extract fields from a new flat definition key
  static ObjectId GetParentId(const rocksdb::Slice& slice);
  static RocksDBEntryType GetType(const rocksdb::Slice& slice);
  static ObjectId GetObjectId(const rocksdb::Slice& slice);

  // --- Deprecated: old key format methods (kept for non-definition keys) ---

  // Create a fully-specified database key
  void constructDatabase(ObjectId database_id);

  void constructDatabaseObject(RocksDBEntryType type, ObjectId database_id,
                               ObjectId id);

  void constructSchemaObject(RocksDBEntryType type, ObjectId database_id,
                             ObjectId schema_id, ObjectId id);

  // Create a fully-specified key for a settings value
  void constructSettingsValue(RocksDBSettingsType st);

  // Extracts the type from a key
  // May be called on any valid key (in our keyspace)
  static RocksDBEntryType type(const RocksDBKey&);
  static RocksDBEntryType type(rocksdb::Slice slice) {
    return type(slice.data(), slice.size());
  }

  static Tick databaseId(const rocksdb::Slice& slice) {
    return GetDatabaseId(slice.data(), slice.size());
  }

  static ObjectId SchemaId(const rocksdb::Slice& slice) {
    return GetSchemaId(slice.data(), slice.size());
  }

  static ObjectId dataSourceId(const rocksdb::Slice& slice) {
    return GetObjectId(slice.data(), slice.size());
  }

  // Returns a reference to the full, constructed key
  rocksdb::Slice string() const { return rocksdb::Slice(*_buffer); }

  size_t size() const { return _buffer->size(); }

  bool operator==(const RocksDBKey& other) const = default;

  std::string* buffer() const { return _buffer; }

  void reset(rocksdb::Slice slice) {
    SDB_ASSERT(_buffer);
    _type = RocksDBEntryType::Placeholder;
    _buffer->assign(slice.data(), slice.size());
  }

 private:
  // Entry type in the definitions CF
  static RocksDBEntryType type(const char* data, size_t size) {
    SDB_ASSERT(data != nullptr);
    SDB_ASSERT(size >= sizeof(char));

    const auto type = static_cast<RocksDBEntryType>(data[0]);
    switch (type) {
      case RocksDBEntryType::SettingsValue:
      case RocksDBEntryType::Role:
      case RocksDBEntryType::Database:
      case RocksDBEntryType::Schema:
      case RocksDBEntryType::Function:
      case RocksDBEntryType::View:
      case RocksDBEntryType::Table:
      case RocksDBEntryType::Index:
      case RocksDBEntryType::ScopeTombstone:
      case RocksDBEntryType::TableTombstone:
      case RocksDBEntryType::IndexTombstone:
      case RocksDBEntryType::TableShard:
      case RocksDBEntryType::IndexShard:
        return type;
      default:
        return RocksDBEntryType::Placeholder;
    }
    return type;
  }

  static Tick GetDatabaseId(const char* data, size_t size);
  static ObjectId GetSchemaId(const char* data, size_t size);
  static ObjectId GetObjectId(const char* data, size_t size);

  RocksDBEntryType _type{RocksDBEntryType::Placeholder};
  std::string* _buffer;
};

class RocksDBKeyWithBuffer : public RocksDBKey {
 public:
  RocksDBKeyWithBuffer() : RocksDBKey{&_buffer} {}
  RocksDBKeyWithBuffer(RocksDBKeyWithBuffer&& rhs)
    : RocksDBKey{&_buffer}, _buffer{std::move(rhs._buffer)} {}

  RocksDBKeyWithBuffer operator=(RocksDBKeyWithBuffer&&) = delete;

 private:
  std::string _buffer;
};

}  // namespace sdb
