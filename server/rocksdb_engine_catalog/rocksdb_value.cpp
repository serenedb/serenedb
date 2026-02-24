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

#include "rocksdb_value.h"

#include "app/app_server.h"
#include "basics/exceptions.h"
#include "basics/number_utils.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

using namespace sdb;
using namespace sdb::rocksutils;

RocksDBValue RocksDBValue::Database(vpack::Slice data) {
  return RocksDBValue(RocksDBEntryType::Database, data);
}

RocksDBValue RocksDBValue::Empty(RocksDBEntryType type) {
  return RocksDBValue(type);
}

vpack::Slice RocksDBValue::data(const RocksDBValue& value) {
  return data(value._buffer.data(), value._buffer.size());
}

vpack::Slice RocksDBValue::data(const rocksdb::Slice& slice) {
  return data(slice.data(), slice.size());
}

vpack::Slice RocksDBValue::data(std::string_view s) {
  return data(s.data(), s.size());
}

RocksDBValue::RocksDBValue(RocksDBEntryType type) : _type(type), _buffer() {}

RocksDBValue::RocksDBValue(RocksDBEntryType type, vpack::Slice data)
  : _type(type), _buffer() {
  switch (_type) {
    case RocksDBEntryType::Role:
    case RocksDBEntryType::Database:
    case RocksDBEntryType::Schema:
    case RocksDBEntryType::Function:
    case RocksDBEntryType::View:
    case RocksDBEntryType::Table:
    case RocksDBEntryType::Index:
    case sdb::RocksDBEntryType::Tombstone:
    case RocksDBEntryType::TableShard:
    case RocksDBEntryType::IndexShard: {
      size_t byte_size = static_cast<size_t>(data.byteSize());
      _buffer.reserve(byte_size);
      _buffer.append(reinterpret_cast<const char*>(data.begin()), byte_size);
    } break;
    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

vpack::Slice RocksDBValue::data(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size >= sizeof(char));
  return vpack::Slice(reinterpret_cast<const uint8_t*>(data));
}
