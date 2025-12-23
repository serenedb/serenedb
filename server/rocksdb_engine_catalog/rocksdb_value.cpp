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

using namespace sdb;
using namespace sdb::rocksutils;

RocksDBValue RocksDBValue::Database(vpack::Slice data) {
  return RocksDBValue(RocksDBEntryType::Database, data);
}

RocksDBValue RocksDBValue::PrimaryIndexValue(RevisionId doc_id) {
  return RocksDBValue(RocksDBEntryType::PrimaryIndexValue, doc_id);
}

RocksDBValue RocksDBValue::EdgeIndexValue(std::string_view vertex_id) {
  return RocksDBValue(RocksDBEntryType::EdgeIndexValue, vertex_id);
}

RocksDBValue RocksDBValue::VPackIndexValue() {
  return RocksDBValue(RocksDBEntryType::VPackIndexValue);
}

RocksDBValue RocksDBValue::VPackIndexValue(vpack::Slice data) {
  return RocksDBValue(RocksDBEntryType::VPackIndexValue, data);
}

RocksDBValue RocksDBValue::UniqueVPackIndexValue(RevisionId doc_id) {
  return RocksDBValue(RocksDBEntryType::UniqueVPackIndexValue, doc_id);
}

RocksDBValue RocksDBValue::UniqueVPackIndexValue(RevisionId doc_id,
                                                 vpack::Slice data) {
  return RocksDBValue(RocksDBEntryType::UniqueVPackIndexValue, doc_id, data);
}

RocksDBValue RocksDBValue::ReplicationApplierConfig(vpack::Slice data) {
  return RocksDBValue(RocksDBEntryType::ReplicationApplierConfig, data);
}

RocksDBValue RocksDBValue::KeyGeneratorValue(vpack::Slice data) {
  return RocksDBValue(RocksDBEntryType::KeyGeneratorValue, data);
}

RocksDBValue RocksDBValue::Empty(RocksDBEntryType type) {
  return RocksDBValue(type);
}

RevisionId RocksDBValue::documentId(const RocksDBValue& value) {
  return documentId(value._buffer.data(), value._buffer.size());
}

RevisionId RocksDBValue::documentId(const rocksdb::Slice& slice) {
  return documentId(slice.data(), slice.size());
}

RevisionId RocksDBValue::documentId(std::string_view s) {
  return documentId(s.data(), s.size());
}

std::string_view RocksDBValue::vertexId(const rocksdb::Slice& s) {
  return vertexId(s.data(), s.size());
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

vpack::Slice RocksDBValue::indexStoredValues(const rocksdb::Slice& slice) {
  SDB_ASSERT(
    vpack::Slice(reinterpret_cast<const uint8_t*>(slice.data())).isArray());
  return data(slice.data(), slice.size());
}

vpack::Slice RocksDBValue::uniqueIndexStoredValues(
  const rocksdb::Slice& slice) {
  SDB_ASSERT(vpack::Slice(reinterpret_cast<const uint8_t*>(slice.data() +
                                                           sizeof(uint64_t)))
               .isArray());
  return data(slice.data() + sizeof(uint64_t), slice.size() - sizeof(uint64_t));
}

RocksDBValue::RocksDBValue(RocksDBEntryType type) : _type(type), _buffer() {}

RocksDBValue::RocksDBValue(RocksDBEntryType type, RevisionId doc_id)
  : _type(type), _buffer() {
  switch (_type) {
    case RocksDBEntryType::UniqueVPackIndexValue:
    case RocksDBEntryType::PrimaryIndexValue: {
      _buffer.reserve(sizeof(uint64_t));
      Uint64ToPersistent(_buffer, doc_id.id());
    } break;
    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

RocksDBValue::RocksDBValue(RocksDBEntryType type, RevisionId doc_id,
                           vpack::Slice data)
  : _type(type), _buffer() {
  switch (_type) {
    case RocksDBEntryType::UniqueVPackIndexValue: {
      size_t byte_size = static_cast<size_t>(data.byteSize());
      _buffer.reserve(sizeof(uint64_t) + byte_size);
      Uint64ToPersistent(_buffer, doc_id.id());
      _buffer.append(reinterpret_cast<const char*>(data.begin()), byte_size);
    } break;

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

RocksDBValue::RocksDBValue(RocksDBEntryType type, vpack::Slice data)
  : _type(type), _buffer() {
  switch (_type) {
    case RocksDBEntryType::VPackIndexValue:
      SDB_ASSERT(data.isArray());
      [[fallthrough]];

    case RocksDBEntryType::Database:
    case RocksDBEntryType::Function:
    case RocksDBEntryType::Collection:
    case RocksDBEntryType::Schema:
    case RocksDBEntryType::TableTombstone:
    case RocksDBEntryType::ScopeTombstone:
    case RocksDBEntryType::View:
    case RocksDBEntryType::Index:
    case RocksDBEntryType::IndexTombstone:
    case RocksDBEntryType::Role:
    case RocksDBEntryType::KeyGeneratorValue:
    case RocksDBEntryType::ReplicationApplierConfig: {
      size_t byte_size = static_cast<size_t>(data.byteSize());
      _buffer.reserve(byte_size);
      _buffer.append(reinterpret_cast<const char*>(data.begin()), byte_size);
      break;
    }

    case RocksDBEntryType::Document:
      SDB_ASSERT(false);  // use for document => get free schellen
      break;

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

RocksDBValue::RocksDBValue(RocksDBEntryType type, std::string_view data)
  : _type(type), _buffer() {
  switch (_type) {
    case RocksDBEntryType::EdgeIndexValue: {
      _buffer.reserve(static_cast<size_t>(data.size()));
      _buffer.append(data.data(), data.size());
      break;
    }

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

RevisionId RocksDBValue::documentId(const char* data, uint64_t size) {
  SDB_ASSERT(data != nullptr && size >= sizeof(RevisionId::BaseType));
  return RevisionId(Uint64FromPersistent(data));
}

std::string_view RocksDBValue::vertexId(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size >= sizeof(char));
  return std::string_view(data, size);
}

vpack::Slice RocksDBValue::data(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size >= sizeof(char));
  return vpack::Slice(reinterpret_cast<const uint8_t*>(data));
}
