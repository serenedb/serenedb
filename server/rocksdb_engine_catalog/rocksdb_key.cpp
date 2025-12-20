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
namespace {

constexpr char kStringSeparator = '\0';
constexpr char kHighBytePrefixExtractor = 0xFFU;

}  // namespace

using namespace rocksutils;

/// verify that a key actually contains the given local document id
bool RocksDBKey::containsRevisionId(RevisionId document_id) const {
  switch (_type) {
    case RocksDBEntryType::Document:
    case RocksDBEntryType::EdgeIndexValue:
    case RocksDBEntryType::VPackIndexValue: {
      uint64_t value;
      absl::big_endian::Store64(&value, document_id.id());
      std::string_view buffer{reinterpret_cast<const char*>(&value),
                              sizeof(value)};

      // and now check if the key actually contains this local document id
      return _buffer->find(buffer) != std::string::npos;
    }

    default: {
      // we should never never get here
      SDB_ASSERT(false);
    }
  }

  return false;
}

void RocksDBKey::constructFromBuffer(std::string_view buffer) {
  // we don't know what the exact type is. we will simply take
  // over the data from the incoming buffer
  _type = RocksDBEntryType::Placeholder;
  *_buffer = buffer;
}

void RocksDBKey::constructDatabase(ObjectId database_id) {
  SDB_ASSERT(database_id.isSet());
  _type = RocksDBEntryType::Database;
  rocksutils::Concat(*_buffer, _type, database_id);
}

void RocksDBKey::constructObject(RocksDBEntryType type, ObjectId database_id,
                                 ObjectId id) {
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

void RocksDBKey::constructDocument(uint64_t object_id, RevisionId document_id) {
  SDB_ASSERT(object_id != 0);
  _type = RocksDBEntryType::Document;
  rocksutils::Concat(*_buffer, object_id, document_id.id());
}

void RocksDBKey::constructPrimaryIndexValue(uint64_t index_id,
                                            std::string_view primary_key) {
  SDB_ASSERT(index_id != 0 && !primary_key.empty());
  _type = RocksDBEntryType::PrimaryIndexValue;
  rocksutils::Concat(*_buffer, index_id, primary_key);
}

void RocksDBKey::constructEdgeIndexValue(uint64_t index_id,
                                         std::string_view vertex_id,
                                         RevisionId document_id) {
  SDB_ASSERT(index_id != 0 && !vertex_id.empty());
  _type = RocksDBEntryType::EdgeIndexValue;
  rocksutils::Concat(
    *_buffer, index_id, vertex_id, kStringSeparator, document_id.id(),
    kHighBytePrefixExtractor);  // high-byte for prefix extractor
}

void RocksDBKey::constructVPackIndexValue(uint64_t index_id,
                                          vpack::Slice index_values,
                                          RevisionId document_id) {
  SDB_ASSERT(index_id != 0 && !index_values.isNone());
  _type = RocksDBEntryType::VPackIndexValue;
  rocksutils::Concat(*_buffer, index_id, index_values, document_id.id());
}

void RocksDBKey::constructUniqueVPackIndexValue(uint64_t index_id,
                                                vpack::Slice index_values) {
  SDB_ASSERT(index_id != 0 && !index_values.isNone());
  _type = RocksDBEntryType::UniqueVPackIndexValue;
  rocksutils::Concat(*_buffer, index_id, index_values);
}

void RocksDBKey::constructCounterValue(uint64_t object_id) {
  SDB_ASSERT(object_id != 0);
  _type = RocksDBEntryType::CounterValue;
  rocksutils::Concat(*_buffer, _type, object_id);
}

void RocksDBKey::constructSettingsValue(RocksDBSettingsType st) {
  SDB_ASSERT(st != RocksDBSettingsType::Invalid);
  _type = RocksDBEntryType::SettingsValue;
  rocksutils::Concat(*_buffer, _type, st);
}

void RocksDBKey::constructReplicationApplierConfig(ObjectId database_id) {
  // databaseId may be 0 for global applier config
  _type = RocksDBEntryType::ReplicationApplierConfig;
  rocksutils::Concat(*_buffer, _type, database_id);
}

void RocksDBKey::constructIndexEstimateValue(uint64_t collection_object_id) {
  SDB_ASSERT(collection_object_id != 0);
  _type = RocksDBEntryType::IndexEstimateValue;
  rocksutils::Concat(*_buffer, _type, collection_object_id);
}

void RocksDBKey::constructKeyGeneratorValue(uint64_t object_id) {
  SDB_ASSERT(object_id != 0);
  _type = RocksDBEntryType::KeyGeneratorValue;
  rocksutils::Concat(*_buffer, _type, object_id);
}

void RocksDBKey::constructRevisionTreeValue(uint64_t collection_object_id) {
  SDB_ASSERT(collection_object_id != 0);
  _type = RocksDBEntryType::RevisionTreeValue;
  rocksutils::Concat(*_buffer, _type, collection_object_id);
}

RocksDBEntryType RocksDBKey::type(const RocksDBKey& key) {
  return type(key._buffer->data(), key._buffer->size());
}

RevisionId RocksDBKey::documentId(const rocksdb::Slice& slice) {
  SDB_ASSERT(slice.size() == 2 * sizeof(uint64_t));
  // last 8 bytes should be the RevisionId
  return RevisionId(Uint64FromPersistent(slice.data() + sizeof(uint64_t)));
}

RevisionId RocksDBKey::indexDocumentId(const rocksdb::Slice slice) {
  const char* data = slice.data();
  const size_t size = slice.size();
  SDB_ASSERT(size >= (2 * sizeof(uint64_t)));
  // last 8 bytes should be the RevisionId
  return RevisionId(Uint64FromPersistent(data + size - sizeof(uint64_t)));
}

RevisionId RocksDBKey::edgeDocumentId(const rocksdb::Slice slice) {
  const char* data = slice.data();
  const size_t size = slice.size();
  SDB_ASSERT(size >= (sizeof(char) * 3 + 2 * sizeof(uint64_t)));
  // 1 byte prefix + 8 byte objectID + _from/_to + 1 byte \0
  // + 8 byte revision ID + 1-byte 0xff
  return RevisionId(
    Uint64FromPersistent(data + size - sizeof(uint64_t) - sizeof(char)));
}

std::string_view RocksDBKey::primaryKey(const rocksdb::Slice& slice) {
  return primaryKey(slice.data(), slice.size());
}

std::string_view RocksDBKey::vertexId(const rocksdb::Slice& slice) {
  return vertexId(slice.data(), slice.size());
}

vpack::Slice RocksDBKey::indexedVPack(const RocksDBKey& key) {
  return indexedVPack(key._buffer->data(), key._buffer->size());
}

vpack::Slice RocksDBKey::indexedVPack(const rocksdb::Slice& slice) {
  return indexedVPack(slice.data(), slice.size());
}

Tick RocksDBKey::databaseId(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size >= sizeof(char));
  RocksDBEntryType type = RocksDBKey::type(data, size);
  switch (type) {
    case RocksDBEntryType::Database:
    case RocksDBEntryType::Function:
    case RocksDBEntryType::Collection:
    case RocksDBEntryType::View:
    case RocksDBEntryType::Role:
    case RocksDBEntryType::ReplicationApplierConfig: {
      SDB_ASSERT(size >= (sizeof(char) + sizeof(uint64_t)));
      return Uint64FromPersistent(data + sizeof(char));
    }

    default:
      SDB_THROW(ERROR_TYPE_ERROR);
  }
}

ObjectId RocksDBKey::dataSourceId(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size >= sizeof(char));
  RocksDBEntryType type = RocksDBKey::type(data, size);
  switch (type) {
    case RocksDBEntryType::Function:
    case RocksDBEntryType::Collection:
    case RocksDBEntryType::View:
    case RocksDBEntryType::Role: {
      SDB_ASSERT(size >= (sizeof(char) + (2 * sizeof(uint64_t))));
      return ObjectId{
        Uint64FromPersistent(data + sizeof(char) + 2 * sizeof(uint64_t))};
    }

    default:
      SDB_THROW(ERROR_TYPE_ERROR);
  }
}

ObjectId RocksDBKey::SchemaId(const char* data, size_t size) {
  SDB_ASSERT(data);
  SDB_ASSERT(size >= sizeof(char));
  const auto type = RocksDBKey::type(data, size);
  SDB_ENSURE(type == RocksDBEntryType::Function ||
               type == RocksDBEntryType::View ||
               type == RocksDBEntryType::ScopeTombstone,
             ERROR_TYPE_ERROR);
  SDB_ASSERT(size >= (sizeof(char) + (3 * sizeof(uint64_t))));
  return ObjectId{Uint64FromPersistent(data + sizeof(char) + sizeof(uint64_t))};
}

uint64_t RocksDBKey::objectId(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size >= sizeof(uint64_t));
  return Uint64FromPersistent(data);
}

std::string_view RocksDBKey::primaryKey(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size > sizeof(uint64_t));
  return std::string_view(data + sizeof(uint64_t), size - sizeof(uint64_t));
}

std::string_view RocksDBKey::vertexId(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size > sizeof(uint64_t) * 2);
  // 8 byte objectID + _from/_to + 1 byte \0 +
  // 8 byte revision ID + 1-byte 0xff
  size_t key_size = size - (sizeof(char) + sizeof(uint64_t)) * 2;
  return std::string_view(data + sizeof(uint64_t), key_size);
}

vpack::Slice RocksDBKey::indexedVPack(const char* data, size_t size) {
  SDB_ASSERT(data != nullptr);
  SDB_ASSERT(size > sizeof(uint64_t), "size = ", size);
  return vpack::Slice(reinterpret_cast<const uint8_t*>(data) +
                      sizeof(uint64_t));
}

}  // namespace sdb
