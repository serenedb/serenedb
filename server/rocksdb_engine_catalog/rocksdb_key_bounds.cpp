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
namespace {
constexpr char kStringSeparator = '\0';
}

static_assert(sizeof(RocksDBEntryType) == 1);

using namespace rocksutils;

RocksDBKeyBounds RocksDBKeyBounds::Empty() {
  return RocksDBKeyBounds::PrimaryIndex(0);
}

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

RocksDBKeyBounds RocksDBKeyBounds::CollectionDocuments(
  uint64_t collection_object_id) {
  return RocksDBKeyBounds(RocksDBEntryType::Document, collection_object_id);
}

RocksDBKeyBounds RocksDBKeyBounds::CollectionDocuments(
  uint64_t collection_object_id, uint64_t lower, uint64_t upper) {
  return RocksDBKeyBounds(RocksDBEntryType::Document, collection_object_id,
                          lower, upper);
}

RocksDBKeyBounds RocksDBKeyBounds::PrimaryIndex(uint64_t index_id) {
  return RocksDBKeyBounds(RocksDBEntryType::PrimaryIndexValue, index_id);
}

RocksDBKeyBounds RocksDBKeyBounds::EdgeIndex(uint64_t index_id) {
  return RocksDBKeyBounds(RocksDBEntryType::EdgeIndexValue, index_id);
}

RocksDBKeyBounds RocksDBKeyBounds::EdgeIndexVertex(uint64_t index_id,
                                                   std::string_view vertex_id) {
  return RocksDBKeyBounds(RocksDBEntryType::EdgeIndexValue, index_id,
                          vertex_id);
}

RocksDBKeyBounds RocksDBKeyBounds::VPackIndex(uint64_t index_id, bool reverse) {
  return RocksDBKeyBounds(RocksDBEntryType::VPackIndexValue, index_id, reverse);
}

RocksDBKeyBounds RocksDBKeyBounds::UniqueVPackIndex(uint64_t index_id,
                                                    bool reverse) {
  return RocksDBKeyBounds(RocksDBEntryType::UniqueVPackIndexValue, index_id,
                          reverse);
}

RocksDBKeyBounds RocksDBKeyBounds::VPackIndex(uint64_t index_id,
                                              vpack::Slice left,
                                              vpack::Slice right) {
  return RocksDBKeyBounds(RocksDBEntryType::VPackIndexValue, index_id, left,
                          right);
}

/// used for seeking lookups
RocksDBKeyBounds RocksDBKeyBounds::UniqueVPackIndex(uint64_t index_id,
                                                    vpack::Slice left,
                                                    vpack::Slice right) {
  return RocksDBKeyBounds(RocksDBEntryType::UniqueVPackIndexValue, index_id,
                          left, right);
}

RocksDBKeyBounds RocksDBKeyBounds::PrimaryIndex(uint64_t index_id,
                                                const std::string& left,
                                                const std::string& right) {
  return RocksDBKeyBounds(RocksDBEntryType::PrimaryIndexValue, index_id, left,
                          right);
}

/// used for point lookups
RocksDBKeyBounds RocksDBKeyBounds::UniqueVPackIndex(uint64_t index_id,
                                                    vpack::Slice left) {
  return RocksDBKeyBounds(RocksDBEntryType::UniqueVPackIndexValue, index_id,
                          left);
}

uint64_t RocksDBKeyBounds::objectId() const {
#ifdef SDB_DEV
  switch (_type) {
    case RocksDBEntryType::Document:
    case RocksDBEntryType::PrimaryIndexValue:
    case RocksDBEntryType::EdgeIndexValue:
    case RocksDBEntryType::VPackIndexValue:
    case RocksDBEntryType::UniqueVPackIndexValue:
      SDB_ASSERT(_internals.buffer().size() > sizeof(uint64_t));
      return Uint64FromPersistent(_internals.buffer().data());
    default:
      SDB_UNREACHABLE();
  }
#else
  return Uint64FromPersistent(_internals.buffer().data());
#endif
}

rocksdb::ColumnFamilyHandle* RocksDBKeyBounds::columnFamily() const {
  switch (_type) {
    case RocksDBEntryType::Placeholder:
      return RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::Invalid);
    case RocksDBEntryType::Document:
      return RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::Documents);
    case RocksDBEntryType::PrimaryIndexValue:
      return RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::PrimaryIndex);
    case RocksDBEntryType::EdgeIndexValue:
      return RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::EdgeIndex);
    case RocksDBEntryType::VPackIndexValue:
    case RocksDBEntryType::UniqueVPackIndexValue:
      return RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::VPackIndex);
    case RocksDBEntryType::Database:
    case RocksDBEntryType::Function:
    case RocksDBEntryType::Collection:
    case RocksDBEntryType::CounterValue:
    case RocksDBEntryType::SettingsValue:
    case RocksDBEntryType::ReplicationApplierConfig:
    case RocksDBEntryType::IndexEstimateValue:
    case RocksDBEntryType::KeyGeneratorValue:
    case RocksDBEntryType::RevisionTreeValue:
    case RocksDBEntryType::View:
    case RocksDBEntryType::Role:
    case RocksDBEntryType::Schema:
    case RocksDBEntryType::TableTombstone:
    case RocksDBEntryType::ScopeTombstone:
    case RocksDBEntryType::IndexTombstone:
    case RocksDBEntryType::Index:
      return RocksDBColumnFamilyManager::get(
        RocksDBColumnFamilyManager::Family::Definitions);
  }
  SDB_THROW(ERROR_TYPE_ERROR);
}

/// bounds to iterate over specified word or edge
RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t id,
                                   std::string_view lower,
                                   std::string_view upper)
  : _type(type) {
  switch (_type) {
    case RocksDBEntryType::PrimaryIndexValue: {
      // format: id lower id upper
      //         start    end
      _internals.reserve(
        sizeof(id) + (lower.size() + sizeof(kStringSeparator)) + sizeof(id) +
        (upper.size() + sizeof(kStringSeparator)));

      // id - lower
      Uint64ToPersistent(_internals.buffer(), id);
      _internals.buffer().append(lower.data(), lower.length());
      _internals.push_back(kStringSeparator);

      // set separator
      _internals.separate();

      // id - upper
      Uint64ToPersistent(_internals.buffer(), id);
      _internals.buffer().append(upper.data(), upper.length());
      _internals.push_back(kStringSeparator);

      break;
    }

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

/// bounds to iterate over entire index
RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t first)
  : _type(type) {
  switch (_type) {
    case RocksDBEntryType::TableTombstone:
    case RocksDBEntryType::ScopeTombstone:
    case RocksDBEntryType::Collection:
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
    case RocksDBEntryType::Document: {
      // Documents are stored as follows:
      // Key: 8-byte object ID of collection + 8-byte document revision ID
      _internals.reserve(3 * sizeof(uint64_t));
      Uint64ToPersistent(_internals.buffer(), first);
      _internals.separate();
      Uint64ToPersistent(_internals.buffer(), first);
      Uint64ToPersistent(_internals.buffer(), UINT64_MAX);
      // 0 - 0xFFFF... no matter the endianess
      break;
    }
    case RocksDBEntryType::PrimaryIndexValue:
    case RocksDBEntryType::EdgeIndexValue: {
      size_t length = 2 * sizeof(uint64_t) + 4 * sizeof(char);
      _internals.reserve(length);
      Uint64ToPersistent(_internals.buffer(), first);
      if (type == RocksDBEntryType::EdgeIndexValue) {
        _internals.push_back('\0');
        _internals.push_back(kStringSeparator);
      }

      _internals.separate();

      if (type == RocksDBEntryType::PrimaryIndexValue) {
        // if we are in big-endian mode, we can cheat a bit...
        // for the upper bound we can use the object id + 1, which will always
        // compare higher in a bytewise comparison
        rocksutils::UintToPersistentBigEndian<uint64_t>(_internals.buffer(),
                                                        first + 1);
        _internals.push_back(0x00U);  // lower/equal to any ascii char
      } else {
        Uint64ToPersistent(_internals.buffer(), first);
        _internals.push_back(0xFFU);  // higher than any ascii char
        if (type == RocksDBEntryType::EdgeIndexValue) {
          _internals.push_back(kStringSeparator);
        }
      }
      break;
    }

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t first,
                                   bool second)
  : _type(type) {
  switch (_type) {
    case RocksDBEntryType::VPackIndexValue:
    case RocksDBEntryType::UniqueVPackIndexValue: {
      const uint8_t max_slice[] = {0x02, 0x03, 0x17};
      vpack::Slice max(max_slice);

      _internals.reserve(2 * sizeof(uint64_t) + max.byteSize());
      Uint64ToPersistent(_internals.buffer(), first);
      _internals.separate();

      if (second) {
        // in case of reverse iteration, this is our starting point, so it must
        // be in the same prefix, otherwise we'll get no results; so here we
        // use the same objectId and the max vpack slice to make sure we find
        // everything
        Uint64ToPersistent(_internals.buffer(), first);
        _internals.buffer().append((const char*)(max.begin()), max.byteSize());
      } else {
        Uint64ToPersistent(_internals.buffer(), first + 1);
      }
      break;
    }

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

/// bounds to iterate over specified word or edge
RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t first,
                                   std::string_view second)
  : _type(type) {
  switch (_type) {
    case RocksDBEntryType::EdgeIndexValue: {
      _internals.reserve(2 * (sizeof(uint64_t) + second.size() + 2) + 1);
      Uint64ToPersistent(_internals.buffer(), first);
      _internals.buffer().append(second.data(), second.length());
      _internals.push_back(kStringSeparator);

      _internals.separate();

      Uint64ToPersistent(_internals.buffer(), first);
      _internals.buffer().append(second.data(), second.length());
      _internals.push_back(kStringSeparator);
      Uint64ToPersistent(_internals.buffer(), UINT64_MAX);
      if (type == RocksDBEntryType::EdgeIndexValue) {
        _internals.push_back(0xFFU);  // high-byte for prefix extractor
      }
      break;
    }

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

/// point lookups for unique vpack indexes
RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t first,
                                   vpack::Slice second)
  : _type(type) {
  switch (_type) {
    case RocksDBEntryType::UniqueVPackIndexValue: {
      size_t start_length =
        sizeof(uint64_t) + static_cast<size_t>(second.byteSize());

      _internals.reserve(start_length);
      Uint64ToPersistent(_internals.buffer(), first);
      _internals.buffer().append(reinterpret_cast<const char*>(second.begin()),
                                 static_cast<size_t>(second.byteSize()));

      _internals.separate();
      // second bound is intentionally left empty!
      break;
    }

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

/// iterate over the specified bounds of the vpack index
RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t first,
                                   vpack::Slice second, vpack::Slice third)
  : _type(type) {
  fill(type, first, second, third);
}

void RocksDBKeyBounds::fill(RocksDBEntryType type, uint64_t first,
                            vpack::Slice second, vpack::Slice third) {
  clear();
  _type = type;
  switch (_type) {
    case RocksDBEntryType::VPackIndexValue:
    case RocksDBEntryType::UniqueVPackIndexValue: {
      size_t start_length =
        sizeof(uint64_t) + static_cast<size_t>(second.byteSize());
      size_t end_length =
        2 * sizeof(uint64_t) + static_cast<size_t>(third.byteSize());

      _internals.reserve(start_length + end_length);
      Uint64ToPersistent(_internals.buffer(), first);
      _internals.buffer().append(reinterpret_cast<const char*>(second.begin()),
                                 static_cast<size_t>(second.byteSize()));

      _internals.separate();

      Uint64ToPersistent(_internals.buffer(), first);
      _internals.buffer().append(reinterpret_cast<const char*>(third.begin()),
                                 static_cast<size_t>(third.byteSize()));
      Uint64ToPersistent(_internals.buffer(), UINT64_MAX);
      break;
    }

    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

RocksDBKeyBounds::RocksDBKeyBounds(RocksDBEntryType type, uint64_t first,
                                   uint64_t second, uint64_t third)
  : _type(type) {
  switch (_type) {
    case RocksDBEntryType::Document: {
      // Documents are stored as follows:
      // Key: 8-byte object ID of collection + 8-byte document revision ID
      rocksutils::Concat(_internals.buffer(), first, second, first, third);
      _internals.separate(sizeof(first) + sizeof(second));
      break;
    }
    default:
      SDB_THROW(ERROR_BAD_PARAMETER);
  }
}

RocksDBKeyBounds GetIndexBounds(IndexType type, uint64_t object_id,
                                bool unique) {
  switch (type) {
    case kTypePrimaryIndex:
      return RocksDBKeyBounds::PrimaryIndex(object_id);
    case kTypeEdgeIndex:
      return RocksDBKeyBounds::EdgeIndex(object_id);
    case kTypeTtlIndex:
    case kTypeSecondaryIndex:
    case kTypeInvertedIndex:
      if (unique) {
        return RocksDBKeyBounds::UniqueVPackIndex(object_id, false);
      }
      return RocksDBKeyBounds::VPackIndex(object_id, false);
    case kTypeUnknown:
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED);
  }
}

}  // namespace sdb
