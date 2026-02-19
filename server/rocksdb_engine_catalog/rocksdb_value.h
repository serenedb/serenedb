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

#include "basics/assert.h"
#include "catalog/identifiers/revision_id.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace sdb {

class RocksDBValue {
 public:
  static RocksDBValue Database(vpack::Slice data);
  static RocksDBValue Object(RocksDBEntryType type, vpack::Slice data) {
    return {type, data};
  }

  //////////////////////////////////////////////////////////////////////////////
  /// Used to construct an empty value of the given type for retrieval
  //////////////////////////////////////////////////////////////////////////////
  static RocksDBValue Empty(RocksDBEntryType type);

  //////////////////////////////////////////////////////////////////////////////
  /// Extracts the VPack data from a value
  ///
  /// May be called only values of the following types: Database, Collection,
  /// Document, and View. Other types will throw.
  //////////////////////////////////////////////////////////////////////////////
  static vpack::Slice data(const RocksDBValue&);
  static vpack::Slice data(const rocksdb::Slice&);
  static vpack::Slice data(std::string_view);

 public:
  RocksDBEntryType type() const noexcept { return _type; }
  //////////////////////////////////////////////////////////////////////////////
  /// Returns a reference to the underlying string buffer.
  //////////////////////////////////////////////////////////////////////////////
  const std::string& string() const { return _buffer; }  // to be used with put
  std::string* buffer() { return &_buffer; }             // to be used with get
  vpack::Slice slice() const {
    return vpack::Slice(reinterpret_cast<const uint8_t*>(_buffer.data()));
  }  // return a slice

  RocksDBValue(RocksDBEntryType type, rocksdb::Slice slice)
    : _type(type), _buffer(slice.data(), slice.size()) {}

  RocksDBValue(const RocksDBValue&) = delete;
  RocksDBValue& operator=(const RocksDBValue&) = delete;

  RocksDBValue(RocksDBValue&& other) noexcept
    : _type(other._type), _buffer(std::move(other._buffer)) {}

  RocksDBValue& operator=(RocksDBValue&& other) noexcept {
    SDB_ASSERT(_type == other._type || _type == RocksDBEntryType::Placeholder);
    _type = other._type;
    _buffer = std::move(other._buffer);
    return *this;
  }

 private:
  explicit RocksDBValue(RocksDBEntryType type);
  RocksDBValue(RocksDBEntryType type, vpack::Slice data);

  static RocksDBEntryType type(const char* data, size_t size);
  static vpack::Slice data(const char* data, size_t size);

  RocksDBEntryType _type;
  std::string _buffer;
};

}  // namespace sdb
