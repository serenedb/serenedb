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
  RocksDBKey(std::string_view key) : _key{key} {}
  RocksDBKey(rocksdb::Slice slice) : _key{slice.data(), slice.size()} {}

 protected:
  std::string_view _key;
};

class DefinitionKey : public RocksDBKey {
 public:
  ObjectId GetParentId() const;
  RocksDBEntryType GetEntryType() const;
  ObjectId GetObjectId() const;

  static std::string Create(ObjectId parent_id, RocksDBEntryType entry,
                            ObjectId id);

  static std::pair<std::string, std::string> CreateInterval(ObjectId parent_id);
  static std::pair<std::string, std::string> CreateInterval(
    ObjectId parent_id, RocksDBEntryType entry);
};

class SettingsKey : public RocksDBKey {
 public:
  static std::string Create(RocksDBSettingsType settings_type);
  RocksDBSettingsType GetSettingsType() const;
  RocksDBEntryType GetEntryType() const;
};

template<typename Key>
class RocksDBKeyWithBuffer {
 public:
  template<typename... Args>
  RocksDBKeyWithBuffer(Args&&... args)
    : _buffer{Key::Create(std::forward<Args>(args)...)} {}

  Key GetKey() const { return Key{_buffer}; }
  const std::string& GetBuffer() const { return _buffer; }

 private:
  std::string _buffer;
};

}  // namespace sdb
