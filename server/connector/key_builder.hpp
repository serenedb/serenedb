////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <velox/type/Type.h>
#include <velox/type/Variant.h>

#include <span>
#include <string>
#include <vector>

#include "basics/containers/bitset.hpp"
#include "basics/fwd.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/secondary_sink_writer.hpp"

namespace sdb::connector {

template<typename Derived>
class KeyBuilderBase {
 public:
  // Builds keys for all values into internal storage.
  // Storage grows monotonically -- first (largest) call allocates, subsequent
  // calls with smaller counts are no-ops on the underlying storage.
  // Returns a span of slices valid until the next BuildKeys call.
  std::span<const rocksdb::Slice> BuildKeys(
    catalog::Column::Id col_id,
    std::span<const std::vector<velox::variant>> values) {
    GrowStorage(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
      _key_storage[i].clear();
      static_cast<Derived*>(this)->BuildFullKey(_key_storage[i], col_id,
                                                values[i]);
      _slice_storage[i] = _key_storage[i];
    }
    return {_slice_storage.data(), values.size()};
  }

 protected:
  void GrowStorage(size_t n) {
    if (n > _key_storage.size()) {
      _key_storage.resize(n);
      _slice_storage.resize(n);
    }
  }

  std::vector<std::string> _key_storage;
  std::vector<rocksdb::Slice> _slice_storage;
};

class PrimaryKeyBuilder : public KeyBuilderBase<PrimaryKeyBuilder> {
 public:
  PrimaryKeyBuilder(ObjectId table_id, velox::RowTypePtr pk_type)
    : _table_id{table_id}, _pk_type{std::move(pk_type)} {
    _column_key.reserve(key_utils::kKeyPrefixSize);
  }

  void BuildFullKey(std::string& key, catalog::Column::Id column_id,
                    std::span<const velox::variant> values) const {
    key_utils::AppendTableKey(key, _table_id);
    key_utils::AppendColumnKey(key, column_id);
    primary_key::Create(values, *_pk_type, key);
  }

  // Patches the prefix of already-built keys for present rows only.
  // Must be called after BuildKeys for the same batch -- reuses the PK tail
  // already written into _key_storage[i], overwriting only the fixed-size
  // prefix (table + column id) with the new col_id via a single memcpy.
  // Returns a compact span of found_count slices (non-contiguous in storage).
  std::span<const rocksdb::Slice> BuildPresentKeys(
    catalog::Column::Id col_id, const irs::bitset& present_rows) {
    _column_key.clear();
    key_utils::AppendTableKey(_column_key, _table_id);
    key_utils::AppendColumnKey(_column_key, col_id);
    SDB_ASSERT(_column_key.size() == key_utils::kKeyPrefixSize);
    size_t key_idx = 0;
    for (size_t i = 0; i < present_rows.size(); ++i) {
      if (present_rows.test(i)) {
        std::memcpy(_key_storage[i].data(), _column_key.data(),
                    key_utils::kKeyPrefixSize);
        _slice_storage[key_idx++] = _key_storage[i];
      }
    }
    return {_slice_storage.data(), key_idx};
  }

  ObjectId _table_id;
  velox::RowTypePtr _pk_type;

 private:
  std::string _column_key;
};

class SecondaryKeyBuilder : public KeyBuilderBase<SecondaryKeyBuilder> {
 public:
  SecondaryKeyBuilder(ObjectId shard_id, velox::RowTypePtr sk_type)
    : _shard_id{shard_id}, _sk_type{std::move(sk_type)} {}

  void BuildFullKey(std::string& key, catalog::Column::Id column_id,
                    std::span<const velox::variant> values) const {
    secondary_key::AppendShardPrefix(key, _shard_id);
    secondary_key::AppendDummyColumnId(key);
    for (size_t i = 0; i < values.size(); ++i) {
      if (values[i].isNull()) {
        secondary_key::AppendNullMarker(key);
      } else {
        secondary_key::AppendNotNullMarker(key);
        primary_key::AppendVariantValue(key, values[i], _sk_type->childAt(i));
      }
    }
  }

  ObjectId _shard_id;
  velox::RowTypePtr _sk_type;
};

}  // namespace sdb::connector
