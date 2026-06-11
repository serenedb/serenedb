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

#include <absl/base/internal/endian.h>

#include <cmath>
#include <cstring>
#include <duckdb.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <span>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "connector/key_utils.hpp"
#include "connector/secondary_sink_writer.hpp"
#include "rocksdb/slice.h"

namespace sdb::connector {

// Encode a duckdb::Value into a binary-sortable RocksDB key suffix.
// Matches the encoding produced by AppendPKValue (write path).
inline void AppendDuckDBValueToKey(std::string& key, const duckdb::Value& v) {
  switch (v.type().id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      key.push_back(v.GetValue<bool>() ? '\x01' : '\x00');
      break;
    case duckdb::LogicalTypeId::TINYINT: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(int8_t));
      key[base] = static_cast<char>(v.GetValue<int8_t>());
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::SMALLINT: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(int16_t));
      absl::big_endian::Store16(key.data() + base, v.GetValue<int16_t>());
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::INTEGER: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, v.GetValue<int32_t>());
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::BIGINT: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, v.GetValue<int64_t>());
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base,
                                v.GetValue<duckdb::timestamp_t>().value);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::DATE: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base,
                                v.GetValue<duckdb::date_t>().days);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB: {
      static constexpr std::string_view kNullEsc{"\0\1", 2};
      static constexpr std::string_view kStringEnd{"\0\0", 2};
      for (char c : duckdb::StringValue::Get(v)) {
        if (c == '\0') {
          key.append(kNullEsc);
        } else {
          key.push_back(c);
        }
      }
      key.append(kStringEnd);
    } break;
    case duckdb::LogicalTypeId::FLOAT: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(float));
      const float f = v.GetValue<float>();
      if (f != 0 && !std::isnan(f)) {
        absl::big_endian::Store32(key.data() + base,
                                  irs::numeric_utils::Ftoi32(f));
        key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      } else if (f == 0) {
        static constexpr char kZero[] = "\x80\x00\x00\x00";
        std::memcpy(key.data() + base, kZero, sizeof(float));
      } else {
        static constexpr char kPosNaN[] = "\xFF\xC0\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(float));
      }
    } break;
    case duckdb::LogicalTypeId::DOUBLE: {
      const auto base = key.size();
      basics::StrAppend(key, sizeof(double));
      const double d = v.GetValue<double>();
      if (d != 0 && !std::isnan(d)) {
        absl::big_endian::Store64(key.data() + base,
                                  irs::numeric_utils::Dtoi64(d));
        key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      } else if (d == 0) {
        static constexpr char kZero[] = "\x80\x00\x00\x00\x00\x00\x00\x00";
        std::memcpy(key.data() + base, kZero, sizeof(double));
      } else {
        static constexpr char kPosNaN[] = "\xFF\xF8\x00\x00\x00\x00\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(double));
      }
      break;
    }
    default:
      SDB_ASSERT(false, "PK point lookup: unsupported key type ",
                 v.type().ToString());
  }
}

template<typename Derived>
class DuckDBKeyBuilderBase {
 public:
  std::span<const rocksdb::Slice> BuildKeys(
    catalog::Column::Id col_id,
    const std::vector<std::vector<duckdb::Value>>& points, size_t offset,
    size_t count) {
    GrowStorage(count);
    for (size_t i = 0; i < count; ++i) {
      _key_storage[i].clear();
      static_cast<Derived*>(this)->BuildFullKey(_key_storage[i], col_id,
                                                points[offset + i]);
      _slice_storage[i] = _key_storage[i];
    }
    return {_slice_storage.data(), count};
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

class DuckDBPrimaryKeyBuilder
  : public DuckDBKeyBuilderBase<DuckDBPrimaryKeyBuilder> {
 public:
  explicit DuckDBPrimaryKeyBuilder(ObjectId table_id)
    : _table_prefix(key_utils::PrepareTableKey(table_id)) {
    _col_prefix.reserve(key_utils::kKeyPrefixSize);
  }

  void BuildFullKey(std::string& key, catalog::Column::Id col_id,
                    const std::vector<duckdb::Value>& values) const {
    key.assign(_table_prefix);
    key_utils::AppendColumnKey(key, col_id);
    for (const auto& v : values) {
      AppendDuckDBValueToKey(key, v);
    }
  }

  // Patches the column-id prefix of keys built by the previous BuildKeys call,
  // compacting only the found-row entries. Returns found_indices.size() slices.
  std::span<const rocksdb::Slice> BuildPresentKeys(
    catalog::Column::Id col_id, std::span<const size_t> found_indices) {
    _col_prefix.assign(_table_prefix);
    key_utils::AppendColumnKey(_col_prefix, col_id);
    SDB_ASSERT(_col_prefix.size() == key_utils::kKeyPrefixSize);
    for (size_t j = 0; j < found_indices.size(); ++j) {
      const size_t i = found_indices[j];
      std::memcpy(_key_storage[i].data(), _col_prefix.data(),
                  key_utils::kKeyPrefixSize);
      _slice_storage[j] = _key_storage[i];
    }
    return {_slice_storage.data(), found_indices.size()};
  }

 private:
  std::string _table_prefix;
  std::string _col_prefix;
};

class DuckDBSecondaryKeyBuilder
  : public DuckDBKeyBuilderBase<DuckDBSecondaryKeyBuilder> {
 public:
  explicit DuckDBSecondaryKeyBuilder(ObjectId shard_id) : _shard_id{shard_id} {}

  void BuildFullKey(std::string& key, catalog::Column::Id /*col_id*/,
                    const std::vector<duckdb::Value>& values) const {
    secondary_key::AppendShardPrefix(key, _shard_id);
    secondary_key::AppendDummyColumnId(key);
    for (const auto& v : values) {
      if (v.IsNull()) {
        secondary_key::AppendNullMarker(key);
      } else {
        secondary_key::AppendNotNullMarker(key);
        AppendDuckDBValueToKey(key, v);
      }
    }
  }

 private:
  ObjectId _shard_id;
};

}  // namespace sdb::connector
