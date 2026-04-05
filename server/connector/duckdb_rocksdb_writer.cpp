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

#include "connector/duckdb_rocksdb_writer.h"

#include <absl/base/internal/endian.h>

#include <cmath>
#include <iresearch/utils/numeric_utils.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/common.h"

namespace sdb::connector {

rocksdb::Slice SerializeScalarValue(const duckdb::Vector& vec,
                                    duckdb::idx_t idx,
                                    const duckdb::LogicalType& type,
                                    std::string& buffer) {
  auto& validity = duckdb::FlatVector::Validity(vec);
  if (!validity.RowIsValid(idx)) {
    return {};  // NULL = empty slice
  }

  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      auto val = duckdb::FlatVector::GetData<bool>(vec)[idx];
      auto sv = val ? kTrueValue : kFalseValue;
      buffer.assign(sv.data(), sv.size());
      return {buffer};
    }
    case duckdb::LogicalTypeId::TINYINT: {
      buffer.resize(sizeof(int8_t));
      buffer[0] = static_cast<char>(
        duckdb::FlatVector::GetData<int8_t>(vec)[idx]);
      return {buffer};
    }
    case duckdb::LogicalTypeId::SMALLINT: {
      buffer.resize(sizeof(int16_t));
      absl::little_endian::Store16(
        buffer.data(), duckdb::FlatVector::GetData<int16_t>(vec)[idx]);
      return {buffer};
    }
    case duckdb::LogicalTypeId::INTEGER: {
      buffer.resize(sizeof(int32_t));
      absl::little_endian::Store32(
        buffer.data(), duckdb::FlatVector::GetData<int32_t>(vec)[idx]);
      return {buffer};
    }
    case duckdb::LogicalTypeId::BIGINT: {
      buffer.resize(sizeof(int64_t));
      absl::little_endian::Store64(
        buffer.data(), duckdb::FlatVector::GetData<int64_t>(vec)[idx]);
      return {buffer};
    }
    case duckdb::LogicalTypeId::FLOAT: {
      buffer.resize(sizeof(float));
      float val = duckdb::FlatVector::GetData<float>(vec)[idx];
      std::memcpy(buffer.data(), &val, sizeof(float));
      return {buffer};
    }
    case duckdb::LogicalTypeId::DOUBLE: {
      buffer.resize(sizeof(double));
      double val = duckdb::FlatVector::GetData<double>(vec)[idx];
      std::memcpy(buffer.data(), &val, sizeof(double));
      return {buffer};
    }
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB: {
      auto str = duckdb::FlatVector::GetData<duckdb::string_t>(vec)[idx];
      auto data = str.GetData();
      auto size = str.GetSize();
      if (size == 0) {
        // Empty string → single \0 byte (distinguishes from NULL)
        buffer.assign(1, '\0');
      } else if (data[0] == '\0') {
        // Starts with \0 → prefix with extra \0
        buffer.resize(size + 1);
        buffer[0] = '\0';
        std::memcpy(buffer.data() + 1, data, size);
      } else {
        buffer.assign(data, size);
      }
      return {buffer};
    }
    case duckdb::LogicalTypeId::TIMESTAMP: {
      buffer.resize(sizeof(int64_t));
      auto val = duckdb::FlatVector::GetData<duckdb::timestamp_t>(vec)[idx];
      absl::little_endian::Store64(buffer.data(), val.value);
      return {buffer};
    }
    case duckdb::LogicalTypeId::DATE: {
      buffer.resize(sizeof(int32_t));
      auto val = duckdb::FlatVector::GetData<duckdb::date_t>(vec)[idx];
      absl::little_endian::Store32(buffer.data(), val.days);
      return {buffer};
    }
    default:
      SDB_ASSERT(false, "Unsupported type for RocksDB serialization");
      return {};
  }
}

void AppendPKValueFromDuckDB(std::string& key, const duckdb::Vector& vec,
                             duckdb::idx_t idx,
                             const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::TINYINT: {
      auto val = duckdb::FlatVector::GetData<int8_t>(vec)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int8_t));
      key[base] = static_cast<char>(val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::SMALLINT: {
      auto val = duckdb::FlatVector::GetData<int16_t>(vec)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int16_t));
      absl::big_endian::Store16(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::INTEGER: {
      auto val = duckdb::FlatVector::GetData<int32_t>(vec)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::BIGINT: {
      auto val = duckdb::FlatVector::GetData<int64_t>(vec)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::VARCHAR: {
      // String PK: escape null bytes (\0 → \0\1) and terminate with \0\0
      // Same encoding as primary_key::AppendTypedValue for StringView
      static constexpr std::string_view kNullEsc{"\0\1", 2};
      static constexpr std::string_view kStringEnd{"\0\0", 2};
      auto str = duckdb::FlatVector::GetData<duckdb::string_t>(vec)[idx];
      auto* data = str.GetData();
      auto size = str.GetSize();
      for (duckdb::idx_t i = 0; i < size; ++i) {
        if (data[i]) {
          key.push_back(data[i]);
        } else {
          key.append(kNullEsc);
        }
      }
      key.append(kStringEnd);
      break;
    }
    case duckdb::LogicalTypeId::FLOAT: {
      // Float PK: sortable encoding via Ftoi32 + sign bit flip
      auto val = duckdb::FlatVector::GetData<float>(vec)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(float));
      if (val != 0 && !std::isnan(val)) {
        absl::big_endian::Store32(key.data() + base,
                                  irs::numeric_utils::Ftoi32(val));
        key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      } else if (val == 0) {
        static constexpr char kZero[] = "\x80\0\0\0";
        std::memcpy(key.data() + base, kZero, sizeof(float));
      } else {
        // NaN → positive NaN canonical form
        static constexpr char kPosNaN[] = "\xFF\xC0\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(float));
      }
      break;
    }
    case duckdb::LogicalTypeId::DOUBLE: {
      // Double PK: sortable encoding via Dtoi64 + sign bit flip
      auto val = duckdb::FlatVector::GetData<double>(vec)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(double));
      if (val != 0 && !std::isnan(val)) {
        absl::big_endian::Store64(key.data() + base,
                                  irs::numeric_utils::Dtoi64(val));
        key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      } else if (val == 0) {
        static constexpr char kZero[] = "\x80\0\0\0\0\0\0\0";
        std::memcpy(key.data() + base, kZero, sizeof(double));
      } else {
        // NaN
        static constexpr char kPosNaN[] =
          "\xFF\xF8\x00\x00\x00\x00\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(double));
      }
      break;
    }
    default:
      SDB_ASSERT(false, "Unsupported PK type");
  }
}

}  // namespace sdb::connector
