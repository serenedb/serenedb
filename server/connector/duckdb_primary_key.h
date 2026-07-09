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
#include <concepts>
#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <ranges>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "basics/primary_key.hpp"  // for AppendSigned, ReadSigned
#include "basics/string_utils.h"
#include "catalog/table_options.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::connector::duckdb_primary_key {

struct PKColumn {
  size_t input_col_idx;
  duckdb::LogicalType type;
};

// Big-endian sorted PK encoding. Appends the row's value (selected by
// `fmt`/`row_idx`) to `key` so that lexicographic byte order matches the
// type's natural order.
inline void AppendPKValue(std::string& key,
                          const duckdb::UnifiedVectorFormat& fmt,
                          duckdb::idx_t row_idx,
                          const duckdb::LogicalType& type) {
  const auto idx = fmt.sel->get_index(row_idx);
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      auto val = duckdb::UnifiedVectorFormat::GetData<bool>(fmt)[idx];
      key.push_back(val ? '\x01' : '\x00');
    } break;
    case duckdb::LogicalTypeId::TINYINT: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int8_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int8_t));
      key[base] = static_cast<char>(val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::SMALLINT: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int16_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int16_t));
      absl::big_endian::Store16(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::INTEGER: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int32_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::BIGINT: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
      auto val =
        duckdb::UnifiedVectorFormat::GetData<duckdb::timestamp_t>(fmt)[idx]
          .value;
      auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::DATE: {
      auto val =
        duckdb::UnifiedVectorFormat::GetData<duckdb::date_t>(fmt)[idx].days;
      auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
    } break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB: {
      // String PK: escape null bytes (\0 -> \0\1) and terminate with \0\0
      static constexpr std::string_view kNullEsc{"\0\1", 2};
      static constexpr std::string_view kStringEnd{"\0\0", 2};
      const auto& str =
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[idx];
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
    } break;
    case duckdb::LogicalTypeId::FLOAT: {
      auto val = duckdb::UnifiedVectorFormat::GetData<float>(fmt)[idx];
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
        static constexpr char kPosNaN[] = "\xFF\xC0\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(float));
      }
    } break;
    case duckdb::LogicalTypeId::DOUBLE: {
      auto val = duckdb::UnifiedVectorFormat::GetData<double>(fmt)[idx];
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
        static constexpr char kPosNaN[] = "\xFF\xF8\x00\x00\x00\x00\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(double));
      }
      break;
    }
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "Unsupported PK type: ", type.ToString());
  }
}

// Build PK column mappings from a chunk-ordered range of catalog columns.
// Maps each PK column ID to its position in `columns` + DuckDB type.
//
// Accepts any random-access range whose elements are catalog::Column (vector,
// span, or a lazy views::transform of an index list, etc.) so the caller can
// describe a projected/reordered chunk without materialising a copy.
template<std::ranges::random_access_range R>
  requires std::same_as<std::remove_cvref_t<std::ranges::range_reference_t<R>>,
                        catalog::Column>
inline std::vector<PKColumn> BuildPKColumns(
  R&& columns, std::span<const catalog::Column::Id> pk_col_ids) {
  std::vector<PKColumn> result;
  result.reserve(pk_col_ids.size());
  const auto begin = std::ranges::begin(columns);
  const auto n = std::ranges::size(columns);
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < n; ++i) {
      const catalog::Column& c = begin[i];
      if (c.GetId() == pk_id) {
        result.push_back(PKColumn{
          .input_col_idx = i,
          .type = c.type,
        });
        break;
      }
    }
  }
  return result;
}

// Convenience: PK columns laid out in catalog order (used by paths whose
// chunk shape mirrors `table.Columns()`).
inline std::vector<PKColumn> BuildPKColumns(const catalog::Table& table) {
  return BuildPKColumns(table.Columns(), table.PKColumns());
}

inline void PreparePKFormats(
  const duckdb::DataChunk& chunk, std::span<const PKColumn> pk_columns,
  std::vector<duckdb::UnifiedVectorFormat>& pk_formats) {
  const auto num_rows = chunk.size();
  pk_formats.resize(pk_columns.size());
  for (size_t c = 0; c < pk_columns.size(); ++c) {
    chunk.data[pk_columns[c].input_col_idx].ToUnifiedFormat(num_rows,
                                                            pk_formats[c]);
  }
}

inline void Create(std::span<const duckdb::UnifiedVectorFormat> pk_formats,
                   std::span<const PKColumn> pk_columns, duckdb::idx_t row_idx,
                   std::string& key) {
  SDB_ASSERT(pk_formats.size() == pk_columns.size());
  for (size_t c = 0; c < pk_columns.size(); ++c) {
    AppendPKValue(key, pk_formats[c], row_idx, pk_columns[c].type);
  }
}

// Sortable signed encoding -- caller must have reserved the id.
inline void AppendGenerated(std::string& key, uint64_t generated_id) {
  primary_key::AppendSigned(key, std::bit_cast<int64_t>(generated_id));
}

}  // namespace sdb::connector::duckdb_primary_key
