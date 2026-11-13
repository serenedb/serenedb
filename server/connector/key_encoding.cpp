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

#include "connector/key_encoding.h"

#include <duckdb/common/types/vector.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string.hpp>

namespace sdb::connector::key_encoding {
namespace {

constexpr char kElemNotNull = '\x01';
constexpr char kElemNull = '\x02';
constexpr char kListEnd = '\x00';

template<typename T>
void AppendNumeric(std::string& key, const duckdb::UnifiedVectorFormat& fmt,
                   duckdb::idx_t idx) {
  irs::byte_type buf[irs::numeric_utils::kNumericTermSize<T>];
  key.append(irs::ViewCast<char>(irs::numeric_utils::EncodeNumericTerm(
    buf, duckdb::UnifiedVectorFormat::GetData<T>(fmt)[idx])));
}

}  // namespace

void AppendScalarValue(std::string& key, const duckdb::UnifiedVectorFormat& fmt,
                       duckdb::idx_t row_idx, const duckdb::LogicalType& type) {
  const auto idx = fmt.sel->get_index(row_idx);
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      auto val = duckdb::UnifiedVectorFormat::GetData<bool>(fmt)[idx];
      key.push_back(val ? '\x01' : '\x00');
    } break;
    case duckdb::LogicalTypeId::TINYINT:
      AppendNumeric<int8_t>(key, fmt, idx);
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      AppendNumeric<int16_t>(key, fmt, idx);
      break;
    case duckdb::LogicalTypeId::INTEGER:
    case duckdb::LogicalTypeId::DATE:
      AppendNumeric<int32_t>(key, fmt, idx);
      break;
    case duckdb::LogicalTypeId::BIGINT:
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ_NS:
      AppendNumeric<int64_t>(key, fmt, idx);
      break;
    case duckdb::LogicalTypeId::UBIGINT:
      AppendNumeric<uint64_t>(key, fmt, idx);
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB: {
      // Escape null bytes (\0 -> \0\1) and terminate with \0\0 so a string
      // orders before any of its extensions.
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
    case duckdb::LogicalTypeId::FLOAT:
      AppendNumeric<float>(key, fmt, idx);
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      AppendNumeric<double>(key, fmt, idx);
      break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("Unsupported key type: ", type.ToString()));
  }
}

void AppendValue(std::string& key,
                 const duckdb::RecursiveUnifiedVectorFormat& vec,
                 duckdb::idx_t row_idx) {
  const auto& type = vec.logical_type;
  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST: {
      const auto idx = vec.unified.sel->get_index(row_idx);
      const auto entry =
        duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
          vec.unified)[idx];
      const auto& child = vec.children[0];
      for (duckdb::idx_t i = 0; i < entry.length; ++i) {
        const auto child_row = entry.offset + i;
        const auto child_idx = child.unified.sel->get_index(child_row);
        if (child.unified.validity.RowIsValid(child_idx)) {
          key.push_back(kElemNotNull);
          AppendValue(key, child, child_row);
        } else {
          key.push_back(kElemNull);
        }
      }
      key.push_back(kListEnd);
    } break;
    case duckdb::LogicalTypeId::ARRAY: {
      const auto idx = vec.unified.sel->get_index(row_idx);
      const auto size = duckdb::ArrayType::GetSize(type);
      const auto& child = vec.children[0];
      for (duckdb::idx_t i = 0; i < size; ++i) {
        const auto child_row = idx * size + i;
        const auto child_idx = child.unified.sel->get_index(child_row);
        if (child.unified.validity.RowIsValid(child_idx)) {
          key.push_back(kElemNotNull);
          AppendValue(key, child, child_row);
        } else {
          key.push_back(kElemNull);
        }
      }
    } break;
    case duckdb::LogicalTypeId::TUPLE:
    case duckdb::LogicalTypeId::STRUCT: {
      for (duckdb::idx_t c = 0; c < vec.children.size(); ++c) {
        const auto& child = vec.children[c];
        const auto child_idx = child.unified.sel->get_index(row_idx);
        if (child.unified.validity.RowIsValid(child_idx)) {
          key.push_back(kElemNotNull);
          AppendValue(key, child, row_idx);
        } else {
          key.push_back(kElemNull);
        }
      }
    } break;
    default:
      AppendScalarValue(key, vec.unified, row_idx, type);
      break;
  }
}

}  // namespace sdb::connector::key_encoding
