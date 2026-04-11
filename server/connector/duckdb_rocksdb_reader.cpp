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

#include "connector/duckdb_rocksdb_reader.h"

#include <cstring>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <iresearch/utils/bytes_utils.hpp>

#include "basics/assert.h"
#include "connector/common.h"

namespace sdb::connector {

// Forward declaration -- defined below after anonymous namespace
void DeserializeListValue(std::string_view value, duckdb::Vector& output,
                          const duckdb::LogicalType& type, duckdb::idx_t idx);

}  // namespace sdb::connector
#include "connector/common.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"

namespace sdb::connector {

// Iterate a RocksDB column iterator, calling `func(row_idx, value)` for each
// row. Returns the number of rows iterated.
template<typename Func>
static duckdb::idx_t IterateColumn(rocksdb::Iterator& it,
                                   duckdb::idx_t max_rows, Func&& func) {
  duckdb::idx_t count = 0;
  while (it.Valid() && count < max_rows) {
    func(count, it.value().ToStringView());
    ++count;
    it.Next();
  }
  rocksutils::CheckIteratorStatus(it);
  return count;
}

template<typename T>
static duckdb::idx_t ReadScalarColumn(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      duckdb::idx_t max_rows) {
  auto* data = duckdb::FlatVector::GetDataMutable<T>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         SDB_ASSERT(value.size() == sizeof(T));
                         std::memcpy(&data[idx], value.data(), sizeof(T));
                       });
}

static duckdb::idx_t ReadBoolColumn(rocksdb::Iterator& it,
                                    duckdb::Vector& output,
                                    duckdb::idx_t max_rows) {
  auto* data = duckdb::FlatVector::GetDataMutable<bool>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         SDB_ASSERT(value.size() == kTrueValue.size());
                         data[idx] = (value == kTrueValue);
                       });
}

static duckdb::idx_t ReadVarcharColumn(rocksdb::Iterator& it,
                                       duckdb::Vector& output,
                                       duckdb::idx_t max_rows) {
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(
    it, max_rows, [&](duckdb::idx_t idx, std::string_view value) {
      if (value.empty()) {
        validity.SetInvalid(idx);
        return;
      }
      // RocksDB strings: leading null byte distinguishes empty string from NULL
      const size_t offset = value[0] == 0 ? 1 : 0;
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddString(output, value.data() + offset,
                                        value.size() - offset);
    });
}

static duckdb::idx_t ReadBlobColumn(rocksdb::Iterator& it,
                                    duckdb::Vector& output,
                                    duckdb::idx_t max_rows) {
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(
    it, max_rows, [&](duckdb::idx_t idx, std::string_view value) {
      if (value.empty()) {
        validity.SetInvalid(idx);
        return;
      }
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddStringOrBlob(output, value.data(),
                                              value.size());
    });
}

static duckdb::idx_t ReadTimestampColumn(rocksdb::Iterator& it,
                                         duckdb::Vector& output,
                                         duckdb::idx_t max_rows) {
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::timestamp_t>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         SDB_ASSERT(value.size() == sizeof(int64_t));
                         int64_t v;
                         std::memcpy(&v, value.data(), sizeof(v));
                         data[idx] = duckdb::timestamp_t(v);
                       });
}

static duckdb::idx_t ReadDateColumn(rocksdb::Iterator& it,
                                    duckdb::Vector& output,
                                    duckdb::idx_t max_rows) {
  auto* data = duckdb::FlatVector::GetDataMutable<duckdb::date_t>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         SDB_ASSERT(value.size() == sizeof(int32_t));
                         int32_t v;
                         std::memcpy(&v, value.data(), sizeof(v));
                         data[idx] = duckdb::date_t(v);
                       });
}

static duckdb::idx_t ReadListColumn(rocksdb::Iterator& it,
                                    duckdb::Vector& output,
                                    const duckdb::LogicalType& type,
                                    duckdb::idx_t max_rows) {
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         DeserializeListValue(value, output, type, idx);
                       });
}

duckdb::idx_t ReadColumnIntoDuckDB(rocksdb::Iterator& it,
                                   duckdb::Vector& output,
                                   const duckdb::LogicalType& type,
                                   duckdb::idx_t max_rows) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return ReadBoolColumn(it, output, max_rows);
    case duckdb::LogicalTypeId::TINYINT:
      return ReadScalarColumn<int8_t>(it, output, max_rows);
    case duckdb::LogicalTypeId::SMALLINT:
      return ReadScalarColumn<int16_t>(it, output, max_rows);
    case duckdb::LogicalTypeId::INTEGER:
      return ReadScalarColumn<int32_t>(it, output, max_rows);
    case duckdb::LogicalTypeId::BIGINT:
      return ReadScalarColumn<int64_t>(it, output, max_rows);
    case duckdb::LogicalTypeId::FLOAT:
      return ReadScalarColumn<float>(it, output, max_rows);
    case duckdb::LogicalTypeId::DOUBLE:
      return ReadScalarColumn<double>(it, output, max_rows);
    case duckdb::LogicalTypeId::VARCHAR:
      return ReadVarcharColumn(it, output, max_rows);
    case duckdb::LogicalTypeId::BLOB:
      return ReadBlobColumn(it, output, max_rows);
    case duckdb::LogicalTypeId::TIMESTAMP:
      return ReadTimestampColumn(it, output, max_rows);
    case duckdb::LogicalTypeId::DATE:
      return ReadDateColumn(it, output, max_rows);
    case duckdb::LogicalTypeId::HUGEINT:
      return ReadScalarColumn<duckdb::hugeint_t>(it, output, max_rows);
    case duckdb::LogicalTypeId::LIST:
      return ReadListColumn(it, output, type, max_rows);
    default:
      // Fallback: read as varchar
      return ReadVarcharColumn(it, output, max_rows);
  }
}

duckdb::idx_t ReadColumnWithRowId(rocksdb::Iterator& it,
                                  duckdb::Vector& col_output,
                                  const duckdb::LogicalType& type,
                                  duckdb::Vector& rowid_output,
                                  size_t key_prefix_size,
                                  duckdb::idx_t max_rows) {
  // We need to read both value AND key for each row in one pass.
  // Can't use the typed readers (they advance the iterator).
  // Instead, do a generic loop extracting both.
  duckdb::idx_t count = 0;

  while (it.Valid() && count < max_rows) {
    // Extract PK bytes from key
    auto key = it.key().ToStringView();
    SDB_ASSERT(key.size() >= key_prefix_size);
    auto pk_bytes = key.substr(key_prefix_size);
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(rowid_output)[count] =
      duckdb::StringVector::AddStringOrBlob(rowid_output, pk_bytes.data(),
                                            pk_bytes.size());

    // Read column value via shared helper
    DeserializeValueIntoDuckDB(it.value().ToStringView(), col_output, type,
                               count);

    ++count;
    it.Next();
  }
  rocksutils::CheckIteratorStatus(it);
  return count;
}

void DeserializeValueIntoDuckDB(std::string_view value, duckdb::Vector& output,
                                const duckdb::LogicalType& type,
                                duckdb::idx_t idx) {
  auto& validity = duckdb::FlatVector::Validity(output);

  if (value.empty()) {
    validity.SetInvalid(idx);
    return;
  }

  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      SDB_ASSERT(value.size() == kTrueValue.size());
      duckdb::FlatVector::GetDataMutable<bool>(output)[idx] =
        (value == kTrueValue);
      break;
    }
    case duckdb::LogicalTypeId::TINYINT: {
      SDB_ASSERT(value.size() == sizeof(int8_t));
      duckdb::FlatVector::GetDataMutable<int8_t>(output)[idx] =
        static_cast<int8_t>(value[0]);
      break;
    }
    case duckdb::LogicalTypeId::SMALLINT: {
      SDB_ASSERT(value.size() == sizeof(int16_t));
      int16_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<int16_t>(output)[idx] = v;
      break;
    }
    case duckdb::LogicalTypeId::INTEGER: {
      SDB_ASSERT(value.size() == sizeof(int32_t));
      int32_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<int32_t>(output)[idx] = v;
      break;
    }
    case duckdb::LogicalTypeId::BIGINT: {
      SDB_ASSERT(value.size() == sizeof(int64_t));
      int64_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<int64_t>(output)[idx] = v;
      break;
    }
    case duckdb::LogicalTypeId::FLOAT: {
      SDB_ASSERT(value.size() == sizeof(float));
      float v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<float>(output)[idx] = v;
      break;
    }
    case duckdb::LogicalTypeId::DOUBLE: {
      SDB_ASSERT(value.size() == sizeof(double));
      double v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<double>(output)[idx] = v;
      break;
    }
    case duckdb::LogicalTypeId::HUGEINT: {
      SDB_ASSERT(value.size() == sizeof(duckdb::hugeint_t));
      duckdb::hugeint_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<duckdb::hugeint_t>(output)[idx] = v;
      break;
    }
    case duckdb::LogicalTypeId::VARCHAR: {
      const size_t offset = value[0] == 0 ? 1 : 0;
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddString(output, value.data() + offset,
                                        value.size() - offset);
      break;
    }
    case duckdb::LogicalTypeId::BLOB: {
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddStringOrBlob(output, value.data(),
                                              value.size());
      break;
    }
    case duckdb::LogicalTypeId::TIMESTAMP: {
      SDB_ASSERT(value.size() == sizeof(int64_t));
      int64_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<duckdb::timestamp_t>(output)[idx] =
        duckdb::timestamp_t(v);
      break;
    }
    case duckdb::LogicalTypeId::DATE: {
      SDB_ASSERT(value.size() == sizeof(int32_t));
      int32_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<duckdb::date_t>(output)[idx] =
        duckdb::date_t(v);
      break;
    }
    case duckdb::LogicalTypeId::LIST: {
      DeserializeListValue(value, output, type, idx);
      break;
    }
    default:
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddString(output, value.data(), value.size());
      break;
  }
}

namespace {

// Deserialize sub-vector elements into a DuckDB child Vector.
// Port of ArrayColumnDecoder::AddImpl (rocksdb_column_decoder.cpp:129).
void DeserializeSubVectorElements(const uint8_t*& ptr, const uint8_t* end,
                                  duckdb::Vector& child,
                                  duckdb::idx_t child_offset,
                                  uint32_t elem_count, bool have_nulls,
                                  bool have_length, uint32_t length_array_size,
                                  const duckdb::LogicalType& child_type) {
  auto& child_validity = duckdb::FlatVector::Validity(child);

  const uint8_t* elem_nulls = nullptr;
  if (have_nulls) {
    elem_nulls = ptr;
    ptr += (elem_count + 7) / 8;
  }

  switch (child_type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      auto* out = duckdb::FlatVector::GetDataMutable<bool>(child);
      auto bool_bytes = (elem_count + 7) / 8;
      for (uint32_t i = 0; i < elem_count; i++) {
        if (elem_nulls && !(elem_nulls[i / 8] & (1 << (i % 8)))) {
          child_validity.SetInvalid(child_offset + i);
        } else {
          out[child_offset + i] = (ptr[i / 8] >> (i % 8)) & 1;
        }
      }
      ptr += bool_bytes;
      break;
    }
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB: {
      // Variable-length: read length array, then string data
      const uint8_t* lptr = ptr;
      ptr += length_array_size;
      for (uint32_t i = 0; i < elem_count; i++) {
        auto len = irs::vread<uint32_t>(lptr);
        if (elem_nulls && !(elem_nulls[i / 8] & (1 << (i % 8)))) {
          child_validity.SetInvalid(child_offset + i);
          ptr += len;
        } else {
          duckdb::FlatVector::GetDataMutable<duckdb::string_t>(
            child)[child_offset + i] =
            duckdb::StringVector::AddString(
              child, reinterpret_cast<const char*>(ptr), len);
          ptr += len;
        }
      }
      break;
    }
    default: {
      // Fixed-width types -- dispatch by type to get correct GetDataMutable<T>
      auto copy_fixed = [&]<typename T>(T*) {
        auto* out = duckdb::FlatVector::GetDataMutable<T>(child);
        std::memcpy(&out[child_offset], ptr, elem_count * sizeof(T));
        ptr += elem_count * sizeof(T);
        if (elem_nulls) {
          for (uint32_t i = 0; i < elem_count; i++) {
            if (!(elem_nulls[i / 8] & (1 << (i % 8)))) {
              child_validity.SetInvalid(child_offset + i);
            }
          }
        }
      };
      switch (child_type.id()) {
        case duckdb::LogicalTypeId::TINYINT:
          copy_fixed(static_cast<int8_t*>(nullptr));
          break;
        case duckdb::LogicalTypeId::SMALLINT:
          copy_fixed(static_cast<int16_t*>(nullptr));
          break;
        case duckdb::LogicalTypeId::INTEGER:
          copy_fixed(static_cast<int32_t*>(nullptr));
          break;
        case duckdb::LogicalTypeId::BIGINT:
          copy_fixed(static_cast<int64_t*>(nullptr));
          break;
        case duckdb::LogicalTypeId::FLOAT:
          copy_fixed(static_cast<float*>(nullptr));
          break;
        case duckdb::LogicalTypeId::DOUBLE:
          copy_fixed(static_cast<double*>(nullptr));
          break;
        case duckdb::LogicalTypeId::TIMESTAMP:
        case duckdb::LogicalTypeId::TIMESTAMP_TZ:
          copy_fixed(static_cast<duckdb::timestamp_t*>(nullptr));
          break;
        case duckdb::LogicalTypeId::DATE:
          copy_fixed(static_cast<duckdb::date_t*>(nullptr));
          break;
        case duckdb::LogicalTypeId::HUGEINT:
          copy_fixed(static_cast<duckdb::hugeint_t*>(nullptr));
          break;
        default:
          SDB_ASSERT(false, "Unsupported fixed-width element type in list");
      }
      break;
    }
  }
}

}  // namespace

void DeserializeListValue(std::string_view value, duckdb::Vector& output,
                          const duckdb::LogicalType& type, duckdb::idx_t idx) {
  auto* ptr = reinterpret_cast<const uint8_t*>(value.data());
  auto* end = ptr + value.size();

  auto elem_count = irs::vread<uint32_t>(ptr);
  if (elem_count == 0) {
    duckdb::ListVector::GetData(output)[idx] = duckdb::list_entry_t{0, 0};
    return;
  }

  auto flags = static_cast<ValueFlags>(*ptr++);
  bool is_constant = (flags & ValueFlags::Constant) != ValueFlags::None;
  bool have_nulls = (flags & ValueFlags::HaveNulls) != ValueFlags::None;
  bool have_length = (flags & ValueFlags::HaveLength) != ValueFlags::None;

  uint32_t length_array_size = 0;
  if (have_length) {
    length_array_size = irs::vread<uint32_t>(ptr);
  }

  auto current_size = duckdb::ListVector::GetListSize(output);
  duckdb::ListVector::Reserve(output, current_size + elem_count);
  duckdb::ListVector::GetData(output)[idx] =
    duckdb::list_entry_t{current_size, elem_count};

  auto& child = duckdb::ListVector::GetEntry(output);
  auto& child_type = duckdb::ListType::GetChildType(type);

  if (is_constant) {
    // Constant: single value replicated
    SDB_ASSERT(!have_nulls);
    auto remaining = static_cast<size_t>(end - ptr);
    if (remaining == 0) {
      // All NULLs
      auto& child_validity = duckdb::FlatVector::Validity(child);
      for (uint32_t i = 0; i < elem_count; i++) {
        child_validity.SetInvalid(current_size + i);
      }
    } else {
      // Single value, replicate
      for (uint32_t i = 0; i < elem_count; i++) {
        auto val_sv =
          std::string_view{reinterpret_cast<const char*>(ptr), remaining};
        DeserializeValueIntoDuckDB(val_sv, child, child_type, current_size + i);
      }
    }
  } else {
    DeserializeSubVectorElements(ptr, end, child, current_size, elem_count,
                                 have_nulls, have_length, length_array_size,
                                 child_type);
  }

  duckdb::ListVector::SetListSize(output, current_size + elem_count);
}

}  // namespace sdb::connector
