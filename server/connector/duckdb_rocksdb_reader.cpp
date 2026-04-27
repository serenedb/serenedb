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
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/map_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <iresearch/utils/bytes_utils.hpp>

#include "basics/assert.h"
#include "connector/common.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"

namespace sdb::connector {

// Forward declarations -- defined below after anonymous namespace
void DeserializeListValue(std::string_view value, duckdb::Vector& output,
                          const duckdb::LogicalType& type, duckdb::idx_t idx);
void DeserializeArrayValue(std::string_view value, duckdb::Vector& output,
                           const duckdb::LogicalType& type, duckdb::idx_t idx);
void DeserializeStructValue(std::string_view value, duckdb::Vector& output,
                            const duckdb::LogicalType& type, duckdb::idx_t idx);
void DeserializeMapValue(std::string_view value, duckdb::Vector& output,
                         const duckdb::LogicalType& type, duckdb::idx_t idx);

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
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

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
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

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
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

  return IterateColumn(
    it, max_rows, [&](duckdb::idx_t idx, std::string_view value) {
      if (value.empty()) {
        validity.SetInvalid(idx);
        return;
      }
      // RocksDB strings: leading null byte distinguishes empty string from
      // NULL
      const size_t offset = value[0] == 0 ? 1 : 0;
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddString(output, value.data() + offset,
                                        value.size() - offset);
    });
}

static duckdb::idx_t ReadBlobColumn(rocksdb::Iterator& it,
                                    duckdb::Vector& output,
                                    duckdb::idx_t max_rows) {
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

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
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

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
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

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
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         DeserializeListValue(value, output, type, idx);
                       });
}

static duckdb::idx_t ReadArrayColumn(rocksdb::Iterator& it,
                                     duckdb::Vector& output,
                                     const duckdb::LogicalType& type,
                                     duckdb::idx_t max_rows) {
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         DeserializeArrayValue(value, output, type, idx);
                       });
}

static duckdb::idx_t ReadMapColumn(rocksdb::Iterator& it,
                                   duckdb::Vector& output,
                                   const duckdb::LogicalType& type,
                                   duckdb::idx_t max_rows) {
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         DeserializeMapValue(value, output, type, idx);
                       });
}

static duckdb::idx_t ReadStructColumn(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      const duckdb::LogicalType& type,
                                      duckdb::idx_t max_rows) {
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

  return IterateColumn(it, max_rows,
                       [&](duckdb::idx_t idx, std::string_view value) {
                         if (value.empty()) {
                           validity.SetInvalid(idx);
                           return;
                         }
                         DeserializeStructValue(value, output, type, idx);
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
    case duckdb::LogicalTypeId::MAP:
      return ReadMapColumn(it, output, type, max_rows);
    case duckdb::LogicalTypeId::STRUCT:
      return ReadStructColumn(it, output, type, max_rows);
    case duckdb::LogicalTypeId::ARRAY:
      return ReadArrayColumn(it, output, type, max_rows);
    case duckdb::LogicalTypeId::ENUM:
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8:
          return ReadScalarColumn<uint8_t>(it, output, max_rows);
        case duckdb::PhysicalType::UINT16:
          return ReadScalarColumn<uint16_t>(it, output, max_rows);
        case duckdb::PhysicalType::UINT32:
          return ReadScalarColumn<uint32_t>(it, output, max_rows);
        default:
          SDB_THROW(ERROR_NOT_IMPLEMENTED, "Unsupported ENUM physical type");
      }
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED, "Unsupported vector type");
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
  auto& validity = duckdb::FlatVector::ValidityMutable(output);

  if (value.empty()) {
    validity.SetInvalid(idx);
    return;
  }

  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      SDB_ASSERT(value.size() == kTrueValue.size());
      duckdb::FlatVector::GetDataMutable<bool>(output)[idx] =
        (value == kTrueValue);
    } break;
    case duckdb::LogicalTypeId::TINYINT: {
      SDB_ASSERT(value.size() == sizeof(int8_t));
      duckdb::FlatVector::GetDataMutable<int8_t>(output)[idx] =
        static_cast<int8_t>(value[0]);
    } break;
    case duckdb::LogicalTypeId::SMALLINT: {
      SDB_ASSERT(value.size() == sizeof(int16_t));
      int16_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<int16_t>(output)[idx] = v;
    } break;
    case duckdb::LogicalTypeId::INTEGER: {
      SDB_ASSERT(value.size() == sizeof(int32_t));
      int32_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<int32_t>(output)[idx] = v;
    } break;
    case duckdb::LogicalTypeId::BIGINT: {
      SDB_ASSERT(value.size() == sizeof(int64_t));
      int64_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<int64_t>(output)[idx] = v;
    } break;
    case duckdb::LogicalTypeId::FLOAT: {
      SDB_ASSERT(value.size() == sizeof(float));
      float v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<float>(output)[idx] = v;
    } break;
    case duckdb::LogicalTypeId::DOUBLE: {
      SDB_ASSERT(value.size() == sizeof(double));
      double v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<double>(output)[idx] = v;
    } break;
    case duckdb::LogicalTypeId::HUGEINT: {
      SDB_ASSERT(value.size() == sizeof(duckdb::hugeint_t));
      duckdb::hugeint_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<duckdb::hugeint_t>(output)[idx] = v;
    } break;
    case duckdb::LogicalTypeId::VARCHAR: {
      const size_t offset = value[0] == 0 ? 1 : 0;
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddString(output, value.data() + offset,
                                        value.size() - offset);
    } break;
    case duckdb::LogicalTypeId::BLOB: {
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddStringOrBlob(output, value.data(),
                                              value.size());
    } break;
    case duckdb::LogicalTypeId::TIMESTAMP: {
      SDB_ASSERT(value.size() == sizeof(int64_t));
      int64_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<duckdb::timestamp_t>(output)[idx] =
        duckdb::timestamp_t(v);
    } break;
    case duckdb::LogicalTypeId::DATE: {
      SDB_ASSERT(value.size() == sizeof(int32_t));
      int32_t v;
      std::memcpy(&v, value.data(), sizeof(v));
      duckdb::FlatVector::GetDataMutable<duckdb::date_t>(output)[idx] =
        duckdb::date_t(v);
    } break;
    case duckdb::LogicalTypeId::LIST: {
      DeserializeListValue(value, output, type, idx);
    } break;
    case duckdb::LogicalTypeId::MAP: {
      DeserializeMapValue(value, output, type, idx);
    } break;
    case duckdb::LogicalTypeId::STRUCT: {
      DeserializeStructValue(value, output, type, idx);
    } break;
    case duckdb::LogicalTypeId::ARRAY: {
      DeserializeArrayValue(value, output, type, idx);
    } break;
    case duckdb::LogicalTypeId::ENUM: {
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8: {
          SDB_ASSERT(value.size() == sizeof(uint8_t));
          duckdb::FlatVector::GetDataMutable<uint8_t>(output)[idx] =
            static_cast<uint8_t>(value[0]);
        } break;
        case duckdb::PhysicalType::UINT16: {
          SDB_ASSERT(value.size() == sizeof(uint16_t));
          uint16_t v;
          std::memcpy(&v, value.data(), sizeof(v));
          duckdb::FlatVector::GetDataMutable<uint16_t>(output)[idx] = v;
        } break;
        case duckdb::PhysicalType::UINT32: {
          SDB_ASSERT(value.size() == sizeof(uint32_t));
          uint32_t v;
          std::memcpy(&v, value.data(), sizeof(v));
          duckdb::FlatVector::GetDataMutable<uint32_t>(output)[idx] = v;
        } break;
        default:
          SDB_ASSERT(false, "Unsupported ENUM physical type");
      }
    } break;
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
  auto& child_validity = duckdb::FlatVector::ValidityMutable(child);

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
    } break;
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
    } break;
    default: {
      // Fixed-width types -- dispatch by type to get correct
      // GetDataMutable<T>
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
    } break;
    case duckdb::LogicalTypeId::LIST: {
      // Format written by WriteListSubVector (after optional null_bitmap):
      //   [offsets: elem_count * sizeof(list_entry_t::offset) bytes]
      //   [sizes:   elem_count * sizeof(list_entry_t::offset) bytes]
      //   [child sub-vector for all list elements combined]
      // Note: each offset/size slot is sizeof(list_entry_t::offset) = 8
      // bytes (uint64_t allocation), but the actual value written is a
      // uint32_t in the first 4 bytes of each slot (see
      // WriteListSubVector).
      static constexpr size_t kSlotSize = sizeof(duckdb::list_entry_t::offset);

      auto& list_child = duckdb::ListVector::GetEntry(child);
      auto& list_child_type = duckdb::ListType::GetChildType(child_type);
      auto current_child_size = duckdb::ListVector::GetListSize(child);
      auto* list_entries = duckdb::ListVector::GetData(child);

      // The offsets/sizes arrays each occupy elem_count * kSlotSize bytes
      // total, but the uint32 values are packed consecutively in the first
      // elem_count * sizeof(uint32_t) bytes (the remainder is
      // uninitialized). Offsets block: [ptr .. ptr + elem_count *
      // kSlotSize) Sizes block:   [ptr + elem_count * kSlotSize .. ptr + 2
      // * elem_count
      // * kSlotSize)

      const auto* sizes_start =
        reinterpret_cast<const uint32_t*>(ptr + elem_count * kSlotSize);
      // sizes_start[i] reads at i * sizeof(uint32_t) = consecutive uint32s.
      uint32_t total_child_elems = 0;
      for (uint32_t i = 0; i < elem_count; i++) {
        total_child_elems += sizes_start[i];
      }
      duckdb::ListVector::Reserve(child,
                                  current_child_size + total_child_elems);
      // Re-fetch list_entries after possible resize.
      list_entries = duckdb::ListVector::GetData(child);

      ptr += elem_count * kSlotSize;  // skip entire offsets block

      // Read consecutive uint32_t sizes, then skip the full sizes block.
      const auto* sizes_array = reinterpret_cast<const uint32_t*>(ptr);
      uint32_t running_offset = current_child_size;
      for (uint32_t i = 0; i < elem_count; i++) {
        auto size = sizes_array[i];
        if (elem_nulls && !(elem_nulls[i / 8] & (1 << (i % 8)))) {
          child_validity.SetInvalid(child_offset + i);
          list_entries[child_offset + i] = {0, 0};
        } else {
          list_entries[child_offset + i] = {running_offset, size};
          running_offset += size;
        }
      }
      ptr += elem_count * kSlotSize;  // skip entire sizes block

      // Parse child sub-vector (all elements from all lists combined).
      if (total_child_elems > 0) {
        auto sub_count = irs::vread<uint32_t>(ptr);
        SDB_ASSERT(sub_count == total_child_elems);
        auto sub_flags = static_cast<ValueFlags>(*ptr++);
        bool sub_constant =
          (sub_flags & ValueFlags::Constant) != ValueFlags::None;
        bool sub_have_nulls =
          (sub_flags & ValueFlags::HaveNulls) != ValueFlags::None;
        bool sub_have_length =
          (sub_flags & ValueFlags::HaveLength) != ValueFlags::None;
        uint32_t sub_length_array_size = 0;
        if (sub_have_length) {
          sub_length_array_size = irs::vread<uint32_t>(ptr);
        }
        if (sub_constant) {
          SDB_ASSERT(!sub_have_nulls);
          auto remaining = static_cast<size_t>(end - ptr);
          if (remaining == 0) {
            auto& lc_validity = duckdb::FlatVector::ValidityMutable(list_child);
            for (uint32_t i = 0; i < total_child_elems; i++) {
              lc_validity.SetInvalid(current_child_size + i);
            }
          } else {
            for (uint32_t i = 0; i < total_child_elems; i++) {
              auto val_sv =
                std::string_view{reinterpret_cast<const char*>(ptr), remaining};
              DeserializeValueIntoDuckDB(val_sv, list_child, list_child_type,
                                         current_child_size + i);
            }
          }
        } else {
          DeserializeSubVectorElements(ptr, end, list_child, current_child_size,
                                       total_child_elems, sub_have_nulls,
                                       sub_have_length, sub_length_array_size,
                                       list_child_type);
        }
      }

      duckdb::ListVector::SetListSize(child,
                                      current_child_size + total_child_elems);
    } break;
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
      auto& child_validity = duckdb::FlatVector::ValidityMutable(child);
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

// Deserialize a single STRUCT row from its serialized byte form.
// Wire format (written by WriteStructValue):
//   [varint(length_array_size)]
//   [varint(len_0)] ... [varint(len_{n-2})]   // lengths of all but last
//   child [child_0_bytes] [child_1_bytes] ... [child_{n-1}_bytes]
//
// len_i == 0 means child i is NULL; otherwise child i has len_i bytes of
// data. The last child's length is implicit (all remaining bytes after the
// header).
void DeserializeStructValue(std::string_view value, duckdb::Vector& output,
                            const duckdb::LogicalType& type,
                            duckdb::idx_t idx) {
  auto* ptr = reinterpret_cast<const uint8_t*>(value.data());
  auto* end = ptr + value.size();

  auto& child_types = duckdb::StructType::GetChildTypes(type);
  auto& children = duckdb::StructVector::GetEntries(output);
  const auto num_children = children.size();

  // Read length_array_size header, then the n-1 per-child lengths.
  auto length_array_size = irs::vread<uint32_t>(ptr);
  const uint8_t* data_start = ptr + length_array_size;

  std::vector<uint32_t> lengths;
  lengths.reserve(num_children > 0 ? num_children - 1 : 0);
  for (size_t i = 0; i + 1 < num_children; ++i) {
    lengths.push_back(irs::vread<uint32_t>(ptr));
  }
  // Advance past the length array to the start of child data.
  ptr = data_start;

  for (size_t i = 0; i < num_children; ++i) {
    size_t child_len;
    if (i + 1 < num_children) {
      child_len = lengths[i];
    } else {
      // Last child: consume all remaining bytes.
      child_len = static_cast<size_t>(end - ptr);
    }

    auto& child = children[i];
    auto& child_type = child_types[i].second;

    if (child_len == 0) {
      duckdb::FlatVector::ValidityMutable(child).SetInvalid(idx);
    } else {
      auto child_sv =
        std::string_view{reinterpret_cast<const char*>(ptr), child_len};
      DeserializeValueIntoDuckDB(child_sv, child, child_type, idx);
    }
    ptr += child_len;
  }
}

// Deserialize a single MAP row from its serialized byte form.
// Wire format (written by WriteMapValue):
//   [1 byte: flags=None]
//   [varint(keys_size)]          -- byte count of keys sub-vector
//   [keys sub-vector bytes]      -- standard WriteSubVector format
//   [values sub-vector bytes]    -- standard WriteSubVector format (rest of
//   data)
//
// Empty map special case: [flags=None][0x00][0x00] (three bytes total,
// where the two 0x00s are WriteSubVector's varint(0) for zero-element
// vectors).
void DeserializeMapValue(std::string_view value, duckdb::Vector& output,
                         const duckdb::LogicalType& type, duckdb::idx_t idx) {
  auto* ptr = reinterpret_cast<const uint8_t*>(value.data());
  auto* end = ptr + value.size();

  auto keys_size = irs::vread<uint32_t>(ptr);
  if (!keys_size) {
    // Empty MAP
    duckdb::ListVector::GetData(output)[idx] =
      duckdb::list_entry_t{duckdb::ListVector::GetListSize(output), 0};
    return;
  }
  const uint8_t* keys_start = ptr;
  const uint8_t* keys_end = ptr + keys_size;

  // Peek at elem_count from keys sub-vector to reserve capacity before
  // writing.
  const uint8_t* peek = keys_start;
  auto elem_count = irs::vread<uint32_t>(peek);
  SDB_ASSERT(elem_count > 0);

  auto current_size = duckdb::ListVector::GetListSize(output);
  duckdb::ListVector::Reserve(output, current_size + elem_count);
  duckdb::ListVector::GetData(output)[idx] =
    duckdb::list_entry_t{current_size, elem_count};

  // MAP is LIST(STRUCT(key, value)) in DuckDB.
  auto& struct_child = duckdb::ListVector::GetEntry(output);
  auto& key_vec = duckdb::StructVector::GetEntries(struct_child)[0];
  auto& val_vec = duckdb::StructVector::GetEntries(struct_child)[1];
  auto& key_type = duckdb::MapType::KeyType(type);
  auto& val_type = duckdb::MapType::ValueType(type);

  // Parse sub-vector header at ptr into flags/have_*/length_array_size.
  // Fills `child` starting at child_offset. Advances ptr.
  auto parse_sub_vector = [&](const uint8_t* sub_end, duckdb::Vector& child,
                              duckdb::idx_t child_offset,
                              const duckdb::LogicalType& child_type) {
    // elem_count already consumed via peek; re-consume from actual ptr.
    [[maybe_unused]] auto n = irs::vread<uint32_t>(ptr);
    auto sub_flags = static_cast<ValueFlags>(*ptr++);
    bool is_constant = (sub_flags & ValueFlags::Constant) != ValueFlags::None;
    bool have_nulls = (sub_flags & ValueFlags::HaveNulls) != ValueFlags::None;
    bool have_length = (sub_flags & ValueFlags::HaveLength) != ValueFlags::None;

    uint32_t length_array_size = 0;
    if (have_length) {
      length_array_size = irs::vread<uint32_t>(ptr);
    }
    if (is_constant) {
      SDB_ASSERT(!have_nulls);
      auto remaining = static_cast<size_t>(sub_end - ptr);
      if (remaining == 0) {
        auto& child_validity = duckdb::FlatVector::ValidityMutable(child);
        for (uint32_t i = 0; i < elem_count; i++) {
          child_validity.SetInvalid(child_offset + i);
        }
      } else {
        for (uint32_t i = 0; i < elem_count; i++) {
          auto val_sv =
            std::string_view{reinterpret_cast<const char*>(ptr), remaining};
          DeserializeValueIntoDuckDB(val_sv, child, child_type,
                                     child_offset + i);
        }
      }
    } else {
      DeserializeSubVectorElements(ptr, sub_end, child, child_offset,
                                   elem_count, have_nulls, have_length,
                                   length_array_size, child_type);
    }
  };

  parse_sub_vector(keys_end, key_vec, current_size, key_type);
  ptr = keys_end;
  parse_sub_vector(end, val_vec, current_size, val_type);

  duckdb::ListVector::SetListSize(output, current_size + elem_count);
}

// Deserialize a single ARRAY value from RocksDB bytes into output[idx].
// Format is identical to WriteFlatSubVector<T>: varint(count) + flags +
// [nulls]
// + raw element bytes. The child vector of an ARRAY is flat at
// idx*array_size.
void DeserializeArrayValue(std::string_view value, duckdb::Vector& output,
                           const duckdb::LogicalType& type, duckdb::idx_t idx) {
  auto* ptr = reinterpret_cast<const uint8_t*>(value.data());
  auto* end = ptr + value.size();

  auto array_size = duckdb::ArrayType::GetSize(type);
  auto& child_type = duckdb::ArrayType::GetChildType(type);
  auto& child = duckdb::ArrayVector::GetEntry(output);

  auto elem_count = irs::vread<uint32_t>(ptr);
  if (elem_count == 0) {
    // Zero-element ARRAY: mark child slots invalid
    auto& child_validity = duckdb::FlatVector::ValidityMutable(child);
    for (duckdb::idx_t i = 0; i < array_size; ++i) {
      child_validity.SetInvalid(idx * array_size + i);
    }
    return;
  }

  auto flags = static_cast<ValueFlags>(*ptr++);
  bool have_nulls = (flags & ValueFlags::HaveNulls) != ValueFlags::None;
  bool have_length = (flags & ValueFlags::HaveLength) != ValueFlags::None;

  uint32_t length_array_size = 0;
  if (have_length) {
    length_array_size = irs::vread<uint32_t>(ptr);
  }

  DeserializeSubVectorElements(ptr, end, child, idx * array_size, elem_count,
                               have_nulls, have_length, length_array_size,
                               child_type);
}

}  // namespace sdb::connector
