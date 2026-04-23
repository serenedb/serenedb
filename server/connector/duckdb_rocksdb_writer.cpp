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
#include <rocksdb/utilities/transaction.h>

#include <cmath>
#include <iresearch/utils/numeric_utils.hpp>

#include "basics/assert.h"
#include "basics/endian.h"
#include "basics/string_utils.h"
#include "connector/common.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kZeroLengthVector{"\0", 1};

template<typename T>
T GetVectorValue(const duckdb::Vector& vec, duckdb::idx_t idx) {
  if (vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
    return duckdb::FlatVector::GetData<T>(vec)[idx];
  }
  if constexpr (std::is_same_v<T, duckdb::string_t>) {
    // string_t not supported by Value::GetValue<>, use StringValue::Get
    auto val = vec.GetValue(idx);
    auto& str = duckdb::StringValue::Get(val);
    return duckdb::string_t{str};
  } else {
    return vec.GetValue(idx).GetValue<T>();
  }
}

}  // namespace

void AppendPKValueFromDuckDB(std::string& key, const duckdb::Vector& vec,
                             duckdb::idx_t idx,
                             const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      auto val = GetVectorValue<bool>(vec, idx);
      key.push_back(val ? '\x01' : '\x00');
      break;
    }
    case duckdb::LogicalTypeId::TINYINT: {
      auto val = GetVectorValue<int8_t>(vec, idx);
      auto base = key.size();
      basics::StrAppend(key, sizeof(int8_t));
      key[base] = static_cast<char>(val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::SMALLINT: {
      auto val = GetVectorValue<int16_t>(vec, idx);
      auto base = key.size();
      basics::StrAppend(key, sizeof(int16_t));
      absl::big_endian::Store16(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::INTEGER: {
      auto val = GetVectorValue<int32_t>(vec, idx);
      auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::BIGINT: {
      auto val = GetVectorValue<int64_t>(vec, idx);
      auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
      // timestamp_t.value is int64 microseconds since epoch
      auto val = GetVectorValue<duckdb::timestamp_t>(vec, idx).value;
      auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::DATE: {
      auto val = GetVectorValue<duckdb::date_t>(vec, idx).days;
      auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::VARCHAR: {
      // String PK: escape null bytes (\0 -> \0\1) and terminate with \0\0
      // Same encoding as primary_key::AppendTypedValue for StringView
      static constexpr std::string_view kNullEsc{"\0\1", 2};
      static constexpr std::string_view kStringEnd{"\0\0", 2};
      auto str = GetVectorValue<duckdb::string_t>(vec, idx);
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
    case duckdb::LogicalTypeId::BLOB: {
      // Same encoding as VARCHAR (escape null bytes)
      static constexpr std::string_view kNullEsc{"\0\1", 2};
      static constexpr std::string_view kStringEnd{"\0\0", 2};
      auto str = GetVectorValue<duckdb::string_t>(vec, idx);
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
      auto val = GetVectorValue<float>(vec, idx);
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
        // NaN -> positive NaN canonical form
        static constexpr char kPosNaN[] = "\xFF\xC0\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(float));
      }
      break;
    }
    case duckdb::LogicalTypeId::DOUBLE: {
      // Double PK: sortable encoding via Dtoi64 + sign bit flip
      auto val = GetVectorValue<double>(vec, idx);
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
        static constexpr char kPosNaN[] = "\xFF\xF8\x00\x00\x00\x00\x00\x00";
        std::memcpy(key.data() + base, kPosNaN, sizeof(double));
      }
      break;
    }
    default:
      SDB_ASSERT(false, "Unsupported PK type");
  }
}

// ---------------------------------------------------------------------------
// DuckDBColumnSerializer -- port of RocksDBDataSinkBase serialization
// ---------------------------------------------------------------------------

DuckDBColumnSerializer::DuckDBColumnSerializer(duckdb::Allocator& allocator)
  : _arena{allocator} {}

void DuckDBColumnSerializer::ResetForNewRow() noexcept {
  _row_slices.clear();
  _arena.Reset();
  _temp_vectors.clear();
}

char* DuckDBColumnSerializer::Allocate(size_t size) {
  return reinterpret_cast<char*>(_arena.Allocate(size));
}

rocksdb::Slice DuckDBColumnSerializer::Finalize(std::string& output) const {
  size_t total = 0;
  for (const auto& s : _row_slices) {
    total += s.size();
  }
  output.clear();
  output.reserve(total);
  for (const auto& s : _row_slices) {
    output.append(s.data(), s.size());
  }
  return {output};
}

// --- WritePrimitive<T> ---

template<>
void DuckDBColumnSerializer::WritePrimitive<bool>(const bool& value) {
  _row_slices.emplace_back(value ? kTrueValue : kFalseValue);
}

template<>
void DuckDBColumnSerializer::WritePrimitive<duckdb::string_t>(
  const duckdb::string_t& value) {
  if (value.GetSize() == 0) {
    _row_slices.emplace_back(kStringPrefix);
    return;
  }
  static_assert(kStringPrefix.size() == 1);
  if (value.GetData()[0] == kStringPrefix.front()) {
    _row_slices.emplace_back(kStringPrefix);
  }
  _row_slices.emplace_back(value.GetData(), value.GetSize());
}

// IMPORTANT: value must be a reference into stable memory (vector data buffer,
// ConstantVector data, or arena). NOT a temporary on the stack.
// Same as old WritePrimitive -- zero-copy, slice points directly at &value.
template<typename T>
void DuckDBColumnSerializer::WritePrimitive(const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(basics::IsLittleEndian());
  _row_slices.emplace_back(reinterpret_cast<const char*>(&value), sizeof(T));
}

template void DuckDBColumnSerializer::WritePrimitive<int8_t>(const int8_t&);
template void DuckDBColumnSerializer::WritePrimitive<int16_t>(const int16_t&);
template void DuckDBColumnSerializer::WritePrimitive<int32_t>(const int32_t&);
template void DuckDBColumnSerializer::WritePrimitive<int64_t>(const int64_t&);
template void DuckDBColumnSerializer::WritePrimitive<float>(const float&);
template void DuckDBColumnSerializer::WritePrimitive<double>(const double&);
template void DuckDBColumnSerializer::WritePrimitive<duckdb::timestamp_t>(
  const duckdb::timestamp_t&);
template void DuckDBColumnSerializer::WritePrimitive<duckdb::date_t>(
  const duckdb::date_t&);
template void DuckDBColumnSerializer::WritePrimitive<duckdb::hugeint_t>(
  const duckdb::hugeint_t&);

// --- TxnWriter ---

namespace {

std::string MergeSlices(const std::vector<rocksdb::Slice>& slices) {
  size_t total = 0;
  for (const auto& s : slices) {
    total += s.size();
  }
  std::string merged;
  merged.reserve(total);
  for (const auto& s : slices) {
    merged.append(s.data(), s.size());
  }
  return merged;
}

}  // namespace

void DuckDBColumnSerializer::TxnWriter::Write(
  const std::vector<rocksdb::Slice>& slices, std::string_view key) const {
  auto merged = MergeSlices(slices);
  auto status = txn->Put(cf, rocksdb::Slice(key.data(), key.size()),
                         rocksdb::Slice(merged));
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "RocksDB write failed: ", status.ToString());
  }
}

void DuckDBColumnSerializer::TxnWriter::WriteNull(std::string_view key) const {
  auto status =
    txn->Put(cf, rocksdb::Slice(key.data(), key.size()), rocksdb::Slice());
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "RocksDB write failed: ", status.ToString());
  }
}

// --- SstWriter ---

void DuckDBColumnSerializer::SstWriter::Write(
  const std::vector<rocksdb::Slice>& slices, std::string_view key) const {
  if (!writer) {
    return;  // noop mode (index-only path)
  }
  auto merged = MergeSlices(slices);
  auto status =
    writer->Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice(merged));
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "SST write failed: ", status.ToString());
  }
}

void DuckDBColumnSerializer::SstWriter::WriteNull(std::string_view key) const {
  if (!writer) {
    return;
  }
  auto status =
    writer->Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice());
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "SST write failed: ", status.ToString());
  }
}

// --- Layer 1: WriteRowSlices ---

template<typename Writer>
void DuckDBColumnSerializer::WriteRowSlices(
  Writer& writer, std::string_view key,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  writer.Write(_row_slices, key);
  for (auto* iw : index_writers) {
    iw->Write(_row_slices, key);
  }
}

// --- Layer 1: WriteConstantColumn ---
// Write same value for all non-skipped rows. No per-row type dispatch.

template<typename Writer>
void DuckDBColumnSerializer::WriteConstantColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  if (duckdb::ConstantVector::IsNull(vec)) {
    // All NULL -- just write null for each row
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      if (row_keys[row].empty()) {
        continue;
      }
      writer.WriteNull(row_keys[row]);
      for (auto* iw : index_writers) {
        iw->Write({}, row_keys[row]);
      }
    }
    return;
  }
  // Serialize once -- use stable pointer from ConstantVector::GetData
  ResetForNewRow();
  auto* const_data = duckdb::ConstantVector::GetData(vec);
  auto type_size = duckdb::GetTypeIdSize(type.InternalType());

  if (type.IsNumeric() || type.id() == duckdb::LogicalTypeId::BOOLEAN ||
      type.id() == duckdb::LogicalTypeId::TIMESTAMP ||
      type.id() == duckdb::LogicalTypeId::TIMESTAMP_TZ ||
      type.id() == duckdb::LogicalTypeId::DATE) {
    // Fixed-width: slice points directly to constant data (stable)
    _row_slices.emplace_back(reinterpret_cast<const char*>(const_data),
                             type_size);
  } else if (type.id() == duckdb::LogicalTypeId::VARCHAR ||
             type.id() == duckdb::LogicalTypeId::BLOB) {
    auto& str = *reinterpret_cast<const duckdb::string_t*>(const_data);
    WritePrimitive(str);
  } else {
    // Complex types (LIST/MAP/STRUCT) -- WriteSingleValue handles them
    // via dispatch to WriteListValue/WriteMapValue/WriteStructValue
    // which point slices into vector child data (stable)
    WriteSingleValue(vec, 0, type);
  }

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

// --- Layer 1: WriteUnifiedColumn ---
// Generic path for DICTIONARY/SEQUENCE/FSST/SHREDDED -- per-row via
// UnifiedVectorFormat.

template<typename Writer>
void DuckDBColumnSerializer::WriteUnifiedColumn(
  Writer& writer, const duckdb::UnifiedVectorFormat& vdata,
  const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  // For scalar types: use vdata.GetData<T>() which points to stable vector
  // data. WritePrimitive with vdata.GetData<T>()[mapped] is safe -- stable
  // pointer. For complex types: WriteSingleValue dispatches to WriteListValue
  // etc. which access child vectors (stable).
  auto write_null = [&](duckdb::idx_t row) {
    writer.WriteNull(row_keys[row]);
    for (auto* iw : index_writers) {
      iw->Write({}, row_keys[row]);
    }
  };

  // Dispatch once by type, then loop rows -- same as old WriteFlatColumn
  // pattern
  auto write_scalar = [&]<typename T>(const T*) {
    auto* data = vdata.GetData<T>();
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      if (row_keys[row].empty()) {
        continue;
      }
      auto idx = vdata.sel->get_index(row);
      if (!vdata.validity.RowIsValid(idx)) {
        write_null(row);
        continue;
      }
      ResetForNewRow();
      WritePrimitive<T>(data[idx]);  // data[idx] is stable vector data
      WriteRowSlices(writer, row_keys[row], index_writers);
    }
  };

  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      write_scalar(static_cast<const bool*>(nullptr));
      return;
    case duckdb::LogicalTypeId::TINYINT:
      write_scalar(static_cast<const int8_t*>(nullptr));
      return;
    case duckdb::LogicalTypeId::SMALLINT:
      write_scalar(static_cast<const int16_t*>(nullptr));
      return;
    case duckdb::LogicalTypeId::INTEGER:
      write_scalar(static_cast<const int32_t*>(nullptr));
      return;
    case duckdb::LogicalTypeId::BIGINT:
      write_scalar(static_cast<const int64_t*>(nullptr));
      return;
    case duckdb::LogicalTypeId::FLOAT:
      write_scalar(static_cast<const float*>(nullptr));
      return;
    case duckdb::LogicalTypeId::DOUBLE:
      write_scalar(static_cast<const double*>(nullptr));
      return;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      write_scalar(static_cast<const duckdb::string_t*>(nullptr));
      return;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      write_scalar(static_cast<const duckdb::timestamp_t*>(nullptr));
      return;
    case duckdb::LogicalTypeId::DATE:
      write_scalar(static_cast<const duckdb::date_t*>(nullptr));
      return;
    case duckdb::LogicalTypeId::HUGEINT:
      write_scalar(static_cast<const duckdb::hugeint_t*>(nullptr));
      return;
    default:
      break;
  }

  // Complex types -- per-row via WriteSingleValue (accesses child vectors,
  // stable)
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    auto idx = vdata.sel->get_index(row);
    if (!vdata.validity.RowIsValid(idx)) {
      write_null(row);
      continue;
    }
    ResetForNewRow();
    WriteSingleValue(vec, idx, type);
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

// --- Layer 1: WriteColumn ---

template<typename Writer>
void DuckDBColumnSerializer::WriteColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  switch (vec.GetVectorType()) {
    case duckdb::VectorType::FLAT_VECTOR:
      break;  // handled below
    case duckdb::VectorType::CONSTANT_VECTOR:
      WriteConstantColumn(writer, vec, type, num_rows, row_keys, index_writers);
      return;
    case duckdb::VectorType::DICTIONARY_VECTOR: {
      duckdb::UnifiedVectorFormat vdata;
      vec.ToUnifiedFormat(num_rows, vdata);
      WriteUnifiedColumn(writer, vdata, vec, type, num_rows, row_keys,
                         index_writers);
      return;
    }
    default: {
      // SEQUENCE, FSST, SHREDDED -- use UnifiedVectorFormat
      duckdb::UnifiedVectorFormat vdata;
      vec.ToUnifiedFormat(num_rows, vdata);
      WriteUnifiedColumn(writer, vdata, vec, type, num_rows, row_keys,
                         index_writers);
      return;
    }
  }

  // FLAT_VECTOR path
  if (type.id() == duckdb::LogicalTypeId::LIST ||
      type.id() == duckdb::LogicalTypeId::MAP ||
      type.id() == duckdb::LogicalTypeId::STRUCT) {
    WriteComplexColumn(writer, vec, type, num_rows, row_keys, index_writers);
    return;
  }

  if (type.id() == duckdb::LogicalTypeId::ARRAY) {
    WriteArrayColumn(writer, vec, type, num_rows, row_keys, index_writers);
    return;
  }

  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      WriteFlatColumn<Writer, bool>(writer, vec, num_rows, row_keys,
                                    index_writers);
      break;
    case duckdb::LogicalTypeId::TINYINT:
      WriteFlatColumn<Writer, int8_t>(writer, vec, num_rows, row_keys,
                                      index_writers);
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      WriteFlatColumn<Writer, int16_t>(writer, vec, num_rows, row_keys,
                                       index_writers);
      break;
    case duckdb::LogicalTypeId::INTEGER:
      WriteFlatColumn<Writer, int32_t>(writer, vec, num_rows, row_keys,
                                       index_writers);
      break;
    case duckdb::LogicalTypeId::BIGINT:
      WriteFlatColumn<Writer, int64_t>(writer, vec, num_rows, row_keys,
                                       index_writers);
      break;
    case duckdb::LogicalTypeId::FLOAT:
      WriteFlatColumn<Writer, float>(writer, vec, num_rows, row_keys,
                                     index_writers);
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      WriteFlatColumn<Writer, double>(writer, vec, num_rows, row_keys,
                                      index_writers);
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      WriteFlatColumn<Writer, duckdb::string_t>(writer, vec, num_rows, row_keys,
                                                index_writers);
      break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      WriteFlatColumn<Writer, duckdb::timestamp_t>(writer, vec, num_rows,
                                                   row_keys, index_writers);
      break;
    case duckdb::LogicalTypeId::DATE:
      WriteFlatColumn<Writer, duckdb::date_t>(writer, vec, num_rows, row_keys,
                                              index_writers);
      break;
    case duckdb::LogicalTypeId::HUGEINT:
      WriteFlatColumn<Writer, duckdb::hugeint_t>(writer, vec, num_rows,
                                                 row_keys, index_writers);
      break;
    case duckdb::LogicalTypeId::ENUM:
      // ENUM stored as ordinals matching the physical type.
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8:
          WriteFlatColumn<Writer, uint8_t>(writer, vec, num_rows, row_keys,
                                           index_writers);
          break;
        case duckdb::PhysicalType::UINT16:
          WriteFlatColumn<Writer, uint16_t>(writer, vec, num_rows, row_keys,
                                            index_writers);
          break;
        case duckdb::PhysicalType::UINT32:
          WriteFlatColumn<Writer, uint32_t>(writer, vec, num_rows, row_keys,
                                            index_writers);
          break;
        default:
          SDB_ASSERT(false, "Unsupported ENUM physical type");
      }
      break;
    default:
      SDB_ASSERT(false, "Unsupported column type for RocksDB serialization");
  }
}

// Explicit instantiations for both writer types
template void
DuckDBColumnSerializer::WriteColumn<DuckDBColumnSerializer::TxnWriter>(
  TxnWriter&, const duckdb::Vector&, const duckdb::LogicalType&, duckdb::idx_t,
  std::vector<std::string>&, std::span<DuckDBSinkIndexWriter*>);
template void
DuckDBColumnSerializer::WriteColumn<DuckDBColumnSerializer::SstWriter>(
  SstWriter&, const duckdb::Vector&, const duckdb::LogicalType&, duckdb::idx_t,
  std::vector<std::string>&, std::span<DuckDBSinkIndexWriter*>);

// --- Layer 1: WriteFlatColumn<Writer, T> ---

template<typename Writer, typename T>
void DuckDBColumnSerializer::WriteFlatColumn(
  Writer& writer, const duckdb::Vector& vec, duckdb::idx_t num_rows,
  std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  auto* raw = duckdb::FlatVector::GetData<T>(vec);
  auto& validity = duckdb::FlatVector::Validity(vec);
  bool may_have_nulls = !validity.CannotHaveNull();

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    if (may_have_nulls && !validity.RowIsValid(row)) {
      writer.WriteNull(row_keys[row]);
      for (auto* iw : index_writers) {
        iw->Write({}, row_keys[row]);
      }
      continue;
    }
    ResetForNewRow();
    WritePrimitive<T>(raw[row]);  // raw[row] is in vector data -- stable
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

// --- Layer 1: WriteComplexColumn ---

template<typename Writer>
void DuckDBColumnSerializer::WriteComplexColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  auto& validity = duckdb::FlatVector::Validity(vec);

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    if (!validity.RowIsValid(row)) {
      writer.WriteNull(row_keys[row]);
      for (auto* iw : index_writers) {
        iw->Write({}, row_keys[row]);
      }
      continue;
    }
    ResetForNewRow();
    switch (type.id()) {
      case duckdb::LogicalTypeId::LIST:
        WriteListValue(vec, row, type);
        break;
      case duckdb::LogicalTypeId::MAP:
        WriteMapValue(vec, row, type);
        break;
      case duckdb::LogicalTypeId::STRUCT:
        WriteStructValue(vec, row, type);
        break;
      case duckdb::LogicalTypeId::ARRAY:
        WriteArrayValue(vec, row, type);
        break;
      default:
        SDB_ASSERT(false);
    }
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

// --- WriteNullBitmap ---

bool DuckDBColumnSerializer::WriteNullBitmap(
  const duckdb::ValidityMask& validity, duckdb::idx_t offset,
  duckdb::idx_t count) {
  if (validity.CannotHaveNull()) {
    return false;
  }
  bool has_nulls = false;
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (!validity.RowIsValid(offset + i)) {
      has_nulls = true;
      break;
    }
  }
  if (!has_nulls) {
    return false;
  }
  auto null_bytes = (count + 7) / 8;
  auto* bitmap = Allocate(null_bytes);
  std::memset(bitmap, 0, null_bytes);
  // bit=1 means valid (same as Velox convention)
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (validity.RowIsValid(offset + i)) {
      bitmap[i / 8] |= static_cast<char>(1 << (i % 8));
    }
  }
  _row_slices.emplace_back(bitmap, null_bytes);
  return true;
}

// --- WriteFlatSubVector<T> (fixed-width) ---

template<typename T>
void DuckDBColumnSerializer::WriteFlatSubVector(const duckdb::Vector& vec,
                                                duckdb::idx_t offset,
                                                duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto& validity = duckdb::FlatVector::Validity(vec);
  auto* raw = duckdb::FlatVector::GetData<T>(vec);

  // Header placeholder
  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  // Nulls
  bool have_nulls = WriteNullBitmap(validity, offset, count);

  // Data: zero-copy bulk slice
  static_assert(basics::IsLittleEndian());
  _row_slices.emplace_back(reinterpret_cast<const char*>(&raw[offset]),
                           count * sizeof(T));

  // Fill header
  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = have_nulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

// Explicit instantiations
template void DuckDBColumnSerializer::WriteFlatSubVector<int8_t>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<int16_t>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<int32_t>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<int64_t>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<float>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<double>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<duckdb::timestamp_t>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<duckdb::date_t>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteFlatSubVector<duckdb::hugeint_t>(
  const duckdb::Vector&, duckdb::idx_t, duckdb::idx_t);

// --- WriteFlatSubVectorBool ---

void DuckDBColumnSerializer::WriteFlatSubVectorBool(const duckdb::Vector& vec,
                                                    duckdb::idx_t offset,
                                                    duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto& validity = duckdb::FlatVector::Validity(vec);
  auto* raw = duckdb::FlatVector::GetData<bool>(vec);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteNullBitmap(validity, offset, count);

  // Pack booleans into bitmap
  auto bool_bytes = (count + 7) / 8;
  auto* bitmap = Allocate(bool_bytes);
  std::memset(bitmap, 0, bool_bytes);
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (raw[offset + i]) {
      bitmap[i / 8] |= static_cast<char>(1 << (i % 8));
    }
  }
  _row_slices.emplace_back(bitmap, bool_bytes);

  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = have_nulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

// --- WriteFlatSubVectorVarchar ---

void DuckDBColumnSerializer::WriteFlatSubVectorVarchar(
  const duckdb::Vector& vec, duckdb::idx_t offset, duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto& validity = duckdb::FlatVector::Validity(vec);
  auto* raw = duckdb::FlatVector::GetData<duckdb::string_t>(vec);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteNullBitmap(validity, offset, count);

  // Length array placeholder
  auto length_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  // String data + collect lengths
  duckdb::unsafe_arena_vector<uint32_t> lengths{_arena};
  lengths.reserve(count);
  uint32_t length_array_size = 0;

  for (duckdb::idx_t i = 0; i < count; i++) {
    if (have_nulls && !validity.RowIsValid(offset + i)) {
      // NULL element: length 0, no data
      lengths.push_back(0);
      length_array_size += irs::bytes_io<uint32_t>::vsize(0);
      _row_slices.emplace_back();  // empty slice
      continue;
    }
    auto& str = raw[offset + i];
    auto len = static_cast<uint32_t>(str.GetSize());
    lengths.push_back(len);
    length_array_size += irs::bytes_io<uint32_t>::vsize(len);
    // Zero-copy: slice points into string_t's data buffer
    _row_slices.emplace_back(str.GetData(), str.GetSize());
  }

  // Write length array
  auto* length_buf = Allocate(length_array_size);
  _row_slices[length_slice_idx] = rocksdb::Slice(length_buf, length_array_size);
  for (auto len : lengths) {
    irs::WriteVarint(len, length_buf);
  }

  // Fill header
  auto header_size = irs::bytes_io<uint32_t>::vsize(count) +
                     sizeof(ValueFlags) +
                     irs::bytes_io<uint32_t>::vsize(length_array_size);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = ValueFlags::HaveLength;
  if (have_nulls) {
    flags |= ValueFlags::HaveNulls;
  }
  *(header++) = std::bit_cast<char>(flags);
  irs::WriteVarint(length_array_size, header);
}

// --- WriteSubVector dispatch ---

void DuckDBColumnSerializer::WriteSubVector(const duckdb::Vector& vec,
                                            duckdb::idx_t offset,
                                            duckdb::idx_t count,
                                            const duckdb::LogicalType& type) {
  if (count == 0) {
    _row_slices.emplace_back(kZeroLengthVector);
    return;
  }

  auto vtype = vec.GetVectorType();

  if (vtype == duckdb::VectorType::CONSTANT_VECTOR) {
    WriteConstantSubVector(vec, count, type);
    return;
  }

  if (vtype == duckdb::VectorType::DICTIONARY_VECTOR) {
    WriteDictionarySubVector(vec, offset, count, type);
    return;
  }

  // FLAT or fallback (SEQUENCE/FSST/SHREDDED -> treat as flat after
  // ToUnifiedFormat)
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      WriteFlatSubVectorBool(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::TINYINT:
      WriteFlatSubVector<int8_t>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      WriteFlatSubVector<int16_t>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::INTEGER:
      WriteFlatSubVector<int32_t>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::BIGINT:
      WriteFlatSubVector<int64_t>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::FLOAT:
      WriteFlatSubVector<float>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      WriteFlatSubVector<double>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      WriteFlatSubVector<duckdb::timestamp_t>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::DATE:
      WriteFlatSubVector<duckdb::date_t>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::HUGEINT:
      WriteFlatSubVector<duckdb::hugeint_t>(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      WriteFlatSubVectorVarchar(vec, offset, count);
      break;
    case duckdb::LogicalTypeId::LIST:
      WriteListSubVector(vec, offset, count, type);
      break;
    case duckdb::LogicalTypeId::MAP:
      WriteMapValue(vec, offset, type);
      break;
    case duckdb::LogicalTypeId::STRUCT:
      WriteStructValue(vec, offset, type);
      break;
    case duckdb::LogicalTypeId::ARRAY:
      WriteArrayValue(vec, offset, type);
      break;
    default:
      SDB_ASSERT(false,
                 "Unsupported sub-vector type for RocksDB serialization");
  }
}

// --- WriteListValue (single array at idx) ---

void DuckDBColumnSerializer::WriteListValue(const duckdb::Vector& vec,
                                            duckdb::idx_t idx,
                                            const duckdb::LogicalType& type) {
  auto& child_type = duckdb::ListType::GetChildType(type);
  switch (vec.GetVectorType()) {
    case duckdb::VectorType::FLAT_VECTOR: {
      auto& entry = duckdb::FlatVector::GetData<duckdb::list_entry_t>(vec)[idx];
      WriteSubVector(duckdb::ListVector::GetEntry(vec), entry.offset,
                     entry.length, child_type);
      return;
    }
    case duckdb::VectorType::CONSTANT_VECTOR: {
      auto& entry = *reinterpret_cast<const duckdb::list_entry_t*>(
        duckdb::ConstantVector::GetData(vec));
      WriteSubVector(duckdb::ListVector::GetEntry(vec), entry.offset,
                     entry.length, child_type);
      return;
    }
    case duckdb::VectorType::DICTIONARY_VECTOR: {
      WriteListValue(duckdb::DictionaryVector::Child(vec),
                     duckdb::DictionaryVector::SelVector(vec).get_index(idx),
                     type);
      return;
    }
    default: {
      duckdb::UnifiedVectorFormat vdata;
      vec.ToUnifiedFormat(idx + 1, vdata);
      auto& entry =
        vdata.GetData<duckdb::list_entry_t>()[vdata.sel->get_index(idx)];
      WriteSubVector(duckdb::ListVector::GetEntry(vec), entry.offset,
                     entry.length, child_type);
      return;
    }
  }
}

// --- WriteListSubVector (batch LIST elements) ---

void DuckDBColumnSerializer::WriteListSubVector(
  const duckdb::Vector& vec, duckdb::idx_t offset, duckdb::idx_t count,
  const duckdb::LogicalType& type) {
  SDB_ASSERT(count > 0);
  auto& validity = duckdb::FlatVector::Validity(vec);
  auto* list_data = duckdb::FlatVector::GetData<duckdb::list_entry_t>(vec);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteNullBitmap(validity, offset, count);

  // Offsets and sizes arrays
  auto indexes_size = sizeof(duckdb::list_entry_t::offset) * count;
  auto* offsets_buf = Allocate(indexes_size);
  auto* sizes_buf = Allocate(indexes_size);
  auto* offsets_ptr = reinterpret_cast<uint32_t*>(offsets_buf);
  auto* sizes_ptr = reinterpret_cast<uint32_t*>(sizes_buf);

  uint32_t current_offset = 0;
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto& entry = list_data[offset + i];
    offsets_ptr[i] = current_offset;
    sizes_ptr[i] = static_cast<uint32_t>(entry.length);
    current_offset += sizes_ptr[i];
  }
  _row_slices.emplace_back(offsets_buf, indexes_size);
  _row_slices.emplace_back(sizes_buf, indexes_size);

  // Elements
  auto& child = duckdb::ListVector::GetEntry(vec);
  auto& child_type = duckdb::ListType::GetChildType(type);
  if (current_offset > 0) {
    auto first_offset = list_data[offset].offset;
    WriteSubVector(child, first_offset, current_offset, child_type);
  }

  // Fill header
  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = have_nulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

// --- WriteMapValue (single MAP at idx) ---

void DuckDBColumnSerializer::WriteMapValue(const duckdb::Vector& vec,
                                           duckdb::idx_t idx,
                                           const duckdb::LogicalType& type) {
  // MAP is stored as LIST(STRUCT(key, value)) in DuckDB
  // Handle vector type for entry access
  if (vec.GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR) {
    auto& child_vec = duckdb::DictionaryVector::Child(vec);
    auto& sel = duckdb::DictionaryVector::SelVector(vec);
    WriteMapValue(child_vec, sel.get_index(idx), type);
    return;
  }
  const duckdb::list_entry_t* entry_ptr;
  if (vec.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    entry_ptr = reinterpret_cast<const duckdb::list_entry_t*>(
      duckdb::ConstantVector::GetData(vec));
  } else {
    entry_ptr = &duckdb::FlatVector::GetData<duckdb::list_entry_t>(vec)[idx];
  }
  auto& entry = *entry_ptr;
  auto& child = duckdb::ListVector::GetEntry(vec);  // STRUCT vector
  auto elem_count = entry.length;
  auto elem_offset = entry.offset;

  if (elem_count == 0) {
    _row_slices.emplace_back(kZeroLengthVector);
    return;
  }

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();  // header placeholder

  // Keys sub-vector
  auto& key_vec = duckdb::StructVector::GetEntries(child)[0];
  auto& key_type = duckdb::MapType::KeyType(type);
  auto slices_before_keys = _row_slices.size();
  WriteSubVector(key_vec, elem_offset, elem_count, key_type);
  uint32_t keys_size = 0;
  for (size_t i = slices_before_keys; i < _row_slices.size(); i++) {
    keys_size += _row_slices[i].size();
  }

  // Values sub-vector
  auto& val_vec = duckdb::StructVector::GetEntries(child)[1];
  auto& val_type = duckdb::MapType::ValueType(type);
  WriteSubVector(val_vec, elem_offset, elem_count, val_type);

  // Fill header: [keys_size_varint] -- NO elem_count prefix
  auto header_size = irs::bytes_io<uint32_t>::vsize(keys_size);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(keys_size, header);
}

// --- WriteScalarField ---

template<typename T>
void DuckDBColumnSerializer::WriteScalarField(const duckdb::Vector& vec,
                                              duckdb::idx_t idx) {
  if (vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
    // FlatVector::GetData<T> returns const T* into the vector's heap-allocated
    // StandardVectorBuffer -- stable for the lifetime of the vector.
    WritePrimitive(duckdb::FlatVector::GetData<T>(vec)[idx]);
  } else {
    // GetVectorValue<T> returns T by value (temporary). Storing &temp in
    // _row_slices would dangle once WriteSingleValue returns.
    // Copy to arena instead -- one extra scalar copy, negligible for per-field
    // use.
    T val = GetVectorValue<T>(vec, idx);
    auto* p = Allocate(sizeof(T));
    std::memcpy(p, &val, sizeof(T));
    _row_slices.emplace_back(p, sizeof(T));
  }
}

// --- WriteSingleValue (one value without sub-vector header) ---
// Port of WriteValue (data_sink.cpp:2000).
// For struct children and map keys/values.

void DuckDBColumnSerializer::WriteSingleValue(const duckdb::Vector& vec,
                                              duckdb::idx_t idx,
                                              const duckdb::LogicalType& type) {
  // Check null -- handle both flat and non-flat vectors
  if (vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
    if (!duckdb::FlatVector::Validity(vec).RowIsValid(idx)) {
      _row_slices.emplace_back();
      return;
    }
  } else {
    auto val = vec.GetValue(idx);
    if (val.IsNull()) {
      _row_slices.emplace_back();
      return;
    }
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      // WritePrimitive<bool> reads the value and stores a static literal --
      // it never takes &value, so a temporary is safe here.
      WritePrimitive(GetVectorValue<bool>(vec, idx));
      break;
    case duckdb::LogicalTypeId::TINYINT:
      WriteScalarField<int8_t>(vec, idx);
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      WriteScalarField<int16_t>(vec, idx);
      break;
    case duckdb::LogicalTypeId::INTEGER:
      WriteScalarField<int32_t>(vec, idx);
      break;
    case duckdb::LogicalTypeId::BIGINT:
      WriteScalarField<int64_t>(vec, idx);
      break;
    case duckdb::LogicalTypeId::FLOAT:
      WriteScalarField<float>(vec, idx);
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      WriteScalarField<double>(vec, idx);
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB: {
      if (vec.GetVectorType() == duckdb::VectorType::FLAT_VECTOR) {
        WritePrimitive(duckdb::FlatVector::GetData<duckdb::string_t>(vec)[idx]);
      } else {
        // Non-flat: copy string to arena, add slices directly
        auto val = vec.GetValue(idx);
        auto& str = duckdb::StringValue::Get(val);
        if (str.empty()) {
          _row_slices.emplace_back(kStringPrefix);
        } else {
          if (str[0] == kStringPrefix.front()) {
            _row_slices.emplace_back(kStringPrefix);
          }
          auto* buf = Allocate(str.size());
          std::memcpy(buf, str.data(), str.size());
          _row_slices.emplace_back(buf, str.size());
        }
      }
      break;
    }
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      WriteScalarField<duckdb::timestamp_t>(vec, idx);
      break;
    case duckdb::LogicalTypeId::DATE:
      WriteScalarField<duckdb::date_t>(vec, idx);
      break;
    case duckdb::LogicalTypeId::HUGEINT:
      WriteScalarField<duckdb::hugeint_t>(vec, idx);
      break;
    case duckdb::LogicalTypeId::LIST:
      WriteListValue(vec, idx, type);
      break;
    case duckdb::LogicalTypeId::MAP:
      WriteMapValue(vec, idx, type);
      break;
    case duckdb::LogicalTypeId::STRUCT:
      WriteStructValue(vec, idx, type);
      break;
    case duckdb::LogicalTypeId::ARRAY:
      WriteArrayValue(vec, idx, type);
      break;
    default:
      SDB_ASSERT(false, "Unsupported type in WriteSingleValue");
  }
}

// --- WriteStructValue (single STRUCT at idx) ---
// Port of WriteRowValue (data_sink.cpp:2107).
// Format: [varint length_data_size][lengths...][child0_value][child1_value]...

void DuckDBColumnSerializer::WriteStructValue(const duckdb::Vector& vec,
                                              duckdb::idx_t idx,
                                              const duckdb::LogicalType& type) {
  if (vec.GetVectorType() == duckdb::VectorType::DICTIONARY_VECTOR) {
    auto& child_vec = duckdb::DictionaryVector::Child(vec);
    auto& sel = duckdb::DictionaryVector::SelVector(vec);
    WriteStructValue(child_vec, sel.get_index(idx), type);
    return;
  }
  auto& children = duckdb::StructVector::GetEntries(vec);
  auto& child_types = duckdb::StructType::GetChildTypes(type);
  auto num_children = children.size();

  std::vector<uint32_t> lengths;
  lengths.reserve(num_children);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();  // length array size placeholder

  auto slices_before = _row_slices.size();
  auto calculate_size = [&]() -> uint32_t {
    uint32_t size = 0;
    for (size_t j = slices_before; j < _row_slices.size(); j++) {
      size += _row_slices[j].size();
    }
    slices_before = _row_slices.size();
    return size;
  };

  for (size_t i = 0; i < num_children; i++) {
    auto& child = children[i];
    auto& child_type = child_types[i].second;
    WriteSingleValue(child, idx, child_type);
    // Don't write last length -- implicit from total size
    if (i + 1 < num_children) {
      lengths.push_back(calculate_size());
    }
  }

  // Compute length_array_size
  uint32_t length_array_size = 0;
  for (auto len : lengths) {
    length_array_size += irs::bytes_io<uint32_t>::vsize(len);
  }

  // Fill header: [varint(length_array_size)][varint lengths...]
  auto header_size =
    irs::bytes_io<uint32_t>::vsize(length_array_size) + length_array_size;
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(length_array_size, header);
  for (auto len : lengths) {
    irs::WriteVarint(len, header);
  }
}

// --- WriteArrayValue (single ARRAY at idx) ---

void DuckDBColumnSerializer::WriteArrayValue(const duckdb::Vector& vec,
                                             duckdb::idx_t idx,
                                             const duckdb::LogicalType& type) {
  auto& child_type = duckdb::ArrayType::GetChildType(type);
  auto array_size = duckdb::ArrayType::GetSize(type);
  switch (vec.GetVectorType()) {
    case duckdb::VectorType::FLAT_VECTOR: {
      const auto& child_vec = duckdb::ArrayVector::GetEntry(vec);
      WriteSubVector(child_vec, idx * array_size, array_size, child_type);
      return;
    }
    case duckdb::VectorType::CONSTANT_VECTOR: {
      // All rows are the same constant -- child data is at offset 0.
      const auto& child_vec = duckdb::ArrayVector::GetEntry(vec);
      WriteSubVector(child_vec, 0, array_size, child_type);
      return;
    }
    case duckdb::VectorType::DICTIONARY_VECTOR: {
      WriteArrayValue(duckdb::DictionaryVector::Child(vec),
                      duckdb::DictionaryVector::SelVector(vec).get_index(idx),
                      type);
      return;
    }
    default: {
      duckdb::UnifiedVectorFormat vdata;
      vec.ToUnifiedFormat(idx + 1, vdata);
      auto mapped_idx = vdata.sel->get_index(idx);
      const auto& child_vec = duckdb::ArrayVector::GetEntry(vec);
      WriteSubVector(child_vec, mapped_idx * array_size, array_size,
                     child_type);
      return;
    }
  }
}

// --- WriteArrayColumn (Layer 1, FLAT_VECTOR path for ARRAY columns) ---

template<typename Writer>
void DuckDBColumnSerializer::WriteArrayColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  auto& validity = duckdb::FlatVector::Validity(vec);

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    if (!validity.RowIsValid(row)) {
      writer.WriteNull(row_keys[row]);
      for (auto* iw : index_writers) {
        iw->Write({}, row_keys[row]);
      }
      continue;
    }
    ResetForNewRow();
    WriteArrayValue(vec, row, type);
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

// --- WriteConstantSubVector ---

void DuckDBColumnSerializer::WriteConstantSubVector(
  const duckdb::Vector& vec, duckdb::idx_t count,
  const duckdb::LogicalType& type) {
  SDB_ASSERT(count > 0);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  if (duckdb::ConstantVector::IsNull(vec)) {
    // All NULL constant
    auto header_size =
      irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
    auto* header = Allocate(header_size);
    _row_slices[header_idx] = rocksdb::Slice(header, header_size);
    irs::WriteVarint(static_cast<uint32_t>(count), header);
    *(header++) = std::bit_cast<char>(ValueFlags::Constant);
    // No data after header = all NULLs
    return;
  }

  // Non-null constant: write single value
  auto val = duckdb::ConstantVector::GetData(vec);
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      WritePrimitive(*reinterpret_cast<const bool*>(val));
      break;
    case duckdb::LogicalTypeId::TINYINT:
      WritePrimitive(*reinterpret_cast<const int8_t*>(val));
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      WritePrimitive(*reinterpret_cast<const int16_t*>(val));
      break;
    case duckdb::LogicalTypeId::INTEGER:
      WritePrimitive(*reinterpret_cast<const int32_t*>(val));
      break;
    case duckdb::LogicalTypeId::BIGINT:
      WritePrimitive(*reinterpret_cast<const int64_t*>(val));
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      WritePrimitive(*reinterpret_cast<const duckdb::string_t*>(val));
      break;
    default:
      // For complex constant, serialize as single element then replicate
      SDB_ASSERT(false,
                 "Constant sub-vector of complex type not yet supported");
  }

  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  *(header++) = std::bit_cast<char>(ValueFlags::Constant);
}

// --- WriteDictNullBitmap ---

bool DuckDBColumnSerializer::WriteDictNullBitmap(
  const duckdb::UnifiedVectorFormat& vdata, duckdb::idx_t offset,
  duckdb::idx_t count) {
  if (vdata.validity.CannotHaveNull()) {
    return false;
  }
  bool has_nulls = false;
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (!vdata.validity.RowIsValid(vdata.sel->get_index(offset + i))) {
      has_nulls = true;
      break;
    }
  }
  if (!has_nulls) {
    return false;
  }
  auto null_bytes = (count + 7) / 8;
  auto* bitmap = Allocate(null_bytes);
  std::memset(bitmap, 0, null_bytes);
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (vdata.validity.RowIsValid(vdata.sel->get_index(offset + i))) {
      bitmap[i / 8] |= static_cast<char>(1 << (i % 8));
    }
  }
  _row_slices.emplace_back(bitmap, null_bytes);
  return true;
}

// --- WriteDictionaryFixedSubVector<T> (fixed-width) ---

template<typename T>
void DuckDBColumnSerializer::WriteDictionaryFixedSubVector(
  const duckdb::UnifiedVectorFormat& vdata, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* data = vdata.GetData<T>();

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteDictNullBitmap(vdata, offset, count);

  // Emit one slice per element pointing into the dictionary's child buffer.
  // For null rows, sel->get_index still returns a valid child position, so
  // dereferencing &data[src_idx] is safe; the null bitmap is authoritative.
  static_assert(basics::IsLittleEndian());
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = vdata.sel->get_index(offset + i);
    _row_slices.emplace_back(reinterpret_cast<const char*>(&data[src_idx]),
                             sizeof(T));
  }

  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = have_nulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

// --- WriteDictionarySubVectorBool ---

void DuckDBColumnSerializer::WriteDictionarySubVectorBool(
  const duckdb::UnifiedVectorFormat& vdata, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = vdata.GetData<bool>();

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteDictNullBitmap(vdata, offset, count);

  auto bool_bytes = (count + 7) / 8;
  auto* bitmap = Allocate(bool_bytes);
  std::memset(bitmap, 0, bool_bytes);
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = vdata.sel->get_index(offset + i);
    if (raw[src_idx]) {
      bitmap[i / 8] |= static_cast<char>(1 << (i % 8));
    }
  }
  _row_slices.emplace_back(bitmap, bool_bytes);

  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = have_nulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

// --- WriteDictionarySubVectorVarchar ---

void DuckDBColumnSerializer::WriteDictionarySubVectorVarchar(
  const duckdb::UnifiedVectorFormat& vdata, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = vdata.GetData<duckdb::string_t>();

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteDictNullBitmap(vdata, offset, count);

  auto length_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  duckdb::unsafe_arena_vector<uint32_t> lengths{_arena};
  lengths.reserve(count);
  uint32_t length_array_size = 0;

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = vdata.sel->get_index(offset + i);
    if (have_nulls && !vdata.validity.RowIsValid(src_idx)) {
      lengths.push_back(0);
      length_array_size += irs::bytes_io<uint32_t>::vsize(0);
      _row_slices.emplace_back();
      continue;
    }
    auto& str = raw[src_idx];
    auto len = static_cast<uint32_t>(str.GetSize());
    lengths.push_back(len);
    length_array_size += irs::bytes_io<uint32_t>::vsize(len);
    _row_slices.emplace_back(str.GetData(), str.GetSize());
  }

  auto* length_buf = Allocate(length_array_size);
  _row_slices[length_slice_idx] = rocksdb::Slice(length_buf, length_array_size);
  for (auto len : lengths) {
    irs::WriteVarint(len, length_buf);
  }

  auto header_size = irs::bytes_io<uint32_t>::vsize(count) +
                     sizeof(ValueFlags) +
                     irs::bytes_io<uint32_t>::vsize(length_array_size);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = ValueFlags::HaveLength;
  if (have_nulls) {
    flags |= ValueFlags::HaveNulls;
  }
  *(header++) = std::bit_cast<char>(flags);
  irs::WriteVarint(length_array_size, header);
}

// --- WriteDictionaryComplexSubVector (materialize-to-flat fallback) ---

void DuckDBColumnSerializer::WriteDictionaryComplexSubVector(
  const duckdb::Vector& vec, duckdb::idx_t offset, duckdb::idx_t count,
  const duckdb::LogicalType& type) {
  // Build a flat vector from the resolved data.
  // IMPORTANT: WriteFlatSubVector<T> stores zero-copy rocksdb::Slices that
  // point directly into the Vector's StandardVectorBuffer (heap memory whose
  // address is stable across Vector moves). The slices are consumed by
  // WriteRowSlices *after* this function returns, so the buffer must stay
  // alive. We move `flat` into _temp_vectors which is cleared by
  // ResetForNewRow() only after WriteRowSlices has finished.
  duckdb::UnifiedVectorFormat vdata;
  vec.ToUnifiedFormat(offset + count, vdata);

  _temp_vectors.emplace_back(type, count);
  duckdb::Vector& flat = _temp_vectors.back();
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = vdata.sel->get_index(offset + i);
    if (!vdata.validity.RowIsValid(src_idx)) {
      duckdb::FlatVector::ValidityMutable(flat).SetInvalid(i);
    } else {
      flat.SetValue(i, vec.GetValue(offset + i));
    }
  }
  WriteSubVector(flat, 0, count, type);
}

// --- WriteDictionarySubVector ---

void DuckDBColumnSerializer::WriteDictionarySubVector(
  const duckdb::Vector& vec, duckdb::idx_t offset, duckdb::idx_t count,
  const duckdb::LogicalType& type) {
  // Dict over a constant child: every sel index resolves to the same single
  // value, so encode as a constant sub-vector directly (no per-row copies).
  auto& dict_child = duckdb::DictionaryVector::Child(vec);
  if (dict_child.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    WriteConstantSubVector(dict_child, count, type);
    return;
  }

  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::ARRAY:
      // Complex sub-vectors require a contiguous child region that the flat
      // path's single-child-blob layout assumes. A scattered dict has no such
      // region, so materialize to flat and recurse.
      WriteDictionaryComplexSubVector(vec, offset, count, type);
      return;
    default:
      break;
  }

  // Primitive fast paths: zero-copy per-element slices into the dict child
  // buffer. ToUnifiedFormat also collapses dict-of-dict chains.
  duckdb::UnifiedVectorFormat vdata;
  vec.ToUnifiedFormat(offset + count, vdata);

  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      WriteDictionarySubVectorBool(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::TINYINT:
      WriteDictionaryFixedSubVector<int8_t>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::SMALLINT:
      WriteDictionaryFixedSubVector<int16_t>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::INTEGER:
      WriteDictionaryFixedSubVector<int32_t>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::BIGINT:
      WriteDictionaryFixedSubVector<int64_t>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::FLOAT:
      WriteDictionaryFixedSubVector<float>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::DOUBLE:
      WriteDictionaryFixedSubVector<double>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      WriteDictionaryFixedSubVector<duckdb::timestamp_t>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::DATE:
      WriteDictionaryFixedSubVector<duckdb::date_t>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::HUGEINT:
      WriteDictionaryFixedSubVector<duckdb::hugeint_t>(vdata, offset, count);
      return;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      WriteDictionarySubVectorVarchar(vdata, offset, count);
      return;
    default:
      SDB_ASSERT(false,
                 "Unsupported dictionary sub-vector type for RocksDB "
                 "serialization");
  }
}

}  // namespace sdb::connector
