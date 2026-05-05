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
T GetVectorValue(const duckdb::UnifiedVectorFormat& fmt,
                 duckdb::idx_t row_idx) {
  return duckdb::UnifiedVectorFormat::GetData<T>(
    fmt)[fmt.sel->get_index(row_idx)];
}

}  // namespace

void AppendPKValue(std::string& key, const duckdb::UnifiedVectorFormat& fmt,
                   duckdb::idx_t row_idx, const duckdb::LogicalType& type) {
  const auto idx = fmt.sel->get_index(row_idx);
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN: {
      auto val = duckdb::UnifiedVectorFormat::GetData<bool>(fmt)[idx];
      key.push_back(val ? '\x01' : '\x00');
      break;
    }
    case duckdb::LogicalTypeId::TINYINT: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int8_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int8_t));
      key[base] = static_cast<char>(val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::SMALLINT: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int16_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int16_t));
      absl::big_endian::Store16(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::INTEGER: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int32_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::BIGINT: {
      auto val = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt)[idx];
      auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ: {
      auto val =
        duckdb::UnifiedVectorFormat::GetData<duckdb::timestamp_t>(fmt)[idx]
          .value;
      auto base = key.size();
      basics::StrAppend(key, sizeof(int64_t));
      absl::big_endian::Store64(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
    case duckdb::LogicalTypeId::DATE: {
      auto val =
        duckdb::UnifiedVectorFormat::GetData<duckdb::date_t>(fmt)[idx].days;
      auto base = key.size();
      basics::StrAppend(key, sizeof(int32_t));
      absl::big_endian::Store32(key.data() + base, val);
      key[base] = static_cast<uint8_t>(key[base]) ^ 0x80;
      break;
    }
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
      break;
    }
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
      break;
    }
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
      SDB_ASSERT(false, "Unsupported PK type: ", type.ToString());
  }
}

DuckDBColumnSerializer::DuckDBColumnSerializer(duckdb::Allocator& allocator)
  : _arena{allocator} {}

void DuckDBColumnSerializer::ResetForNewRow() noexcept {
  _row_slices.clear();
  _arena.Reset();
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

// value must be in stable memory -- _row_slices stores &value.
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

void DuckDBColumnSerializer::SstWriter::Write(
  const std::vector<rocksdb::Slice>& slices, std::string_view key) const {
  if (!writer) {
    return;
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

template<typename Writer>
void DuckDBColumnSerializer::WriteRowSlices(
  Writer& writer, std::string_view key,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  writer.Write(_row_slices, key);
  for (auto* iw : index_writers) {
    iw->Write(_row_slices, key);
  }
}

template<typename Writer>
void DuckDBColumnSerializer::WriteConstantColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  if (duckdb::ConstantVector::IsNull(vec)) {
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
  ResetForNewRow();
  auto* const_data = duckdb::ConstantVector::GetData(vec);
  auto type_size = duckdb::GetTypeIdSize(type.InternalType());

  if (type.IsNumeric() || type.id() == duckdb::LogicalTypeId::BOOLEAN ||
      type.id() == duckdb::LogicalTypeId::TIMESTAMP ||
      type.id() == duckdb::LogicalTypeId::TIMESTAMP_TZ ||
      type.id() == duckdb::LogicalTypeId::DATE ||
      type.id() == duckdb::LogicalTypeId::ENUM) {
    _row_slices.emplace_back(reinterpret_cast<const char*>(const_data),
                             type_size);
  } else if (type.id() == duckdb::LogicalTypeId::VARCHAR ||
             type.id() == duckdb::LogicalTypeId::BLOB) {
    auto& str = *reinterpret_cast<const duckdb::string_t*>(const_data);
    WritePrimitive(str);
  } else {
    duckdb::RecursiveUnifiedVectorFormat rdata;
    duckdb::Vector::RecursiveToUnifiedFormat(vec, num_rows, rdata);
    WriteComplexValue(rdata, 0, type);
  }

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

template<typename Writer>
void DuckDBColumnSerializer::WriteUnifiedColumn(
  Writer& writer, const duckdb::RecursiveUnifiedVectorFormat& rdata,
  const duckdb::LogicalType& type, duckdb::idx_t num_rows,
  std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  auto write_null = [&](duckdb::idx_t row) {
    writer.WriteNull(row_keys[row]);
    for (auto* iw : index_writers) {
      iw->Write({}, row_keys[row]);
    }
  };

  auto write_scalar = [&]<typename T>(const T*) {
    auto* data = duckdb::UnifiedVectorFormat::GetData<T>(rdata.unified);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      if (row_keys[row].empty()) {
        continue;
      }
      auto idx = rdata.unified.sel->get_index(row);
      if (!rdata.unified.validity.RowIsValid(idx)) {
        write_null(row);
        continue;
      }
      ResetForNewRow();
      WritePrimitive<T>(data[idx]);
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
    case duckdb::LogicalTypeId::ENUM:
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8:
          write_scalar(static_cast<const uint8_t*>(nullptr));
          return;
        case duckdb::PhysicalType::UINT16:
          write_scalar(static_cast<const uint16_t*>(nullptr));
          return;
        case duckdb::PhysicalType::UINT32:
          write_scalar(static_cast<const uint32_t*>(nullptr));
          return;
        default:
          SDB_ASSERT(false,
                     "Unsupported ENUM physical type in WriteUnifiedColumn");
      }
      return;
    default:
      break;
  }

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    auto idx = rdata.unified.sel->get_index(row);
    if (!rdata.unified.validity.RowIsValid(idx)) {
      write_null(row);
      continue;
    }
    ResetForNewRow();
    WriteComplexValue(rdata, row, type);
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

template<typename Writer>
void DuckDBColumnSerializer::WriteColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  switch (vec.GetVectorType()) {
    case duckdb::VectorType::FLAT_VECTOR:
      break;
    case duckdb::VectorType::CONSTANT_VECTOR:
      WriteConstantColumn(writer, vec, type, num_rows, row_keys, index_writers);
      return;
    default: {
      duckdb::RecursiveUnifiedVectorFormat rdata;
      duckdb::Vector::RecursiveToUnifiedFormat(vec, num_rows, rdata);
      WriteUnifiedColumn(writer, rdata, type, num_rows, row_keys,
                         index_writers);
      return;
    }
  }

  if (type.id() == duckdb::LogicalTypeId::LIST ||
      type.id() == duckdb::LogicalTypeId::MAP ||
      type.id() == duckdb::LogicalTypeId::STRUCT ||
      type.id() == duckdb::LogicalTypeId::ARRAY) {
    WriteComplexColumn(writer, vec, type, num_rows, row_keys, index_writers);
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
      const rocksdb::Slice null_slice;
      for (auto* iw : index_writers) {
        iw->Write({&null_slice, 1}, row_keys[row]);
      }
      continue;
    }
    ResetForNewRow();
    WritePrimitive<T>(raw[row]);
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

template<typename Writer>
void DuckDBColumnSerializer::WriteComplexColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkIndexWriter*> index_writers) {
  duckdb::RecursiveUnifiedVectorFormat rdata;
  duckdb::Vector::RecursiveToUnifiedFormat(vec, num_rows, rdata);

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (row_keys[row].empty()) {
      continue;
    }
    auto idx = rdata.unified.sel->get_index(row);
    if (!rdata.unified.validity.RowIsValid(idx)) {
      writer.WriteNull(row_keys[row]);
      const rocksdb::Slice null_slice;
      for (auto* iw : index_writers) {
        iw->Write({&null_slice, 1}, row_keys[row]);
      }
      continue;
    }
    ResetForNewRow();
    WriteComplexValue(rdata, row, type);
    WriteRowSlices(writer, row_keys[row], index_writers);
  }
}

bool DuckDBColumnSerializer::WriteNullBitmap(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  if (fmt.validity.CannotHaveNull()) {
    return false;
  }
  bool has_nulls = false;
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (!fmt.validity.RowIsValid(fmt.sel->get_index(offset + i))) {
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
  // bit=1 means valid.
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (fmt.validity.RowIsValid(fmt.sel->get_index(offset + i))) {
      bitmap[i / 8] |= static_cast<char>(1 << (i % 8));
    }
  }
  _row_slices.emplace_back(bitmap, null_bytes);
  return true;
}

template<typename T>
void DuckDBColumnSerializer::WriteSubVectorPrimitive(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = duckdb::UnifiedVectorFormat::GetData<T>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteNullBitmap(fmt, offset, count);

  static_assert(basics::IsLittleEndian());
  if (fmt.sel == duckdb::FlatVector::IncrementalSelectionVector()) {
    _row_slices.emplace_back(reinterpret_cast<const char*>(&raw[offset]),
                             count * sizeof(T));
  } else {
    for (duckdb::idx_t i = 0; i < count; i++) {
      auto src_idx = fmt.sel->get_index(offset + i);
      _row_slices.emplace_back(reinterpret_cast<const char*>(&raw[src_idx]),
                               sizeof(T));
    }
  }

  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = have_nulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

template void DuckDBColumnSerializer::WriteSubVectorPrimitive<int8_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<int16_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<int32_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<int64_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<float>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<double>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void
DuckDBColumnSerializer::WriteSubVectorPrimitive<duckdb::timestamp_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<duckdb::date_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void
DuckDBColumnSerializer::WriteSubVectorPrimitive<duckdb::hugeint_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<uint8_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<uint16_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template void DuckDBColumnSerializer::WriteSubVectorPrimitive<uint32_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);

// --- WriteSubVectorBool ---

void DuckDBColumnSerializer::WriteSubVectorBool(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteNullBitmap(fmt, offset, count);

  auto bool_bytes = (count + 7) / 8;
  auto* bitmap = Allocate(bool_bytes);
  std::memset(bitmap, 0, bool_bytes);
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (raw[fmt.sel->get_index(offset + i)]) {
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

void DuckDBColumnSerializer::WriteSubVectorVarchar(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteNullBitmap(fmt, offset, count);

  auto length_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  duckdb::unsafe_arena_vector<uint32_t> lengths{_arena};
  lengths.reserve(count);
  uint32_t length_array_size = 0;

  for (duckdb::idx_t i = 0; i < count; i++) {
    auto src_idx = fmt.sel->get_index(offset + i);
    if (have_nulls && !fmt.validity.RowIsValid(src_idx)) {
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

void DuckDBColumnSerializer::WriteSubVector(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t offset,
  duckdb::idx_t count, const duckdb::LogicalType& type) {
  if (count == 0) {
    _row_slices.emplace_back(kZeroLengthVector);
    return;
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      WriteSubVectorBool(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::TINYINT:
      WriteSubVectorPrimitive<int8_t>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      WriteSubVectorPrimitive<int16_t>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::INTEGER:
      WriteSubVectorPrimitive<int32_t>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::BIGINT:
      WriteSubVectorPrimitive<int64_t>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::FLOAT:
      WriteSubVectorPrimitive<float>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      WriteSubVectorPrimitive<double>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      WriteSubVectorPrimitive<duckdb::timestamp_t>(rdata.unified, offset,
                                                   count);
      break;
    case duckdb::LogicalTypeId::DATE:
      WriteSubVectorPrimitive<duckdb::date_t>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::HUGEINT:
      WriteSubVectorPrimitive<duckdb::hugeint_t>(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::ENUM:
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8:
          WriteSubVectorPrimitive<uint8_t>(rdata.unified, offset, count);
          break;
        case duckdb::PhysicalType::UINT16:
          WriteSubVectorPrimitive<uint16_t>(rdata.unified, offset, count);
          break;
        case duckdb::PhysicalType::UINT32:
          WriteSubVectorPrimitive<uint32_t>(rdata.unified, offset, count);
          break;
        default:
          SDB_ASSERT(false, "Unsupported ENUM physical type in WriteSubVector");
      }
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      WriteSubVectorVarchar(rdata.unified, offset, count);
      break;
    case duckdb::LogicalTypeId::LIST:
      WriteListSubVector(rdata, offset, count, type);
      break;
    case duckdb::LogicalTypeId::MAP:
      // TODO: emit MAP as a true sub-vector instead of one element at a time.
      SDB_ASSERT(count == 1, "MAP sub-vector with count != 1 not supported");
      WriteMapValue(rdata, offset, type);
      break;
    case duckdb::LogicalTypeId::STRUCT:
      SDB_ASSERT(count == 1, "STRUCT sub-vector with count != 1 not supported");
      WriteStructValue(rdata, offset, type);
      break;
    case duckdb::LogicalTypeId::ARRAY:
      SDB_ASSERT(count == 1, "ARRAY sub-vector with count != 1 not supported");
      WriteArrayValue(rdata, offset, type);
      break;
    default:
      SDB_ASSERT(false,
                 "Unsupported sub-vector type for RocksDB serialization: ",
                 type.ToString());
  }
}

void DuckDBColumnSerializer::WriteListValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  auto& child_type = duckdb::ListType::GetChildType(type);
  const auto resolved = rdata.unified.sel->get_index(idx);
  const auto& entry =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
      rdata.unified)[resolved];
  WriteSubVector(rdata.children[0], entry.offset, entry.length, child_type);
}

// Assumes contiguous child layout (FLAT-LIST); asserted below.
void DuckDBColumnSerializer::WriteListSubVector(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t offset,
  duckdb::idx_t count, const duckdb::LogicalType& type) {
  SDB_ASSERT(count > 0);
  auto& fmt = rdata.unified;
  auto* list_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  bool have_nulls = WriteNullBitmap(fmt, offset, count);

  // Offsets and sizes arrays
  auto indexes_size = sizeof(duckdb::list_entry_t::offset) * count;
  auto* offsets_buf = Allocate(indexes_size);
  auto* sizes_buf = Allocate(indexes_size);
  auto* offsets_ptr = reinterpret_cast<uint32_t*>(offsets_buf);
  auto* sizes_ptr = reinterpret_cast<uint32_t*>(sizes_buf);

  uint32_t current_offset = 0;
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto& entry = list_data[fmt.sel->get_index(offset + i)];
    offsets_ptr[i] = current_offset;
    sizes_ptr[i] = static_cast<uint32_t>(entry.length);
    current_offset += sizes_ptr[i];
  }
  _row_slices.emplace_back(offsets_buf, indexes_size);
  _row_slices.emplace_back(sizes_buf, indexes_size);

  // Elements: emit the contiguous range covering all selected list bodies.
  // The child sub-vector encoder handles its own format dispatch via
  // rdata.children[0].unified.
  auto& child_type = duckdb::ListType::GetChildType(type);
  if (current_offset > 0) {
    auto first_offset = list_data[fmt.sel->get_index(offset)].offset;
    WriteSubVector(rdata.children[0], first_offset, current_offset, child_type);
  }

  auto header_size = irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = have_nulls ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
}

// MAP is LIST(STRUCT(key, value)) in DuckDB.
void DuckDBColumnSerializer::WriteMapValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  auto& fmt = rdata.unified;
  const auto resolved = fmt.sel->get_index(idx);
  const auto& entry =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(fmt)[resolved];

  if (entry.length == 0) {
    _row_slices.emplace_back(kZeroLengthVector);
    return;
  }

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  const auto& struct_rdata = rdata.children[0];
  const auto& key_rdata = struct_rdata.children[0];
  const auto& val_rdata = struct_rdata.children[1];

  auto& key_type = duckdb::MapType::KeyType(type);
  auto slices_before_keys = _row_slices.size();
  WriteSubVector(key_rdata, entry.offset, entry.length, key_type);
  uint32_t keys_size = 0;
  for (size_t i = slices_before_keys; i < _row_slices.size(); i++) {
    keys_size += _row_slices[i].size();
  }

  auto& val_type = duckdb::MapType::ValueType(type);
  WriteSubVector(val_rdata, entry.offset, entry.length, val_type);

  // [keys_size_varint] -- no elem_count prefix.
  auto header_size = irs::bytes_io<uint32_t>::vsize(keys_size);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(keys_size, header);
}

template<typename T>
void DuckDBColumnSerializer::WriteScalarField(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t row_idx) {
  WritePrimitive(
    duckdb::UnifiedVectorFormat::GetData<T>(fmt)[fmt.sel->get_index(row_idx)]);
}

static bool IsNestedType(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::ARRAY:
      return true;
    default:
      return false;
  }
}

// Null entries emit an empty slice.
void DuckDBColumnSerializer::WriteScalarValue(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t row_idx,
  const duckdb::LogicalType& type) {
  const auto idx = fmt.sel->get_index(row_idx);
  if (!fmt.validity.RowIsValid(idx)) {
    _row_slices.emplace_back();
    return;
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      // OK to pass a temporary: WritePrimitive<bool> stores a static literal
      // slice.
      WritePrimitive(GetVectorValue<bool>(fmt, row_idx));
      break;
    case duckdb::LogicalTypeId::TINYINT:
      WriteScalarField<int8_t>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::SMALLINT:
      WriteScalarField<int16_t>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::INTEGER:
      WriteScalarField<int32_t>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::BIGINT:
      WriteScalarField<int64_t>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::FLOAT:
      WriteScalarField<float>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::DOUBLE:
      WriteScalarField<double>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      WritePrimitive(
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[idx]);
      break;
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      WriteScalarField<duckdb::timestamp_t>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::DATE:
      WriteScalarField<duckdb::date_t>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::HUGEINT:
      WriteScalarField<duckdb::hugeint_t>(fmt, row_idx);
      break;
    case duckdb::LogicalTypeId::ENUM:
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8:
          WriteScalarField<uint8_t>(fmt, row_idx);
          break;
        case duckdb::PhysicalType::UINT16:
          WriteScalarField<uint16_t>(fmt, row_idx);
          break;
        case duckdb::PhysicalType::UINT32:
          WriteScalarField<uint32_t>(fmt, row_idx);
          break;
        default:
          SDB_ASSERT(false,
                     "Unsupported ENUM physical type in WriteScalarValue");
      }
      break;
    default:
      SDB_ASSERT(false,
                 "Unsupported type in WriteScalarValue: ", type.ToString(),
                 " -- nested types go through WriteComplexValue");
  }
}

void DuckDBColumnSerializer::WriteComplexValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t row_idx,
  const duckdb::LogicalType& type) {
  // Catches inner nulls (e.g. a null STRUCT child of a STRUCT) -- top-level
  // callers already skip the row-level nulls.
  if (!rdata.unified.validity.RowIsValid(
        rdata.unified.sel->get_index(row_idx))) {
    _row_slices.emplace_back();
    return;
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST:
      WriteListValue(rdata, row_idx, type);
      break;
    case duckdb::LogicalTypeId::MAP:
      WriteMapValue(rdata, row_idx, type);
      break;
    case duckdb::LogicalTypeId::STRUCT:
      WriteStructValue(rdata, row_idx, type);
      break;
    case duckdb::LogicalTypeId::ARRAY:
      WriteArrayValue(rdata, row_idx, type);
      break;
    default:
      SDB_ASSERT(false, "Non-nested type passed to WriteComplexValue: ",
                 type.ToString());
  }
}

// Format: [varint length_data_size][lengths...][child0_value][child1_value]...
void DuckDBColumnSerializer::WriteStructValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  const auto resolved = rdata.unified.sel->get_index(idx);
  auto& child_types = duckdb::StructType::GetChildTypes(type);
  auto num_children = rdata.children.size();
  SDB_ASSERT(num_children == child_types.size());

  std::vector<uint32_t> lengths;
  lengths.reserve(num_children);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

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
    const auto& child_rdata = rdata.children[i];
    auto& child_type = child_types[i].second;
    if (IsNestedType(child_type)) {
      WriteComplexValue(child_rdata, resolved, child_type);
    } else {
      WriteScalarValue(child_rdata.unified, resolved, child_type);
    }
    // Skip the last length: implied by total size.
    if (i + 1 < num_children) {
      lengths.push_back(calculate_size());
    }
  }

  uint32_t length_array_size = 0;
  for (auto len : lengths) {
    length_array_size += irs::bytes_io<uint32_t>::vsize(len);
  }

  auto header_size =
    irs::bytes_io<uint32_t>::vsize(length_array_size) + length_array_size;
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(length_array_size, header);
  for (auto len : lengths) {
    irs::WriteVarint(len, header);
  }
}

void DuckDBColumnSerializer::WriteArrayValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  auto& child_type = duckdb::ArrayType::GetChildType(type);
  auto array_size = duckdb::ArrayType::GetSize(type);
  const auto resolved = rdata.unified.sel->get_index(idx);
  WriteSubVector(rdata.children[0], resolved * array_size, array_size,
                 child_type);
}

}  // namespace sdb::connector
