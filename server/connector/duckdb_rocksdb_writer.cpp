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
#include <iresearch/utils/bytes_utils.hpp>
#include <iresearch/utils/numeric_utils.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/common.h"
#include "connector/indexonly_marker.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "query/transaction.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"

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
size_t DuckDBColumnSerializer::WritePrimitive<bool>(const bool& value) {
  _row_slices.emplace_back(value ? kTrueValue : kFalseValue);
  return 1;
}

size_t DuckDBColumnSerializer::WriteGeometryRaw(const duckdb::string_t& value) {
  // Unlike VARCHAR/BLOB this emits no disambiguating prefix byte -- WKB has
  // a fixed layout (byte-order + type + body, minimum 5 bytes), so an empty
  // serialized value is unambiguously NULL (the reader treats empty values
  // as NULL via validity).
  _row_slices.emplace_back(value.GetData(), value.GetSize());
  return value.GetSize();
}

template<>
size_t DuckDBColumnSerializer::WritePrimitive<duckdb::string_t>(
  const duckdb::string_t& value) {
  if (value.GetSize() == 0) {
    _row_slices.emplace_back(kStringPrefix);
    return kStringPrefix.size();
  }
  static_assert(kStringPrefix.size() == 1);
  size_t bytes = 0;
  if (value.GetData()[0] == kStringPrefix.front()) {
    _row_slices.emplace_back(kStringPrefix);
    bytes += kStringPrefix.size();
  }
  _row_slices.emplace_back(value.GetData(), value.GetSize());
  bytes += value.GetSize();
  return bytes;
}

// value must be in stable memory -- _row_slices stores &value.
template<typename T>
size_t DuckDBColumnSerializer::WritePrimitive(const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  static_assert(std::endian::native == std::endian::little);
  _row_slices.emplace_back(reinterpret_cast<const char*>(&value), sizeof(T));
  return sizeof(T);
}

template size_t DuckDBColumnSerializer::WritePrimitive<int8_t>(const int8_t&);
template size_t DuckDBColumnSerializer::WritePrimitive<int16_t>(const int16_t&);
template size_t DuckDBColumnSerializer::WritePrimitive<int32_t>(const int32_t&);
template size_t DuckDBColumnSerializer::WritePrimitive<int64_t>(const int64_t&);
template size_t DuckDBColumnSerializer::WritePrimitive<float>(const float&);
template size_t DuckDBColumnSerializer::WritePrimitive<double>(const double&);
template size_t DuckDBColumnSerializer::WritePrimitive<duckdb::timestamp_t>(
  const duckdb::timestamp_t&);
template size_t DuckDBColumnSerializer::WritePrimitive<duckdb::date_t>(
  const duckdb::date_t&);
template size_t DuckDBColumnSerializer::WritePrimitive<duckdb::hugeint_t>(
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

DuckDBColumnSerializer::TxnWriter::TxnWriter(
  query::Transaction& sdb_txn, rocksdb::ColumnFamilyHandle* cf) noexcept
  : _sdb_txn{&sdb_txn}, _txn{&sdb_txn.GetRocksDBTransaction()}, _cf{cf} {}

void DuckDBColumnSerializer::TxnWriter::Write(
  const std::vector<rocksdb::Slice>& slices, std::string_view key) {
  if (IsIndexOnly()) {
    // IndexOnly columns skip main storage; replay only knows the Default
    // CF for marker blobs, so non-Default CF would silently lose the value.
    SDB_ASSERT(_cf == RocksDBColumnFamilyManager::get(
                        RocksDBColumnFamilyManager::Family::Default));
    indexonly_marker::EmitCP(*_sdb_txn, key, slices);
    return;
  }
  auto merged = MergeSlices(slices);
  auto status = _txn->Put(_cf, rocksdb::Slice(key.data(), key.size()),
                          rocksdb::Slice(merged));
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "RocksDB write failed: ", status.ToString());
  }
}

void DuckDBColumnSerializer::TxnWriter::WriteNull(std::string_view key) {
  if (IsIndexOnly()) {
    // IndexOnly NULL is a marker with empty value bytes; same Default-CF
    // constraint as Write().
    SDB_ASSERT(_cf == RocksDBColumnFamilyManager::get(
                        RocksDBColumnFamilyManager::Family::Default));
    indexonly_marker::EmitCP(*_sdb_txn, key, {});
    return;
  }
  auto status =
    _txn->Put(_cf, rocksdb::Slice(key.data(), key.size()), rocksdb::Slice());
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "RocksDB write failed: ", status.ToString());
  }
}

void DuckDBColumnSerializer::TxnWriter::EmitRowDelete(std::string_view key) {
  // Same Default-CF constraint as Write().
  SDB_ASSERT(_cf == RocksDBColumnFamilyManager::get(
                      RocksDBColumnFamilyManager::Family::Default));
  indexonly_marker::EmitRD(*_sdb_txn, key);
}

void DuckDBColumnSerializer::SstWriter::Write(
  const std::vector<rocksdb::Slice>& slices, std::string_view key) {
  if (!_writer || IsIndexOnly()) {
    // SST has no marker channel; durability of IndexOnly values relies on
    // the inverted index's own commit.
    return;
  }
  auto merged = MergeSlices(slices);
  auto status = _writer->Put(rocksdb::Slice(key.data(), key.size()),
                             rocksdb::Slice(merged));
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "SST write failed: ", status.ToString());
  }
}

void DuckDBColumnSerializer::SstWriter::WriteNull(std::string_view key) {
  if (!_writer || IsIndexOnly()) {
    return;
  }
  auto status =
    _writer->Put(rocksdb::Slice(key.data(), key.size()), rocksdb::Slice());
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "SST write failed: ", status.ToString());
  }
}

template<typename Writer>
void DuckDBColumnSerializer::WriteRowSlices(
  Writer& writer, std::string_view key,
  std::span<DuckDBSinkColumnWriter*> index_writers) {
  // Writer decides what "Write" means for the current column (regular Put,
  // IndexOnly WAL marker, or Option-C silent skip on the SST path).
  writer.Write(_row_slices, key);
  for (auto* iw : index_writers) {
    iw->Write(_row_slices, key);
  }
}

template<typename Writer>
void DuckDBColumnSerializer::WriteConstantColumn(
  Writer& writer, const duckdb::Vector& vec, const duckdb::LogicalType& type,
  duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
  std::span<DuckDBSinkColumnWriter*> index_writers) {
  if (duckdb::ConstantVector::IsNull(vec)) {
    const rocksdb::Slice null_slice;
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      if (row_keys[row].empty()) {
        continue;
      }
      writer.WriteNull(row_keys[row]);
      for (auto* iw : index_writers) {
        iw->Write({&null_slice, 1}, row_keys[row]);
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
  } else if (type.id() == duckdb::LogicalTypeId::GEOMETRY) {
    SDB_ASSERT(type.InternalType() == duckdb::PhysicalType::VARCHAR);
    auto& wkb = *reinterpret_cast<const duckdb::string_t*>(const_data);
    WriteGeometryRaw(wkb);
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
  std::span<DuckDBSinkColumnWriter*> index_writers) {
  auto write_null = [&](duckdb::idx_t row) {
    writer.WriteNull(row_keys[row]);
    const rocksdb::Slice null_slice;
    for (auto* iw : index_writers) {
      iw->Write({&null_slice, 1}, row_keys[row]);
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
    case duckdb::LogicalTypeId::GEOMETRY: {
      SDB_ASSERT(type.InternalType() == duckdb::PhysicalType::VARCHAR);
      auto* data =
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(rdata.unified);
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
        WriteGeometryRaw(data[idx]);
        WriteRowSlices(writer, row_keys[row], index_writers);
      }
      return;
    }
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
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG(
              "Unsupported ENUM physical type in WriteUnifiedColumn: ",
              magic_enum::enum_name(duckdb::EnumType::GetPhysicalType(type))));
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
  Writer& writer, const duckdb::Vector& vec, duckdb::idx_t num_rows,
  std::vector<std::string>& row_keys,
  std::span<DuckDBSinkColumnWriter*> index_writers, ColumnDescriptor col) {
  // Tell the writer which column we're streaming so its Write/WriteNull
  // can branch (regular Put vs IndexOnly WAL marker vs SST silent skip).
  writer.SwitchColumn(col);
  const auto& type = col.type;
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
      type.id() == duckdb::LogicalTypeId::ARRAY ||
      type.id() == duckdb::LogicalTypeId::VARIANT) {
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
    case duckdb::LogicalTypeId::GEOMETRY: {
      SDB_ASSERT(type.InternalType() == duckdb::PhysicalType::VARCHAR);
      auto* raw = duckdb::FlatVector::GetData<duckdb::string_t>(vec);
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
        WriteGeometryRaw(raw[row]);
        WriteRowSlices(writer, row_keys[row], index_writers);
      }
    } break;
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
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          ERR_MSG("Unsupported ENUM physical type: ",
                                  magic_enum::enum_name(
                                    duckdb::EnumType::GetPhysicalType(type))));
      }
      break;
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("Unsupported column type for RocksDB serialization: ",
                magic_enum::enum_name(type.id())));
  }
}

template void
DuckDBColumnSerializer::WriteColumn<DuckDBColumnSerializer::TxnWriter>(
  TxnWriter&, const duckdb::Vector&, duckdb::idx_t, std::vector<std::string>&,
  std::span<DuckDBSinkColumnWriter*>, ColumnDescriptor);
template void
DuckDBColumnSerializer::WriteColumn<DuckDBColumnSerializer::SstWriter>(
  SstWriter&, const duckdb::Vector&, duckdb::idx_t, std::vector<std::string>&,
  std::span<DuckDBSinkColumnWriter*>, ColumnDescriptor);
template void
DuckDBColumnSerializer::WriteColumn<DuckDBColumnSerializer::NoopWriter>(
  NoopWriter&, const duckdb::Vector&, duckdb::idx_t, std::vector<std::string>&,
  std::span<DuckDBSinkColumnWriter*>, ColumnDescriptor);

template<typename Writer, typename T>
void DuckDBColumnSerializer::WriteFlatColumn(
  Writer& writer, const duckdb::Vector& vec, duckdb::idx_t num_rows,
  std::vector<std::string>& row_keys,
  std::span<DuckDBSinkColumnWriter*> index_writers) {
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
  std::span<DuckDBSinkColumnWriter*> index_writers) {
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

// Returns the bitmap size in bytes (0 = no bitmap emitted, all rows valid).
size_t DuckDBColumnSerializer::WriteNullBitmap(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  if (fmt.validity.CannotHaveNull()) {
    return 0;
  }
  bool has_nulls = false;
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (!fmt.validity.RowIsValid(fmt.sel->get_index(offset + i))) {
      has_nulls = true;
      break;
    }
  }
  if (!has_nulls) {
    return 0;
  }
  size_t null_bytes = (count + 7) / 8;
  auto* bitmap = Allocate(null_bytes);
  std::memset(bitmap, 0, null_bytes);
  // bit=1 means valid.
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (fmt.validity.RowIsValid(fmt.sel->get_index(offset + i))) {
      bitmap[i / 8] |= static_cast<char>(1 << (i % 8));
    }
  }
  _row_slices.emplace_back(bitmap, null_bytes);
  return null_bytes;
}

template<typename T>
size_t DuckDBColumnSerializer::WriteSubVectorPrimitive(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = duckdb::UnifiedVectorFormat::GetData<T>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  size_t null_bytes = WriteNullBitmap(fmt, offset, count);

  static_assert(std::endian::native == std::endian::little);
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

  size_t header_size =
    irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = null_bytes != 0 ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
  return header_size + null_bytes + count * sizeof(T);
}

template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<int8_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<int16_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<int32_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<int64_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<float>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<double>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t
DuckDBColumnSerializer::WriteSubVectorPrimitive<duckdb::timestamp_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<duckdb::date_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t
DuckDBColumnSerializer::WriteSubVectorPrimitive<duckdb::hugeint_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<uint8_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<uint16_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);
template size_t DuckDBColumnSerializer::WriteSubVectorPrimitive<uint32_t>(
  const duckdb::UnifiedVectorFormat&, duckdb::idx_t, duckdb::idx_t);

size_t DuckDBColumnSerializer::WriteSubVectorBool(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  size_t null_bytes = WriteNullBitmap(fmt, offset, count);

  size_t bool_bytes = (count + 7) / 8;
  auto* bitmap = Allocate(bool_bytes);
  std::memset(bitmap, 0, bool_bytes);
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (raw[fmt.sel->get_index(offset + i)]) {
      bitmap[i / 8] |= static_cast<char>(1 << (i % 8));
    }
  }
  _row_slices.emplace_back(bitmap, bool_bytes);

  size_t header_size =
    irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = null_bytes != 0 ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
  return header_size + null_bytes + bool_bytes;
}

size_t DuckDBColumnSerializer::WriteSubVectorVarchar(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t offset,
  duckdb::idx_t count) {
  SDB_ASSERT(count > 0);
  auto* raw = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  size_t null_bytes = WriteNullBitmap(fmt, offset, count);
  bool have_nulls = null_bytes != 0;

  auto length_slice_idx = _row_slices.size();
  _row_slices.emplace_back();

  duckdb::unsafe_arena_vector<uint32_t> lengths{_arena};
  lengths.reserve(count);
  size_t length_array_size = 0;
  size_t payload_bytes = 0;

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
    payload_bytes += len;
    _row_slices.emplace_back(str.GetData(), str.GetSize());
  }

  auto* length_buf = Allocate(length_array_size);
  _row_slices[length_slice_idx] = rocksdb::Slice(length_buf, length_array_size);
  for (auto len : lengths) {
    irs::WriteVarint(len, length_buf);
  }

  size_t header_size = irs::bytes_io<uint32_t>::vsize(count) +
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
  return header_size + null_bytes + length_array_size + payload_bytes;
}

size_t DuckDBColumnSerializer::WriteSubVector(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t offset,
  duckdb::idx_t count, const duckdb::LogicalType& type) {
  if (count == 0) {
    _row_slices.emplace_back(kZeroLengthVector);
    return kZeroLengthVector.size();
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return WriteSubVectorBool(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::TINYINT:
      return WriteSubVectorPrimitive<int8_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::SMALLINT:
      return WriteSubVectorPrimitive<int16_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::INTEGER:
      return WriteSubVectorPrimitive<int32_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::BIGINT:
      return WriteSubVectorPrimitive<int64_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::UTINYINT:
      return WriteSubVectorPrimitive<uint8_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::USMALLINT:
      return WriteSubVectorPrimitive<uint16_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::UINTEGER:
      return WriteSubVectorPrimitive<uint32_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::UBIGINT:
      return WriteSubVectorPrimitive<uint64_t>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::FLOAT:
      return WriteSubVectorPrimitive<float>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::DOUBLE:
      return WriteSubVectorPrimitive<double>(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return WriteSubVectorPrimitive<duckdb::timestamp_t>(rdata.unified, offset,
                                                          count);
    case duckdb::LogicalTypeId::DATE:
      return WriteSubVectorPrimitive<duckdb::date_t>(rdata.unified, offset,
                                                     count);
    case duckdb::LogicalTypeId::HUGEINT:
      return WriteSubVectorPrimitive<duckdb::hugeint_t>(rdata.unified, offset,
                                                        count);
    case duckdb::LogicalTypeId::ENUM:
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8:
          return WriteSubVectorPrimitive<uint8_t>(rdata.unified, offset, count);
        case duckdb::PhysicalType::UINT16:
          return WriteSubVectorPrimitive<uint16_t>(rdata.unified, offset,
                                                   count);
        case duckdb::PhysicalType::UINT32:
          return WriteSubVectorPrimitive<uint32_t>(rdata.unified, offset,
                                                   count);
        default:
          SDB_THROW(ERROR_NOT_IMPLEMENTED,
                    "Unsupported ENUM physical type in WriteSubVector");
      }
      return 0;
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
    case duckdb::LogicalTypeId::GEOMETRY:
      // GEOMETRY: same string_t layout; sub-vector format already uses a
      // length array so no prefix disambiguation is needed.
      SDB_ASSERT(type.id() != duckdb::LogicalTypeId::GEOMETRY ||
                 type.InternalType() == duckdb::PhysicalType::VARCHAR);
      return WriteSubVectorVarchar(rdata.unified, offset, count);
    case duckdb::LogicalTypeId::LIST:
      return WriteListSubVector(rdata, offset, count, type);
    case duckdb::LogicalTypeId::MAP: {
      size_t bytes = 0;
      for (duckdb::idx_t i = 0; i < count; ++i) {
        bytes += WriteMapValue(rdata, offset + i, type);
      }
      return bytes;
    }
    case duckdb::LogicalTypeId::VARIANT:
    case duckdb::LogicalTypeId::STRUCT:
      return WriteStructSubVector(rdata, offset, count, type);
    case duckdb::LogicalTypeId::ARRAY: {
      size_t bytes = 0;
      for (duckdb::idx_t i = 0; i < count; ++i) {
        bytes += WriteArrayValue(rdata, offset + i, type);
      }
      return bytes;
    }
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "Unsupported sub-vector type for RocksDB serialization: ",
                type.ToString());
      return 0;
  }
}

size_t DuckDBColumnSerializer::WriteListValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  auto& child_type = duckdb::ListType::GetChildType(type);
  const auto resolved = rdata.unified.sel->get_index(idx);
  const auto& entry =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(
      rdata.unified)[resolved];
  return WriteSubVector(rdata.children[0], entry.offset, entry.length,
                        child_type);
}

// Assumes contiguous child layout (FLAT-LIST); asserted below.
size_t DuckDBColumnSerializer::WriteListSubVector(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t offset,
  duckdb::idx_t count, const duckdb::LogicalType& type) {
  SDB_ASSERT(count > 0);
  auto& fmt = rdata.unified;
  auto* list_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(fmt);

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  size_t null_bytes = WriteNullBitmap(fmt, offset, count);

  // Per-list size array. Offsets are not stored -- the reader recomputes
  // them via running-sum while iterating sizes.
  size_t sizes_bytes = sizeof(uint32_t) * count;
  auto* sizes_buf = Allocate(sizes_bytes);
  auto* sizes_ptr = reinterpret_cast<uint32_t*>(sizes_buf);

  uint32_t total_child_count = 0;
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto& entry = list_data[fmt.sel->get_index(offset + i)];
    sizes_ptr[i] = static_cast<uint32_t>(entry.length);
    total_child_count += sizes_ptr[i];
  }
  _row_slices.emplace_back(sizes_buf, sizes_bytes);

  auto& child_type = duckdb::ListType::GetChildType(type);
  size_t child_bytes = 0;
  if (total_child_count > 0) {
    auto first_offset = list_data[fmt.sel->get_index(offset)].offset;
    child_bytes = WriteSubVector(rdata.children[0], first_offset,
                                 total_child_count, child_type);
  }

  size_t header_size =
    irs::bytes_io<uint32_t>::vsize(count) + sizeof(ValueFlags);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(static_cast<uint32_t>(count), header);
  auto flags = null_bytes != 0 ? ValueFlags::HaveNulls : ValueFlags::None;
  *(header++) = std::bit_cast<char>(flags);
  return header_size + null_bytes + sizes_bytes + child_bytes;
}

// MAP is LIST(STRUCT(key, value)) in DuckDB.
size_t DuckDBColumnSerializer::WriteMapValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  auto& fmt = rdata.unified;
  const auto resolved = fmt.sel->get_index(idx);
  const auto& entry =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(fmt)[resolved];

  if (entry.length == 0) {
    _row_slices.emplace_back(kZeroLengthVector);
    return kZeroLengthVector.size();
  }

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  const auto& struct_rdata = rdata.children[0];
  const auto& key_rdata = struct_rdata.children[0];
  const auto& val_rdata = struct_rdata.children[1];

  auto& key_type = duckdb::MapType::KeyType(type);
  size_t keys_size =
    WriteSubVector(key_rdata, entry.offset, entry.length, key_type);

  auto& val_type = duckdb::MapType::ValueType(type);
  size_t vals_size =
    WriteSubVector(val_rdata, entry.offset, entry.length, val_type);

  // [keys_size_varint] -- no elem_count prefix.
  auto header_size = irs::bytes_io<uint32_t>::vsize(keys_size);
  auto* header = Allocate(header_size);
  _row_slices[header_idx] = rocksdb::Slice(header, header_size);
  irs::WriteVarint(keys_size, header);
  return header_size + keys_size + vals_size;
}

// Mirror of the variable-length sub-vector format used for VARCHAR: header
// (count + flags + length_array_size), optional null bitmap, varint length
// array, then per-element struct bodies (NULL slots contribute 0 bytes).
size_t DuckDBColumnSerializer::WriteStructSubVector(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t offset,
  duckdb::idx_t count, const duckdb::LogicalType& type) {
  SDB_ASSERT(count > 0);
  auto& fmt = rdata.unified;
  const auto num_children = duckdb::StructType::GetChildTypes(type).size();
  _row_slices.reserve(_row_slices.size() + 3 + count * (1 + num_children));

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();  // header placeholder

  size_t null_bytes = WriteNullBitmap(fmt, offset, count);
  bool have_nulls = null_bytes != 0;

  auto length_slice_idx = _row_slices.size();
  _row_slices.emplace_back();  // length array placeholder

  duckdb::unsafe_arena_vector<uint32_t> lengths{_arena};
  lengths.reserve(count);
  size_t length_array_size = 0;
  size_t total_struct_bytes = 0;

  for (duckdb::idx_t i = 0; i < count; i++) {
    size_t struct_size = 0;
    if (!have_nulls ||
        fmt.validity.RowIsValid(fmt.sel->get_index(offset + i))) {
      struct_size = WriteStructValue(rdata, offset + i, type);
    }
    lengths.push_back(struct_size);
    length_array_size += irs::bytes_io<uint32_t>::vsize(struct_size);
    total_struct_bytes += struct_size;
  }

  auto* length_buf = Allocate(length_array_size);
  _row_slices[length_slice_idx] = rocksdb::Slice(length_buf, length_array_size);
  for (auto len : lengths) {
    irs::WriteVarint(len, length_buf);
  }

  size_t header_size = irs::bytes_io<uint32_t>::vsize(count) +
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
  return header_size + null_bytes + length_array_size + total_struct_bytes;
}

template<typename T>
size_t DuckDBColumnSerializer::WriteScalarField(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t row_idx) {
  return WritePrimitive(
    duckdb::UnifiedVectorFormat::GetData<T>(fmt)[fmt.sel->get_index(row_idx)]);
}

static bool IsNestedType(const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::ARRAY:
    case duckdb::LogicalTypeId::VARIANT:
      return true;
    default:
      return false;
  }
}

// Null entries emit an empty slice.
size_t DuckDBColumnSerializer::WriteScalarValue(
  const duckdb::UnifiedVectorFormat& fmt, duckdb::idx_t row_idx,
  const duckdb::LogicalType& type) {
  const auto idx = fmt.sel->get_index(row_idx);
  if (!fmt.validity.RowIsValid(idx)) {
    _row_slices.emplace_back();
    return 0;
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      // OK to pass a temporary: WritePrimitive<bool> stores a static literal
      // slice.
      return WritePrimitive(GetVectorValue<bool>(fmt, row_idx));
    case duckdb::LogicalTypeId::TINYINT:
      return WriteScalarField<int8_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::SMALLINT:
      return WriteScalarField<int16_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::INTEGER:
      return WriteScalarField<int32_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::BIGINT:
      return WriteScalarField<int64_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::UTINYINT:
      return WriteScalarField<uint8_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::USMALLINT:
      return WriteScalarField<uint16_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::UINTEGER:
      return WriteScalarField<uint32_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::UBIGINT:
      return WriteScalarField<uint64_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::FLOAT:
      return WriteScalarField<float>(fmt, row_idx);
    case duckdb::LogicalTypeId::DOUBLE:
      return WriteScalarField<double>(fmt, row_idx);
    case duckdb::LogicalTypeId::VARCHAR:
    case duckdb::LogicalTypeId::BLOB:
      return WritePrimitive(
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[idx]);
    case duckdb::LogicalTypeId::GEOMETRY:
      SDB_ASSERT(type.InternalType() == duckdb::PhysicalType::VARCHAR);
      return WriteGeometryRaw(
        duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[idx]);
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return WriteScalarField<duckdb::timestamp_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::DATE:
      return WriteScalarField<duckdb::date_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::HUGEINT:
      return WriteScalarField<duckdb::hugeint_t>(fmt, row_idx);
    case duckdb::LogicalTypeId::ENUM:
      switch (duckdb::EnumType::GetPhysicalType(type)) {
        case duckdb::PhysicalType::UINT8:
          return WriteScalarField<uint8_t>(fmt, row_idx);
        case duckdb::PhysicalType::UINT16:
          return WriteScalarField<uint16_t>(fmt, row_idx);
        case duckdb::PhysicalType::UINT32:
          return WriteScalarField<uint32_t>(fmt, row_idx);
        default:
          SDB_THROW(ERROR_NOT_IMPLEMENTED,
                    "Unsupported ENUM physical type in WriteScalarValue");
          return 0;
      }
    default:
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "Unsupported type in WriteScalarValue: ", type.ToString(),
                " -- nested types go through WriteComplexValue");
      return 0;
  }
}

size_t DuckDBColumnSerializer::WriteComplexValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t row_idx,
  const duckdb::LogicalType& type) {
  // Catches inner nulls (e.g. a null STRUCT child of a STRUCT) -- top-level
  // callers already skip the row-level nulls.
  if (!rdata.unified.validity.RowIsValid(
        rdata.unified.sel->get_index(row_idx))) {
    _row_slices.emplace_back();
    return 0;
  }
  switch (type.id()) {
    case duckdb::LogicalTypeId::LIST:
      return WriteListValue(rdata, row_idx, type);
    case duckdb::LogicalTypeId::MAP:
      return WriteMapValue(rdata, row_idx, type);
    case duckdb::LogicalTypeId::VARIANT:
    case duckdb::LogicalTypeId::STRUCT:
      return WriteStructValue(rdata, row_idx, type);
    case duckdb::LogicalTypeId::ARRAY:
      return WriteArrayValue(rdata, row_idx, type);
    default:
      SDB_ASSERT(false, "Non-nested type passed to WriteComplexValue: ",
                 type.ToString());
      return 0;
  }
}

// Format: [varint length_data_size][lengths...][child0_value][child1_value]...
size_t DuckDBColumnSerializer::WriteStructValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  const auto resolved = rdata.unified.sel->get_index(idx);
  auto& child_types = duckdb::StructType::GetChildTypes(type);
  auto num_children = rdata.children.size();
  SDB_ASSERT(num_children == child_types.size());

  duckdb::unsafe_arena_vector<uint32_t> lengths{_arena};
  if (num_children > 1) {
    lengths.reserve(num_children - 1);
  }

  auto header_idx = _row_slices.size();
  _row_slices.emplace_back();

  size_t total_children_bytes = 0;
  for (size_t i = 0; i < num_children; i++) {
    const auto& child_rdata = rdata.children[i];
    auto& child_type = child_types[i].second;
    size_t child_bytes =
      IsNestedType(child_type)
        ? WriteComplexValue(child_rdata, resolved, child_type)
        : WriteScalarValue(child_rdata.unified, resolved, child_type);
    total_children_bytes += child_bytes;
    // Skip the last length: implied by total size.
    if (i + 1 < num_children) {
      lengths.push_back(child_bytes);
    }
  }

  size_t length_array_size = 0;
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
  return header_size + total_children_bytes;
}

size_t DuckDBColumnSerializer::WriteArrayValue(
  const duckdb::RecursiveUnifiedVectorFormat& rdata, duckdb::idx_t idx,
  const duckdb::LogicalType& type) {
  auto& child_type = duckdb::ArrayType::GetChildType(type);
  auto array_size = duckdb::ArrayType::GetSize(type);
  const auto resolved = rdata.unified.sel->get_index(idx);
  return WriteSubVector(rdata.children[0], resolved * array_size, array_size,
                        child_type);
}

}  // namespace sdb::connector
