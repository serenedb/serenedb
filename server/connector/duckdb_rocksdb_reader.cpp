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

#include "basics/assert.h"
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
  auto* data = duckdb::FlatVector::GetData<T>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(it, max_rows, [&](duckdb::idx_t idx,
                                         std::string_view value) {
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
  auto* data = duckdb::FlatVector::GetData<bool>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(
    it, max_rows, [&](duckdb::idx_t idx, std::string_view value) {
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
      duckdb::FlatVector::GetData<duckdb::string_t>(output)[idx] =
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
      duckdb::FlatVector::GetData<duckdb::string_t>(output)[idx] =
        duckdb::StringVector::AddStringOrBlob(output, value.data(),
                                              value.size());
    });
}

static duckdb::idx_t ReadTimestampColumn(rocksdb::Iterator& it,
                                         duckdb::Vector& output,
                                         duckdb::idx_t max_rows) {
  auto* data = duckdb::FlatVector::GetData<duckdb::timestamp_t>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(
    it, max_rows, [&](duckdb::idx_t idx, std::string_view value) {
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
  auto* data = duckdb::FlatVector::GetData<duckdb::date_t>(output);
  auto& validity = duckdb::FlatVector::Validity(output);

  return IterateColumn(
    it, max_rows, [&](duckdb::idx_t idx, std::string_view value) {
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
    default:
      // Fallback: read as varchar
      return ReadVarcharColumn(it, output, max_rows);
  }
}

}  // namespace sdb::connector
