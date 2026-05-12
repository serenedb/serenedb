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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/data_pointer.hpp>
#include <string>
#include <vector>

#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;
class DataChunk;

}  // namespace duckdb
namespace irs {

class IndexOutput;

namespace columnstore {

struct FooterColumnEntry;  // defined in format.cpp

// Per-column writer. Accepts duckdb::Vector batches; full row group ->
// compress + emit one DataPointer.
class ColumnWriter final {
 public:
  // skip_validity=true emits zero validity DataPointers; reader treats
  // ValidityGroupCount()==0 as all-valid. PK uses this.
  ColumnWriter(field_id id, std::string name, duckdb::LogicalType type,
               uint64_t row_group_size, duckdb::DatabaseInstance& db,
               IndexOutput& out, FooterColumnEntry& entry,
               bool skip_validity = false);

  ColumnWriter(const ColumnWriter&) = delete;
  ColumnWriter& operator=(const ColumnWriter&) = delete;

  // start_row must be monotonically non-decreasing; gaps become nulls.
  // Validity bits in `vec` are honored.
  void Append(uint64_t start_row, duckdb::Vector& vec, duckdb::idx_t count);

  void AppendChunk(uint64_t start_row, duckdb::DataChunk& chunk,
                   duckdb::idx_t col_idx = 0);

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }
  uint64_t RowGroupSize() const noexcept { return _row_group_size; }
  bool SkipValidity() const noexcept { return _skip_validity; }
  duckdb::CompressionType Compression() const noexcept {
    return _forced_compression;
  }

  // Applies to leaf data only; validity / LIST lengths always run AUTO.
  // Codec/type compatibility is validated at catalog time
  // (ValidateColumnCompression).
  void SetCompression(duckdb::CompressionType compression) noexcept {
    _forced_compression = compression;
  }

  void Finalize();  // Called by Writer::Commit.

 private:
  void FlushRowGroup();

  field_id _id;
  std::string _name;
  duckdb::LogicalType _type;
  uint64_t _row_group_size;
  duckdb::DatabaseInstance* _db;
  IndexOutput* _out;
  FooterColumnEntry* _entry;
  duckdb::Vector _staging;
  uint64_t _filled = 0;
  uint64_t _row_group_first_doc = 0;
  bool _skip_validity = false;
  duckdb::CompressionType _forced_compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
};

}  // namespace columnstore
}  // namespace irs
