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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/data_pointer.hpp>

#include "iresearch/types.hpp"

namespace duckdb {

class DataChunk;
class HyperLogLog;

}  // namespace duckdb
namespace irs {

class WriteContext;
struct FooterColumnEntry;

class ColumnWriter final {
 public:
  ColumnWriter(field_id id, duckdb::LogicalType type, uint32_t row_group_size,
               WriteContext& write_ctx, FooterColumnEntry& entry,
               bool skip_validity, bool hyperloglog);

  ColumnWriter(const ColumnWriter&) = delete;
  ColumnWriter& operator=(const ColumnWriter&) = delete;

  void Append(uint64_t start_row, const duckdb::Vector& vec,
              duckdb::idx_t count);

  void Append(uint64_t start_row, const duckdb::Vector& vec,
              const duckdb::SelectionVector& sel, duckdb::idx_t count);

  template<typename Fill>
  void PushInStaging(uint64_t row, Fill&& fill) {
    PadNullsTo(row);
    fill(_staging, _filled);
    ++_filled;
    if (_filled == _row_group_size) {
      FlushRowGroup();
    }
  }

  field_id Id() const noexcept { return _id; }
  const duckdb::LogicalType& Type() const noexcept { return _type; }
  uint32_t RowGroupSize() const noexcept { return _row_group_size; }
  bool SkipValidity() const noexcept { return _skip_validity; }
  bool HasHyperLogLog() const noexcept;
  duckdb::CompressionType Compression() const noexcept {
    return _forced_compression;
  }

  void SetCompression(duckdb::CompressionType compression) noexcept {
    _forced_compression = compression;
  }

  void Finalize();

  void SetHyperLogLog(duckdb::shared_ptr<duckdb::HyperLogLog> hll);

  // Pad `_filled` up to `target_row` with null entries. Used by merge to
  // span the doc-id range of a source that has no row in this column
  // (so doc-id indexed reads stay aligned across sources).
  void PadNullsTo(uint64_t target_row);

 private:
  void FlushRowGroup();

  field_id _id;
  duckdb::LogicalType _type;
  uint32_t _row_group_size;
  WriteContext* _write_ctx;
  FooterColumnEntry* _entry;
  duckdb::Vector _staging;
  uint64_t _filled = 0;
  uint64_t _row_group_first_doc = 0;
  bool _skip_validity = false;
  bool _hyperloglog = false;
  duckdb::CompressionType _forced_compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
};

}  // namespace irs
