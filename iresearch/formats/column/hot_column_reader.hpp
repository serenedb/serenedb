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

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/distinct_statistics.hpp>
#include <memory>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"

namespace duckdb {

class DatabaseInstance;

}  // namespace duckdb
namespace irs {

class HotChunkStats final {
 public:
  duckdb::BaseStatistics Get(const duckdb::LogicalType& type,
                             const WriteChunk& chunk);
  void MergeDistinct(DistinctStats mode, const WriteChunk& chunk,
                     duckdb::DistinctStatistics& out);

 private:
  absl::flat_hash_map<const void*, duckdb::BaseStatistics> _full;
  absl::flat_hash_map<const void*, std::unique_ptr<duckdb::DistinctStatistics>>
    _full_distinct;
  duckdb::Vector _hashes{duckdb::LogicalType::HASH, nullptr};
};

class HotColumnReader final : public ColumnReader {
 public:
  static std::unique_ptr<ColumnReader> Make(duckdb::DatabaseInstance& db,
                                            StagedColumn&& column,
                                            HotChunkStats* stats);

  HotColumnReader(field_id id, duckdb::LogicalType type,
                  std::vector<ColumnBlockMeta> blocks,
                  std::vector<WriteChunk> chunks,
                  std::vector<std::unique_ptr<ColumnReader>> children);

  bool InMemory() const noexcept final { return true; }

  bool FetchPoint(PointReader& reader, uint64_t row, duckdb::Vector& out,
                  duckdb::idx_t out_offset) const final;

  bool ValidPoint(PointReader& reader, uint64_t row,
                  duckdb::Vector& scratch) const final {
    return FetchPoint(reader, row, scratch, 0);
  }

  uint64_t RowGroupEnd(uint64_t row) const noexcept final;

  uint64_t GatherCursor(const ScanState& s) const noexcept final {
    return s.st.offset_in_column;
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final;

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final;

  void Skip(ScanState& s, duckdb::idx_t count) const final {
    s.st.offset_in_column += count;
  }

  duckdb::idx_t GatherFilter(ScanState& s, uint64_t anchor, duckdb::idx_t span,
                             duckdb::SelectionVector& sel,
                             duckdb::idx_t sel_count,
                             const duckdb::TableFilter& filter,
                             duckdb::TableFilterState& filter_state,
                             NullCheckKind null_check,
                             duckdb::Vector& scratch) const final;

  IRS_COLUMN_READER_GATHER_SCATTER
  IRS_COLUMN_READER_GATHER_DENSE

 protected:
  void NewOutputVector(ScanState&) const final {}

 private:
  size_t ChunkOf(uint64_t row) const noexcept;
  void CopyRows(uint64_t row, duckdb::idx_t count, duckdb::Vector& out,
                duckdb::idx_t out_offset, bool share_strings) const;

  std::vector<WriteChunk> _chunks;
  std::vector<uint64_t> _starts;
};

}  // namespace irs
