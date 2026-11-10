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

#include <absl/algorithm/container.h>

#include <array>
#include <cstdint>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/internal/gather_arms.hpp"

namespace irs {

class ColumnPatch final {
 public:
  static constexpr uint32_t kChunkShift = 11;
  static constexpr uint64_t kChunkRows = uint64_t{1} << kChunkShift;
  static constexpr uint32_t kGroupShift = 6;
  static constexpr size_t kGroupChunks = size_t{1} << kGroupShift;

  struct Chunk {
    Chunk(const duckdb::LogicalType& type, duckdb::idx_t capacity);

    std::vector<uint16_t> offsets;
    std::vector<uint64_t> ticks;
    duckdb::Vector values;
    duckdb::BaseStatistics stats;
    uint64_t bytes = 0;
  };

  struct Group {
    explicit Group(const duckdb::LogicalType& type);

    std::array<std::shared_ptr<const Chunk>, kGroupChunks> chunks;
    duckdb::BaseStatistics stats;
  };

  explicit ColumnPatch(duckdb::LogicalType type);

  static bool Patchable(const duckdb::LogicalType& type) noexcept;

  static std::shared_ptr<const ColumnPatch> Merge(
    const ColumnPatch* base, const duckdb::LogicalType& type,
    std::span<const uint64_t> rows, const duckdb::Vector& values,
    std::span<const uint64_t> ticks);
  static std::shared_ptr<const ColumnPatch> Merge(
    const ColumnPatch* base, const duckdb::LogicalType& type,
    std::span<const uint64_t> rows, const duckdb::Vector& values,
    uint64_t tick) {
    return Merge(base, type, rows, values, std::span<const uint64_t>{&tick, 1});
  }
  static std::shared_ptr<const ColumnPatch> Merge(const ColumnPatch* base,
                                                  const ColumnPatch& added,
                                                  uint64_t tick);

  const duckdb::LogicalType& Type() const noexcept { return _type; }
  uint64_t Count() const noexcept { return _count; }
  uint64_t Bytes() const noexcept { return _bytes; }
  uint64_t MaxTick() const noexcept { return _max_tick; }
  const duckdb::BaseStatistics& Stats() const noexcept { return _stats; }

  void Entries(std::vector<uint64_t>& rows, duckdb::Vector& values,
               std::vector<uint64_t>& ticks) const;

  const Chunk* FindChunk(uint64_t chunk) const noexcept;
  bool Overlaps(uint64_t first, uint64_t last) const noexcept;
  void MergeStats(uint64_t first, uint64_t last,
                  duckdb::BaseStatistics& stats) const;

  template<typename Visit>
  void ForEach(uint64_t first, uint64_t last, const Visit& visit) const {
    if (first >= last) {
      return;
    }
    const auto first_group = first >> (kChunkShift + kGroupShift);
    auto it = absl::c_lower_bound(
      _groups, first_group,
      [](const auto& entry, uint64_t group) { return entry.first < group; });
    for (; it != _groups.end(); ++it) {
      const uint64_t group_row = it->first << (kChunkShift + kGroupShift);
      if (group_row >= last) {
        return;
      }
      for (size_t c = 0; c < kGroupChunks; ++c) {
        const auto& chunk = it->second->chunks[c];
        const uint64_t chunk_row = group_row + (uint64_t{c} << kChunkShift);
        if (!chunk || chunk_row + kChunkRows <= first) {
          continue;
        }
        if (chunk_row >= last) {
          return;
        }
        for (size_t i = 0; i < chunk->offsets.size(); ++i) {
          const uint64_t row = chunk_row + chunk->offsets[i];
          if (row >= last) {
            return;
          }
          if (row >= first) {
            visit(row, *chunk, i);
          }
        }
      }
    }
  }

 private:
  duckdb::LogicalType _type;
  std::vector<std::pair<uint64_t, std::shared_ptr<const Group>>> _groups;
  uint64_t _count = 0;
  uint64_t _bytes = 0;
  uint64_t _max_tick = 0;
  duckdb::BaseStatistics _stats;
};

struct FieldPatch {
  field_id field = 0;
  std::shared_ptr<const ColumnPatch> patch;
};

class NullColumnReader final : public ColumnReader {
 public:
  NullColumnReader(duckdb::DatabaseInstance& db, field_id id,
                   const duckdb::LogicalType& type, uint64_t rows);

  bool InMemory() const noexcept final { return true; }
  uint64_t RowGroupEnd(uint64_t) const noexcept final { return _row_count; }
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
  bool FetchPoint(PointReader& reader, uint64_t row, duckdb::Vector& out,
                  duckdb::idx_t out_offset) const final;
  bool ValidPoint(PointReader&, uint64_t, duckdb::Vector&) const final {
    return false;
  }

  IRS_COLUMN_READER_GATHER_SCATTER
  IRS_COLUMN_READER_GATHER_DENSE

 protected:
  void NewOutputVector(ScanState&) const final {}
};

class PatchedColumnReader final : public ColumnReader {
 public:
  PatchedColumnReader(const ColumnReader& base,
                      std::shared_ptr<const ColumnPatch> patch);
  PatchedColumnReader(std::unique_ptr<const ColumnReader> base,
                      std::shared_ptr<const ColumnPatch> patch);

  const ColumnReader& Base() const noexcept { return _base; }
  const std::shared_ptr<const ColumnPatch>& Patch() const noexcept {
    return _patch;
  }

  bool InMemory() const noexcept final { return _base.InMemory(); }
  bool Patched() const noexcept final { return true; }
  uint64_t RowGroupEnd(uint64_t row) const noexcept final {
    return _base.RowGroupEnd(row);
  }
  ScanState InitScan(ReadContext& ctx) const final {
    return _base.InitScan(ctx);
  }
  uint64_t GatherCursor(const ScanState& s) const noexcept final {
    return _base.GatherCursor(s);
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final;
  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final;
  void Skip(ScanState& s, duckdb::idx_t count) const final {
    _base.Skip(s, count);
  }
  void GatherScatter(ScanState& s, uint64_t anchor,
                     const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                     duckdb::Vector& out, duckdb::idx_t at) const final;
  void GatherDense(ScanState& s, uint64_t anchor,
                   const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                   duckdb::idx_t span, duckdb::Vector& out) const final;
  duckdb::idx_t GatherFilter(ScanState& s, uint64_t anchor, duckdb::idx_t span,
                             duckdb::SelectionVector& sel,
                             duckdb::idx_t sel_count,
                             const duckdb::TableFilter& filter,
                             duckdb::TableFilterState& filter_state,
                             NullCheckKind null_check,
                             duckdb::Vector& scratch) const final;
  bool FetchPoint(PointReader& reader, uint64_t row, duckdb::Vector& out,
                  duckdb::idx_t out_offset) const final;
  bool ValidPoint(PointReader& reader, uint64_t row,
                  duckdb::Vector& scratch) const final;

 protected:
  void NewOutputVector(ScanState& s) const final { ResetOutput(_base, s); }

 private:
  void ApplySelected(uint64_t anchor, const duckdb::SelectionVector& sel,
                     duckdb::idx_t hits, duckdb::Vector& out,
                     duckdb::idx_t at) const;

  std::unique_ptr<const ColumnReader> _owned;
  const ColumnReader& _base;
  std::shared_ptr<const ColumnPatch> _patch;
};

}  // namespace irs
