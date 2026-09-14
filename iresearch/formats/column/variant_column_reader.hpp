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

#include "iresearch/formats/column/column_reader.hpp"

namespace irs {

struct VariantRgScan {
  std::unique_ptr<ColumnReader::ScanState> unshredded;
  std::unique_ptr<ColumnReader::ScanState> shredded;
  std::unique_ptr<ColumnReader::VectorScratch> intermediate;
  uint64_t local_pos = 0;
  std::unique_ptr<ColumnReader::ScanState> leaf;
  const ColumnReader* leaf_reader = nullptr;
};

struct VariantScanState {
  std::vector<VariantRgScan> rgs;
  uint64_t cursor = 0;
  std::unique_ptr<ColumnReader::VectorScratch> scratch;
  int8_t leaf_resolved = -1;
  duckdb::LogicalType leaf_type;
  std::vector<bool> rg_leaf;
};

class VariantColumnReader final : public ColumnReader {
 public:
  VariantColumnReader(field_id id, duckdb::LogicalType type,
                      std::unique_ptr<ColumnReader> validity,
                      std::vector<VariantRgMeta>&& rgs);

  uint64_t GatherCursor(const ScanState& s) const noexcept final {
    return s.variant ? s.variant->cursor : 0;
  }

  duckdb::idx_t Scan(ScanState& s, duckdb::Vector& result,
                     duckdb::idx_t count) const final;

  duckdb::idx_t ScanCount(ScanState& s, duckdb::Vector& result,
                          duckdb::idx_t count,
                          duckdb::idx_t result_offset) const final;

  void Skip(ScanState& s, duckdb::idx_t count) const final;

  void GatherScatter(ScanState& s, uint64_t anchor,
                     const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                     duckdb::Vector& out, duckdb::idx_t at) const final;

  void GatherDense(ScanState& s, uint64_t anchor,
                   const duckdb::SelectionVector& sel, duckdb::idx_t hits,
                   duckdb::idx_t span, duckdb::Vector& out) const final;

  bool CachedUniformShreddedLeaf(ScanState& s,
                                 std::span<const std::string_view> path,
                                 duckdb::LogicalType& leaf_type) const {
    auto& vs = VariantState(s);
    if (vs.leaf_resolved < 0) {
      vs.leaf_resolved =
        ResolveUniformShreddedLeaf(path, vs.leaf_type, vs.rg_leaf) ? 1 : 0;
    }
    leaf_type = vs.leaf_type;
    return vs.leaf_resolved == 1;
  }

  bool RgLeafOk(ScanState& s, size_t rg) const {
    return VariantState(s).rg_leaf[rg];
  }

  void ExtractShreddedLeafRun(ScanState& s, size_t rg,
                              std::span<const std::string_view> path,
                              duckdb::idx_t local_start, duckdb::idx_t run,
                              duckdb::Vector& out, duckdb::idx_t out_off,
                              bool allow_entire) const;

  std::pair<const ColumnReader*, ScanState*> ShreddedLeafScan(
    ScanState& s, size_t rg, std::span<const std::string_view> path) const;

  uint64_t RowGroupEnd(uint64_t row) const noexcept final {
    uint64_t begin, end;
    VariantRgSpan(row, begin, end);
    return end;
  }

  size_t VariantRgSpan(uint64_t row, uint64_t& begin, uint64_t& end) const {
    const auto it =
      std::upper_bound(_variant_offsets.begin(), _variant_offsets.end(), row);
    const auto rg = static_cast<size_t>(it - _variant_offsets.begin()) - 1;
    begin = _variant_offsets[rg];
    end = _variant_offsets[rg + 1];
    return rg;
  }

  template<typename Fn>
  void ForEachVariantRun(uint64_t start_row, duckdb::idx_t count,
                         Fn&& fn) const {
    duckdb::idx_t done = 0;
    while (done < count) {
      const uint64_t row = start_row + done;
      const auto it =
        std::upper_bound(_variant_offsets.begin(), _variant_offsets.end(), row);
      const auto rg = static_cast<size_t>(it - _variant_offsets.begin()) - 1;
      const uint64_t rg_begin = _variant_offsets[rg];
      const uint64_t rg_end = _variant_offsets[rg + 1];
      const auto run = std::min<duckdb::idx_t>(
        count - done, static_cast<duckdb::idx_t>(rg_end - row));
      fn(rg, static_cast<duckdb::idx_t>(row - rg_begin), run, done);
      done += run;
    }
  }

  void NewOutputVector(ScanState& s) const final;

 private:
  struct VariantRg {
    std::unique_ptr<ColumnReader> unshredded;
    std::unique_ptr<ColumnReader> shredded;
    duckdb::LogicalType intermediate_type;
  };

  VariantScanState& VariantState(ScanState& s) const {
    if (!s.variant) {
      s.variant = std::make_unique<VariantScanState>();
      s.variant->rgs.resize(_variant_rgs.size());
    }
    return *s.variant;
  }

  bool ResolveUniformShreddedLeaf(std::span<const std::string_view> path,
                                  duckdb::LogicalType& leaf_type,
                                  std::vector<bool>& rg_leaf) const;
  void SeekVariantRg(ScanState& s, size_t rg, duckdb::idx_t target_local) const;
  void ReconstructVariantRun(ScanState& s, size_t rg, duckdb::idx_t local_start,
                             duckdb::idx_t run, duckdb::Vector& result,
                             duckdb::idx_t out_off) const;
  static duckdb::Vector& ScratchVector(VariantScanState& vs,
                                       const duckdb::LogicalType& type);
  static const ColumnReader* FindShreddedLeaf(
    const ColumnReader& node, std::span<const std::string_view> path);
  std::vector<VariantRg> _variant_rgs;
  std::vector<uint64_t> _variant_offsets;
};

}  // namespace irs
