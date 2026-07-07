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

#include "iresearch/formats/column/variant_column_reader.hpp"

#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <duckdb/storage/statistics/variant_stats.hpp>
#include <duckdb/storage/table/variant_column_data.hpp>
#include <memory>
#include <utility>

#include "basics/assert.h"
#include "iresearch/formats/column/internal/gather_arms.hpp"

namespace irs {

VariantColumnReader::VariantColumnReader(field_id id, duckdb::LogicalType type,
                                         std::unique_ptr<ColumnReader> validity,
                                         std::vector<VariantRgMeta>&& rgs)
  : ColumnReader{id, std::move(type), {}, std::move(validity), {}} {
  _variant_rgs.reserve(rgs.size());
  _variant_offsets.reserve(rgs.size() + 1);
  _variant_offsets.push_back(0);
  auto stats = duckdb::VariantStats::CreateEmpty(_type);
  for (auto& rg : rgs) {
    VariantRg vrg;
    vrg.unshredded = Make(std::move(*rg.unshredded));
    if (rg.shredded) {
      vrg.shredded = Make(std::move(*rg.shredded));
      duckdb::child_list_t<duckdb::LogicalType> intermediate_children;
      intermediate_children.emplace_back("unshredded", vrg.unshredded->Type());
      intermediate_children.emplace_back("shredded", vrg.shredded->Type());
      vrg.intermediate_type =
        duckdb::LogicalType::STRUCT(std::move(intermediate_children));
    }
    auto rg_s = duckdb::VariantStats::CreateEmpty(_type);
    duckdb::VariantStats::SetUnshreddedStats(
      rg_s, vrg.unshredded->MergedStatistics());
    if (vrg.shredded) {
      duckdb::VariantStats::SetShreddedStats(rg_s,
                                             vrg.shredded->MergedStatistics());
    }
    // Like duckdb's checkpoint stats, an unshredded row group stays
    // UNINITIALIZED so mixed segments merge to SHREDDED, not INCONSISTENT.
    stats.Merge(rg_s);
    _row_count += rg.row_count;
    _variant_offsets.push_back(_row_count);
    _variant_rgs.push_back(std::move(vrg));
  }
  FinishStats(std::move(stats));
}

void VariantColumnReader::NewOutputVector(ScanState& s) const {
  ColumnReader::NewOutputVector(s);
  if (!s.variant) {
    return;
  }
  for (size_t i = 0; i < s.variant->rgs.size(); ++i) {
    auto& vs = s.variant->rgs[i];
    if (vs.unshredded) {
      ResetOutput(*_variant_rgs[i].unshredded, *vs.unshredded);
    }
    if (vs.shredded) {
      ResetOutput(*_variant_rgs[i].shredded, *vs.shredded);
    }
    if (vs.leaf && vs.leaf_reader) {
      ResetOutput(*vs.leaf_reader, *vs.leaf);
    }
  }
}

void VariantColumnReader::SeekVariantRg(ScanState& s, size_t rg,
                                        duckdb::idx_t target_local) const {
  const auto& rg_meta = _variant_rgs[rg];
  auto& vstate = VariantState(s).rgs[rg];
  if (!vstate.unshredded) {
    vstate.unshredded =
      std::make_unique<ScanState>(rg_meta.unshredded->InitScan(*s.ctx));
    if (rg_meta.shredded) {
      vstate.shredded =
        std::make_unique<ScanState>(rg_meta.shredded->InitScan(*s.ctx));
    }
    vstate.local_pos = 0;
  }
  if (target_local > vstate.local_pos) {
    const auto skip =
      static_cast<duckdb::idx_t>(target_local - vstate.local_pos);
    rg_meta.unshredded->Skip(*vstate.unshredded, skip);
    if (rg_meta.shredded) {
      rg_meta.shredded->Skip(*vstate.shredded, skip);
    }
    vstate.local_pos = target_local;
  }
}

const ColumnReader* VariantColumnReader::FindShreddedLeaf(
  const ColumnReader& node, std::span<const std::string_view> path) {
  if (!duckdb::VariantShreddedStats::IsFullyShredded(node.MergedStatistics())) {
    return nullptr;
  }
  if (path.empty()) {
    if (node.Type().id() == duckdb::LogicalTypeId::STRUCT) {
      const auto& tv =
        node.StructField(duckdb::VariantColumnData::TYPED_VALUE_INDEX);
      return tv.Type().IsNested() ? nullptr : &tv;
    }
    return node.Type().IsNested() ? nullptr : &node;
  }
  if (node.Type().id() != duckdb::LogicalTypeId::STRUCT) {
    return nullptr;
  }
  const auto& tv =
    node.StructField(duckdb::VariantColumnData::TYPED_VALUE_INDEX);
  if (tv.Type().id() != duckdb::LogicalTypeId::STRUCT) {
    return nullptr;
  }
  const auto& fields = duckdb::StructType::GetChildTypes(tv.Type());
  for (size_t fi = 0; fi < fields.size(); ++fi) {
    if (duckdb::StringUtil::CIEquals(path[0],
                                     fields[fi].first.GetIdentifierName())) {
      return FindShreddedLeaf(tv.StructField(fi), path.subspan(1));
    }
  }
  return nullptr;
}

bool VariantColumnReader::ResolveUniformShreddedLeaf(
  std::span<const std::string_view> path, duckdb::LogicalType& leaf_type,
  std::vector<bool>& rg_leaf) const {
  rg_leaf.assign(_variant_rgs.size(), false);
  if (path.empty() || _variant_rgs.empty()) {
    return false;
  }
  bool have = false;
  for (size_t i = 0; i < _variant_rgs.size(); ++i) {
    const auto& rg = _variant_rgs[i];
    const ColumnReader* leaf =
      rg.shredded ? FindShreddedLeaf(*rg.shredded, path) : nullptr;
    if (!leaf || leaf->Type().IsNested()) {
      continue;
    }
    if (!have) {
      leaf_type = leaf->Type();
      have = true;
    } else if (leaf->Type() != leaf_type) {
      return false;
    }
    rg_leaf[i] = true;
  }
  return have;
}

std::pair<const ColumnReader*, ColumnReader::ScanState*>
VariantColumnReader::ShreddedLeafScan(
  ScanState& s, size_t rg, std::span<const std::string_view> path) const {
  auto& vstate = VariantState(s).rgs[rg];
  if (!vstate.leaf) {
    vstate.leaf_reader = FindShreddedLeaf(*_variant_rgs[rg].shredded, path);
    SDB_ASSERT(vstate.leaf_reader);
    vstate.leaf =
      std::make_unique<ScanState>(vstate.leaf_reader->InitScan(*s.ctx));
  }
  return {vstate.leaf_reader, vstate.leaf.get()};
}

void VariantColumnReader::ExtractShreddedLeafRun(
  ScanState& s, size_t rg, std::span<const std::string_view> path,
  duckdb::idx_t local_start, duckdb::idx_t run, duckdb::Vector& out,
  duckdb::idx_t out_off, bool allow_entire) const {
  const auto [leaf_reader, leaf] = ShreddedLeafScan(s, rg, path);
  const uint64_t cur = leaf_reader->GatherCursor(*leaf);
  SDB_ASSERT(local_start >= cur);
  if (local_start > cur) {
    leaf_reader->Skip(*leaf, local_start - cur);
  }
  if (allow_entire) {
    leaf_reader->Scan(*leaf, out, run);
  } else {
    leaf_reader->ScanCount(*leaf, out, run, out_off);
  }
}

duckdb::Vector& VariantColumnReader::ScratchVector(
  VariantScanState& vs, const duckdb::LogicalType& type) {
  if (!vs.scratch) {
    vs.scratch = std::make_unique<VectorScratch>(type);
  }
  return vs.scratch->Reset();
}

void VariantColumnReader::ReconstructVariantRun(ScanState& s, size_t rg,
                                                duckdb::idx_t local_start,
                                                duckdb::idx_t run,
                                                duckdb::Vector& result,
                                                duckdb::idx_t out_off) const {
  const auto& rg_meta = _variant_rgs[rg];
  SeekVariantRg(s, rg, local_start);
  auto& vs = *s.variant;
  auto& vstate = vs.rgs[rg];

  if (!rg_meta.shredded) {
    rg_meta.unshredded->ScanCount(*vstate.unshredded, result, run, out_off);
  } else {
    if (!vstate.intermediate) {
      vstate.intermediate =
        std::make_unique<VectorScratch>(rg_meta.intermediate_type);
    }
    auto& intermediate = vstate.intermediate->Reset();
    auto& entries = duckdb::StructVector::GetEntries(intermediate);
    rg_meta.unshredded->ScanCount(*vstate.unshredded, entries[0], run, 0);
    rg_meta.shredded->ScanCount(*vstate.shredded, entries[1], run, 0);
    duckdb::FlatVector::SetSize(intermediate, run);
    auto& scratch = ScratchVector(vs, result.GetType());
    duckdb::VariantUtils::UnshredVariantData(intermediate, scratch, run);
    duckdb::VectorOperations::Copy(scratch, result, run, 0, out_off);
  }
  vstate.local_pos += run;
}

duckdb::idx_t VariantColumnReader::Scan(ScanState& s, duckdb::Vector& result,
                                        duckdb::idx_t count) const {
  NewOutputVector(s);
  return ScanCount(s, result, count, /*result_offset=*/0);
}

duckdb::idx_t VariantColumnReader::ScanCount(
  ScanState& s, duckdb::Vector& result, duckdb::idx_t count,
  duckdb::idx_t result_offset) const {
  if (count == 0) {
    return 0;
  }
  auto& vs = VariantState(s);
  ForEachVariantRun(vs.cursor, count,
                    [&](size_t rg, duckdb::idx_t local_start, duckdb::idx_t run,
                        duckdb::idx_t done) {
                      ReconstructVariantRun(s, rg, local_start, run, result,
                                            result_offset + done);
                    });
  if (_validity) {
    _validity->ColumnReader::ScanCount(s.child_states[0], result, count,
                                       result_offset);
  }
  vs.cursor += count;
  return count;
}

void VariantColumnReader::Skip(ScanState& s, duckdb::idx_t count) const {
  if (count == 0) {
    return;
  }
  if (_validity) {
    _validity->ColumnReader::Skip(s.child_states[0], count);
  }
  auto& vs = VariantState(s);
  ForEachVariantRun(
    vs.cursor, count,
    [&](size_t rg, duckdb::idx_t local_start, duckdb::idx_t run,
        duckdb::idx_t /*done*/) { SeekVariantRg(s, rg, local_start + run); });
  vs.cursor += count;
}

void VariantColumnReader::GatherScatter(ScanState& s, uint64_t anchor,
                                        const duckdb::SelectionVector& sel,
                                        duckdb::idx_t hits, duckdb::Vector& out,
                                        duckdb::idx_t at) const {
  column_internal::ScatterRuns(*this, s, anchor, sel, hits, out, at);
}

void VariantColumnReader::GatherDense(ScanState& s, uint64_t anchor,
                                      const duckdb::SelectionVector& sel,
                                      duckdb::idx_t hits, duckdb::idx_t span,
                                      duckdb::Vector& out) const {
  column_internal::DenseRuns(*this, s, anchor, sel, hits, span, out);
}

}  // namespace irs
