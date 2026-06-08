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

#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/variant.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/scalar/variant_utils.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace duckdb {

class ClientContext;

}  // namespace duckdb

#include "basics/assert.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace irs {

struct IotaRange {
  using contiguous_range_tag = void;
  uint64_t start;
  uint64_t count;
  constexpr size_t size() const noexcept { return static_cast<size_t>(count); }
  constexpr uint64_t operator[](size_t i) const noexcept { return start + i; }
};

struct MaterializeState {
  struct VariantRgState {
    std::unique_ptr<MaterializeState> unshredded;
    std::unique_ptr<MaterializeState> shredded;
  };

  ColumnReader::RangeScan data_scan;
  ColumnReader::RangeScan validity_scan;
  ColumnReader::ListOffsetState list_offsets;
  duckdb::Vector offsets_scratch{duckdb::LogicalType::UBIGINT,
                                 STANDARD_VECTOR_SIZE};
  RgWindow rg_hint;
  std::vector<std::unique_ptr<MaterializeState>> children;
  ReadContext* ctx = nullptr;
  std::vector<VariantRgState> variant_rg_states;
  RgWindow variant_rg_hint;
  std::vector<std::unique_ptr<MaterializeState>> extract_leaf_states;

  MaterializeState(const ColumnReader& reader, ReadContext& ctx)
    : data_scan{reader, ctx, /*validity_side=*/false},
      validity_scan{reader, ctx, /*validity_side=*/true},
      list_offsets{reader, ctx},
      ctx{&ctx} {}
};

inline std::unique_ptr<MaterializeState> MakeMaterializeState(
  const ColumnReader& reader, ReadContext& ctx) {
  auto state = std::make_unique<MaterializeState>(reader, ctx);
  switch (reader.Type().id()) {
    case duckdb::LogicalTypeId::ARRAY:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      state->children.push_back(MakeMaterializeState(*child, ctx));
    } break;
    case duckdb::LogicalTypeId::STRUCT: {
      state->children.reserve(reader.StructFieldCount());
      for (size_t fi = 0; fi < reader.StructFieldCount(); ++fi) {
        state->children.push_back(
          MakeMaterializeState(reader.StructField(fi), ctx));
      }
    } break;
    case duckdb::LogicalTypeId::VARIANT:
      break;
    default:
      break;
  }
  return state;
}

inline MaterializeState::VariantRgState& EnsureVariantRgState(
  const ColumnReader& reader, MaterializeState& state, size_t rg) {
  if (state.variant_rg_states.empty()) {
    state.variant_rg_states.resize(reader.VariantRgCount());
  }
  auto& slot = state.variant_rg_states[rg];
  if (!slot.unshredded) {
    const auto& rgr = reader.VariantRg(rg);
    slot.unshredded = MakeMaterializeState(*rgr.unshredded, *state.ctx);
    if (rgr.shredded) {
      slot.shredded = MakeMaterializeState(*rgr.shredded_node, *state.ctx);
    }
  }
  return slot;
}

template<typename DocIds>
void MaterializeNode(const ColumnReader& reader, MaterializeState& state,
                     const DocIds& doc_ids, duckdb::Vector& out_vec,
                     duckdb::idx_t output_start, bool may_use_entire = false);

inline void ReconstructVariantRun(MaterializeState::VariantRgState& rgstate,
                                  const ColumnReader::VariantRgReader& rgr,
                                  uint64_t local_start, size_t run,
                                  const duckdb::ValidityMask* variant_validity,
                                  duckdb::idx_t valid_off,
                                  duckdb::Vector& out_variant) {
  const auto apply_nulls = [&](duckdb::Vector& target) {
    if (!variant_validity || variant_validity->AllValid()) {
      return;
    }
    auto& dst = duckdb::FlatVector::ValidityMutable(target);
    for (size_t k = 0; k < run; ++k) {
      if (!variant_validity->RowIsValid(valid_off + k)) {
        dst.SetInvalid(k);
      }
    }
  };
  if (!rgr.shredded) {
    MaterializeNode(*rgr.unshredded, *rgstate.unshredded,
                    IotaRange{local_start, run}, out_variant, 0);
    apply_nulls(out_variant);
    return;
  }
  duckdb::child_list_t<duckdb::LogicalType> ct;
  ct.emplace_back("unshredded", rgr.unshredded->Type());
  ct.emplace_back("shredded", rgr.shredded_node->Type());
  duckdb::Vector intermediate{duckdb::LogicalType::STRUCT(ct), run};
  auto& ch = duckdb::StructVector::GetEntries(intermediate);
  MaterializeNode(*rgr.unshredded, *rgstate.unshredded,
                  IotaRange{local_start, run}, ch[0], /*output_start=*/0);
  MaterializeNode(*rgr.shredded_node, *rgstate.shredded,
                  IotaRange{local_start, run}, ch[1], /*output_start=*/0);
  apply_nulls(intermediate);
  duckdb::VariantUtils::UnshredVariantData(intermediate, out_variant, run);
}

inline constexpr size_t kShreddedTypedValueIndex = 0;

inline const ColumnReader* ResolveShreddedLeaf(
  const ColumnReader& shredded_node, std::span<const std::string> path) {
  const ColumnReader* node = &shredded_node;
  for (const auto& field : path) {
    if (node->Type().id() != duckdb::LogicalTypeId::STRUCT) {
      return nullptr;
    }
    const ColumnReader& typed = node->StructField(kShreddedTypedValueIndex);
    if (typed.Type().id() != duckdb::LogicalTypeId::STRUCT) {
      return nullptr;
    }
    const auto& children = duckdb::StructType::GetChildTypes(typed.Type());
    size_t idx = children.size();
    for (size_t c = 0; c < children.size(); ++c) {
      if (duckdb::StringUtil::CIEquals(field, children[c].first)) {
        idx = c;
        break;
      }
    }
    if (idx == children.size()) {
      return nullptr;
    }
    node = &typed.StructField(idx);
  }
  if (node->Type().id() == duckdb::LogicalTypeId::STRUCT) {
    const ColumnReader& leaf = node->StructField(kShreddedTypedValueIndex);
    return leaf.Type().IsNested() ? nullptr : &leaf;
  }
  return node->Type().IsNested() ? nullptr : node;
}

inline MaterializeState& EnsureExtractLeafState(MaterializeState& state,
                                                size_t rg,
                                                const ColumnReader& leaf,
                                                size_t rg_count) {
  if (state.extract_leaf_states.empty()) {
    state.extract_leaf_states.resize(rg_count);
  }
  auto& slot = state.extract_leaf_states[rg];
  if (!slot) {
    slot = MakeMaterializeState(leaf, *state.ctx);
  }
  return *slot;
}

// DocIds duck-types: `.size()` + `operator[](size_t) -> uint64_t`.
// Tag with `using contiguous_range_tag = void;` to opt into the
// single-Scan fast path for fully-contiguous ranges (see IotaRange).
template<typename DocIds>
void MaterializeNode(const ColumnReader& reader, MaterializeState& state,
                     const DocIds& doc_ids, duckdb::Vector& out_vec,
                     duckdb::idx_t output_start, bool may_use_entire) {
  if (doc_ids.size() == 0) {
    return;
  }
  if (reader.HasValidity()) {
    ColumnReader::ScanRowsBatched(state.validity_scan, doc_ids, out_vec,
                                  output_start);
  }
  switch (reader.Type().id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      const auto array_size = reader.ArraySize();
      SDB_ASSERT(array_size > 0);
      auto& child_out = duckdb::ArrayVector::GetChildMutable(out_vec);
      auto& child_state = *state.children[0];
      size_t i = 0;
      while (i < doc_ids.size()) {
        const size_t run = ConsecutiveRunLength(doc_ids, i);
        const uint64_t elem_start =
          static_cast<uint64_t>(doc_ids[i]) * array_size;
        const auto child_out_start =
          static_cast<duckdb::idx_t>((output_start + i) * array_size);
        MaterializeNode(*child, child_state,
                        IotaRange{elem_start, run * array_size}, child_out,
                        child_out_start, /*may_use_entire=*/false);
        i += run;
      }
      return;
    }
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      if (reader.RowCount() == 0) {
        return;
      }
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      auto* list_entries =
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(out_vec);
      auto& child_out = duckdb::ListVector::GetChildMutable(out_vec);
      auto& child_state = *state.children[0];
      size_t i = 0;
      while (i < doc_ids.size()) {
        const uint64_t doc0 = static_cast<uint64_t>(doc_ids[i]);
        state.rg_hint = reader.Locate(doc0, state.rg_hint);
        const auto& window = state.rg_hint;
        const size_t run = ConsecutiveRunLength(doc_ids, i, window.end);
        const uint64_t first_in_rg = doc0 - window.begin;
        const uint64_t first_start = state.list_offsets.Read(
          window.rg, first_in_rg, static_cast<duckdb::idx_t>(run),
          state.offsets_scratch);
        const auto* ends =
          duckdb::FlatVector::GetData<uint64_t>(state.offsets_scratch);
        const auto child_run_start = duckdb::ListVector::GetListSize(out_vec);
        uint64_t prev = first_start;
        for (size_t k = 0; k < run; ++k) {
          const uint64_t end = ends[k];
          list_entries[output_start + i + k] = duckdb::list_entry_t{
            child_run_start + (prev - first_start), end - prev};
          prev = end;
        }
        const uint64_t total_len = prev - first_start;
        if (total_len > 0) {
          duckdb::ListVector::Reserve(out_vec, child_run_start + total_len);
          MaterializeNode(*child, child_state,
                          IotaRange{first_start, total_len}, child_out,
                          child_run_start, /*may_use_entire=*/false);
          duckdb::ListVector::SetListSize(out_vec, child_run_start + total_len);
        }
        i += run;
      }
      return;
    }
    case duckdb::LogicalTypeId::STRUCT: {
      auto& entries = duckdb::StructVector::GetEntries(out_vec);
      SDB_ASSERT(entries.size() == reader.StructFieldCount());
      for (size_t fi = 0; fi < entries.size(); ++fi) {
        MaterializeNode(reader.StructField(fi), *state.children[fi], doc_ids,
                        entries[fi], output_start, may_use_entire);
      }
      return;
    }
    case duckdb::LogicalTypeId::VARIANT: {
      size_t i = 0;
      while (i < doc_ids.size()) {
        const uint64_t doc0 = static_cast<uint64_t>(doc_ids[i]);
        state.variant_rg_hint =
          reader.LocateVariantRg(doc0, state.variant_rg_hint);
        const auto& w = state.variant_rg_hint;
        const size_t run = ConsecutiveRunLength(doc_ids, i, w.end);
        const auto& rgr = reader.VariantRg(w.rg);
        auto& rgstate = EnsureVariantRgState(reader, state, w.rg);
        const uint64_t local_start = doc0 - w.begin;

        const auto& vmask = duckdb::FlatVector::Validity(out_vec);
        duckdb::Vector tmp_variant{duckdb::LogicalType::VARIANT(), run};
        ReconstructVariantRun(rgstate, rgr, local_start, run, &vmask,
                              output_start + i, tmp_variant);
        duckdb::VectorOperations::Copy(tmp_variant, out_vec, run, 0,
                                       output_start + i);
        i += run;
      }
      return;
    }
    default: {
      if (reader.RowCount() == 0) {
        return;
      }
      ColumnReader::ScanRowsBatched(state.data_scan, doc_ids, out_vec,
                                    output_start, may_use_entire);
      return;
    }
  }
}

inline void CastExtractInto(duckdb::ClientContext& context, duckdb::Vector& src,
                            duckdb::Vector& dst, size_t run,
                            duckdb::idx_t dst_offset) {
  const auto count = static_cast<duckdb::idx_t>(run);
  if (src.GetType() == dst.GetType()) {
    duckdb::VectorOperations::Copy(src, dst, count, 0, dst_offset);
    return;
  }
  duckdb::Vector casted{dst.GetType(), count};
  duckdb::VectorOperations::Cast(context, src, casted, count);
  duckdb::VectorOperations::Copy(casted, dst, count, 0, dst_offset);
}

template<typename DocIds>
void MaterializeExtractNode(const ColumnReader& reader, MaterializeState& state,
                            const DocIds& doc_ids,
                            std::span<const std::string> path,
                            const duckdb::LogicalType& scan_type,
                            duckdb::Vector& out_vec, duckdb::idx_t output_start,
                            duckdb::ClientContext& context) {
  SDB_ASSERT(reader.Type().id() == duckdb::LogicalTypeId::VARIANT);
  SDB_ASSERT(!path.empty());
  if (doc_ids.size() == 0) {
    return;
  }
  const size_t rg_count = reader.VariantRgCount();
  size_t i = 0;
  while (i < doc_ids.size()) {
    const uint64_t doc0 = static_cast<uint64_t>(doc_ids[i]);
    state.variant_rg_hint = reader.LocateVariantRg(doc0, state.variant_rg_hint);
    const auto& w = state.variant_rg_hint;
    const size_t run = ConsecutiveRunLength(doc_ids, i, w.end);
    const auto& rgr = reader.VariantRg(w.rg);
    const uint64_t local_start = doc0 - w.begin;

    const ColumnReader* leaf = nullptr;
    if (rgr.shredded && rgr.fully_shredded) {
      leaf = ResolveShreddedLeaf(*rgr.shredded_node, path);
    }
    if (leaf != nullptr) {
      auto& leaf_state = EnsureExtractLeafState(state, w.rg, *leaf, rg_count);
      if (leaf->Type() == scan_type) {
        MaterializeNode(*leaf, leaf_state, IotaRange{local_start, run}, out_vec,
                        output_start + i, /*may_use_entire=*/true);
      } else {
        duckdb::Vector scratch{leaf->Type(), static_cast<duckdb::idx_t>(run)};
        MaterializeNode(*leaf, leaf_state, IotaRange{local_start, run}, scratch,
                        /*output_start=*/0, /*may_use_entire=*/true);
        CastExtractInto(context, scratch, out_vec, run, output_start + i);
      }
      i += run;
      continue;
    }

    auto& rgstate = EnsureVariantRgState(reader, state, w.rg);
    duckdb::Vector tmp_variant{duckdb::LogicalType::VARIANT(),
                               static_cast<duckdb::idx_t>(run)};
    if (reader.HasValidity()) {
      duckdb::Vector vmask_holder{duckdb::LogicalType::BOOLEAN,
                                  static_cast<duckdb::idx_t>(run)};
      state.validity_scan.Scan(doc0, static_cast<duckdb::idx_t>(run),
                               vmask_holder, 0);
      const auto& vmask = duckdb::FlatVector::Validity(vmask_holder);
      ReconstructVariantRun(rgstate, rgr, local_start, run, &vmask, 0,
                            tmp_variant);
    } else {
      ReconstructVariantRun(rgstate, rgr, local_start, run, nullptr, 0,
                            tmp_variant);
    }
    duckdb::vector<duckdb::VariantPathComponent> components;
    components.reserve(path.size());
    for (const auto& f : path) {
      components.emplace_back(f);
    }
    duckdb::Vector extracted{duckdb::LogicalType::VARIANT(),
                             static_cast<duckdb::idx_t>(run)};
    duckdb::VariantUtils::VariantExtract(tmp_variant, components, extracted,
                                         static_cast<duckdb::idx_t>(run));
    CastExtractInto(context, extracted, out_vec, run, output_start + i);
    i += run;
  }
}

}  // namespace irs
