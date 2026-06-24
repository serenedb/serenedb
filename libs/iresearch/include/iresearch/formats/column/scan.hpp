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

#include <absl/strings/match.h>

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
#include <string_view>
#include <vector>

namespace duckdb {

class ClientContext;

}  // namespace duckdb

#include <duckdb/common/allocator.hpp>

#include "basics/assert.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/column/vector_pool.hpp"

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

  struct ExtractLeafSlot {
    const ColumnReader* leaf = nullptr;
    std::unique_ptr<MaterializeState> state;

    bool IsResolved() const noexcept { return leaf != nullptr; }

    bool IsValid() const noexcept {
      return static_cast<bool>(leaf) == static_cast<bool>(state);
    }
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
  std::vector<ExtractLeafSlot> extract_leaf_slots;
  VectorPool scratch;

  MaterializeState(const ColumnReader& reader, ReadContext& ctx)
    : data_scan{reader, ctx, /*validity_side=*/false},
      validity_scan{reader, ctx, /*validity_side=*/true},
      list_offsets{reader, ctx},
      ctx{&ctx},
      scratch{duckdb::Allocator::Get(ctx.Database())} {}
};

std::unique_ptr<MaterializeState> MakeMaterializeState(
  const ColumnReader& reader, ReadContext& ctx);

MaterializeState::VariantRgState& EnsureVariantRgState(
  const ColumnReader& reader, MaterializeState& state, size_t rg);

template<typename DocIds>
void MaterializeNode(const ColumnReader& reader, MaterializeState& state,
                     const DocIds& doc_ids, duckdb::Vector& out_vec,
                     duckdb::idx_t output_start, bool may_use_entire = false);

void ReconstructVariantRun(
  MaterializeState::VariantRgState& rgstate,
  const ColumnReader::VariantRgReader& row_group_reader, uint64_t local_start,
  size_t run, duckdb::Vector& out_variant, duckdb::idx_t out_off,
  VectorPool& scratch);

inline constexpr size_t kShreddedTypedValueIndex = 0;

size_t FindStructFieldIndex(const duckdb::LogicalType& struct_type,
                            std::string_view field);

const ColumnReader* ResolveShreddedLeaf(const ColumnReader& shredded_node,
                                        std::span<const std::string_view> path);

MaterializeState::ExtractLeafSlot& EnsureExtractLeaf(
  MaterializeState& state, size_t rg,
  const ColumnReader::VariantRgReader& rg_reader,
  std::span<const std::string_view> path, size_t rg_count);

struct VariantRgSlice {
  const ColumnReader::VariantRgReader& rg_reader;
  size_t rg_index;
  uint64_t rg_offset;
  size_t out_offset;
  size_t count;
  uint64_t first_doc;
};

template<typename DocIds, typename Fn>
inline void ForEachVariantRgSlice(const ColumnReader& reader,
                                  MaterializeState& state,
                                  const DocIds& doc_ids, Fn&& fn) {
  size_t i = 0;
  while (i < doc_ids.size()) {
    const auto doc0 = static_cast<uint64_t>(doc_ids[i]);
    state.variant_rg_hint = reader.LocateVariantRg(doc0, state.variant_rg_hint);
    const RgWindow& window = state.variant_rg_hint;
    const size_t run = ConsecutiveRunLength(doc_ids, i, window.end);
    fn(VariantRgSlice{.rg_reader = reader.VariantRg(window.rg),
                      .rg_index = window.rg,
                      .rg_offset = doc0 - window.begin,
                      .out_offset = i,
                      .count = run,
                      .first_doc = doc0});
    i += run;
  }
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
        const auto doc0 = static_cast<uint64_t>(doc_ids[i]);
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
    case duckdb::LogicalTypeId::UNION:
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
      ForEachVariantRgSlice(
        reader, state, doc_ids, [&](const VariantRgSlice& slice) {
          auto& rgstate = EnsureVariantRgState(reader, state, slice.rg_index);
          const auto out_off = output_start + slice.out_offset;
          ReconstructVariantRun(rgstate, slice.rg_reader, slice.rg_offset,
                                slice.count, out_vec, out_off, state.scratch);
        });
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

void CastExtractInto(duckdb::ClientContext& context, duckdb::Vector& src,
                     duckdb::Vector& dst, size_t run, duckdb::idx_t dst_offset,
                     VectorPool& scratch);

template<typename DocIds>
void MaterializeExtractLeaf(const ColumnReader& leaf,
                            MaterializeState& leaf_state, const DocIds& rows,
                            const duckdb::LogicalType& scan_type,
                            duckdb::Vector& out_vec, duckdb::idx_t out_offset,
                            duckdb::ClientContext& context,
                            bool may_use_entire = false) {
  if (leaf.Type() == scan_type) {
    MaterializeNode(leaf, leaf_state, rows, out_vec, out_offset,
                    may_use_entire);
    return;
  }
  auto scratch = leaf_state.scratch.Acquire(leaf.Type());
  MaterializeNode(leaf, leaf_state, rows, *scratch, /*output_start=*/0,
                  may_use_entire);
  CastExtractInto(context, *scratch, out_vec, rows.size(), out_offset,
                  leaf_state.scratch);
}

template<typename DocIds>
void MaterializeVariantExtractNode(
  const ColumnReader& reader, MaterializeState& state, const DocIds& doc_ids,
  std::span<const std::string_view> path, const duckdb::LogicalType& scan_type,
  duckdb::Vector& out_vec, duckdb::idx_t output_start,
  duckdb::ClientContext& context);

template<typename DocIds>
void MaterializeStructExtractNode(
  const ColumnReader& reader, MaterializeState& state, const DocIds& doc_ids,
  std::span<const std::string_view> path, const duckdb::LogicalType& scan_type,
  duckdb::Vector& out_vec, duckdb::idx_t output_start,
  duckdb::ClientContext& context, bool may_use_entire = false) {
  SDB_ASSERT(reader.Type().id() == duckdb::LogicalTypeId::STRUCT);
  SDB_ASSERT(!path.empty());
  if (doc_ids.size() == 0) {
    return;
  }
  const auto* leaf = &reader;
  auto* leaf_state = &state;
  for (size_t pi = 0; pi < path.size(); ++pi) {
    if (leaf->Type().id() == duckdb::LogicalTypeId::VARIANT) {
      MaterializeVariantExtractNode(*leaf, *leaf_state, doc_ids,
                                    path.subspan(pi), scan_type, out_vec,
                                    output_start, context);
      return;
    }
    if (leaf->Type().id() != duckdb::LogicalTypeId::STRUCT) {
      return;
    }
    const size_t idx = FindStructFieldIndex(leaf->Type(), path[pi]);
    if (idx == leaf->StructFieldCount()) {
      return;
    }
    SDB_ASSERT(idx < leaf_state->children.size());
    leaf = &leaf->StructField(idx);
    leaf_state = leaf_state->children[idx].get();
  }
  MaterializeExtractLeaf(*leaf, *leaf_state, doc_ids, scan_type, out_vec,
                         output_start, context, may_use_entire);
}

template<typename DocIds>
void MaterializeVariantExtractNode(
  const ColumnReader& reader, MaterializeState& state, const DocIds& doc_ids,
  std::span<const std::string_view> path, const duckdb::LogicalType& scan_type,
  duckdb::Vector& out_vec, duckdb::idx_t output_start,
  duckdb::ClientContext& context) {
  SDB_ASSERT(reader.Type().id() == duckdb::LogicalTypeId::VARIANT);
  SDB_ASSERT(!path.empty());
  if (doc_ids.size() == 0) {
    return;
  }
  const size_t rg_count = reader.VariantRgCount();
  duckdb::vector<duckdb::VariantPathComponent> components;
  auto non_shredded = state.scratch.Acquire(duckdb::LogicalType::VARIANT());
  auto extracted = state.scratch.Acquire(duckdb::LogicalType::VARIANT());
  ForEachVariantRgSlice(
    reader, state, doc_ids, [&](const VariantRgSlice& slice) {
      auto& slot = EnsureExtractLeaf(state, slice.rg_index, slice.rg_reader,
                                     path, rg_count);
      SDB_ASSERT(slot.IsValid());
      if (slot.IsResolved()) {
        MaterializeExtractLeaf(
          *slot.leaf, *slot.state, IotaRange{slice.rg_offset, slice.count},
          scan_type, out_vec, output_start + slice.out_offset, context);
        return;
      }
      // Unshreded path
      if (components.empty()) {
        components.reserve(path.size());
        for (const auto& field : path) {
          components.emplace_back(std::string{field});
        }
      }
      auto& rgstate = EnsureVariantRgState(reader, state, slice.rg_index);
      const auto count = static_cast<duckdb::idx_t>(slice.count);
      ReconstructVariantRun(rgstate, slice.rg_reader, slice.rg_offset,
                            slice.count, *non_shredded, 0, state.scratch);
      duckdb::VariantUtils::VariantExtract(*non_shredded, components,
                                           *extracted, count);
      if (out_vec.GetType().id() == duckdb::LogicalTypeId::VARIANT) {
        duckdb::VectorOperations::Copy(*extracted, out_vec, slice.count, 0,
                                       output_start + slice.out_offset);
      } else {
        CastExtractInto(context, *extracted, out_vec, slice.count,
                        output_start + slice.out_offset, state.scratch);
      }
    });
}

template<typename DocIds>
void MaterializeExtractNode(const ColumnReader& reader, MaterializeState& state,
                            const DocIds& doc_ids,
                            std::span<const std::string_view> path,
                            const duckdb::LogicalType& scan_type,
                            duckdb::Vector& out_vec, duckdb::idx_t output_start,
                            duckdb::ClientContext& context,
                            bool may_use_entire = false) {
  if (reader.Type().id() == duckdb::LogicalTypeId::STRUCT) {
    MaterializeStructExtractNode(reader, state, doc_ids, path, scan_type,
                                 out_vec, output_start, context,
                                 may_use_entire);
  } else {
    MaterializeVariantExtractNode(reader, state, doc_ids, path, scan_type,
                                  out_vec, output_start, context);
  }
}

}  // namespace irs
