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

#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <memory>
#include <vector>

#include "basics/assert.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/read_context.hpp"

namespace irs::columnstore {

struct IotaRange {
  using contiguous_range_tag = void;
  uint64_t start;
  uint64_t count;
  constexpr size_t size() const noexcept { return static_cast<size_t>(count); }
  constexpr uint64_t operator[](size_t i) const noexcept { return start + i; }
};

struct MaterializeState {
  ColumnReader::RangeScan data_scan;
  ColumnReader::RangeScan validity_scan;
  ColumnReader::ListOffsetState list_offsets;
  duckdb::Vector offsets_scratch{duckdb::LogicalType::UBIGINT,
                                 STANDARD_VECTOR_SIZE};
  RgWindow rg_hint;
  std::vector<std::unique_ptr<MaterializeState>> children;

  MaterializeState(const ColumnReader& reader, ReadContext& ctx)
    : data_scan{reader, ctx, /*validity_side=*/false},
      validity_scan{reader, ctx, /*validity_side=*/true},
      list_offsets{reader, ctx} {}
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
    case duckdb::LogicalTypeId::VARIANT:
    case duckdb::LogicalTypeId::STRUCT: {
      state->children.reserve(reader.StructFieldCount());
      for (size_t fi = 0; fi < reader.StructFieldCount(); ++fi) {
        state->children.push_back(
          MakeMaterializeState(reader.StructField(fi), ctx));
      }
    } break;
    default:
      break;
  }
  return state;
}

// DocIds duck-types: `.size()` + `operator[](size_t) -> uint64_t`.
// Tag with `using contiguous_range_tag = void;` to opt into the
// single-Scan fast path for fully-contiguous ranges (see IotaRange).
template<typename DocIds>
void MaterializeNode(const ColumnReader& reader, MaterializeState& state,
                     const DocIds& doc_ids, duckdb::Vector& out_vec,
                     duckdb::idx_t output_start, bool may_use_entire = false) {
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
    case duckdb::LogicalTypeId::VARIANT:
    case duckdb::LogicalTypeId::STRUCT: {
      auto& entries = duckdb::StructVector::GetEntries(out_vec);
      SDB_ASSERT(entries.size() == reader.StructFieldCount());
      for (size_t fi = 0; fi < entries.size(); ++fi) {
        MaterializeNode(reader.StructField(fi), *state.children[fi], doc_ids,
                        entries[fi], output_start, may_use_entire);
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

}  // namespace irs::columnstore
