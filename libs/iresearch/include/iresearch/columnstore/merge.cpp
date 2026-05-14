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

#include "iresearch/columnstore/merge.hpp"

#include <algorithm>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <memory>
#include <optional>
#include <vector>

#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/hnsw.hpp"

namespace irs::columnstore {
namespace {

struct NodeState {
  std::optional<ColumnReader::RangeScan> data_scan;
  std::optional<ColumnReader::RangeScan> validity_scan;
  ColumnReader::ListOffsetState list_offsets;
  duckdb::Vector offsets_scratch{duckdb::LogicalType::UBIGINT,
                                 STANDARD_VECTOR_SIZE};
  RgWindow rg_hint;
  std::vector<std::unique_ptr<NodeState>> children;
};

std::unique_ptr<NodeState> MakeNodeState(const ColumnReader& reader) {
  auto state = std::make_unique<NodeState>();
  switch (reader.Type().id()) {
    case duckdb::LogicalTypeId::ARRAY:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST:
      state->children.push_back(MakeNodeState(*reader.Child()));
      break;
    case duckdb::LogicalTypeId::STRUCT:
      state->children.reserve(reader.StructFieldCount());
      for (size_t fi = 0; fi < reader.StructFieldCount(); ++fi) {
        state->children.push_back(MakeNodeState(reader.StructField(fi)));
      }
      break;
    default:
      break;
  }
  return state;
}

void ReadRange(const ColumnReader& reader, NodeState& state, uint64_t start,
               duckdb::idx_t count, duckdb::Vector& out,
               duckdb::idx_t out_offset) {
  if (count == 0) {
    return;
  }
  if (reader.HasValidity()) {
    if (!state.validity_scan) {
      state.validity_scan.emplace(reader, /*validity_side=*/true);
    }
    state.validity_scan->Scan(start, count, out, out_offset);
  }
  switch (reader.Type().id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      const auto array_size = reader.ArraySize();
      auto& child_out = duckdb::ArrayVector::GetChildMutable(out);
      ReadRange(*reader.Child(), *state.children[0], start * array_size,
                count * array_size, child_out, out_offset * array_size);
      return;
    }
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      auto* list_entries =
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(out);
      auto& child_out = duckdb::ListVector::GetChildMutable(out);
      auto& child_state = *state.children[0];
      duckdb::idx_t scanned = 0;
      while (scanned < count) {
        const uint64_t row0 = start + scanned;
        state.rg_hint = reader.Locate(row0, state.rg_hint);
        const auto& window = state.rg_hint;
        const duckdb::idx_t in_window = static_cast<duckdb::idx_t>(
          std::min<uint64_t>(count - scanned, window.end - row0));
        const uint64_t first_in_rg = row0 - window.begin;
        const uint64_t first_start =
          reader.ReadListOffsets(state.list_offsets, window.rg, first_in_rg,
                                 in_window, state.offsets_scratch);
        const auto* ends =
          duckdb::FlatVector::GetData<uint64_t>(state.offsets_scratch);
        const auto child_run_start = duckdb::ListVector::GetListSize(out);
        uint64_t prev = first_start;
        for (duckdb::idx_t k = 0; k < in_window; ++k) {
          list_entries[out_offset + scanned + k] = duckdb::list_entry_t{
            child_run_start + (prev - first_start), ends[k] - prev};
          prev = ends[k];
        }
        const uint64_t total_len = prev - first_start;
        if (total_len > 0) {
          duckdb::ListVector::Reserve(out, child_run_start + total_len);
          ReadRange(*reader.Child(), child_state, first_start,
                    static_cast<duckdb::idx_t>(total_len), child_out,
                    child_run_start);
          duckdb::ListVector::SetListSize(out, child_run_start + total_len);
        }
        scanned += in_window;
      }
      return;
    }
    case duckdb::LogicalTypeId::STRUCT: {
      auto& entries = duckdb::StructVector::GetEntries(out);
      for (size_t fi = 0; fi < entries.size(); ++fi) {
        ReadRange(reader.StructField(fi), *state.children[fi], start, count,
                  entries[fi], out_offset);
      }
      return;
    }
    default: {
      if (!state.data_scan) {
        state.data_scan.emplace(reader, /*validity_side=*/false);
      }
      state.data_scan->Scan(start, count, out, out_offset);
      return;
    }
  }
}

}  // namespace

void MergeInto(std::span<const Reader* const> sources,
               std::span<const DocumentMask* const> source_masks,
               Writer& output) {
  const Reader* schema_src = nullptr;
  for (const auto* src : sources) {
    if (src) {
      schema_src = src;
      break;
    }
  }
  if (!schema_src) {
    return;
  }

  for (const auto& first_col : schema_src->Columns()) {
    const auto field_id_v = first_col->Id();
    auto& cw = output.OpenColumn(field_id_v, first_col->Type());

    uint64_t out_doc = 0;
    for (size_t s = 0; s < sources.size(); ++s) {
      const auto* src = sources[s];
      if (!src) {
        continue;
      }
      const auto* col = src->Column(field_id_v);
      if (!col) {
        continue;
      }
      if (src->HasHNSW(field_id_v)) {
        output.AttachHNSW(field_id_v, src->HNSW(field_id_v)->Info());
      }
      SDB_ASSERT(col->Type() == first_col->Type(),
                 "schema evolution between merge sources not supported");
      const auto* mask = source_masks[s];

      auto state = MakeNodeState(*col);

      for (size_t rg = 0; rg < col->RowGroupCount(); ++rg) {
        const auto rg_count = col->RowGroupRowCount(rg);
        const auto rg_first_doc = col->RowGroupOffset(rg);

        duckdb::idx_t scanned = 0;
        while (scanned < rg_count) {
          const auto take =
            std::min<duckdb::idx_t>(rg_count - scanned, STANDARD_VECTOR_SIZE);
          duckdb::Vector batch{
            col->Type(), STANDARD_VECTOR_SIZE,
            duckdb::VectorDataInitialization::ZERO_INITIALIZE};
          ReadRange(*col, *state, rg_first_doc + scanned, take, batch,
                    /*out_offset=*/0);

          if (!mask || mask->empty()) {
            cw.Append(out_doc, batch, take);
            out_doc += take;
          } else {
            duckdb::SelectionVector sel{take};
            duckdb::idx_t kept = 0;
            for (duckdb::idx_t i = 0; i < take; ++i) {
              const auto src_doc = static_cast<doc_id_t>(
                rg_first_doc + scanned + i + doc_limits::min());
              if (mask->contains(src_doc)) {
                continue;
              }
              sel.set_index(kept++, i);
            }
            if (kept > 0) {
              cw.Append(out_doc, batch, sel, kept);
              out_doc += kept;
            }
          }
          scanned += take;
        }
      }
    }
  }
}

}  // namespace irs::columnstore
