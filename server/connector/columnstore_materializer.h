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

#include <algorithm>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/types.hpp"

namespace duckdb {

class DatabaseInstance;
}

namespace irs {

class Directory;
struct SegmentMeta;

}  // namespace irs
namespace sdb::connector {
namespace cs_internal {

// Contiguous integer range [start, start+count). Used to recurse into
// ARRAY element ranges and LIST per-row element ranges where the doc-id
// range is purely arithmetic from the parent row. Satisfies the implicit
// "doc-id range" duck-type (`.size()` + `operator[](size_t) -> uint64_t`)
// required by MaterializeNode below; the constraint is enforced at
// instantiation, not via a concept.
struct IotaRange {
  using contiguous_range_tag = void;
  uint64_t start;
  uint64_t count;
  constexpr size_t size() const noexcept { return static_cast<size_t>(count); }
  constexpr uint64_t operator[](size_t i) const noexcept { return start + i; }
};

// Recursive scan state mirrored after duckdb::ColumnScanState's
// child_states. Lives on the binding and persists across batches so the
// open ColumnSegments inside ScanCursor / ListOffsetState are reused --
// otherwise every batch re-opens (and re-reads) every cs segment.
struct MaterializerNodeState {
  // Primitive-leaf cursors (lazy-init on first use; ColumnReader's
  // RangeScan is move-only and needs a reader reference, so optional
  // works as a deferred slot).
  std::optional<irs::columnstore::ColumnReader::RangeScan> data_scan;
  std::optional<irs::columnstore::ColumnReader::RangeScan> validity_scan;
  // LIST/MAP cursors and helpers.
  irs::columnstore::ColumnReader::ListOffsetState list_offsets;
  duckdb::Vector offsets_scratch{duckdb::LogicalType::UBIGINT,
                                 STANDARD_VECTOR_SIZE};
  irs::columnstore::RgWindow rg_hint;
  // Pre-allocated child states for ARRAY/LIST/MAP (1 entry) and
  // STRUCT (one per field).
  std::vector<std::unique_ptr<MaterializerNodeState>> children;
};

inline std::unique_ptr<MaterializerNodeState> MakeMaterializerNodeState(
  const irs::columnstore::ColumnReader& reader) {
  auto state = std::make_unique<MaterializerNodeState>();
  switch (reader.Type().id()) {
    case duckdb::LogicalTypeId::ARRAY:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      state->children.push_back(MakeMaterializerNodeState(*child));
    } break;
    case duckdb::LogicalTypeId::STRUCT: {
      state->children.reserve(reader.StructFieldCount());
      for (size_t fi = 0; fi < reader.StructFieldCount(); ++fi) {
        state->children.push_back(
          MakeMaterializerNodeState(reader.StructField(fi)));
      }
    } break;
    default:
      break;
  }
  return state;
}

template<typename DocIds>
void MaterializeNode(const irs::columnstore::ColumnReader& reader,
                     MaterializerNodeState& state, const DocIds& doc_ids,
                     duckdb::Vector& out_vec, duckdb::idx_t output_start,
                     bool may_use_entire = false) {
  if (doc_ids.size() == 0) {
    return;
  }
  using irs::columnstore::ColumnReader;
  if (reader.HasValidity()) {
    if (!state.validity_scan) {
      state.validity_scan.emplace(reader, /*validity_side=*/true);
    }
    ColumnReader::ScanRowsBatched(*state.validity_scan, doc_ids, out_vec,
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
      // Coalesce runs of consecutive parent doc_ids into one child
      // recursion -- the element ranges are contiguous in child space.
      size_t i = 0;
      while (i < doc_ids.size()) {
        const size_t run = irs::columnstore::ConsecutiveRunLength(doc_ids, i);
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
      // MAP shares LIST's physical layout (PhysicalType::LIST with a
      // STRUCT<k,v> element vector); the same accessors work.
      if (reader.RowCount() == 0) {
        return;
      }
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      auto* list_entries =
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(out_vec);
      auto& child_out = duckdb::ListVector::GetChildMutable(out_vec);
      auto& child_state = *state.children[0];
      // Coalesce consecutive parent doc_ids that fall inside the same
      // row group: scan their offsets in one ColumnSegment::Scan call,
      // build list_entries from the cumulative offsets, then recurse on
      // the child once with the full element range.
      size_t i = 0;
      while (i < doc_ids.size()) {
        const uint64_t doc0 = static_cast<uint64_t>(doc_ids[i]);
        state.rg_hint = reader.Locate(doc0, state.rg_hint);
        const auto& window = state.rg_hint;
        const size_t run =
          irs::columnstore::ConsecutiveRunLength(doc_ids, i, window.end);
        const uint64_t first_in_rg = doc0 - window.begin;
        const uint64_t first_start = reader.ReadListOffsets(
          state.list_offsets, window.rg, first_in_rg,
          static_cast<duckdb::idx_t>(run), state.offsets_scratch);
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
      // Same recursion as duckdb::StructColumnData::ScanCount: validity
      // already on out_vec, each field scans the same doc_ids into its
      // own slot.
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
      if (!state.data_scan) {
        state.data_scan.emplace(reader, /*validity_side=*/false);
      }
      ColumnReader::ScanRowsBatched(*state.data_scan, doc_ids, out_vec,
                                    output_start, may_use_entire);
      return;
    }
  }
}

}  // namespace cs_internal

class ColumnstoreMaterializer {
 public:
  ColumnstoreMaterializer(const irs::columnstore::Reader& reader,
                          std::span<const irs::field_id> column_ids,
                          std::span<const duckdb::idx_t> output_slots);

  bool HasAny() const noexcept { return !_bound.empty(); }

  void SelectByDocIds(std::span<const irs::doc_id_t> doc_ids,
                      duckdb::DataChunk& output,
                      duckdb::idx_t output_start = 0) const;

  void Scan(uint64_t start_doc, duckdb::idx_t count, duckdb::DataChunk& output);

 private:
  struct Binding {
    const irs::columnstore::ColumnReader* reader;
    duckdb::idx_t output_slot;
    std::unique_ptr<cs_internal::MaterializerNodeState> state;
  };

  std::vector<Binding> _bound;
};

}  // namespace sdb::connector
