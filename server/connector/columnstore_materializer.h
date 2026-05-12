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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
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

template<typename T>
concept DocIdRange = requires(const T& t, size_t i) {
  { t.size() } -> std::convertible_to<size_t>;
  { t[i] } -> std::convertible_to<uint64_t>;
};

namespace cs_internal {

// Contiguous integer range [start, start+count) shaped as a DocIdRange.
// Used to recurse into ARRAY element ranges and LIST per-row element
// ranges where the doc-id range is purely arithmetic from the parent row.
struct IotaRange {
  uint64_t start;
  uint64_t count;
  constexpr size_t size() const noexcept { return static_cast<size_t>(count); }
  constexpr uint64_t operator[](size_t i) const noexcept { return start + i; }
};
static_assert(DocIdRange<IotaRange>);

// Generic recursive materializer. Mirrors duckdb::ColumnData::ScanCount's
// per-type recursion: scan parent validity into the output vector first,
// then dispatch on the structural payload (primitive data / ARRAY child /
// LIST length+child). Recursing through the same entry point honours the
// child's own validity and nested structural layout, so e.g. NULL elements
// inside a non-null list roundtrip correctly.
template<DocIdRange DocIds>
void MaterializeNode(const irs::columnstore::ColumnReader& reader,
                     const DocIds& doc_ids, duckdb::Vector& out_vec,
                     duckdb::idx_t output_start) {
  if (doc_ids.size() == 0) {
    return;
  }
  using irs::columnstore::ColumnReader;
  if (reader.HasValidity()) {
    ColumnReader::RangeScan validity_scan{reader, /*validity_side=*/true};
    ColumnReader::ScanRowsBatched(validity_scan, doc_ids, out_vec,
                                  output_start);
  }
  switch (reader.Type().id()) {
    case duckdb::LogicalTypeId::ARRAY: {
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      const auto array_size = reader.ArraySize();
      SDB_ASSERT(array_size > 0);
      auto& child_out = duckdb::ArrayVector::GetChildMutable(out_vec);
      for (size_t i = 0; i < doc_ids.size(); ++i) {
        const uint64_t parent_doc = static_cast<uint64_t>(doc_ids[i]);
        const uint64_t elem_start = parent_doc * array_size;
        const auto child_out_start =
          static_cast<duckdb::idx_t>((output_start + i) * array_size);
        MaterializeNode(*child, IotaRange{elem_start, array_size}, child_out,
                        child_out_start);
      }
      return;
    }
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::LIST: {
      // MAP shares LIST's physical layout (PhysicalType::LIST with a
      // STRUCT<k,v> element vector); the same accessors work.
      if (reader.RowGroupCount() == 0) {
        return;
      }
      const auto* child = reader.Child();
      SDB_ASSERT(child);
      auto* list_entries =
        duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(out_vec);
      auto& child_out = duckdb::ListVector::GetChildMutable(out_vec);
      for (size_t i = 0; i < doc_ids.size(); ++i) {
        const uint64_t doc = static_cast<uint64_t>(doc_ids[i]);
        const auto window = reader.Locate(doc);
        const uint64_t in_rg = doc - window.begin;
        const auto offsets = reader.ListOffsets(window.rg);
        SDB_ASSERT(in_rg + 1 < offsets.size());
        const uint64_t local_start = offsets[in_rg];
        const uint64_t length = offsets[in_rg + 1] - local_start;
        const uint64_t global_start =
          reader.RowGroupElementStart(window.rg) + local_start;
        const auto prior_size = duckdb::ListVector::GetListSize(out_vec);
        list_entries[output_start + i] =
          duckdb::list_entry_t{prior_size, length};
        if (length == 0) {
          continue;
        }
        duckdb::ListVector::Reserve(out_vec, prior_size + length);
        MaterializeNode(*child, IotaRange{global_start, length}, child_out,
                        prior_size);
        duckdb::ListVector::SetListSize(out_vec, prior_size + length);
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
        MaterializeNode(reader.StructField(fi), doc_ids, entries[fi],
                        output_start);
      }
      return;
    }
    default: {
      if (reader.RowGroupCount() == 0) {
        return;
      }
      ColumnReader::RangeScan data{reader, /*validity_side=*/false};
      ColumnReader::ScanRowsBatched(data, doc_ids, out_vec, output_start);
      return;
    }
  }
}

}  // namespace cs_internal

// Materialises `doc_ids` from `reader` into out_vec[output_start..].
template<DocIdRange DocIds>
void MaterializeColumnRange(const irs::columnstore::ColumnReader& reader,
                            const DocIds& doc_ids, duckdb::Vector& out_vec,
                            duckdb::idx_t output_start) {
  cs_internal::MaterializeNode(reader, doc_ids, out_vec, output_start);
}

// One instance per (segment, projection set) for INCLUDEd columns flagged
// store_values=true. Non-INCLUDEd columns fall through to IndexSource.
class ColumnstoreMaterializer {
 public:
  // column_ids[i] -> output.data[output_slots[i]]. Columns absent from
  // this segment's .cs are skipped (caller materializes via IndexSource).
  ColumnstoreMaterializer(const irs::Directory& dir,
                          const irs::SegmentMeta& meta,
                          duckdb::DatabaseInstance& db,
                          std::span<const irs::field_id> column_ids,
                          std::span<const duckdb::idx_t> output_slots);

  bool HasAny() const noexcept { return !_bound.empty(); }

  void SelectByDocIds(std::span<const irs::doc_id_t> doc_ids,
                      duckdb::DataChunk& output,
                      duckdb::idx_t output_start = 0) const;

  // Sequential Scan reuses the open ColumnSegment + ColumnScanState across
  // calls; mutates per-binding cursors so it's not const.
  void Scan(uint64_t start_doc, duckdb::idx_t count, duckdb::DataChunk& output);

 private:
  struct Binding {
    const irs::columnstore::ColumnReader* reader;
    duckdb::idx_t output_slot;
    irs::columnstore::ColumnReader::RangeScan data_scan;
    irs::columnstore::ColumnReader::RangeScan validity_scan;
  };

  irs::columnstore::Reader _reader;
  std::vector<Binding> _bound;
};

}  // namespace sdb::connector
