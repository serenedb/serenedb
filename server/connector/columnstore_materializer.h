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
  { t[i] } -> std::convertible_to<irs::doc_id_t>;
};

namespace cs_internal {

inline void MaterializeListRow(const irs::columnstore::ColumnReader& reader,
                               uint64_t doc_id, duckdb::Vector& out_vec,
                               duckdb::idx_t out_idx) {
  const auto window = reader.Locate(doc_id);
  const uint64_t in_rg = doc_id - window.begin;
  const auto offsets = reader.ListOffsets(window.rg);
  SDB_ASSERT(in_rg + 1 < offsets.size());
  const uint64_t local_start = offsets[in_rg];
  const uint64_t length = offsets[in_rg + 1] - local_start;
  const uint64_t global_start =
    reader.RowGroupElementStart(window.rg) + local_start;

  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(out_vec);
  const duckdb::idx_t prior_size = duckdb::ListVector::GetListSize(out_vec);
  list_entries[out_idx] = duckdb::list_entry_t{prior_size, length};
  if (length == 0) {
    return;
  }
  duckdb::ListVector::Reserve(out_vec, prior_size + length);
  duckdb::Vector& child_out = duckdb::ListVector::GetChildMutable(out_vec);
  const auto* child_reader = reader.Child();
  SDB_ASSERT(child_reader);

  irs::columnstore::ColumnReader::RangeScan range{*child_reader};
  range.Scan(global_start, length, child_out, prior_size);
  duckdb::ListVector::SetListSize(out_vec, prior_size + length);
}

template<DocIdRange DocIds>
void MaterializeArrayRange(const irs::columnstore::ColumnReader& reader,
                           const DocIds& doc_ids, duckdb::Vector& out_vec,
                           duckdb::idx_t output_start) {
  const auto array_size = reader.ArraySize();
  SDB_ASSERT(array_size > 0);
  const auto* child_reader = reader.Child();
  SDB_ASSERT(child_reader);
  duckdb::Vector& child_out = duckdb::ArrayVector::GetChildMutable(out_vec);

  irs::columnstore::ColumnReader::RangeScan range{*child_reader};
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    const uint64_t elem_start = static_cast<uint64_t>(doc_ids[i]) * array_size;
    const duckdb::idx_t child_out_start =
      (output_start + i) * static_cast<duckdb::idx_t>(array_size);
    range.Scan(elem_start, array_size, child_out, child_out_start);
  }
}

}  // namespace cs_internal

// Materializes one column for sorted-ascending doc_ids into
// out_vec[output_start..]. Handles primitives, ARRAY and LIST.
template<DocIdRange DocIds>
void MaterializeColumnRange(const irs::columnstore::ColumnReader& reader,
                            const DocIds& doc_ids, duckdb::Vector& out_vec,
                            duckdb::idx_t output_start) {
  if (doc_ids.size() == 0) {
    return;
  }
  const auto type_id = reader.Type().id();
  // ARRAY has no top-level data side; short-circuit before the
  // RowGroupCount == 0 guard.
  if (type_id == duckdb::LogicalTypeId::ARRAY) {
    cs_internal::MaterializeArrayRange(reader, doc_ids, out_vec, output_start);
    return;
  }
  if (reader.RowGroupCount() == 0) {
    return;
  }
  if (type_id == duckdb::LogicalTypeId::LIST) {
    for (size_t i = 0; i < doc_ids.size(); ++i) {
      cs_internal::MaterializeListRow(reader, static_cast<uint64_t>(doc_ids[i]),
                                      out_vec, output_start + i);
    }
    return;
  }
  using irs::columnstore::ColumnReader;
  {
    ColumnReader::RangeScan data{reader, /*validity_side=*/false};
    ColumnReader::ScanRowsBatched(data, doc_ids, out_vec, output_start);
  }
  if (reader.HasValidity()) {
    ColumnReader::RangeScan validity{reader, /*validity_side=*/true};
    ColumnReader::ScanRowsBatched(validity, doc_ids, out_vec, output_start);
  }
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
