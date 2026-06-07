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
#include <duckdb/storage/data_pointer.hpp>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "iresearch/types.hpp"

// Internal header shared between col_reader.cpp / col_writer.cpp (footer
// ser/de) and column_writer.cpp (FlushRowGroup pushes per-row-group
// DataPointers into the tree). Not part of the public API.

namespace irs {

// Recursive metadata tree mirroring DuckDB's PersistentColumnData
// (third_party/duckdb/src/include/duckdb/storage/table/column_data.hpp).
// We diverge from DuckDB by carrying validity as a parallel slot
// instead of a child column -- VALIDITY is a separate codec flow in our
// CompressColumn so a sibling slot mirrors what the writer emits more
// directly than nesting it as child[0].
//
// Layout per type:
//   primitive:
//     pointers           -> per-row-group data DataPointers
//     validity_pointers  -> per-row-group validity DataPointers
//                           (empty when skip_validity is true)
//     child_columns      -> empty
//   ARRAY(child, dim):
//     pointers           -> empty (fixed-size: no self data on disk)
//     validity_pointers  -> array-level validity per row group
//     child_columns      -> [element]; element rows = parent rows * dim
//   LIST(child):
//     pointers           -> per-row UBIGINT lengths, codec-picked
//     validity_pointers  -> row-level validity per row group
//     child_columns      -> [element]; element rows = sum of lengths
//   STRUCT (not implemented): pointers empty, child_columns one per field.
struct PersistentColumnData {
  duckdb::LogicalType type;
  std::vector<duckdb::DataPointer> pointers;
  std::vector<duckdb::DataPointer> validity_pointers;
  std::vector<PersistentColumnData> child_columns;
  // LIST/MAP only, transient writer state -- not serialised. Cumulative
  // child element count across all row groups written so far for this
  // node, used to bias per-RG offsets into the column-global space.
  uint64_t list_global_running = 0;
};

// Top-level entry for one column: id + recursive metadata root.
struct FooterColumnEntry {
  field_id id;
  PersistentColumnData root;
};

inline duckdb::DataPointer CloneDataPointer(const duckdb::DataPointer& p) {
  // segment_state is intentionally dropped: it has no clone, and the cs
  // read path doesn't consult it (only VisitBlockIds during DuckDB
  // checkpoint cleanup does, which we never run).
  duckdb::DataPointer out{p.statistics.Copy()};
  out.row_start = p.row_start;
  out.tuple_count = p.tuple_count;
  out.block_pointer = p.block_pointer;
  out.compression_type = p.compression_type;
  return out;
}

inline PersistentColumnData Clone(const PersistentColumnData& src) {
  PersistentColumnData out;
  out.type = src.type;
  out.pointers.reserve(src.pointers.size());
  for (const auto& p : src.pointers) {
    out.pointers.emplace_back(CloneDataPointer(p));
  }
  out.validity_pointers.reserve(src.validity_pointers.size());
  for (const auto& p : src.validity_pointers) {
    out.validity_pointers.emplace_back(CloneDataPointer(p));
  }
  out.child_columns.reserve(src.child_columns.size());
  for (const auto& c : src.child_columns) {
    out.child_columns.emplace_back(Clone(c));
  }
  return out;
}

}  // namespace irs
