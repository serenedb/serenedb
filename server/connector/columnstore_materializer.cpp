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

#include "connector/columnstore_materializer.h"

#include "iresearch/index/index_meta.hpp"
#include "iresearch/store/directory.hpp"

namespace sdb::connector {

ColumnstoreMaterializer::ColumnstoreMaterializer(
  const irs::Directory& dir, const irs::SegmentMeta& meta,
  duckdb::DatabaseInstance& db, std::span<const irs::field_id> column_ids,
  std::span<const duckdb::idx_t> output_slots)
  : _reader{dir, meta.name, db} {
  SDB_ASSERT(column_ids.size() == output_slots.size());
  _bound.reserve(column_ids.size());
  for (size_t i = 0; i < column_ids.size(); ++i) {
    if (const auto* r = _reader.Column(column_ids[i])) {
      _bound.push_back(Binding{
        .reader = r,
        .output_slot = output_slots[i],
        .data_scan = irs::columnstore::ColumnReader::RangeScan{*r, false},
        .validity_scan = irs::columnstore::ColumnReader::RangeScan{*r, true},
      });
    }
  }
}

void ColumnstoreMaterializer::SelectByDocIds(
  std::span<const irs::doc_id_t> doc_ids, duckdb::DataChunk& output,
  duckdb::idx_t output_start) const {
  if (_bound.empty() || doc_ids.empty()) {
    return;
  }
  for (const auto& b : _bound) {
    auto& out_vec = output.data[b.output_slot];
    const auto type_id = b.reader->Type().id();
    if (output_start == 0 && (type_id == duckdb::LogicalTypeId::LIST ||
                              type_id == duckdb::LogicalTypeId::MAP)) {
      duckdb::ListVector::SetListSize(out_vec, 0);
    }
    MaterializeColumnRange(*b.reader, doc_ids, out_vec, output_start);
  }
}

void ColumnstoreMaterializer::Scan(uint64_t start_doc, duckdb::idx_t count,
                                   duckdb::DataChunk& output) {
  if (_bound.empty() || count == 0) {
    return;
  }
  for (auto& b : _bound) {
    auto& out_vec = output.data[b.output_slot];
    const auto type_id = b.reader->Type().id();
    if (type_id == duckdb::LogicalTypeId::LIST ||
        type_id == duckdb::LogicalTypeId::MAP ||
        type_id == duckdb::LogicalTypeId::ARRAY ||
        type_id == duckdb::LogicalTypeId::STRUCT) {
      // Nested types are walked from scratch each Scan; cached cursors
      // only help the primitive leaf path.
      if (type_id == duckdb::LogicalTypeId::LIST ||
          type_id == duckdb::LogicalTypeId::MAP) {
        duckdb::ListVector::SetListSize(out_vec, 0);
      }
      MaterializeColumnRange(*b.reader, cs_internal::IotaRange{start_doc, count},
                             out_vec, 0);
      continue;
    }
    if (b.reader->RowGroupCount() > 0) {
      b.data_scan.Scan(start_doc, count, out_vec, 0);
    }
    if (b.reader->HasValidity()) {
      b.validity_scan.Scan(start_doc, count, out_vec, 0);
    }
  }
}

}  // namespace sdb::connector
