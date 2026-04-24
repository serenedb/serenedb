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

#include "connector/duckdb_ann_filter.h"

#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/row_materializer.h"
#include "connector/search_pk_lookup.h"
#include "pg/connection_context.h"

namespace sdb::connector {

void InitAnnFilter(
  std::unique_ptr<ANNFilter>& filter, duckdb::ClientContext& context,
  const std::vector<duckdb::unique_ptr<duckdb::Expression>>& filter_expressions,
  const std::vector<catalog::Column::Id>& filter_column_ids, ObjectId index_id,
  const rocksdb::Snapshot* rocks_snapshot,
  const SereneDBScanBindData& bind_data) {
  std::vector<duckdb::idx_t> filter_projection(filter_column_ids.size());
  std::vector<duckdb::LogicalType> filter_types(filter_column_ids.size());
  for (size_t i = 0; i < filter_column_ids.size(); ++i) {
    filter_projection[i] = i;
    const auto cat_id = filter_column_ids[i];
    const auto it = std::find(bind_data.column_ids.begin(),
                              bind_data.column_ids.end(), cat_id);
    if (it == bind_data.column_ids.end()) {
      filter_projection.clear();
      break;
    }
    filter_types[i] = bind_data.column_types[it - bind_data.column_ids.begin()];
  }
  if (!filter_projection.empty()) {
    auto filter_mat = MakeRowMaterializer(
      context, bind_data, rocks_snapshot, /*all_pks=*/{}, filter_projection,
      filter_types, filter_column_ids, nullptr);

    // Deep-copy the stashed expressions so the bind_data copy survives
    // subsequent query invocations that share this plan.
    std::vector<duckdb::unique_ptr<duckdb::Expression>> expr_copies;
    expr_copies.reserve(filter_expressions.size());
    for (const auto& e : filter_expressions) {
      expr_copies.push_back(e->Copy());
    }

    auto& search_snapshot =
      GetSereneDBContext(context).EnsureSearchSnapshot(index_id);

    filter = std::make_unique<ANNFilter>(
      context, search_snapshot.reader, std::move(filter_mat),
      std::move(expr_copies), std::move(filter_types));
  }
}

bool ANNFilter::is_member(faiss::idx_t id) const {
  auto [seg_id, doc_id] = irs::UnpackSegmentWithDoc(id);
  if (_it_segment_id != seg_id || !_it.iter) {
    if (!OpenSegmentPkIterator(_reader[seg_id], _it)) {
      return false;
    }
    _it_segment_id = seg_id;
  } else if (_it.iter->value() > doc_id) {
    _it.iter->reset();
  }
  if (_it.iter->seek(doc_id) != doc_id) {
    return false;
  }

  auto val = _it.value->value;

  std::string_view pk{reinterpret_cast<const char*>(val.data()), val.size()};

  _scratch.Reset();
  std::array<std::string_view, 1> pks{pk};
  _materializer->Materialize(pks, _scratch);
  _scratch.SetCardinality(1);

  _bool_out.Reset();
  _bool_out.SetCardinality(1);
  _executor.Execute(_scratch, _bool_out);

  for (duckdb::idx_t i = 0; i < _bool_out.ColumnCount(); ++i) {
    auto& vec = _bool_out.data[i];
    auto value = vec.GetValue(0);
    if (value.IsNull()) {
      return false;
    }
    if (!duckdb::BooleanValue::Get(value)) {
      return false;
    }
  }
  return true;
}

}  // namespace sdb::connector
