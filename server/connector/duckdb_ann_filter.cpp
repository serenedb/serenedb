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

#include <algorithm>
#include <array>

#include "basics/assert.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_function.h"
#include "connector/lookup.h"
#include "connector/search_pk_lookup.h"
#include "pg/connection_context.h"

namespace sdb::connector {

bool CompositeScanFilter::is_member(faiss::idx_t id) const {
  return absl::c_all_of(_filters,
                        [id](const auto& f) { return f->Accept(id); });
}

void InitAnnFilterContext(std::unique_ptr<ANNFilterContext>& filter,
                          duckdb::ClientContext& context,
                          const VectorSearchScan& scan,
                          const rocksdb::Snapshot* rocks_snapshot,
                          const SereneDBScanBindData& bind_data) {
  if (!scan.filter_expression || scan.filter_column_ids.empty()) {
    return;
  }
  containers::FlatHashMap<catalog::Column::Id, size_t> columns_to_indexes;
  for (size_t i = 0; i < bind_data.column_ids.size(); ++i) {
    columns_to_indexes[bind_data.column_ids[i]] = i;
  }
  std::vector<duckdb::idx_t> filter_projection(scan.filter_column_ids.size());
  std::vector<duckdb::LogicalType> filter_types(scan.filter_column_ids.size());
  for (size_t i = 0; i < scan.filter_column_ids.size(); ++i) {
    filter_projection[i] = i;
    const auto cid = scan.filter_column_ids[i];
    auto it = columns_to_indexes.find(cid);
    SDB_ASSERT(it != columns_to_indexes.end());
    SDB_ASSERT(it->second < bind_data.column_types.size());
    filter_types[i] = bind_data.column_types[it->second];
  }

  GetSereneDBContext(context).EnsureSearchSnapshot(scan.index_id);

  filter = std::make_unique<ANNFilterContext>(ANNFilterContext{
    .context = context,
    .scan = scan,
    .bind_data = bind_data,
    .rocksdb_snapshot = rocks_snapshot,
    .filter_types = std::move(filter_types),
    .filter_projection = std::move(filter_projection),
  });
}

ANNFilter::ANNFilter(const ANNFilterContext& ctx, const irs::SubReader& segment)
  : _ctx{ctx}, _segment{segment}, _executor{ctx.context} {
  _executor.AddExpression(*ctx.scan.filter_expression);
  duckdb::vector<duckdb::LogicalType> scratch_types{ctx.filter_types.begin(),
                                                    ctx.filter_types.end()};
  _scratch.Initialize(duckdb::Allocator::Get(ctx.context), scratch_types);

  duckdb::vector<duckdb::LogicalType> bool_types{1,
                                                 duckdb::LogicalType::BOOLEAN};
  _bool_out.Initialize(duckdb::Allocator::Get(ctx.context), bool_types);

  auto opened = OpenSegmentPkIterator(_segment, _it);
  SDB_ASSERT(opened);
}

bool ANNFilter::Accept(faiss::idx_t id) const {
  SDB_ASSERT(_it);
  auto [_, doc_id] = irs::UnpackSegmentWithDoc(id);
  if (_it.iter->value() > doc_id) {
    _it.iter->reset();
  }
  if (_it.iter->seek(doc_id) != doc_id) {
    return false;
  }

  std::string_view pk = irs::ViewCast<char>(_it.value->value);
  std::array<std::string_view, 1> pks{pk};

  _scratch.Reset();
  // LookupRows fills only real-column slots; ANNFilter projects exclusively
  // real bind columns (no rowid/score/etc), so no virtual-slot cleanup needed.
  LookupRows(_ctx.context, _ctx.bind_data, _ctx.rocksdb_snapshot,
             _ctx.filter_projection, _ctx.filter_types,
             _ctx.scan.filter_column_ids, nullptr, pks, _file_lookup_session,
             _scratch);
  _scratch.SetCardinality(1);

  _bool_out.Reset();
  _bool_out.SetCardinality(1);
  _executor.Execute(_scratch, _bool_out);

  auto value = _bool_out.data[0].GetValue(0);
  return !value.IsNull() && duckdb::BooleanValue::Get(value);
}

TextScanFilter::TextScanFilter(const irs::Filter::Query& proxy_query,
                               const irs::SubReader& segment) {
  _it = proxy_query.execute({.segment = segment});
}

bool TextScanFilter::Accept(faiss::idx_t id) const {
  auto [_, doc_id] = irs::UnpackSegmentWithDoc(id);
  return _it->seek(doc_id) == doc_id;
}

}  // namespace sdb::connector
