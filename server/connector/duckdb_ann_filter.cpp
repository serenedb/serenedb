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
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/pk_batch_helpers.h"
#include "connector/search_pk_lookup.h"
#include "pg/connection_context.h"

namespace sdb::connector {

ANNFilter::ANNFilter(duckdb::ClientContext& context,
                     const irs::IndexReader& reader,
                     const SereneDBScanBindData& bind_data,
                     const rocksdb::Snapshot* snapshot,
                     rocksdb::Transaction* txn,
                     std::vector<duckdb::idx_t> filter_projected_columns,
                     std::vector<duckdb::LogicalType> filter_types,
                     std::vector<catalog::Column::Id> filter_bind_column_ids,
                     std::vector<duckdb::unique_ptr<duckdb::Expression>> exprs)
  : _context(context),
    _reader(reader),
    _bind_data(bind_data),
    _snapshot(snapshot),
    _txn(txn),
    _filter_projected_columns(std::move(filter_projected_columns)),
    _filter_types(std::move(filter_types)),
    _filter_bind_column_ids(std::move(filter_bind_column_ids)),
    _exprs(std::move(exprs)),
    _executor(context) {
  for (const auto& e : _exprs) {
    _executor.AddExpression(*e);
  }
  duckdb::vector<duckdb::LogicalType> scratch_types(_filter_types.begin(),
                                                    _filter_types.end());
  _scratch.Initialize(duckdb::Allocator::DefaultAllocator(), scratch_types);

  duckdb::vector<duckdb::LogicalType> bool_types(_exprs.size(),
                                                 duckdb::LogicalType::BOOLEAN);
  _bool_out.Initialize(duckdb::Allocator::DefaultAllocator(), bool_types);
}

void InitAnnFilter(
  std::unique_ptr<ANNFilter>& filter, duckdb::ClientContext& context,
  const std::vector<duckdb::unique_ptr<duckdb::Expression>>& filter_expressions,
  const std::vector<catalog::Column::Id>& filter_column_ids, ObjectId index_id,
  const rocksdb::Snapshot* rocks_snapshot,
  const SereneDBScanBindData& bind_data) {
  std::vector<duckdb::idx_t> filter_projection(filter_column_ids.size());
  std::vector<duckdb::LogicalType> filter_types(filter_column_ids.size());
  for (size_t i = 0; i < filter_column_ids.size(); ++i) {
    const auto cat_id = filter_column_ids[i];
    const auto it = absl::c_find(bind_data.column_ids, cat_id);
    if (it == bind_data.column_ids.end()) {
      filter_projection.clear();
      break;
    }
    // Filter scratch slot i = bind-column index for filter_column_ids[i];
    // IndexSource::Materialize projects bind_column_ids[bind_col] onto the
    // matching source column and writes into output.data[i].
    filter_projection[i] =
      static_cast<duckdb::idx_t>(it - bind_data.column_ids.begin());
    filter_types[i] = bind_data.column_types[filter_projection[i]];
  }
  if (filter_projection.empty()) {
    return;
  }

  std::vector<duckdb::unique_ptr<duckdb::Expression>> expr_copies;
  expr_copies.reserve(filter_expressions.size());
  for (const auto& e : filter_expressions) {
    expr_copies.push_back(e->Copy());
  }

  auto& search_snapshot =
    GetSereneDBContext(context).EnsureSearchSnapshot(index_id);

  filter = std::make_unique<ANNFilter>(
    context, search_snapshot.reader, bind_data, rocks_snapshot, /*txn=*/nullptr,
    std::move(filter_projection), std::move(filter_types),
    std::vector<catalog::Column::Id>(bind_data.column_ids.begin(),
                                     bind_data.column_ids.end()),
    std::move(expr_copies));
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

  std::string_view pk = irs::ViewCast<char>(_it.value->value);

  if (!_index_source) {
    _index_source = MakeIndexSource(_context, _bind_data, _snapshot, _txn,
                                    _filter_projected_columns, _filter_types,
                                    _filter_bind_column_ids);
  }
  if (std::holds_alternative<std::monostate>(_pk_batch)) {
    _pk_batch = _index_source->CreatePkBatch();
  }

  std::visit(
    [&](auto& pk_alt) {
      using T = std::decay_t<decltype(pk_alt)>;
      if constexpr (std::is_same_v<T, std::monostate>) {
        SDB_ASSERT(false, "_pk_batch must be initialised");
      } else {
        pk_alt.Reset();
        if constexpr (std::is_same_v<T, PrimaryKeysBytes>) {
          pk_alt.EnsureInit(duckdb::Allocator::DefaultAllocator());
        }
        AppendPrimaryKey(pk_alt, pk);
      }
    },
    _pk_batch);

  _scratch.Reset();
  _index_source->Materialize(_context, _pk_batch, 0, 1, _scratch);
  _scratch.SetCardinality(1);

  _bool_out.Reset();
  _bool_out.SetCardinality(1);
  _executor.Execute(_scratch, _bool_out);

  // TODO(mbkkt) Maybe store as member?
  duckdb::UnifiedVectorFormat fmt;
  for (duckdb::idx_t i = 0; i < _bool_out.ColumnCount(); ++i) {
    auto& vec = _bool_out.data[i];
    vec.ToUnifiedFormat(1, fmt);
    const auto idx = fmt.sel->get_index(0);
    if (!fmt.validity.RowIsValid(idx)) {
      return false;
    }
    if (!duckdb::UnifiedVectorFormat::GetData<bool>(fmt)[idx]) {
      return false;
    }
  }
  return true;
}

}  // namespace sdb::connector
