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

#include "basics/assert.h"
#include "basics/duckdb_engine.h"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"
#include "connector/index_source_factory.h"
#include "connector/pk_batch_helpers.h"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {

void InitAnnFilterContext(
  std::unique_ptr<ANNFilterContext>& filter, duckdb::ClientContext& context,
  const duckdb::Expression* filter_expression,
  const std::vector<catalog::Column::Id>& filter_column_ids,
  const rocksdb::Snapshot* rocks_snapshot,
  const SereneDBScanBindData& bind_data) {
  if (!filter_expression || filter_column_ids.empty()) {
    return;
  }
  containers::FlatHashMap<catalog::Column::Id, size_t> columns_to_indexes;
  for (size_t i = 0; i < bind_data.column_ids.size(); ++i) {
    columns_to_indexes[bind_data.column_ids[i]] = i;
  }
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

  filter = std::make_unique<ANNFilterContext>(ANNFilterContext{
    .context = context,
    .filter_expr = filter_expression->Copy(),
    .filter_types = std::move(filter_types),
    .bind_data = bind_data,
    .rocksdb_snapshot = rocks_snapshot,
    .filter_projection = std::move(filter_projection),
    .filter_column_ids = filter_column_ids,
  });
}

ANNFilter::ANNFilter(const ANNFilterContext& ctx)
  : _ctx{ctx},
    _executor{ctx.context},
    _pk_cursor{duckdb::DatabaseInstance::GetDatabase(ctx.context)} {
  _executor.AddExpression(*ctx.filter_expr);
  duckdb::vector<duckdb::LogicalType> scratch_types{ctx.filter_types.begin(),
                                                    ctx.filter_types.end()};
  _scratch.Initialize(duckdb::Allocator::Get(ctx.context), scratch_types);

  duckdb::vector<duckdb::LogicalType> bool_types{1,
                                                 duckdb::LogicalType::BOOLEAN};
  _bool_out.Initialize(duckdb::Allocator::Get(ctx.context), bool_types);
}

void ANNFilter::Reset(const irs::IndexReader& reader, size_t seg_idx) {
  const auto [cs_reader, pk_col] = SegmentPkColumn(reader, seg_idx);
  SDB_ASSERT(cs_reader && pk_col,
             "ANNFilter::Reset: segment lacks a PK columnstore column");
  _pk_cursor.Reset(*cs_reader, *pk_col);
}

bool ANNFilter::Accept(faiss::idx_t id) const {
  auto [_, doc_id] = irs::UnpackSegmentWithDoc(id);
  // HNSW returns candidates in score order, so per-candidate PK lookups
  // have no useful row-group locality; FetchRow is the right shape here.
  const auto bytes = _pk_cursor.FetchDoc(doc_id);
  const std::string_view pk{reinterpret_cast<const char*>(bytes.data()),
                            bytes.size()};
  if (pk.empty()) {
    return false;
  }

  if (!_index_source) {
    _index_source = MakeIndexSource(
      _ctx.context, _ctx.bind_data, _ctx.rocksdb_snapshot, _ctx.rocksdb_txn,
      _ctx.filter_projection, _ctx.filter_types, _ctx.bind_data.column_ids);
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
  _index_source->Materialize(_ctx.context, _pk_batch, 0, 1, _scratch);
  // SetChildCardinality (not SetCardinality) so each child Vector's flat-size
  // is also updated to 1; ExpressionExecutor on the new DuckDB asserts that
  // the chunk count matches each input Vector's flat-size and otherwise
  // surfaces a `BinaryExecutor: left has 0 rows but right has 1` mismatch.
  _scratch.SetChildCardinality(1);

  _bool_out.Reset();
  _bool_out.SetChildCardinality(1);
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

TextScanFilter::TextScanFilter(const irs::Filter::Query& query)
  : _query{query} {}

void TextScanFilter::Reset(const irs::SubReader& segment) {
  _it = _query.execute({.segment = segment});
  SDB_ASSERT(_it);
}

bool TextScanFilter::Accept(faiss::idx_t id) const {
  auto [_, doc_id] = irs::UnpackSegmentWithDoc(id);
  return _it->seek(doc_id) == doc_id;
}

void CompositeScanFilter::Reset(const irs::IndexReader& reader,
                                size_t seg_idx) {
  if (_text) {
    _text->Reset(reader[seg_idx]);
  }
  if (_ann) {
    _ann->Reset(reader, seg_idx);
  }
}

bool CompositeScanFilter::is_member(faiss::idx_t id) const {
  if (_text && !_text->Accept(id)) {
    return false;
  }
  if (_ann && !_ann->Accept(id)) {
    return false;
  }
  return true;
}

}  // namespace sdb::connector
