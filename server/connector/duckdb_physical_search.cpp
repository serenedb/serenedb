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

#include "connector/duckdb_physical_search.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/formats/column/hnsw_index.hpp>
#include <iresearch/index/index_reader.hpp>

#include "basics/assert.h"
#include "connector/search_remove_filter.hpp"

namespace sdb::connector {

namespace {

struct SearchGlobalSourceState : public duckdb::GlobalSourceState {
  SearchResults results;
  size_t current_idx = 0;
  bool search_done = false;
};

void CollectHit(const irs::IndexReader& reader, uint64_t packed_id, float dis,
                SearchResults& results) {
  auto [seg_id, doc_id] = irs::UnpackSegmentWithDoc(packed_id);
  if (seg_id >= reader.size()) {
    return;
  }
  const auto& segment = reader[seg_id];
  const auto* pk_col = segment.column(kPkFieldName);
  if (!pk_col) {
    return;
  }
  auto pk_iter = pk_col->iterator(irs::ColumnHint::Normal);
  if (!pk_iter) {
    return;
  }
  const auto* pk_val = irs::get<irs::PayAttr>(*pk_iter);
  if (!pk_val) {
    return;
  }
  if (pk_iter->seek(doc_id) != doc_id) {
    return;
  }
  auto val = pk_val->value;
  results.pk_keys.emplace_back(reinterpret_cast<const char*>(val.data()),
                               val.size());
  results.distances.push_back(dis);
}

}  // namespace

SereneDBPhysicalVectorSearchBase::SereneDBPhysicalVectorSearchBase(
  duckdb::PhysicalPlan& plan, std::shared_ptr<search::InvertedIndexShard> index,
  std::string field_name, std::vector<float> query_vector,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, duckdb::OnConflictAction on_conflict)
  : duckdb::PhysicalOperator{plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality},
    _index{std::move(index)},
    _field_name{std::move(field_name)},
    _query_vector{std::move(query_vector)},
    _on_conflict{on_conflict} {}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalVectorSearchBase::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  return duckdb::make_uniq<SearchGlobalSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalVectorSearchBase::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& state = input.global_state.Cast<SearchGlobalSourceState>();

  if (!state.search_done) {
    auto snapshot = _index->GetInvertedIndexSnapshot();
    SDB_ASSERT(snapshot, "Search index has no snapshot");
    RunSearch(snapshot->reader, state.results);
    state.search_done = true;
  }

  const auto& results = state.results;

  const size_t n_results = results.pk_keys.size();
  if (state.current_idx >= n_results) {
    chunk.SetCardinality(0);
    return duckdb::SourceResultType::FINISHED;
  }

  const size_t batch_size =
    std::min<size_t>(STANDARD_VECTOR_SIZE, n_results - state.current_idx);

  // Column 0: BLOB — raw PK bytes
  auto& pk_vec = chunk.data[0];
  auto* pk_data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(pk_vec);
  for (size_t i = 0; i < batch_size; ++i) {
    const auto& pk = results.pk_keys[state.current_idx + i];
    pk_data[i] =
      duckdb::StringVector::AddStringOrBlob(pk_vec, pk.data(), pk.size());
  }

  // Column 1 (optional): FLOAT — distance
  if (chunk.ColumnCount() >= 2) {
    auto* dist_data = duckdb::FlatVector::GetDataMutable<float>(chunk.data[1]);
    for (size_t i = 0; i < batch_size; ++i) {
      dist_data[i] = results.distances[state.current_idx + i];
    }
  }

  state.current_idx += batch_size;
  chunk.SetCardinality(static_cast<duckdb::idx_t>(batch_size));

  return state.current_idx >= n_results
           ? duckdb::SourceResultType::FINISHED
           : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

SereneDBPhysicalANNSearch::SereneDBPhysicalANNSearch(
  duckdb::PhysicalPlan& plan, std::shared_ptr<search::InvertedIndexShard> index,
  std::string field_name, std::vector<float> query_vector, size_t top_k,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, duckdb::OnConflictAction on_conflict)
  : SereneDBPhysicalVectorSearchBase{plan,
                                     std::move(index),
                                     std::move(field_name),
                                     std::move(query_vector),
                                     std::move(types),
                                     estimated_cardinality,
                                     on_conflict},
    _top_k{top_k} {}

void SereneDBPhysicalANNSearch::RunSearch(const irs::DirectoryReader& reader,
                                          SearchResults& results) const {
  if (_top_k == 0 || reader.size() == 0) {
    return;
  }

  std::vector<float> dis(_top_k, std::numeric_limits<float>::max());
  std::vector<int64_t> ids(_top_k, -1);

  irs::HNSWSearchInfo info{
    .query = reinterpret_cast<const irs::byte_type*>(_query_vector.data()),
    .top_k = _top_k,
  };

  reader.Search(_field_name, info, dis.data(), ids.data());

  results.pk_keys.reserve(_top_k);
  results.distances.reserve(_top_k);

  for (size_t i = 0; i < _top_k; ++i) {
    if (ids[i] == -1) {
      continue;
    }
    CollectHit(reader, static_cast<uint64_t>(ids[i]), dis[i], results);
  }
}

SereneDBPhysicalRangeSearch::SereneDBPhysicalRangeSearch(
  duckdb::PhysicalPlan& plan, std::shared_ptr<search::InvertedIndexShard> index,
  std::string field_name, std::vector<float> query_vector, float radius,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, duckdb::OnConflictAction on_conflict)
  : SereneDBPhysicalVectorSearchBase{plan,
                                     std::move(index),
                                     std::move(field_name),
                                     std::move(query_vector),
                                     std::move(types),
                                     estimated_cardinality,
                                     on_conflict},
    _radius{radius} {}

void SereneDBPhysicalRangeSearch::RunSearch(const irs::DirectoryReader& reader,
                                            SearchResults& results) const {
  if (reader.size() == 0) {
    return;
  }

  std::vector<float> dis;
  std::vector<int64_t> ids;

  irs::HNSWRangeSearchInfo info{
    .query = reinterpret_cast<const irs::byte_type*>(_query_vector.data()),
    .radius = _radius,
  };

  reader.RangeSearch(_field_name, info, dis, ids);

  results.pk_keys.reserve(ids.size());
  results.distances.reserve(ids.size());

  for (size_t i = 0; i < ids.size(); ++i) {
    CollectHit(reader, static_cast<uint64_t>(ids[i]), dis[i], results);
  }
}

}  // namespace sdb::connector
