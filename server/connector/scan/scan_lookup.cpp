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

#include <absl/algorithm/container.h>

#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/index/hit_batcher.hpp>
#include <iresearch/index/index_source.hpp>

#include "connector/column_id.h"
#include "connector/index_source_factory.h"
#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"
#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"
#include "connector/term_dict.h"

namespace sdb::connector {
namespace {

bool LooksUpByPk(const ScanBindData& bind) {
  return bind.relation.kind == ScanEntryKind::InvertedIndex &&
         bind.relation.ScannedIndex().pk.column == catalog::PkColumnKind::Has &&
         (!bind.view || bind.view->fast_path);
}

struct LookupLocalState final : duckdb::LocalTableFunctionState {
  std::vector<duckdb::idx_t> pk_slots;
  std::vector<duckdb::idx_t> pk_columns;
  std::vector<duckdb::LogicalType> pk_types;
  std::vector<duckdb::idx_t> store_slots;
  std::vector<irs::ColumnstoreProjection> store_projections;
  search::InvertedIndexSnapshotPtr snapshot;
  std::unique_ptr<irs::IndexSource> source;
  std::unique_ptr<irs::HitBatcher> batcher;
  duckdb::DataChunk pks;
  duckdb::DataChunk pk_out;
  duckdb::DataChunk stored;
  duckdb::DataChunk batch;
  std::vector<duckdb::idx_t> order;
  std::vector<irs::ScoreDoc> hits;
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
};

duckdb::unique_ptr<duckdb::LocalTableFunctionState> LookupInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState*) {
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  auto state = duckdb::make_uniq<LookupLocalState>();
  std::vector<duckdb::LogicalType> store_types;
  const auto output_count = input.projection_ids.empty()
                              ? input.column_indexes.size()
                              : input.projection_ids.size();
  for (duckdb::idx_t c = 0; c < output_count; ++c) {
    const auto& column_index =
      input
        .column_indexes[input.projection_ids.empty() ? c
                                                     : input.projection_ids[c]];
    const auto bind_col = column_index.GetPrimaryIndex();
    auto type = column_index.HasType() ? column_index.GetScanType()
                                       : bind.columns.types[bind_col];
    if (IsStoreServed(bind, column_index)) {
      state->store_projections.push_back(
        {.output_slot = state->store_slots.size(),
         .column_id = bind.columns.ids[bind_col]});
      state->store_slots.push_back(c);
      store_types.push_back(std::move(type));
    } else {
      state->pk_slots.push_back(c);
      state->pk_columns.push_back(bind_col);
      state->pk_types.push_back(std::move(type));
    }
  }
  if (!state->pk_slots.empty()) {
    state->pk_out.Initialize(context.client, state->pk_types);
  }
  if (!state->store_slots.empty()) {
    state->stored.Initialize(context.client, store_types);
    state->batch.Initialize(context.client, store_types);
  }
  state->batcher = std::make_unique<irs::HitBatcher>(
    state->store_projections,
    state->pk_slots.empty() ? irs::field_limits::invalid()
                            : term_dict::kPKFieldId,
    false);
  return state;
}

void AppendBatch(duckdb::ClientContext& context, LookupLocalState& state,
                 duckdb::idx_t& appended) {
  state.batch.Reset();
  const auto emitted = state.batcher->Emit(state.batch);
  for (duckdb::idx_t c = 0; c < state.batch.ColumnCount(); ++c) {
    duckdb::VectorOperations::Copy(state.batch.data[c], state.stored.data[c],
                                   emitted.count, 0, appended);
  }
  if (emitted.pk) {
    if (state.pks.ColumnCount() == 0) {
      state.pks.Initialize(context, std::span{&emitted.pk->GetType(), 1});
    }
    duckdb::VectorOperations::Copy(*emitted.pk, state.pks.data[0],
                                   emitted.count, 0, appended);
  }
  appended += emitted.count;
}

void FetchByAddress(duckdb::ClientContext& context, LookupLocalState& state) {
  const auto& reader = state.snapshot->reader;
  duckdb::idx_t appended = 0;
  std::span<const irs::ScoreDoc> hits = state.hits;
  while (!hits.empty()) {
    const auto seg = hits.front().segment_idx;
    const auto in_segment = static_cast<size_t>(
      absl::c_find_if(hits,
                      [&](const auto& hit) { return hit.segment_idx != seg; }) -
      hits.begin());
    state.batcher->BeginSegment(seg, reader[seg].GetColReader(), &context);
    FeedHits(*state.batcher, hits.first(in_segment), false,
             [&] { AppendBatch(context, state, appended); });
    hits = hits.subspan(in_segment);
  }
  SDB_ASSERT(appended == state.hits.size());
}

duckdb::OperatorResultType LookupFunction(duckdb::ExecutionContext& context,
                                          duckdb::TableFunctionInput& data,
                                          duckdb::DataChunk& input,
                                          duckdb::DataChunk& output) {
  const auto& bind = data.bind_data->Cast<ScanBindData>();
  auto& state = data.local_state->Cast<LookupLocalState>();
  if (!state.snapshot) {
    state.snapshot = bind.Snapshot(context.client);
  }
  const auto count = input.size();
  const auto addresses = input.data[0].Values<int64_t>();
  state.order.resize(count);
  absl::c_iota(state.order, 0);
  absl::c_sort(state.order, [&](duckdb::idx_t a, duckdb::idx_t b) {
    return addresses[a].GetValue() < addresses[b].GetValue();
  });
  state.hits.resize(count);
  for (duckdb::idx_t r = 0; r < count; ++r) {
    const auto address = addresses[state.order[r]].GetValue();
    state.hits[r] = {.doc = static_cast<irs::doc_id_t>(address),
                     .segment_idx = static_cast<uint32_t>(address >> 32)};
  }
  state.stored.Reset();
  FetchByAddress(context.client, state);
  auto found = count;
  if (state.pk_slots.empty()) {
    for (duckdb::idx_t r = 0; r < count; ++r) {
      state.sel.set_index(r, r);
      data.pk_survivors[r] = state.order[r];
    }
  } else {
    if (!state.source) {
      state.source =
        MakeIndexSource(context.client, bind, *state.snapshot, state.pk_columns,
                        state.pk_types, bind.columns.ids, nullptr);
    }
    state.pk_out.Reset();
    found = state.source->Materialize(context.client, state.pks.data[0], count,
                                      state.pk_out);
    const auto survivors = state.source->Survivors();
    for (duckdb::idx_t r = 0; r < found; ++r) {
      state.sel.set_index(r, survivors[r]);
      data.pk_survivors[r] = state.order[survivors[r]];
    }
  }
  output.SetChildCardinality(found);
  for (duckdb::idx_t i = 0; i < state.pk_slots.size(); ++i) {
    output.data[state.pk_slots[i]].Reference(state.pk_out.data[i]);
  }
  for (duckdb::idx_t i = 0; i < state.store_slots.size(); ++i) {
    output.data[state.store_slots[i]].Slice(state.stored.data[i], state.sel,
                                            found);
  }
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> LookupToStringValue(
  duckdb::TableFunctionToStringInput& input) {
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> result;
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  if (!bind.lookup.label.empty()) {
    result.insert("Lookup", bind.lookup.label);
  }
  return result;
}

}  // namespace

duckdb::vector<duckdb::column_t> IResearchGetRowIdColumns(
  duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData>) {
  return {kColumnIdentifierDocAddress};
}

bool IResearchSupportsLookup(duckdb::ClientContext&,
                             const duckdb::LogicalGet& get,
                             duckdb::vector<bool>& fetchable,
                             bool& filters_rows) {
  const auto& bind = get.bind_data->Cast<ScanBindData>();
  filters_rows = filters_rows || !bind.IsMatchAll();
  if (bind.ts_dict.Active()) {
    return false;
  }
  const auto& column_ids = get.GetColumnIds();
  if (bind.score.HasTopK() &&
      (bind.score.vector ||
       !absl::c_any_of(get.table_filters, [&](const auto& entry) {
         return IsLookupServed(bind, column_ids[entry.GetIndex()]);
       }))) {
    return false;
  }
  for (duckdb::idx_t i = 0; i < column_ids.size(); ++i) {
    fetchable[i] =
      fetchable[i] &&
      ((LooksUpByPk(bind) && IsLookupServed(bind, column_ids[i])) ||
       IsStoreServed(bind, column_ids[i]));
  }
  return true;
}

duckdb::TableFunction IResearchGetLookupFunction(duckdb::ClientContext&,
                                                 const duckdb::LogicalGet&) {
  duckdb::TableFunction lookup;
  lookup.init_local = LookupInitLocal;
  lookup.in_out_function = LookupFunction;
  lookup.to_string_value = LookupToStringValue;
  return lookup;
}

}  // namespace sdb::connector
