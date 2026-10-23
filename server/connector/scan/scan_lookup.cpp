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

#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/index/index_source.hpp>

#include "connector/column_id.h"
#include "connector/index_source_factory.h"
#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"
#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"
#include "connector/view_fast_path.h"
#include "query/config.h"

namespace sdb::connector {
namespace {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> LookupInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  auto state = duckdb::make_uniq<ScanGlobalState>();
  InitScanState(*state, &context, bind, input);
  state->scan = &bind;
  state->snapshot = bind.Snapshot(context);
  state->reader = &state->snapshot->reader;
  state->total_segments = state->reader->size();
  state->workers =
    duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads();
  ClassifyColumnstoreProjections(*state, bind);
  return state;
}

struct LookupLocalState final : duckdb::LocalTableFunctionState,
                                FetchLocalState {
  duckdb::DataChunk tmp;
  duckdb::DataChunk fetched;
  std::unique_ptr<duckdb::Vector> pk;
  std::vector<duckdb::idx_t> order;
  std::vector<irs::ScoreDoc> hits;
};

duckdb::unique_ptr<duckdb::LocalTableFunctionState> LookupInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState* global_state) {
  const auto& g = global_state->Cast<ScanGlobalState>();
  auto state = duckdb::make_uniq<LookupLocalState>();
  state->tmp.Initialize(context.client, g.projected_types);
  state->fetched.Initialize(context.client, g.projected_types);
  return state;
}

duckdb::OperatorResultType LookupFunction(duckdb::ExecutionContext& context,
                                          duckdb::TableFunctionInput& data,
                                          duckdb::DataChunk& input,
                                          duckdb::DataChunk& output) {
  auto& g = data.global_state->Cast<ScanGlobalState>();
  auto& l = data.local_state->Cast<LookupLocalState>();
  const auto count = input.size();
  const auto addresses = input.data[0].Values<int64_t>();
  l.order.resize(count);
  absl::c_iota(l.order, 0);
  absl::c_sort(l.order, [&](duckdb::idx_t a, duckdb::idx_t b) {
    return addresses[a].GetValue() < addresses[b].GetValue();
  });
  l.hits.resize(count);
  for (duckdb::idx_t r = 0; r < count; ++r) {
    const auto address = addresses[l.order[r]].GetValue();
    l.hits[r] = {.doc = static_cast<irs::doc_id_t>(address),
                 .segment_idx = static_cast<uint32_t>(address >> 32)};
  }
  l.fetched.Reset();
  duckdb::idx_t appended = 0;
  std::span<const irs::ScoreDoc> hits = l.hits;
  while (!hits.empty()) {
    const auto seg = hits.front().segment_idx;
    const auto in_segment = static_cast<size_t>(
      absl::c_find_if(hits,
                      [&](const auto& hit) { return hit.segment_idx != seg; }) -
      hits.begin());
    AppendHits(context.client, g, l, l.tmp, seg, hits.first(in_segment), true,
               l.fetched, l.pk, appended);
    hits = hits.subspan(in_segment);
  }
  auto found = count;
  if (g.needs_lookup) {
    EnsureIndexSource(context.client, g, l);
    found =
      l.index_source->Materialize(context.client, *l.pk, count, l.fetched);
    const auto survivors = l.index_source->Survivors();
    for (duckdb::idx_t r = 0; r < found; ++r) {
      data.pk_survivors[r] = l.order[survivors[r]];
    }
  } else {
    absl::c_copy(l.order, data.pk_survivors.begin());
  }
  for (duckdb::idx_t c = 0; c < output.ColumnCount(); ++c) {
    output.data[c].Reference(l.fetched.data[g.output_projection_ids.empty()
                                              ? c
                                              : g.output_projection_ids[c]]);
  }
  output.SetChildCardinality(found);
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

struct SourceLookupLocalState final : duckdb::LocalTableFunctionState {
  search::InvertedIndexSnapshotPtr snapshot;
  std::unique_ptr<irs::IndexSource> source;
};

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SourceLookupInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState*) {
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  auto state = duckdb::make_uniq<SourceLookupLocalState>();
  std::vector<duckdb::idx_t> columns;
  std::vector<duckdb::LogicalType> types;
  const auto output_count = input.projection_ids.empty()
                              ? input.column_indexes.size()
                              : input.projection_ids.size();
  for (duckdb::idx_t c = 0; c < output_count; ++c) {
    const auto& column_index =
      input
        .column_indexes[input.projection_ids.empty() ? c
                                                     : input.projection_ids[c]];
    const auto bind_col = column_index.GetPrimaryIndex();
    columns.push_back(bind_col);
    types.push_back(column_index.HasType() ? column_index.GetScanType()
                                           : bind.columns.types[bind_col]);
  }
  state->snapshot = bind.Snapshot(context.client);
  state->source = MakeIndexSource(context.client, bind, *state->snapshot,
                                  columns, types, bind.columns.ids);
  return state;
}

duckdb::OperatorResultType SourceLookupFunction(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInput& data,
  duckdb::DataChunk& input, duckdb::DataChunk& output) {
  auto& state = data.local_state->Cast<SourceLookupLocalState>();
  input.Flatten();
  const auto found = state.source->Materialize(context.client, input.data[0],
                                               input.size(), output);
  absl::c_copy(state.source->Survivors().first(found),
               data.pk_survivors.begin());
  output.SetChildCardinality(found);
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

duckdb::TableFunction SourceGetLookupFunction(duckdb::ClientContext&,
                                              const duckdb::LogicalGet&) {
  duckdb::TableFunction lookup;
  lookup.init_local = SourceLookupInitLocal;
  lookup.in_out_function = SourceLookupFunction;
  lookup.to_string_value = LookupToStringValue;
  return lookup;
}

}  // namespace

duckdb::vector<duckdb::column_t> IResearchGetRowIdColumns(
  duckdb::ClientContext&, duckdb::optional_ptr<duckdb::FunctionData>) {
  return {kColumnIdentifierDocAddress};
}

bool IResearchSupportsLookup(duckdb::ClientContext& context,
                             const duckdb::LogicalGet& get,
                             duckdb::TableFunctionLookupInfo& info) {
  const auto& bind = get.bind_data->Cast<ScanBindData>();
  info.filters_rows = !bind.IsMatchAll();
  info.applies_limit = bind.score.HasTopK();
  if (bind.ts_dict.Active()) {
    return false;
  }
  const auto by_pk = LooksUpByPk(bind);
  const auto& column_ids = get.GetColumnIds();
  for (duckdb::idx_t i = 0; i < column_ids.size(); ++i) {
    info.from_source[i] = by_pk && IsLookupServed(bind, column_ids[i]);
    info.fetchable[i] =
      info.fetchable[i] &&
      (info.from_source[i] || IsStoreServed(bind, column_ids[i]));
  }
  if (by_pk) {
    info.source_row_id_columns = {kColumnIdentifierGeneratedPk};
    info.source_row_id_names = {
      bind.view ? FormatSourceRowId(*bind.view->fast_path) : "rowid"};
    info.prefer_source_lookup =
      GetBoolSetting(context, "sdb_lookup_join_source_hop");
  }
  return true;
}

duckdb::unique_ptr<duckdb::LogicalGet> IResearchGetLookupSource(
  duckdb::ClientContext&, const duckdb::LogicalGet& get,
  const duckdb::vector<duckdb::ProjectionIndex>& columns,
  duckdb::TableIndex table_index) {
  duckdb::TableFunction function{"index_source", {}, nullptr};
  function.get_lookup_function = SourceGetLookupFunction;
  function.projection_pushdown = true;
  auto source = duckdb::make_uniq<duckdb::LogicalGet>(
    table_index, duckdb::BoundTableFunction{function}, get.bind_data->Copy(),
    get.returned_types, get.names, duckdb::virtual_column_map_t{});
  for (const auto column : columns) {
    source->GetMutableColumnIds().push_back(get.GetColumnIds()[column]);
  }
  return source;
}

duckdb::TableFunction IResearchGetLookupFunction(duckdb::ClientContext&,
                                                 const duckdb::LogicalGet&) {
  duckdb::TableFunction lookup;
  lookup.init_global = LookupInitGlobal;
  lookup.init_local = LookupInitLocal;
  lookup.in_out_function = LookupFunction;
  lookup.to_string_value = IndexLookupToStringValue;
  return lookup;
}

}  // namespace sdb::connector
