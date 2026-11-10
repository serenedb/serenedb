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

#include "connector/duckdb_physical_search_update.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/planner/constraints/bound_not_null_constraint.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catalog/entry/search_table.h"
#include "catalog/transaction.h"
#include "connector/column_id.h"
#include "connector/primary_key.h"
#include "connector/row_position_deduplicator.h"
#include "connector/search_sink_rows.h"
#include "search/search_table_changes.h"
#include "search/store.h"

namespace sdb::connector {
namespace {

struct PatchedColumn {
  irs::field_id field = 0;
  duckdb::LogicalType type;
  duckdb::column_t input = 0;
  duckdb::PhysicalIndex index;
};

struct SearchUpdateGlobalState final : duckdb::GlobalSinkState {
  SearchSinkTable table;
  catalog::SessionTransaction* sdb_txn = nullptr;

  duckdb::vector<duckdb::column_t> new_row_src;
  std::vector<PatchedColumn> patched;

  primary_key::PKColumn rowid;
  std::optional<RowPositionDeduplicator> updated_rows;

  absl::Mutex mutex;
  duckdb::idx_t update_count = 0;
  // RETURNING only: the rows as this statement left them.
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchUpdateLocalState final : duckdb::LocalSinkState {
  explicit SearchUpdateLocalState(uint64_t budget) : rows{budget} {}

  SearchSinkRows rows;
  irs::DocRemovalBuilder positions;
  std::vector<search::UnitPatch> patches;
  duckdb::idx_t update_count = 0;
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchUpdateSourceState final : duckdb::GlobalSourceState {
  duckdb::ColumnDataScanState scan;
};

}  // namespace

SereneDBSearchUpdate::SereneDBSearchUpdate(
  duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
  duckdb::vector<duckdb::PhysicalIndex> columns,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> constraints,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk,
  duckdb::RowIdHandling row_id_handling, bool patch,
  duckdb::idx_t update_column_count)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(table),
    _columns(std::move(columns)),
    _expressions(std::move(expressions)),
    _constraints(std::move(constraints)),
    _return_chunk(return_chunk),
    _patch(patch),
    _update_column_count(update_column_count),
    _row_id_handling(row_id_handling) {}

std::string SereneDBSearchUpdate::GetName() const { return "UPDATE"; }

duckdb::InsertionOrderPreservingMap<std::string>
SereneDBSearchUpdate::ParamsToString() const {
  duckdb::InsertionOrderPreservingMap<std::string> result;
  result["Table"] = _table.name.GetIdentifierName();
  result["Row Ids"] = duckdb::EnumUtil::ToString(_row_id_handling);
  if (_patch) {
    std::vector<std::string_view> names;
    names.reserve(_update_column_count);
    for (duckdb::idx_t i = 0; i < _update_column_count; ++i) {
      names.push_back(
        _table.GetColumns().GetColumn(_columns[i]).Name().GetIdentifierName());
    }
    result["Patched Columns"] = absl::StrJoin(names, ", ");
  }
  return result;
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchUpdate::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchUpdateGlobalState>();
  auto& transaction = catalog::SessionOf(context, _table.catalog);

  auto& target = state->table;
  target.store = _table.Storage();
  target.catalog = &_table.catalog;
  target.table_id = _table.oid;
  transaction.SearchTxn().RegisterWriter(target.store, _table);
  _table.RequireCurrentSchema();

  const auto& columns = _table.GetColumns();
  target.column_ids.reserve(columns.PhysicalColumnCount());
  target.types.reserve(columns.PhysicalColumnCount());
  for (const auto& column : columns.Physical()) {
    target.column_ids.emplace_back(TableColumnId(column));
    target.types.push_back(column.Type());
  }
  target.buffer_bytes =
    duckdb::DBConfig::GetConfig(context).options.checkpoint_wal_size;

  const auto p = target.column_ids.size();
  const bool whole_row = !_patch || _return_chunk;
  SDB_ASSERT(!whole_row || _columns.size() == p,
             "search UPDATE must project every table column");
  if (whole_row) {
    state->new_row_src.assign(p, duckdb::DConstants::INVALID_INDEX);
  }
  for (size_t i = 0; i < _columns.size(); ++i) {
    const auto index = _columns[i].index;
    SDB_ASSERT(index < p,
               "projected update column is not a stored table column");
    SDB_ASSERT(
      _expressions[i]->GetExpressionType() == duckdb::ExpressionType::BOUND_REF,
      "search UPDATE expects every SET value to be projected");
    const auto input =
      _expressions[i]->Cast<duckdb::BoundReferenceExpression>().Index();
    if (_patch && i < _update_column_count) {
      state->patched.push_back({.field = target.column_ids[index],
                                .type = target.types[index],
                                .input = input,
                                .index = _columns[i]});
    }
    if (whole_row) {
      state->new_row_src[index] = input;
    }
  }

  const auto& input_types = children[0].get().GetTypes();
  SDB_ASSERT(_table.GetRowIdColumns().size() == 1 && !input_types.empty());
  state->rowid = {input_types.size() - 1, input_types.back()};
  if (_row_id_handling != duckdb::RowIdHandling::ASSUME_UNIQUE) {
    state->updated_rows.emplace(
      transaction.SearchTxn().PinnedReader(context, *target.store));
  }

  state->sdb_txn = &transaction;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }
  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBSearchUpdate::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
  const auto threads =
    context.pipeline ? context.pipeline->GetMaxThreads() : duckdb::idx_t{1};
  auto lstate = duckdb::make_uniq<SearchUpdateLocalState>(
    gstate.table.buffer_bytes / std::max<duckdb::idx_t>(1, threads));
  if (_return_chunk) {
    lstate->returned.emplace(context.client, GetTypes());
  }
  return lstate;
}

duckdb::SinkResultType SereneDBSearchUpdate::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchUpdateGlobalState>();
  auto& lstate = input.local_state.Cast<SearchUpdateLocalState>();
  if (gstate.updated_rows &&
      gstate.updated_rows->KeepFirst(chunk, gstate.rowid.input_col_idx) == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  const auto num_rows = chunk.size();
  if (const auto& progress = duckdb::ClientConfig::GetConfig(context.client)
                               .sink_progress_callback) {
    progress(num_rows, chunk.GetAllocationSize());
  }

  if (_patch) {
    for (const auto& constraint : _constraints) {
      if (constraint->type != duckdb::ConstraintType::NOT_NULL) {
        continue;
      }
      const auto& index =
        constraint->Cast<duckdb::BoundNotNullConstraint>().index;
      const auto it = absl::c_find_if(gstate.patched, [&](const auto& column) {
        return column.index == index;
      });
      if (it == gstate.patched.end()) {
        continue;
      }
      duckdb::DataChunk row;
      row.InitializeEmpty(gstate.table.types);
      row.data[index.index].Reference(chunk.data[it->input]);
      row.SetCardinalityUnsafe(num_rows);
      duckdb::DataTable::VerifyRowConstraint(context.client, _table,
                                             *constraint, row);
    }
    const auto& reader = gstate.sdb_txn->SearchTxn().PinnedReader(
      context.client, *gstate.table.store);
    const auto positions =
      chunk.data[gstate.rowid.input_col_idx].Values<int64_t>();
    std::vector<std::pair<uint64_t, duckdb::sel_t>> order;
    order.reserve(num_rows);
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      order.emplace_back(static_cast<uint64_t>(positions.GetValueUnsafe(i)),
                         static_cast<duckdb::sel_t>(i));
    }
    absl::c_sort(order);
    std::vector<uint64_t> rows;
    duckdb::SelectionVector sel{num_rows};
    for (size_t i = 0; i < order.size();) {
      const auto index = RowPositionIndex(order[i].first);
      rows.clear();
      for (; i < order.size() && RowPositionIndex(order[i].first) == index;
           ++i) {
        sel.set_index(rows.size(), order[i].second);
        rows.push_back(RowPositionDoc(order[i].first) - irs::doc_limits::min());
      }
      const auto unit =
        irs::utils::downCast<irs::SegmentReader>(reader[index]).GetImpl()->Id();
      for (const auto& column : gstate.patched) {
        const duckdb::Vector values{chunk.data[column.input], sel, rows.size()};
        search::AddUnitPatch(lstate.patches, unit, column.field, column.type,
                             rows, values);
      }
    }
    if (lstate.returned) {
      duckdb::DataChunk new_row;
      new_row.InitializeEmpty(gstate.table.types);
      new_row.ReferenceColumns(chunk, gstate.new_row_src);
      lstate.returned->Append(new_row);
    }
    lstate.update_count += num_rows;
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  search::AddRowPositions(lstate.positions,
                          chunk.data[gstate.rowid.input_col_idx]);

  duckdb::DataChunk new_row;
  new_row.InitializeEmpty(gstate.table.types);
  new_row.ReferenceColumns(chunk, gstate.new_row_src);
  duckdb::DataTable::VerifyRowConstraints(context.client, _table, _constraints,
                                          new_row);
  lstate.rows.Append(context.client, gstate.table, new_row);

  if (lstate.returned) {
    lstate.returned->Append(new_row);
  }
  lstate.update_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBSearchUpdate::Combine(
  duckdb::ExecutionContext& context,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<SearchUpdateGlobalState>();
  auto& lstate = input.local_state.Cast<SearchUpdateLocalState>();
  lstate.rows.Flush();
  absl::MutexLock lock{&gstate.mutex};
  auto& search_txn = gstate.sdb_txn->SearchTxn();
#ifdef SDB_DEV
  SDB_ASSERT(!gstate.updated_rows ||
             gstate.updated_rows->Covers(
               search_txn.PinnedReader(context.client, *gstate.table.store)));
#endif
  lstate.rows.HandOver(context.client, gstate.table, search_txn);
  if (!lstate.positions.Empty()) {
    search_txn.AddRemoval(context.client, gstate.table.store,
                          std::move(lstate.positions));
  }
  if (!lstate.patches.empty()) {
    search_txn.AddPatches(gstate.table.store, std::move(lstate.patches));
  }
  gstate.update_count += lstate.update_count;
  if (lstate.returned && lstate.returned->Count() != 0) {
    gstate.returned->Combine(*lstate.returned);
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchUpdate::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  auto state = duckdb::make_uniq<SearchUpdateSourceState>();
  auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
  if (gstate.returned) {
    gstate.returned->InitializeScan(state->scan);
  }
  return state;
}

duckdb::SourceResultType SereneDBSearchUpdate::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchUpdateSourceState>();
  auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
  if (gstate.returned) {
    gstate.returned->Scan(source.scan, chunk);
    return chunk.size() == 0 ? duckdb::SourceResultType::FINISHED
                             : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.update_count));
  return duckdb::SourceResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBSearchUpdate::Finalize(
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<SearchUpdateGlobalState>();
  SDB_PARK_ONCE_ON_FAILURE("search_update_before_key_check");
  if (_patch) {
    gstate.sdb_txn->SearchTxn().CheckPatches(*gstate.table.store);
  } else {
    gstate.sdb_txn->SearchTxn().CheckKeys(context, *gstate.table.store);
  }
  return duckdb::SinkFinalizeType::READY;
}

}  // namespace sdb::connector
