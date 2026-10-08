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

#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

struct SearchUpdateGlobalState final : duckdb::GlobalSinkState {
  std::shared_ptr<search::SearchTable> search_table;
  query::Transaction* sdb_txn = nullptr;

  std::vector<ColumnId> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  duckdb::vector<duckdb::column_t> new_row_src;
  duckdb::optional_ptr<duckdb::SequenceCatalogEntry> generated_pk_seq;

  std::vector<primary_key::PKColumn> old_pk_columns;

  std::shared_lock<std::shared_mutex> table_lock;
  uint64_t write_buffer_max_bytes = 0;
  duckdb::idx_t update_count = 0;
  // RETURNING only: the rows as this statement left them.
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
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(table),
    _columns(std::move(columns)),
    _expressions(std::move(expressions)),
    _return_chunk(return_chunk) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchUpdate::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchUpdateGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);

  state->search_table = _table.Storage();
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};
  conn_ctx.SearchTxn().RegisterWriter(state->search_table, _table);

  const auto& columns = _table.GetColumns();
  state->column_ids.reserve(columns.LogicalColumnCount());
  for (const auto& column : columns.Logical()) {
    state->column_ids.emplace_back(column.Oid());
  }
  state->chunk_types = columns.GetColumnTypes();
  state->write_buffer_max_bytes = state->search_table->GetWriteBufferMaxBytes();

  const auto p = state->column_ids.size();
  state->new_row_src.assign(p, duckdb::DConstants::INVALID_INDEX);
  SDB_ASSERT(_columns.size() == p,
             "search UPDATE must project every non-generated-PK column");
  for (size_t i = 0; i < _columns.size(); ++i) {
    const auto index = _columns[i].index;
    SDB_ASSERT(index < p,
               "projected update column is not a stored table column");
    SDB_ASSERT(
      _expressions[i]->GetExpressionType() == duckdb::ExpressionType::BOUND_REF,
      "search UPDATE expects every SET value to be projected");
    state->new_row_src[index] =
      _expressions[i]->Cast<duckdb::BoundReferenceExpression>().Index();
  }

  const auto row_ids = _table.GetRowIdColumns().size();
  const auto& input_types = children[0].get().GetTypes();
  const auto width = input_types.size();
  SDB_ASSERT(row_ids <= width);
  state->old_pk_columns.reserve(row_ids);
  for (auto i = width - row_ids; i < width; ++i) {
    state->old_pk_columns.emplace_back(i, input_types[i]);
  }

  state->generated_pk_seq = _table.GeneratedPkSequence(context);
  SDB_ASSERT(state->generated_pk_seq);

  state->sdb_txn = &conn_ctx;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }
  return state;
}

duckdb::SinkResultType SereneDBSearchUpdate::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchUpdateGlobalState>();
  const auto num_rows = chunk.size();

  // Buffered, not removed here: the removal reaches iresearch when the write
  // buffer is replayed, ordered against exactly the rows that precede it. The
  // new row is buffered straight after, so it still outranks the removal of the
  // version it replaces.
  duckdb::UnifiedVectorFormat old_pk;
  chunk.data[gstate.old_pk_columns[0].input_col_idx].ToUnifiedFormat(num_rows,
                                                                     old_pk);
  const auto* old_pk_data =
    duckdb::UnifiedVectorFormat::GetData<int64_t>(old_pk);
  std::vector<int64_t> old_rows;
  old_rows.reserve(num_rows);
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    old_rows.push_back(old_pk_data[old_pk.sel->get_index(row)]);
  }
  gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, old_rows);

  duckdb::DataChunk new_row;
  new_row.InitializeEmpty(gstate.chunk_types);
  new_row.ReferenceColumns(chunk, gstate.new_row_src);

  const uint64_t pk_base = gstate.generated_pk_seq->NextValues(
    duckdb::DuckTransaction::Get(context.client,
                                 gstate.generated_pk_seq->catalog),
    num_rows);
  // TODO(Dronplane): Maybe we can re-use generated PKs from delete if PK is not
  // changed. Looks not big win now. But for future optimizations.
  auto& search_txn = gstate.sdb_txn->SearchTxn();
  search_txn.AddInlineInsertChunk(
    gstate.search_table,
    duckdb::BufferManager::GetBufferManager(context.client), gstate.chunk_types,
    gstate.column_ids, _table.catalog, new_row, pk_base);

  // After the new row, never between it and the removal above: a flush replays
  // the buffer in issue order, so the pair has to reach iresearch together for
  // the new version to outrank the removal of the one it replaces.
  if (search_txn.BufferedBytes(_table.oid) > gstate.write_buffer_max_bytes) {
    search_txn.FlushBuffer(gstate.search_table, context.client);
  }

  if (gstate.returned) {
    gstate.returned->Append(new_row);
  }

  gstate.update_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
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
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& state = input.global_state.Cast<SearchUpdateGlobalState>();
  if (state.table_lock.owns_lock()) {
    state.table_lock.unlock();
  }
  return duckdb::SinkFinalizeType::READY;
}

}  // namespace sdb::connector
