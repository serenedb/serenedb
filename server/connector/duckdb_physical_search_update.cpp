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
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "connector/duckdb_client_state.h"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

struct SearchUpdateGlobalState : duckdb::GlobalSinkState {
  duckdb::idx_t table_id;
  std::shared_ptr<search::SearchTable> search_table;
  query::Transaction* sdb_txn = nullptr;

  std::vector<duckdb::idx_t> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  std::vector<primary_key::PKColumn> new_pk_columns;
  duckdb::vector<duckdb::column_t> new_row_src;
  duckdb::optional_ptr<duckdb::SequenceCatalogEntry> generated_pk_seq;
  std::unique_ptr<SearchSinkInsertBaseImpl> insert_sink;

  std::vector<primary_key::PKColumn> old_pk_columns;

  std::shared_lock<std::shared_mutex> table_lock;
  duckdb::idx_t update_count = 0;
  // RETURNING only: the rows as this statement left them.
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchUpdateSourceState : duckdb::GlobalSourceState {
  bool finished = false;
  duckdb::ColumnDataScanState scan;
};

}  // namespace

SereneDBSearchUpdate::SereneDBSearchUpdate(
  duckdb::PhysicalPlan& plan, SearchWriteTarget target,
  std::vector<duckdb::idx_t> pk_col_indices,
  std::vector<duckdb::PhysicalIndex> update_columns,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _target(std::move(target)),
    _pk_col_indices(std::move(pk_col_indices)),
    _update_columns(std::move(update_columns)),
    _return_chunk(return_chunk) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchUpdate::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchUpdateGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);

  state->table_id = _target.table_id;

  state->search_table = _target.data;
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};

  state->column_ids = _target.column_ids;
  state->chunk_types = _target.chunk_types;

  const auto p = state->column_ids.size();
  state->new_row_src.assign(p, 0);
  SDB_ASSERT(_update_columns.size() == p,
             "search UPDATE must project every non-generated-PK column");
  // Each projected column names its own slot in the chunk the sink writes: the
  // entry lists exactly the columns iresearch stores, in that order.
  for (size_t i = 0; i < _update_columns.size(); ++i) {
    const auto index = _update_columns[i].index;
    SDB_ASSERT(index < p,
               "projected update column is not a stored table column");
    state->new_row_src[index] = i;
  }

  state->new_pk_columns = _target.pk_columns;
  state->old_pk_columns = RowIdentityPKColumns(_target, _pk_col_indices);
  state->generated_pk_seq = _target.generated_pk_seq;

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
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto& trx = gstate.sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
    gstate.search_table, [&] { return gstate.search_table->GetTransaction(); });

  SearchSinkDeleteBaseImpl remover{trx};
  remover.InitImpl(num_rows);
  std::vector<duckdb::UnifiedVectorFormat> old_pk_formats;
  primary_key::PreparePKFormats(chunk, gstate.old_pk_columns, old_pk_formats);
  std::vector<std::string> wal_pks;
  wal_pks.reserve(num_rows);
  std::string pk;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    pk.clear();
    primary_key::Create(old_pk_formats, gstate.old_pk_columns, row, pk);
    remover.DeleteRowImpl(pk);
    wal_pks.emplace_back(pk);
  }
  remover.FinishImpl();
  gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, wal_pks);

  duckdb::DataChunk new_row;
  new_row.InitializeEmpty(gstate.chunk_types);
  for (size_t col = 0; col < gstate.column_ids.size(); ++col) {
    new_row.data[col].Reference(chunk.data[gstate.new_row_src[col]]);
  }
  new_row.SetCardinality(num_rows);

  if (!gstate.insert_sink) {
    gstate.insert_sink = MakeSearchTableInsertSink(trx);
  }
  const bool uses_generated_pk = gstate.generated_pk_seq != nullptr;
  const uint64_t pk_base =
    uses_generated_pk ? gstate.generated_pk_seq->NextValues(
                          duckdb::DuckTransaction::Get(
                            context.client, gstate.generated_pk_seq->catalog),
                          num_rows)
                      : 0;
  // TODO(Dronplane): Maybe we can re-use generated PKs from delete if PK is not
  // changed. Looks not big win now. But for future optimizations.
  WriteChunkToSearchSink(*gstate.insert_sink, new_row, gstate.column_ids,
                         gstate.new_pk_columns, uses_generated_pk, pk_base);
  gstate.sdb_txn->SearchTxn().AddInlineInsertChunk(
    gstate.search_table,
    duckdb::BufferManager::GetBufferManager(context.client), gstate.chunk_types,
    new_row, uses_generated_pk, pk_base);

  if (gstate.returned) {
    // The new row, which is what postgres' RETURNING reports for an UPDATE. The
    // projection under this sink carries every stored column, so new_row_src
    // already says where each of them arrived.
    duckdb::DataChunk row;
    row.InitializeEmpty(GetTypes());
    row.ReferenceColumns(chunk, gstate.new_row_src);
    gstate.returned->Append(row);
  }

  gstate.update_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBSearchUpdate::Finalize(
  duckdb::Pipeline& /*pipeline*/, duckdb::Event& /*event*/,
  duckdb::ClientContext& /*context*/,
  duckdb::OperatorSinkFinalizeInput& /*input*/) const {
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchUpdate::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  auto state = duckdb::make_uniq<SearchUpdateSourceState>();
  if (sink_state != nullptr) {
    auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
    if (gstate.returned) {
      gstate.returned->InitializeScan(state->scan);
    }
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
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.update_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
