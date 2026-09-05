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

#include "connector/duckdb_physical_search_delete.h"

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <vector>

#include "connector/duckdb_client_state.h"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/transaction.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

struct SearchDeleteGlobalState : duckdb::GlobalSinkState {
  query::Transaction* sdb_txn = nullptr;
  std::vector<primary_key::PKColumn> pk_columns;
  duckdb::idx_t delete_count = 0;
};

struct SearchTableDeleteState final : SearchDeleteGlobalState {
  std::shared_ptr<search::SearchTable> search_table;
  std::shared_lock<std::shared_mutex> table_lock;
  // RETURNING only: the rows this statement removed.
  std::optional<duckdb::ColumnDataCollection> returned;

  irs::IndexWriter::Transaction& Trx() {
    return sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
      search_table, [&] { return search_table->GetTransaction(); });
  }
};

struct IndexDeleteState final : SearchDeleteGlobalState {
  std::unique_ptr<irs::IndexWriter::Transaction> trx;

  irs::IndexWriter::Transaction& Trx() { return *trx; }
};

struct SearchDeleteSourceState : duckdb::GlobalSourceState {
  bool finished = false;
  duckdb::ColumnDataScanState scan;
};

}  // namespace

SereneDBSearchDelete::SereneDBSearchDelete(
  duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk,
  duckdb::vector<duckdb::idx_t> return_columns)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(&table),
    _return_chunk(return_chunk),
    _return_columns(std::move(return_columns)) {
  _pk_columns.reserve(expressions.size());
  for (const auto& expr : expressions) {
    const auto& ref = expr->Cast<duckdb::BoundReferenceExpression>();
    _pk_columns.push_back(
      {.input_col_idx = ref.Index(), .type = ref.GetReturnType()});
  }
}

SereneDBSearchDelete::SereneDBSearchDelete(
  duckdb::PhysicalPlan& plan,
  std::shared_ptr<search::InvertedIndexStorage> storage,
  std::shared_ptr<const irs::IndexFieldOptions> field_options,
  std::vector<primary_key::PKColumn> pk_columns,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _pk_columns(std::move(pk_columns)),
    _index_storage(std::move(storage)),
    _field_options(std::move(field_options)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto& conn_ctx = GetSereneDBContext(context);

  if (IsReindexDelete()) {
    // Index road: PlanDelete admitted the target only on an internal
    // connection (a REINDEX pass's removes). The pk term is the (file_index,
    // row_number) halves, encoded exactly as the build wrote them.
    auto state = duckdb::make_uniq<IndexDeleteState>();
    state->trx = std::make_unique<irs::IndexWriter::Transaction>(
      _index_storage->GetTransaction());
    state->trx->SetFieldOptions(_field_options);
    SDB_ASSERT(_pk_columns.size() == 2);
    state->pk_columns = _pk_columns;
    return state;
  }

  auto state = duckdb::make_uniq<SearchTableDeleteState>();
  state->search_table = _table->EnsureStorage();
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};

  state->pk_columns = _pk_columns;

  state->sdb_txn = &conn_ctx;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }
  return state;
}

duckdb::SinkResultType SereneDBSearchDelete::Sink(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  return IsReindexDelete()
           ? SinkImpl(chunk, input.global_state.Cast<IndexDeleteState>())
           : SinkImpl(chunk, input.global_state.Cast<SearchTableDeleteState>());
}

template<typename GlobalState>
duckdb::SinkResultType SereneDBSearchDelete::SinkImpl(
  duckdb::DataChunk& chunk, GlobalState& gstate) const {
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  constexpr bool kTable = std::is_same_v<GlobalState, SearchTableDeleteState>;

  // The removal term is the bare PK, encoded exactly as the insert wrote it
  // (Create -> key_encoding::AppendScalarValue). For a no-PK table the rowid
  // column holds the
  // generated PK (materialised by the scan), so the same encoding matches.
  SearchSinkDeleteBaseImpl remover{gstate.Trx()};
  remover.InitImpl(num_rows);

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

  std::vector<std::string> wal_pks;
  if constexpr (kTable) {
    wal_pks.reserve(num_rows);
  }
  std::string pk;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    pk.clear();
    primary_key::Create(pk_formats, gstate.pk_columns, row, pk);
    remover.DeleteRowImpl(pk);  // live iresearch removal
    if constexpr (kTable) {
      wal_pks.emplace_back(pk);  // WAL delete payload
    }
  }
  remover.FinishImpl();  // hands the removal filter to the trx

  if constexpr (kTable) {
    gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, wal_pks);
    if (gstate.returned) {
      duckdb::DataChunk row;
      row.InitializeEmpty(GetTypes());
      const auto stored = _table->GetColumns().LogicalColumnCount();
      SDB_ASSERT(GetTypes().size() == stored + _pk_columns.size());
      for (duckdb::idx_t i = 0; i < stored; ++i) {
        SDB_ASSERT(_return_columns[i] != duckdb::DConstants::INVALID_INDEX);
        row.data[i].Reference(chunk.data[_return_columns[i]]);
      }
      for (size_t i = 0; i < _pk_columns.size(); ++i) {
        row.data[stored + i].Reference(
          chunk.data[_pk_columns[i].input_col_idx]);
      }
      row.SetCardinality(num_rows);
      gstate.returned->Append(row);
    }
  }

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBSearchDelete::Finalize(
  duckdb::Pipeline& /*pipeline*/, duckdb::Event& /*event*/,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  if (IsReindexDelete()) {
    auto& gstate = input.global_state.Cast<IndexDeleteState>();
    gstate.trx->RegisterFlush();
    const auto tick =
      search::TickDomain::Instance().Advance(gstate.trx->GetQueries() + 1);
    if (!gstate.trx->Commit(tick)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("failed to commit the removes for index with id ",
                              _index_storage->GetId()));
    }
    gstate.trx.reset();
  }
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchDelete::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  auto state = duckdb::make_uniq<SearchDeleteSourceState>();
  if (!IsReindexDelete() && sink_state) {
    auto& gstate = sink_state->Cast<SearchTableDeleteState>();
    if (gstate.returned) {
      gstate.returned->InitializeScan(state->scan);
    }
  }
  return state;
}

duckdb::SourceResultType SereneDBSearchDelete::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchDeleteSourceState>();
  if (!IsReindexDelete()) {
    auto& gstate = sink_state->Cast<SearchTableDeleteState>();
    if (gstate.returned) {
      gstate.returned->Scan(source.scan, chunk);
      return chunk.size() == 0 ? duckdb::SourceResultType::FINISHED
                               : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
    }
  }
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0,
                 duckdb::Value::BIGINT(
                   sink_state->Cast<SearchDeleteGlobalState>().delete_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
