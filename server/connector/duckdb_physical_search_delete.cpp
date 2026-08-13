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

#include <absl/algorithm/container.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_primary_key.h"
#include "connector/search_remove_filter.hpp"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

struct SearchDeleteGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<search::SearchTable> search_table;
  query::Transaction* sdb_txn = nullptr;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
  std::shared_lock<std::shared_mutex> table_lock;
  duckdb::idx_t delete_count = 0;
};

struct SearchDeleteSourceState : duckdb::GlobalSourceState {
  bool finished = false;
};

}  // namespace

SereneDBSearchDelete::SereneDBSearchDelete(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  std::shared_ptr<const catalog::InvertedIndex> index,
  std::vector<duckdb::idx_t> pk_col_indices,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)),
    _index(std::move(index)),
    _pk_col_indices(std::move(pk_col_indices)) {
  SDB_ASSERT(!_table != !_index);
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchDeleteGlobalState>();

  if (_index) {
    // Index road: PlanDelete admitted the target only on an internal
    // connection (the refresh's own pass). The recipe follows the
    // generated_pk column's declared shape -- a STRUCT's children or the
    // scalar itself; the index's connection-registered transaction is
    // created with the first chunk.
    SDB_ASSERT(_pk_col_indices.size() == 1);
    state->pk_columns = duckdb_primary_key::PkTermColumns(
      _pk_col_indices[0], children[0].get().types[_pk_col_indices[0]]);
    return state;
  }

  state->table_id = _table->GetId();

  state->search_table = _table->GetData();
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};

  // Map each PK chunk position to its type (PlanDelete's layout): explicit-PK
  // columns by their declared type; the no-PK generated rowid as BIGINT.
  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();
  for (size_t i = 0; i < _pk_col_indices.size(); ++i) {
    duckdb::LogicalType pk_type = duckdb::LogicalType::BIGINT;
    if (i < pk_col_ids.size()) {
      const auto it = absl::c_find_if(columns, [&](const catalog::Column& col) {
        return col.GetId() == pk_col_ids[i];
      });
      if (it != columns.end()) {
        pk_type = it->type;
      }
    }
    state->pk_columns.push_back(duckdb_primary_key::PKColumn{
      .input_col_idx = _pk_col_indices[i],
      .type = pk_type,
    });
  }

  state->sdb_txn = &GetSereneDBContext(context);
  return state;
}

duckdb::SinkResultType SereneDBSearchDelete::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchDeleteGlobalState>();
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

  // The removal term is rebuilt exactly as the insert wrote it, through the
  // same recipe (Create over gstate.pk_columns): a no-PK table's rowid
  // column holds the generated PK, a view index's (file, row) struct rides
  // PKColumn.struct_child.
  auto& trx =
    _index ? GetSereneDBContext(context.client).EnsureIndexTransaction(_index)
           : gstate.sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
               gstate.search_table,
               [&] { return gstate.search_table->GetTransaction(); });

  SearchSinkDeleteBaseImpl remover{trx};
  remover.InitImpl(num_rows);

  std::vector<std::string> wal_pks;
  if (_table) {
    wal_pks.reserve(num_rows);
  }
  std::string pk;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    pk.clear();
    duckdb_primary_key::Create(pk_formats, gstate.pk_columns, row, pk);
    remover.DeleteRowImpl(pk);  // live iresearch removal
    if (_table) {
      wal_pks.emplace_back(pk);  // WAL delete payload
    }
  }
  remover.FinishImpl();  // hands the removal filter to the trx
  if (_table) {
    gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, wal_pks);
  }

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchDelete::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  return duckdb::make_uniq<SearchDeleteSourceState>();
}

duckdb::SourceResultType SereneDBSearchDelete::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchDeleteSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SearchDeleteGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.delete_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
