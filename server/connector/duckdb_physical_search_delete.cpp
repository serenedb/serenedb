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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/primary_key.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
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
  SDB_ASSERT(_pk_col_indices.size() == 1);

  if (_index) {
    // Index road: the index's transaction is registered with the connection
    // and created lazily in Sink (EnsureIndexTransaction); no table state.
    return state;
  }

  state->table_id = _table->GetId();
  state->search_table = _table->GetData();
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};
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

  // Rebuild each removal term exactly as the writer wrote it
  // (duckdb_physical_create_index.cpp): a scalar generated PK is the sortable
  // signed rowid; a view-index (file, row) struct is the raw unsigned file half
  // plus the sortable signed row half.
  auto& pk_col = chunk.data[_pk_col_indices[0]];
  std::string pk;
  if (pk_col.GetType().id() == duckdb::LogicalTypeId::STRUCT) {
    auto& entries = duckdb::StructVector::GetEntries(pk_col);
    SDB_ASSERT(entries.size() == 2);
    duckdb::UnifiedVectorFormat file_fmt;
    duckdb::UnifiedVectorFormat row_fmt;
    entries[0].ToUnifiedFormat(num_rows, file_fmt);
    entries[1].ToUnifiedFormat(num_rows, row_fmt);
    const auto* files =
      duckdb::UnifiedVectorFormat::GetData<uint64_t>(file_fmt);
    const auto* rows = duckdb::UnifiedVectorFormat::GetData<int64_t>(row_fmt);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      pk.clear();
      primary_key::AppendUnsigned(pk, files[file_fmt.sel->get_index(row)]);
      primary_key::AppendSigned(pk, rows[row_fmt.sel->get_index(row)]);
      remover.DeleteRowImpl(pk);
      if (_table) {
        wal_pks.emplace_back(pk);
      }
    }
  } else {
    duckdb::UnifiedVectorFormat gen_pk;
    pk_col.ToUnifiedFormat(num_rows, gen_pk);
    const auto* gen_pk_data =
      duckdb::UnifiedVectorFormat::GetData<int64_t>(gen_pk);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      pk.clear();
      primary_key::AppendSigned(pk, gen_pk_data[gen_pk.sel->get_index(row)]);
      remover.DeleteRowImpl(pk);
      if (_table) {
        wal_pks.emplace_back(pk);
      }
    }
  }

  remover.FinishImpl();
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
