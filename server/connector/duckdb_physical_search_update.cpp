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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_primary_key.h"
#include "connector/key_utils.hpp"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

struct SearchUpdateGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<search::SearchTable> search_table;
  query::Transaction* sdb_txn = nullptr;
  std::string table_key;

  // Insert half (the new-row image, in catalog order).
  std::vector<catalog::Column::Id> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  std::vector<duckdb_primary_key::PKColumn> new_pk_columns;
  // catalog slot p <- input chunk slot new_row_src[p]; re-orders the projected
  // new-row block (op.columns order) into catalog (insert) order.
  std::vector<duckdb::idx_t> new_row_src;
  std::shared_ptr<catalog::Sequence> generated_pk_seq;
  std::unique_ptr<SearchSinkInsertBaseImpl> insert_sink;

  // Delete half (the old rows, by their old PK / rowid in the virtuals).
  std::vector<duckdb_primary_key::PKColumn> old_pk_columns;

  std::shared_lock<std::shared_mutex> table_lock;
  duckdb::idx_t update_count = 0;
};

struct SearchUpdateSourceState : duckdb::GlobalSourceState {
  bool finished = false;
};

}  // namespace

SereneDBSearchUpdate::SereneDBSearchUpdate(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  std::vector<duckdb::idx_t> pk_col_indices,
  std::vector<duckdb::PhysicalIndex> update_columns,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)),
    _pk_col_indices(std::move(pk_col_indices)),
    _update_columns(std::move(update_columns)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchUpdate::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchUpdateGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);

  state->search_table = _table->GetData();
  SDB_ASSERT(state->search_table,
             "SereneDBSearchUpdate dispatched against a non-Fast table");
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};

  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();

  // Insert (catalog) order: every column except the synthetic generated PK.
  containers::FlatHashMap<catalog::Column::Id, size_t> pos_of;
  for (const auto& col : columns) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    pos_of[col.GetId()] = state->column_ids.size();
    state->column_ids.push_back(col.GetId());
    state->chunk_types.push_back(col.type);
  }

  // BindUpdateConstraints projected every physical column, so the new-row block
  // has one slot per non-generated-PK column.
  const auto p = state->column_ids.size();
  state->new_row_src.assign(p, 0);
  SDB_ASSERT(_update_columns.size() == p,
             "search UPDATE must project every non-generated-PK column");
  for (size_t i = 0; i < _update_columns.size(); ++i) {
    const auto& col = columns[_update_columns[i].index];
    auto it = pos_of.find(col.GetId());
    SDB_ASSERT(it != pos_of.end(),
               "projected update column is not a stored table column");
    state->new_row_src[it->second] = i;
  }

  state->new_pk_columns = duckdb_primary_key::BuildPKColumns(*_table);

  for (size_t i = 0; i < _pk_col_indices.size(); ++i) {
    duckdb::LogicalType pk_type = duckdb::LogicalType::BIGINT;
    if (i < pk_col_ids.size()) {
      for (const auto& col : columns) {
        if (col.GetId() == pk_col_ids[i]) {
          pk_type = col.type;
          break;
        }
      }
    }
    state->old_pk_columns.push_back(duckdb_primary_key::PKColumn{
      .input_col_idx = _pk_col_indices[i],
      .type = pk_type,
    });
  }

  if (pk_col_ids.empty()) {
    state->generated_pk_seq =
      snapshot->GetObject<catalog::Sequence>(_table->GetGeneratedPkSeqId());
    SDB_ASSERT(state->generated_pk_seq);
  }

  state->sdb_txn = &conn_ctx;
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
  duckdb_primary_key::PreparePKFormats(chunk, gstate.old_pk_columns,
                                       old_pk_formats);
  std::vector<std::string> wal_pks;
  wal_pks.reserve(num_rows);
  std::string key_buffer;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    duckdb_primary_key::MakeColumnKey(
      old_pk_formats, gstate.old_pk_columns, row, gstate.table_key,
      [&](std::string_view row_lock_key) {
        const auto pk = row_lock_key.substr(sizeof(ObjectId));
        remover.DeleteRowImpl(pk);
        wal_pks.emplace_back(pk);
      },
      key_buffer);
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
    gstate.insert_sink = MakeSearchTableInsertSink(trx, gstate.column_ids);
  }
  const bool uses_generated_pk = gstate.generated_pk_seq != nullptr;
  const uint64_t pk_base =
    uses_generated_pk ? gstate.generated_pk_seq->ReserveWriteUnsafe(num_rows)
                      : 0;
  // TODO(Dronplane): Maybe we can re-use generated PKs from delete if PK is not
  // changed. Looks not big win now. But for future optimizations.
  WriteChunkToSearchSink(*gstate.insert_sink, new_row, gstate.column_ids,
                         gstate.new_pk_columns, gstate.table_key,
                         uses_generated_pk, pk_base);
  gstate.sdb_txn->SearchTxn().AddInlineInsertChunk(
    gstate.search_table,
    duckdb::BufferManager::GetBufferManager(context.client), gstate.chunk_types,
    new_row, uses_generated_pk, pk_base);

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
  return duckdb::make_uniq<SearchUpdateSourceState>();
}

duckdb::SourceResultType SereneDBSearchUpdate::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchUpdateSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.update_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
