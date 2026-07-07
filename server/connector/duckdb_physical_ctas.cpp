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

#include "connector/duckdb_physical_ctas.h"

#include <atomic>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <duckdb/transaction/transaction_manager.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "catalog/catalog.h"
#include "catalog/store/store.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/progress_registry.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

// Wraps the nested PhysicalInsert's sink state and holds the second
// __sdb_store transaction the whole load runs under. Abort/error compensation
// is NOT done here: a sink state dies with the cached physical plan (possibly
// long after the statement's MetaTransaction), so it must never touch the
// transaction machinery. The abort path is the transaction_abort_cleanup hook
// registered on SereneDBClientState, run at TransactionPreRollback while the
// MetaTransaction is alive.
struct CTASGlobalSinkState final : public duckdb::GlobalSinkState {
  CTASGlobalSinkState(duckdb::AttachedDatabase& store_db, ObjectId database_id,
                      std::string database_name, std::string schema_name,
                      std::string table_name)
    : store_db(store_db),
      database_id(database_id),
      database_name(std::move(database_name)),
      schema_name(std::move(schema_name)),
      table_name(std::move(table_name)) {}

  // Pipeline::TryGetMaxThreads queries the sink's global state to size the
  // load. Forward so a wrapped PhysicalBatchInsert scales its memory budget by
  // thread count instead of staying pinned at the single-thread minimum.
  duckdb::idx_t MaxThreads(duckdb::idx_t source_max_threads) override {
    return insert_gstate ? insert_gstate->MaxThreads(source_max_threads)
                         : source_max_threads;
  }

  duckdb::AttachedDatabase& store_db;
  // Valid until committed/rolled back; the manager destroys it afterwards.
  duckdb::optional_ptr<duckdb::DuckTransaction> second_txn;
  ObjectId database_id;
  std::string database_name;
  std::string schema_name;
  std::string table_name;
  duckdb::unique_ptr<duckdb::GlobalSinkState> insert_gstate;
  bool finalized = false;
  // Summed in Sink (operator-agnostic: the nested batch/insert global-state
  // types are unrelated and BatchInsertGlobalState is not header-visible). A
  // CTAS into a fresh store table never drops rows, so the chunk-size sum is
  // the exact inserted count.
  std::atomic<duckdb::idx_t> insert_count{0};

  pg::ProgressMetrics* progress = nullptr;
};

struct CTASSourceState final : public duckdb::GlobalSourceState {
  bool done = false;
};

}  // namespace

SereneDBPhysicalCTAS::SereneDBPhysicalCTAS(
  duckdb::PhysicalPlan& plan, duckdb::PhysicalOperator& insert,
  ObjectId database_id, std::string database_name, std::string schema_name,
  catalog::CreateTableOptions options, ObjectId table_id,
  duckdb::OnCreateConflict on_conflict, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(
      plan, duckdb::PhysicalOperatorType::CREATE_TABLE_AS,
      {duckdb::LogicalType::BIGINT}, estimated_cardinality),
    _insert(insert),
    _database_id(database_id),
    _database_name(std::move(database_name)),
    _schema_name(std::move(schema_name)),
    _options(std::move(options)),
    _table_id(table_id),
    _on_conflict(on_conflict) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalCTAS::GetGlobalSinkState(duckdb::ClientContext& context) const {
  // Create the facade catalog entry (tombstoned, no store table) here -- at
  // execution, once -- with the pre-allocated id so the store table name the
  // insert operator already encoded matches.
  auto& catalog_impl = catalog::GetCatalog();
  // CREATE OR REPLACE TABLE AS: drop the pre-existing table (cascade) before
  // creating the tombstoned replacement. The drop commits first, so a crash
  // before Finalize leaves the old table dropped (tombstone resolves on
  // recovery) and the new table tombstoned/invisible -- the same recovery
  // contract as a fresh CTAS. Done at execution (once), not plan time.
  if (_on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
    auto snapshot = catalog_impl.GetCatalogSnapshot();
    if (snapshot->GetTable(catalog::NoAccessCheck(), _database_id, _schema_name,
                           _options.name)) {
      auto drop_result = catalog_impl.DropTable(
        catalog::ActingAs(context), _database_name, _schema_name, _options.name,
        /*cascade=*/true);
      if (!drop_result.ok()) {
        SDB_THROW(std::move(drop_result));
      }
    }
  }
  // A valid table id puts CreateTable in CTAS mode: tombstoned, no store table.
  catalog::CreateTableOperationOptions op_options;
  op_options.table_id = _table_id;
  // Ownership is attributed to the creating role via the access context.
  auto create_result =
    catalog_impl.CreateTable(catalog::ActingAs(context), _database_id,
                             _schema_name, _options, op_options);
  if (!create_result.ok()) {
    SDB_THROW(std::move(create_result));
  }

  auto& store_db = *duckdb::DatabaseManager::Get(context).GetDatabase(
    context, duckdb::Identifier{catalog::kStoreDatabaseName});
  auto& meta = duckdb::MetaTransaction::Get(context);
  auto state = duckdb::make_uniq<CTASGlobalSinkState>(
    store_db, _database_id, _database_name, _schema_name, _options.name);

  auto& second_txn = store_db.GetTransactionManager()
                       .StartTransaction(context)
                       .Cast<duckdb::DuckTransaction>();
  second_txn.SetReadWrite();
  second_txn.active_query = meta.GetActiveQuery();
  state->second_txn = &second_txn;

  // Route the store-table create + insert onto the second transaction for the
  // life of the pipeline. Pushed before the nested sink state is built, so the
  // CTAS-variant insert creates the store table under the second transaction.
  meta.PushTransactionOverride(store_db, second_txn);
  // Mark __sdb_store as the modified database so creating the store catalog
  // entry passes DuckSchemaEntry's modified-database check. The facade catalog
  // forwards its writes, so it never occupies the single-writable-db slot.
  meta.ModifyDatabase(store_db,
                      duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY |
                        duckdb::DatabaseModificationType::INSERT_DATA);
  auto sdb_state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  SDB_ASSERT(sdb_state);
  SDB_ASSERT(!sdb_state->transaction_abort_cleanup);
  // Abort path: run while the MetaTransaction is alive. The override slot is
  // separate from the meta's transaction map, so the meta's own rollback never
  // rolls the side transaction back -- this hook is its sole owner.
  sdb_state->transaction_abort_cleanup =
    [&store_db, &second_txn, database_name = _database_name,
     schema_name = _schema_name,
     table_name = _options.name](duckdb::MetaTransaction& meta_txn) {
      meta_txn.PopTransactionOverride(store_db);
      store_db.GetTransactionManager().RollbackTransaction(second_txn);
      std::ignore = catalog::GetCatalog().DropTable(
        catalog::NoAccessCheck(), database_name, schema_name, table_name, true);
    };
  auto& metrics = sdb_state->Progress();
  metrics.SetCommand(pg::ProgressCommand::CreateTableAs);
  metrics.SetPhase(pg::progress_phase::CreateTableAs::Ingesting);
  pg::ProgressMetrics::Set(metrics.relid, static_cast<int64_t>(_table_id.id()));
  // The CTAS operator's own estimate is its single count row; the expected
  // ingest size is the source child's estimate.
  if (!children.empty() && children[0].get().estimated_cardinality > 0) {
    pg::ProgressMetrics::Set(
      metrics.tuples_total,
      static_cast<int64_t>(children[0].get().estimated_cardinality));
  }
  state->progress = &metrics;
  state->insert_gstate = _insert.GetGlobalSinkState(context);
  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBPhysicalCTAS::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  return _insert.GetLocalSinkState(context);
}

duckdb::SinkResultType SereneDBPhysicalCTAS::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<CTASGlobalSinkState>();
  gstate.insert_count.fetch_add(chunk.size(), std::memory_order_relaxed);
  duckdb::OperatorSinkInput insert_input{
    *gstate.insert_gstate, input.local_state, input.interrupt_state};
  const auto result = _insert.Sink(context, chunk, insert_input);

#ifdef SDB_FAULT_INJECTION
  // Tuple/byte counting happens in the nested insert's sink via the
  // sink_progress_callback; pause after it so the counters of the ingested
  // chunk are visible while parked.
  if (gstate.progress) {
    SDB_WAIT_ON_FAILURE("pause_ctas_mid_ingest");
  }
#endif

  return result;
}

duckdb::SinkNextBatchType SereneDBPhysicalCTAS::NextBatch(
  duckdb::ExecutionContext& context,
  duckdb::OperatorSinkNextBatchInput& input) const {
  auto& gstate = input.global_state.Cast<CTASGlobalSinkState>();
  duckdb::OperatorSinkNextBatchInput insert_input{
    *gstate.insert_gstate, input.local_state, input.interrupt_state};
  return _insert.NextBatch(context, insert_input);
}

duckdb::SinkCombineResultType SereneDBPhysicalCTAS::Combine(
  duckdb::ExecutionContext& context,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<CTASGlobalSinkState>();
  duckdb::OperatorSinkCombineInput insert_input{
    *gstate.insert_gstate, input.local_state, input.interrupt_state};
  return _insert.Combine(context, insert_input);
}

duckdb::SinkFinalizeType SereneDBPhysicalCTAS::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<CTASGlobalSinkState>();
  duckdb::OperatorSinkFinalizeInput insert_input{*gstate.insert_gstate,
                                                 input.interrupt_state};
  auto result = _insert.Finalize(pipeline, event, context, insert_input);

  // Rows are staged in the second transaction but the catalog table is still
  // tombstone-named; a crash here must drop it on recovery.
  SDB_IF_FAILURE("crash_sst_sink_after_ingest") { SDB_IMMEDIATE_ABORT(); }

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateTableAs::Committing);
  }
  // Commit point: from here the side transaction is consumed either way, so
  // the abort hook must not fire anymore (a failed commit already rolled the
  // transaction back inside the manager).
  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    sdb_state->transaction_abort_cleanup = nullptr;
  }
  duckdb::MetaTransaction::Get(context).PopTransactionOverride(gstate.store_db);
  auto err = gstate.store_db.GetTransactionManager().CommitTransaction(
    context, *gstate.second_txn);
  if (err.HasError()) {
    std::ignore = catalog::GetCatalog().DropTable(
      catalog::NoAccessCheck(), gstate.database_name, gstate.schema_name,
      gstate.table_name, true);
    err.Throw("Failed to commit CREATE TABLE AS data transaction");
  }

  SDB_IF_FAILURE("crash_before_remove_tombstone") { SDB_IMMEDIATE_ABORT(); }
  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateTableAs::Finalizing);
  }
  auto r = catalog::GetCatalog().RemoveTombstone(
    gstate.database_id, gstate.schema_name, gstate.table_name);
  if (!r.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("Failed to remove tombstone: ", r.errorMessage()));
  }
  gstate.finalized = true;
  return result;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalCTAS::GetGlobalSourceState(duckdb::ClientContext&) const {
  return duckdb::make_uniq<CTASSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalCTAS::GetDataInternal(
  duckdb::ExecutionContext&, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& src = input.global_state.Cast<CTASSourceState>();
  if (src.done) {
    return duckdb::SourceResultType::FINISHED;
  }
  src.done = true;
  duckdb::idx_t count = 0;
  if (sink_state) {
    count = sink_state->Cast<CTASGlobalSinkState>().insert_count.load(
      std::memory_order_relaxed);
  }
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(static_cast<int64_t>(count)));
  return duckdb::SourceResultType::FINISHED;
}

}  // namespace sdb::connector
