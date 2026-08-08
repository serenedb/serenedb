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
#include <duckdb/transaction/meta_transaction.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "catalog/catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"

namespace sdb::connector {
namespace {

// Wraps the nested PhysicalInsert's sink state. Abort/error compensation is
// NOT done here: a sink state dies with the cached physical plan (possibly long
// after the statement's MetaTransaction), so it must never touch the
// transaction machinery. The abort path is the transaction_abort_cleanup hook
// registered on SereneDBClientState, run at TransactionPreRollback while the
// MetaTransaction is alive.
struct CTASGlobalSinkState final : public duckdb::GlobalSinkState {
  // Pipeline::TryGetMaxThreads queries the sink's global state to size the
  // load. Forward so a wrapped PhysicalBatchInsert scales its memory budget by
  // thread count instead of staying pinned at the single-thread minimum.
  duckdb::idx_t MaxThreads(duckdb::idx_t source_max_threads) override {
    return insert_gstate ? insert_gstate->MaxThreads(source_max_threads)
                         : source_max_threads;
  }

  duckdb::unique_ptr<duckdb::GlobalSinkState> insert_gstate;
  bool finalized = false;
  // Summed in Sink (operator-agnostic: the nested batch/insert global-state
  // types are unrelated and BatchInsertGlobalState is not header-visible). A
  // CTAS into a fresh relation never drops rows, so the chunk-size sum is the
  // exact inserted count.
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
  std::shared_ptr<duckdb::CreateTableInfo> options, ObjectId table_id,
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
  // The relation is created here -- at execution, once -- with the id the plan
  // pre-allocated, and the load that follows appends into the rows that entry
  // owns.
  auto& catalog_impl = catalog::GetCatalog();
  const auto table_name = std::string{catalog::TableNameOf(*_options)};
  // CREATE OR REPLACE TABLE AS: drop the pre-existing table (cascade) before
  // creating the replacement. Both halves belong to this transaction, so a
  // crash before it commits leaves neither -- the same recovery contract as a
  // fresh CTAS. Done at execution (once), not plan time.
  if (_on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
    const auto schema_id =
      catalog::FindSchemaId(&context, _database_id, _schema_name);
    if (schema_id.isSet() &&
        catalog::FindTable(&context, schema_id, table_name)) {
      catalog_impl.DropTable(catalog::ActingAs(context), _database_name,
                             _schema_name, table_name, /*cascade=*/true,
                             /*missing_ok=*/false);
    }
  }
  catalog::CreateTableOperationOptions op_options;
  op_options.table_id = _table_id;
  // Ownership is attributed to the creating role via the access context.
  catalog_impl.CreateTable(catalog::ActingAs(context), _database_id,
                           _schema_name, catalog::Clone(*_options), {},
                           op_options);

  auto state = duckdb::make_uniq<CTASGlobalSinkState>();
  // Handed to the load rather than looked up by it: the nested insert asks the
  // schema for the table it is filling, and the answer is this entry.
  auto target = catalog::FindRelationEntry(&context, _database_id, _schema_name,
                                           table_name);

  auto sdb_state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey);
  SDB_ASSERT(sdb_state);
  SDB_ASSERT(!sdb_state->transaction_abort_cleanup);
  sdb_state->ctas_target = target;
  // Abort path: run while the MetaTransaction is alive. The catalog record was
  // written the moment the relation was created, so an abandoned statement has
  // to take it back.
  sdb_state->transaction_abort_cleanup =
    [database_name = _database_name, schema_name = _schema_name, table_name](
      duckdb::MetaTransaction&, duckdb::ClientContext&) {
      catalog::GetCatalog().DropTable(catalog::NoAccessCheck(), database_name,
                                      schema_name, table_name,
                                      /*cascade=*/true, /*missing_ok=*/true);
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

  // Rows and entry are both staged on this statement's transaction and neither
  // is committed; a crash here must leave neither.
  SDB_IF_FAILURE("crash_sst_sink_after_ingest") { SDB_IMMEDIATE_ABORT(); }

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateTableAs::Committing);
  }
  // The load is over, so the statement's own rollback is enough to take the
  // relation back and this hook has nothing left to compensate for.
  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    sdb_state->transaction_abort_cleanup = nullptr;
    sdb_state->ctas_target = nullptr;
  }

  SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateTableAs::Finalizing);
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
