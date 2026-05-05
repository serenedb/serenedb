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

#include "connector/duckdb_physical_truncate.h"

#include <duckdb/common/types/data_chunk.hpp>

#include "connector/duckdb_client_state.h"
#include "connector/duckdb_truncate_function.h"
#include "pg/connection_context.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

namespace {

struct TruncateSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
  int64_t deleted = 0;
};

}  // namespace

SereneDBPhysicalTruncate::SereneDBPhysicalTruncate(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)) {}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalTruncate::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<TruncateSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalTruncate::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& state = input.global_state.Cast<TruncateSourceState>();
  if (state.finished) {
    return duckdb::SourceResultType::FINISHED;
  }

  auto& conn_ctx = GetSereneDBContext(context.client);
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  // Capture row count BEFORE the wipe so the DELETE Count tag is meaningful.
  // TruncateResolvedTable resets the table_shard's num_rows to 0 inside the
  // noexcept phase-2 block, so we can't read it afterwards.
  if (auto table_shard = snapshot->GetTableShard(_table->GetId())) {
    state.deleted = table_shard->GetTableStats().num_rows;
  }
  TruncateResolvedTable(conn_ctx, snapshot, _table);

  state.finished = true;
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(state.deleted));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
