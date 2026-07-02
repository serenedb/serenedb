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

#include "connector/duckdb_physical_progress.h"

#include <duckdb/execution/execution_context.hpp>

#include "basics/debugging.h"
#include "connector/duckdb_client_state.h"
#include "pg/progress_tracker.h"

namespace sdb::connector {

SereneDBPhysicalProgress::SereneDBPhysicalProgress(
  duckdb::PhysicalPlan& plan, duckdb::PhysicalOperator& child,
  ObjectId table_id)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             child.types, child.estimated_cardinality),
    _table_id(table_id) {
  children.push_back(child);
}

duckdb::OperatorResultType SereneDBPhysicalProgress::Execute(
  duckdb::ExecutionContext& context, duckdb::DataChunk& input,
  duckdb::DataChunk& chunk, duckdb::GlobalOperatorState&,
  duckdb::OperatorState&) const {
  if (auto state = context.client.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey);
      state && state->progress) {
    state->progress->Add(pg::copy_progress::Param::TuplesProcessed,
                         static_cast<int64_t>(input.size()));
    state->progress->Add(pg::copy_progress::Param::BytesProcessed,
                         static_cast<int64_t>(input.GetAllocationSize()));
    sdb::WaitWhileFailurePointDebugging("pause_sst_sink_mid_copy");
  }
  chunk.Reference(input);
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

}  // namespace sdb::connector
