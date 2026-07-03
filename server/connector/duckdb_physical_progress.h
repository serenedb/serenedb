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

#pragma once

#include <duckdb/execution/physical_operator.hpp>

#include "catalog/identifiers/object_id.h"
#include "pg/progress_tracker.h"

namespace sdb::connector {

// Pass-through operator feeding the session progress reporter
// (pg_stat_progress_copy) with per-chunk tuple/byte counts: above the native
// insert for COPY FROM, below the file sink for COPY TO.
class SereneDBPhysicalProgress final : public duckdb::PhysicalOperator {
 public:
  SereneDBPhysicalProgress(
    duckdb::PhysicalPlan& plan, duckdb::PhysicalOperator& child,
    ObjectId table_id,
    pg::copy_progress::Command command = pg::copy_progress::Command::CopyFrom,
    pg::copy_progress::Type type = pg::copy_progress::Type::File);

  duckdb::unique_ptr<duckdb::GlobalOperatorState> GetGlobalOperatorState(
    duckdb::ClientContext& context) const final;

  duckdb::OperatorResultType Execute(duckdb::ExecutionContext& context,
                                     duckdb::DataChunk& input,
                                     duckdb::DataChunk& chunk,
                                     duckdb::GlobalOperatorState& gstate,
                                     duckdb::OperatorState& state) const final;
  bool ParallelOperator() const final { return true; }

  std::string GetName() const final { return "SDB_PROGRESS"; }

  ObjectId TargetTableId() const noexcept { return _table_id; }

 private:
  ObjectId _table_id;
  pg::copy_progress::Command _command;
  pg::copy_progress::Type _type;
};

}  // namespace sdb::connector
