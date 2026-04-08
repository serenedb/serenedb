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

#include <duckdb.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/index/index_type.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>

#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"

namespace sdb::connector {

class SereneDBSchemaEntry;

// Physical operator for CREATE INDEX on SereneDB tables.
// Replaces DuckDB's native PhysicalCreateIndex which requires DuckTableEntry.
//
// Pipeline: TableScan -> SereneDBPhysicalCreateIndex (sink)
//
// Lifecycle:
//   Sink:               receive data chunks, write to index shard (backfill)
//   GetGlobalSinkState: create index in catalog with tombstone
//   Finalize:           CommitWait (inverted) + RemoveTombstone
//   On error:           destructor drops the index (rollback)
class SereneDBPhysicalCreateIndex final : public duckdb::PhysicalOperator {
 public:
  SereneDBPhysicalCreateIndex(duckdb::PhysicalPlan& plan,
                              std::shared_ptr<catalog::Table> table,
                              ObjectId database_id,
                              duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
                              SereneDBSchemaEntry& schema_entry,
                              duckdb::idx_t estimated_cardinality);

  // Sink interface
  bool IsSink() const override { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const override;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const override;
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const override;

  // Source interface -- returns CREATE INDEX tag
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const override;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const override;
  bool IsSource() const override { return true; }

 private:
  std::shared_ptr<catalog::Table> _table;
  ObjectId _database_id;
  duckdb::unique_ptr<duckdb::CreateIndexInfo> _info;
  SereneDBSchemaEntry& _schema_entry;
};

// create_plan callback registered with DuckDB's index type system.
// Called by PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex) when
// the index type has a custom plan function.
duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input);

}  // namespace sdb::connector
