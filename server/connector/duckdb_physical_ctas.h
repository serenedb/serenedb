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
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

namespace sdb::connector {

// Physical operator for CREATE TABLE AS SELECT on SereneDB tables.
// Creates the catalog table (tombstoned) in GetGlobalSinkState, appends
// rows natively to the hidden store table, and removes the tombstone in
// Finalize; the destructor drops the table when the query fails.
class SereneDBPhysicalCTAS : public duckdb::PhysicalOperator {
 public:
  SereneDBPhysicalCTAS(duckdb::PhysicalPlan& plan,
                       duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
                       duckdb::SchemaCatalogEntry& schema,
                       duckdb::idx_t estimated_cardinality);

  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const final;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const final;
  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return false; }

  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;
  bool IsSource() const final { return true; }

  std::string GetName() const final { return "SDB_CREATE_TABLE_AS"; }

 private:
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> _info;
  duckdb::SchemaCatalogEntry& _schema;
};

}  // namespace sdb::connector
