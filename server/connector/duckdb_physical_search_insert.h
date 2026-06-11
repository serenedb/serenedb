////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <memory>

#include "catalog/table.h"

namespace sdb::connector {

class SereneDBSearchInsert final : public duckdb::PhysicalOperator {
 public:
  // Insert mode: pre-existing target table.
  SereneDBSearchInsert(duckdb::PhysicalPlan& plan,
                       std::shared_ptr<catalog::Table> table,
                       duckdb::vector<duckdb::LogicalType> types,
                       duckdb::idx_t estimated_cardinality);

  // CTAS mode: create the target table from `info` in GetGlobalSinkState.
  SereneDBSearchInsert(duckdb::PhysicalPlan& plan,
                       duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
                       duckdb::SchemaCatalogEntry& schema,
                       duckdb::idx_t estimated_cardinality);

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const final;
  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext& context) const final;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;
  duckdb::SinkCombineResultType Combine(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkCombineInput& input) const final;
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const final;

  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;
  bool IsSource() const final { return true; }

 private:
  // Insert mode: the pre-existing target table. Null in CTAS mode.
  std::shared_ptr<catalog::Table> _table;

  // CTAS mode only -- mutually exclusive with _table; null in insert mode.
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> _ctas_info;
  duckdb::SchemaCatalogEntry* _ctas_schema = nullptr;
};

}  // namespace sdb::connector
