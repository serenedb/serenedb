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

#include "connector/search_table_dispatch.h"

namespace sdb::connector {

class SereneDBSearchInsert final : public duckdb::PhysicalOperator {
 public:
  // Insert mode: pre-existing target table. `return_chunk` is RETURNING: the
  // operator then hands back the rows it inserted rather than their count, and
  // `types` is the whole row.
  SereneDBSearchInsert(duckdb::PhysicalPlan& plan, SearchWriteTarget target,
                       duckdb::vector<duckdb::LogicalType> types,
                       duckdb::idx_t estimated_cardinality, bool return_chunk);

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
  // Insert mode: the pre-existing target, resolved off its entry at plan time.
  // Unset in CTAS mode, where the table does not exist until the sink runs.
  SearchWriteTarget _target;

  // CTAS mode only -- mutually exclusive with _target; null in insert mode.
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> _ctas_info;
  duckdb::SchemaCatalogEntry* _ctas_schema = nullptr;

  bool _return_chunk = false;
};

}  // namespace sdb::connector
