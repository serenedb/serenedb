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
#include <duckdb/planner/column_binding.hpp>
#include <memory>
#include <vector>

#include "connector/search_table_dispatch.h"

namespace sdb::connector {

class SereneDBSearchUpdate final : public duckdb::PhysicalOperator {
 public:
  // `return_chunk` is RETURNING: the operator then hands back the rows as it
  // left them rather than their count, and `types` is the whole row.
  SereneDBSearchUpdate(duckdb::PhysicalPlan& plan, SearchWriteTarget target,
                       std::vector<duckdb::idx_t> pk_col_indices,
                       std::vector<duckdb::PhysicalIndex> update_columns,
                       duckdb::vector<duckdb::LogicalType> types,
                       duckdb::idx_t estimated_cardinality, bool return_chunk);

  bool IsSink() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const final;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const final;

  bool IsSource() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;

 private:
  SearchWriteTarget _target;
  std::vector<duckdb::idx_t> _pk_col_indices;
  std::vector<duckdb::PhysicalIndex> _update_columns;
  bool _return_chunk = false;
};

}  // namespace sdb::connector
