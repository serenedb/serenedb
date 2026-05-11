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
#include <duckdb/planner/bound_constraint.hpp>

#include "catalog/table.h"

namespace sdb::connector {

class SereneDBPhysicalUpdate final : public duckdb::PhysicalOperator {
 public:
  // Input chunk layout (set up in SereneDBCatalog::PlanUpdate):
  //   [resolved SET vals, pk_virtuals, idx_virtuals, rowid]
  SereneDBPhysicalUpdate(
    duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
    std::vector<duckdb::idx_t> pk_col_indices,
    std::vector<duckdb::PhysicalIndex> update_columns,
    std::vector<duckdb::idx_t> indexed_col_indices,
    duckdb::idx_t estimated_cardinality,
    duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>
      bound_constraints);

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
  std::shared_ptr<catalog::Table> _table;
  std::vector<duckdb::idx_t> _pk_col_indices;
  std::vector<duckdb::PhysicalIndex> _update_columns;
  std::vector<duckdb::idx_t> _indexed_col_indices;
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>
    _bound_constraints;
  bool _update_pk = false;
  // Per-PK-column chunk index for building NEW PK keys.
  // For updated PK cols: SET position; for non-updated: pk_virtual position.
  std::vector<duckdb::idx_t> _new_pk_col_indices;
};

}  // namespace sdb::connector
