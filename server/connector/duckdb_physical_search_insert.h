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
#include <duckdb/planner/bound_constraint.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <memory>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "search/writes.h"

namespace sdb::connector {

class SereneDBSearchInsert : public duckdb::PhysicalOperator {
 public:
  // Insert mode: pre-existing target table. `return_chunk` is RETURNING: the
  // operator then hands back the rows it inserted rather than their count, and
  // `types` is the whole row.
  SereneDBSearchInsert(
    duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
    duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> constraints,
    duckdb::vector<duckdb::LogicalType> types,
    duckdb::idx_t estimated_cardinality, bool return_chunk);

  // CTAS mode: create the target table from `info` in GetGlobalSinkState.
  SereneDBSearchInsert(duckdb::PhysicalPlan& plan,
                       duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
                       duckdb::idx_t estimated_cardinality);

  std::string GetName() const final {
    return _ctas_info ? "CREATE_TABLE_AS" : "INSERT";
  }

  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const override;
  duckdb::unique_ptr<duckdb::LocalSinkState> GetLocalSinkState(
    duckdb::ExecutionContext& context) const override;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const override;
  duckdb::SinkCombineResultType Combine(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkCombineInput& input) const override;
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

  void MarkMergeInsert(search::KeyConflict conflict) noexcept {
    _conflict = conflict;
  }

 protected:
  void InitSinkState(duckdb::ClientContext& context,
                     duckdb::GlobalSinkState& state) const;

  // Insert mode: the pre-existing target. Unset in CTAS mode, where the table
  // does not exist until the sink runs.
  duckdb::optional_ptr<const catalog::SearchTableEntry> _table;
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> _constraints;
  bool _return_chunk = false;
  search::KeyConflict _conflict = search::KeyConflict::Unique;

 private:
  // CTAS mode only -- mutually exclusive with _table; null in insert mode.
  // It carries the target schema, so nothing has to resolve one.
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> _ctas_info;
};

class SereneDBSearchUpsert final : public SereneDBSearchInsert {
 public:
  SereneDBSearchUpsert(
    duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
    duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> constraints,
    duckdb::vector<duckdb::LogicalType> types,
    duckdb::idx_t estimated_cardinality, bool return_chunk, bool replace,
    std::vector<ColumnId> target);

  duckdb::InsertionOrderPreservingMap<std::string> ParamsToString() const final;

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

 private:
  bool _replace;
  std::vector<ColumnId> _target;
};

}  // namespace sdb::connector
