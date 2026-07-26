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
#include <duckdb/execution/index/index_type.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>

#include "catalog/catalog.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"

namespace sdb::connector {

class SereneDBSchemaEntry;

// One column of the relation a CREATE INDEX reads. A base table's list comes
// off its entry and a view's off the view body, so the operator carries the
// three facts both can answer with rather than either relation's own shape.
struct IndexRelationColumn {
  std::string name;
  duckdb::LogicalType type;
  catalog::ColumnId id;
};

// Physical operator for CREATE INDEX on SereneDB tables.
// Replaces DuckDB's native PhysicalCreateIndex which requires DuckTableEntry.
//
// Pipeline: TableScan -> SereneDBPhysicalCreateIndex (sink)
//
// Lifecycle:
//   Sink:               receive data chunks, write to index storage (backfill)
//   GetGlobalSinkState: create the index in the catalog (visible to this
//                       transaction only, until it commits)
//   Finalize:           Refresh (inverted)
//   On error:           destructor drops the index (rollback)
class SereneDBPhysicalCreateIndex final : public duckdb::PhysicalOperator {
 public:
  // Set by the planner when it splices the expression projection under this
  // operator (see SereneDBCreateIndexPlan).
  void SetExpressionSlotBase(duckdb::idx_t base) noexcept {
    _expression_slot_base = base;
  }
  bool HasProjectedExpressions() const noexcept {
    return _expression_slot_base.IsValid();
  }

  // `relation` is the SereneDB-catalog object the index is built on: either
  // a table or a view definition (foreign-source-backed).
  // `columns` is the relation's column list and `pk_positions` the positions
  // in it the row identity is built from -- empty for a view and for a table
  // that declares no key.
  // `bound_expressions` carries the IndexBinder's output (one per
  // `info->parsed_expressions`). For a bare column ref the slot is set but
  // unused; for an arbitrary expression we normalise + serialise
  // it via helpers to emit `ExpressionSpecific`.
  SereneDBPhysicalCreateIndex(
    duckdb::PhysicalPlan& plan, catalog::IndexRelation relation,
    std::vector<IndexRelationColumn> columns,
    std::vector<duckdb::LogicalIndex> pk_positions, ObjectId database_id,
    duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
    std::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions,
    duckdb::unique_ptr<duckdb::Expression> bound_where,
    SereneDBSchemaEntry& schema_entry, duckdb::idx_t estimated_cardinality);

  bool IsSink() const final { return true; }
  bool ParallelSink() const final;
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

  // Source interface -- returns CREATE INDEX tag
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;
  bool IsSource() const final { return true; }

  ObjectId DatabaseId() const noexcept { return _database_id; }
  ObjectId TargetRelationId() const noexcept { return _relation.GetId(); }

 private:
  // Returns the `_relation` cast to a Table when it is one; nullptr for views.
  const catalog::CreateTableInfo* TableOrNull() const noexcept;
  bool IsDuckDBTable() const noexcept;

  catalog::IndexRelation _relation;
  std::vector<IndexRelationColumn> _columns;
  // Positions in `_columns` the row identity is built from; empty for a view
  // and for a table with no declared primary key.
  std::vector<duckdb::LogicalIndex> _pk_positions;
  ObjectId _database_id;
  duckdb::unique_ptr<duckdb::CreateIndexInfo> _info;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _bound_expressions;
  // Bound partial-index predicate (info->where_clause); null for full indexes.
  duckdb::unique_ptr<duckdb::Expression> _bound_where;
  // First chunk slot holding a pipeline-computed indexed expression (0 when the
  // index has none); the projection appends them after the scanned columns.
  // Unset when the planner spliced no expression projection, which is not the
  // same fact as a base of 0.
  duckdb::optional_idx _expression_slot_base;
  bool _feeds_inverted = false;
  SereneDBSchemaEntry& _schema_entry;
};

// create_plan callback registered with DuckDB's index type system.
// Called by PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex) when
// the index type has a custom plan function.
duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input);

}  // namespace sdb::connector
