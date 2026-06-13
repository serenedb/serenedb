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

#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
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
  // `relation` is the SereneDB-catalog object the index is built on: either
  // a `catalog::Table` or a `catalog::PgSqlView`
  // (foreign-source-backed). `view_columns` is the synthesised column list
  // when `relation` is a view (Tables expose Columns() directly); ignored
  // for tables.
  // `bound_expressions` carries the IndexBinder's output (one per
  // `info->parsed_expressions`). For a bare column ref the slot is set but
  // unused; for an arbitrary expression we normalise + serialise
  // it via helpers to emit `ExpressionSpecific`.
  SereneDBPhysicalCreateIndex(
    duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Object> relation,
    std::vector<catalog::Column> view_columns, ObjectId database_id,
    duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
    std::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions,
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
  ObjectId TargetRelationId() const noexcept { return _relation->GetId(); }

 private:
  // Returns the columns of the relation. For tables: `Table::Columns()`;
  // for views: the `_view_columns` list synthesised from the view's bound
  // output schema.
  const std::vector<catalog::Column>& Columns() const noexcept;

  // Returns the `_relation` cast to a Table when it is one; nullptr for views.
  catalog::Table* TableOrNull() const noexcept;

  std::shared_ptr<catalog::Object> _relation;
  // Empty when `_relation` is a Table (use Columns()); populated when view.
  std::vector<catalog::Column> _view_columns;
  ObjectId _database_id;
  duckdb::unique_ptr<duckdb::CreateIndexInfo> _info;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _bound_expressions;
  SereneDBSchemaEntry& _schema_entry;
};

// create_plan callback registered with DuckDB's index type system.
// Called by PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex) when
// the index type has a custom plan function.
duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input);

}  // namespace sdb::connector
