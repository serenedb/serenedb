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
#include <optional>

#include "basics/down_cast.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/pk_spec.h"
#include "catalog/table.h"
#include "connector/file_manifest.h"

namespace sdb::connector {

class SereneDBSchemaEntry;

struct SereneDBCreateIndexInfo final : duckdb::CreateIndexInfo {
  SereneDBCreateIndexInfo() = default;
  explicit SereneDBCreateIndexInfo(duckdb::CreateIndexInfo&& base)
    : duckdb::CreateIndexInfo(base) {
    base.CopyProperties(*this);
    expressions = std::move(base.expressions);
    parsed_expressions = std::move(base.parsed_expressions);
    where_clause = std::move(base.where_clause);
  }

  ObjectId source_index;

  std::vector<std::string> delta_files;
  // New-file ids are `delta_file_base + listing ordinal` -- the pass scans
  // `WHERE file_index IN (ordinals)` and projects `file_index + base`.
  uint64_t delta_file_base = 0;

  std::shared_ptr<const search::FileManifest> manifest;

  std::optional<catalog::PkSpec> fast_path_pk_spec;
  duckdb::LogicalType generated_pk_type;
  std::vector<duckdb::idx_t> kept_positions;

  // What this statement is, derived from the driver-written fields: a plain
  // CREATE INDEX, or one of the two REINDEX passes.
  enum class ReindexPass : uint8_t {
    None,
    Delta,
    Rebuild,
  };
  ReindexPass Pass() const noexcept {
    if (!source_index.isSet()) {
      return ReindexPass::None;
    }
    return delta_files.empty() ? ReindexPass::Rebuild : ReindexPass::Delta;
  }

  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const override {
    auto base = duckdb::CreateIndexInfo::Copy();
    auto result = duckdb::make_uniq<SereneDBCreateIndexInfo>(
      std::move(base->Cast<duckdb::CreateIndexInfo>()));
    result->source_index = source_index;
    result->delta_files = delta_files;
    result->delta_file_base = delta_file_base;
    result->manifest = manifest;
    result->fast_path_pk_spec = fast_path_pk_spec;
    result->generated_pk_type = generated_pk_type;
    result->kept_positions = kept_positions;
    return result;
  }
};

// Physical operator for CREATE INDEX on SereneDB tables.
// Replaces DuckDB's native PhysicalCreateIndex which requires DuckTableEntry.
//
// Pipeline: TableScan -> SereneDBPhysicalCreateIndex (sink)
//
// Lifecycle:
//   Sink:               receive data chunks, write to index storage (backfill)
//   GetGlobalSinkState: create index in catalog with tombstone
//   Finalize:           Refresh (inverted) + RemoveTombstone
//   On error:           destructor drops the index (rollback)
//
// REINDEX passes (`SereneDBCreateIndexInfo::Pass()`) fill the EXISTING
// index like a plain CREATE INDEX: no catalog object is touched, the
// driver commits the removes before the pass, the sinks commit their
// docs above them (domain ticks), and Finalize publishes the new
// manifest. A died pass leaves the version mismatched, so the next tick
// relaunches.
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
  ObjectId TargetRelationId() const noexcept { return _relation->GetId(); }

 private:
  // Returns the columns of the relation. For tables: `Table::Columns()`;
  // for views: the `_view_columns` list synthesised from the view's bound
  // output schema.
  const std::vector<catalog::Column>& Columns() const noexcept;

  // Returns the `_relation` cast to a Table when it is one; nullptr for views.
  catalog::Table* TableOrNull() const noexcept;
  bool IsDuckDBTable() const noexcept;

  std::shared_ptr<catalog::Object> _relation;
  // Empty when `_relation` is a Table (use Columns()); populated when view.
  std::vector<catalog::Column> _view_columns;
  ObjectId _database_id;
  duckdb::unique_ptr<duckdb::CreateIndexInfo> _info;
  std::vector<duckdb::unique_ptr<duckdb::Expression>> _bound_expressions;
  // Bound partial-index predicate (info->where_clause); null for full indexes.
  duckdb::unique_ptr<duckdb::Expression> _bound_where;
  // The statement's SereneDBCreateIndexInfo view of _info: the bind
  // captures (manifest, pk spec/type) plus the optional REINDEX pass
  // identity ride the statement info itself, so prepared re-executions
  // carry them by construction. The bind hook upgrades every create to the
  // subclass, so this never fails.
  const SereneDBCreateIndexInfo& Info() const noexcept {
    return basics::downCast<const SereneDBCreateIndexInfo>(*_info);
  }
  using ReindexPass = SereneDBCreateIndexInfo::ReindexPass;
  bool IsReindexPass() const noexcept {
    return Info().Pass() != ReindexPass::None;
  }
  bool IsDeltaPass() const noexcept {
    return Info().Pass() == ReindexPass::Delta;
  }
  bool IsRebuildPass() const noexcept {
    return Info().Pass() == ReindexPass::Rebuild;
  }

  SereneDBSchemaEntry& _schema_entry;
};

// create_plan callback registered with DuckDB's index type system.
// Called by PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex) when
// the index type has a custom plan function.
duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input);

}  // namespace sdb::connector
