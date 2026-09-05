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
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/execution/index/index_type.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <optional>

#include "catalog1/catalog.h"
#include "catalog1/permissions.h"
#include "connector/column_id.h"
#include "connector/file_manifest.h"

namespace sdb::catalog {

class SereneDBSchemaEntry;

}  // namespace sdb::catalog
namespace sdb::connector {

struct SereneDBCreateIndexInfo final : duckdb::CreateIndexInfo {
  SereneDBCreateIndexInfo() = default;
  explicit SereneDBCreateIndexInfo(duckdb::CreateIndexInfo&& base)
    : duckdb::CreateIndexInfo(base) {
    base.CopyProperties(*this);
    expressions = std::move(base.expressions);
    parsed_expressions = std::move(base.parsed_expressions);
    where_clause = std::move(base.where_clause);
  }

  // The index the pass refreshes, empty for a plain CREATE INDEX. It lives in
  // the schema this statement is qualified with, so the name is the whole
  // handle.
  duckdb::Identifier source_index;

  std::vector<std::string> delta_files;
  // New-file ids are `delta_file_base + listing ordinal` -- the pass scans
  // `WHERE file_index IN (ordinals)` and projects `file_index + base`.
  uint64_t delta_file_base = 0;

  std::shared_ptr<const search::FileManifest> manifest;

  duckdb::LogicalType generated_pk_type;

  // What this statement is, derived from the driver-written fields: a plain
  // CREATE INDEX, or one of the two REINDEX passes.
  enum class ReindexPass : uint8_t {
    None,
    Delta,
    Rebuild,
  };
  ReindexPass Pass() const noexcept {
    if (source_index.empty()) {
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
    result->generated_pk_type = generated_pk_type;
    return result;
  }
};

// One column of the relation a CREATE INDEX reads. A base table's list comes
// off its entry and a view's off the view body, so the operator carries the
// three facts both can answer with rather than either relation's own shape.
struct IndexRelationColumn {
  std::string name;
  duckdb::LogicalType type;
  ColumnId id;
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
//
// REINDEX passes (`SereneDBCreateIndexInfo::Pass()`) fill the EXISTING
// index like a plain CREATE INDEX: no catalog object is touched, the
// driver commits the removes before the pass, the sinks commit their
// docs above them (domain ticks), and Finalize publishes the new
// manifest. A died pass leaves the version mismatched, so the next tick
// relaunches.
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

  // `relation` is the catalog entry the index is built on: either a table or a
  // view (foreign-source-backed), which is where its id, its name and the
  // authority over it are read from.
  // `columns` is the relation's column list.
  // `bound_expressions` carries the IndexBinder's output (one per
  // `info->parsed_expressions`). For a bare column ref the slot is set but
  // unused; for an arbitrary expression we normalise + serialise
  // it via helpers into a `catalog::ExpressionData`.
  SereneDBPhysicalCreateIndex(
    duckdb::PhysicalPlan& plan, duckdb::CatalogEntry& relation,
    std::vector<IndexRelationColumn> columns, duckdb::idx_t database_id,
    duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions,
    duckdb::DuckSchemaEntry& schema_entry, duckdb::idx_t estimated_cardinality);

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

 private:
  // Returns the `_relation` cast to a Table when it is one; nullptr for views.
  duckdb::TableCatalogEntry* TableOrNull() const noexcept;
  bool IsDuckDBTable() const noexcept;

  // Not const: the build reads and publishes into the relation's own storage.
  duckdb::CatalogEntry& _relation;
  std::vector<IndexRelationColumn> _columns;
  duckdb::idx_t _database_id;
  duckdb::unique_ptr<duckdb::CreateIndexInfo> _info;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> _bound_expressions;

  duckdb::optional_ptr<const SereneDBCreateIndexInfo> Extras() const noexcept {
    return dynamic_cast<const SereneDBCreateIndexInfo*>(_info.get());
  }
  using ReindexPass = SereneDBCreateIndexInfo::ReindexPass;
  bool IsReindexPass() const noexcept {
    const auto extras = Extras();
    return extras && extras->Pass() != ReindexPass::None;
  }

  // First chunk slot holding a pipeline-computed indexed expression (0 when the
  // index has none); the projection appends them after the scanned columns.
  // Unset when the planner spliced no expression projection, which is not the
  // same fact as a base of 0.
  duckdb::optional_idx _expression_slot_base;
  duckdb::DuckSchemaEntry& _schema_entry;
};

// create_plan callback registered with DuckDB's index type system.
// Called by PhysicalPlanGenerator::CreatePlan(LogicalCreateIndex) when
// the index type has a custom plan function.
duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input);

}  // namespace sdb::connector
