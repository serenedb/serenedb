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
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/execution/operator/persistent/physical_batch_insert.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <memory>
#include <string>

#include "catalog/identifiers/object_id.h"
#include "catalog/sequence.h"

namespace sdb::connector {

// Physical operator for CREATE TABLE AS SELECT into a kSearch table.
// Inherits PhysicalBatchInsert (INSERT-mode) so bulk-source CTAS (parquet,
// file scan, persistent-table scan) gets the row-group bulk-write path.
//
// Lifecycle (mirrors duckdb_physical_ctas.cpp's RocksDB CTAS):
//   - Plan time (PlanCreateTableAs): allocates `cache_table_id`, creates
//     the per-shard cache table in `sdb_cache$` via a separate connection,
//     and constructs this operator with the cache DuckTableEntry. The
//     user catalog is NOT touched yet -- mutating it during planning
//     bumps the catalog version and trips PreparedStatementData::
//     RequireRebind, causing DuckDB to re-bind and re-plan.
//   - GetGlobalSinkState: creates the serened catalog Table (tombstoned)
//     using the pre-allocated `cache_table_id`. Resolves the
//     generated-PK sequence from the resulting catalog snapshot. Marks
//     the cache DB transaction read-write so commit accepts the writes
//     (mirrors the SetReadWrite handshake in SereneDBCachedInsert).
//   - Sink: prepends `sdb_op$=0` + `sdb_pk$=ReserveWriteUnsafe(N)` to
//     each chunk and forwards to PhysicalBatchInsert::Sink.
//   - Finalize: parent finalizes the bulk write, then we remove the
//     tombstone on the serened catalog Table.
//
// On any failure between catalog write and tombstone removal, the
// catalog entry stays tombstoned and the drop-task cleans up both sides.
class SereneDBCachedCtas final : public duckdb::PhysicalBatchInsert {
 public:
  SereneDBCachedCtas(
    duckdb::PhysicalPlan& plan,
    duckdb::DuckTableEntry& cache_table,
    duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>>
      bound_constraints,
    duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
    duckdb::SchemaCatalogEntry& user_schema, ObjectId database_id,
    ObjectId cache_table_id, duckdb::idx_t user_col_count,
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

 private:
  // CTAS info + target user schema (immutable plan-time inputs).
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> _info;
  duckdb::SchemaCatalogEntry& _user_schema;
  // Serened identifiers captured at plan time.
  ObjectId _database_id;
  ObjectId _cache_table_id;
  duckdb::idx_t _user_col_count;
  // GetGlobalSinkState writes these once (single-execution operator);
  // Sink/Finalize read them. Marked mutable so the const Sink hooks can
  // observe state set by the const GetGlobalSinkState hook.
  mutable std::string _user_table_name;
  mutable std::shared_ptr<catalog::Sequence> _gen_pk_seq;
};

}  // namespace sdb::connector
