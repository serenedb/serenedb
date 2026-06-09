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

// Parallel physical operator for writing into a search-backed table
// (StorageKind::kSearch). It is the *single* write path for search tables --
// regular INSERT, COPY FROM, INSERT...SELECT, and CREATE TABLE AS SELECT all
// route here. (RocksDB needs separate SST / CTAS operators because its bulk
// path differs from its row path; search has no such split.) Selected at
// plan time by SereneDBCatalog::PlanInsert / PlanCreateTableAs when the
// target reports kSearch -- the rocksdb-bound operators are bypassed
// entirely (different write contract: no DuckDBColumnSerializer, no
// secondary RocksDB indexes, all rows land in iresearch via the shared
// search-table iresearch sink, SearchSinkInsertBaseImpl in search-table mode).
//
// Two construction modes:
//   * Insert mode  -- target table already exists; ctor takes the Table.
//   * CTAS mode    -- target table is created (tombstoned) in
//                     GetGlobalSinkState from the BoundCreateTableInfo, and
//                     the tombstone is removed in Finalize on success
//                     (rollback handled by the global-state dtor). The data
//                     path (Sink / buffer / drain / markers) is identical to
//                     insert mode.
//
// Lifecycle (per design D15):
//   Sink(chunk):     write the chunk into iresearch via WriteChunkToSearchSink
//                    (the shared search-table sink) AND, on the inline path,
//                    buffer it in the per-txn LocalTableChanges for the INLINE
//                    WAL record; bulk threads stream a chunk file instead.
//   Finalize():      register this statement's writes on the txn (seg_ids /
//                    column ids). The iresearch transaction + the WAL record
//                    are committed by query::Transaction::Commit later (same
//                    path InvertedIndexShard already uses).
//
// Scope: no PK conflict detection yet (duplicate INSERTs silently
// double-write). Reads land in M4; conflict resolution in M6.
class SereneDBSearchInsert final : public duckdb::PhysicalOperator {
 public:
  // Insert mode: pre-existing target table.
  SereneDBSearchInsert(duckdb::PhysicalPlan& plan,
                       std::shared_ptr<catalog::Table> table,
                       duckdb::vector<duckdb::LogicalType> types,
                       duckdb::idx_t estimated_cardinality);

  // CTAS mode: create the target table from `info` in GetGlobalSinkState.
  // Output type is hardcoded to BIGINT (the inserted-row count), like
  // SereneDBPhysicalCTAS.
  SereneDBSearchInsert(duckdb::PhysicalPlan& plan,
                       duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
                       duckdb::SchemaCatalogEntry& schema,
                       duckdb::idx_t estimated_cardinality);

  // Sink interface
  bool IsSink() const final { return true; }
  // Parallel: each sink thread writes its own iresearch segment via its own
  // IndexWriter::Transaction (see GetLocalSinkState), handed to
  // query::Transaction at Combine and committed on the shared tick. The
  // engine fans out based on source parallelism, so INSERT...VALUES still
  // runs single-threaded while INSERT...SELECT / COPY / CTAS parallelise.
  // Always insert-only (UPDATE/DELETE on search tables land in M6), so no
  // removes ever flow through here -- the zero-queries invariant asserted in
  // Transaction::Commit holds. Order-independent (PK is identity, not row
  // order), so no batch index / SinkOrderDependent needed.
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

  // Source interface -- returns insert count
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;
  bool IsSource() const final { return true; }

 private:
  // Insert mode: the pre-existing target table. Null in CTAS mode (the
  // table is created at GetGlobalSinkState time and lives on the global
  // sink state from then on).
  std::shared_ptr<catalog::Table> _table;

  // CTAS mode only -- mutually exclusive with _table. The
  // BoundCreateTableInfo carries the column list + WITH options; the
  // schema is where the new table lands. Raw pointer (not optional_ptr)
  // so dereferencing in the const GetGlobalSinkState yields a non-const
  // SchemaCatalogEntry&; null in insert mode.
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> _ctas_info;
  duckdb::SchemaCatalogEntry* _ctas_schema = nullptr;
};

}  // namespace sdb::connector
