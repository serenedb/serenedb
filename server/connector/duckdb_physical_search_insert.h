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
#include <memory>

#include "catalog/table.h"

namespace sdb::connector {

// Parallel physical operator for INSERT into a search-backed table
// (StorageKind::kSearch). Selected at plan time by SereneDBCatalog::PlanInsert
// when the target shard reports kSearch -- the rocksdb-bound
// SereneDBPhysicalInsert is bypassed entirely (different write contract:
// no DuckDBColumnSerializer, no secondary RocksDB indexes, all rows land
// in iresearch via SearchTableSinkWriter).
//
// Lifecycle (per design D15):
//   Sink(chunk):     append the input chunk to the per-txn LocalTableChanges
//                    buffer for this table_id. No iresearch write yet.
//   Finalize():      drain the buffer -- one SwitchColumn per column, one
//                    per-row PK Write -- through SearchTableSinkWriter, then
//                    emit WAL markers via EmitInsertsForBuffer. The iresearch
//                    transaction is committed by query::Transaction::Commit
//                    later (same path InvertedIndexShard already uses).
//
// PR 3.4 scope: minimal write path. No PK conflict detection (duplicate INSERTs
// silently double-write); generated-PK tables are rejected (need explicit PK).
// Reads land in M4; conflict resolution in M6.
class SereneDBSearchInsert final : public duckdb::PhysicalOperator {
 public:
  SereneDBSearchInsert(duckdb::PhysicalPlan& plan,
                       std::shared_ptr<catalog::Table> table,
                       duckdb::vector<duckdb::LogicalType> types,
                       duckdb::idx_t estimated_cardinality);

  // Sink interface
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

  // Source interface -- returns insert count
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;
  bool IsSource() const final { return true; }

 private:
  std::shared_ptr<catalog::Table> _table;
};

}  // namespace sdb::connector
