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
#include <memory>
#include <vector>

#include "catalog/table.h"

namespace sdb::connector {

// DELETE on a StorageKind::kSearch table. Single-threaded like the RocksDB
// delete operator (no ParallelSink / local sink state): each row's PK is
// encoded and (1) handed to the shard's serial iresearch trx as a removal and
// (2) recorded on the transaction as the WAL delete payload.
class SereneDBSearchDelete final : public duckdb::PhysicalOperator {
 public:
  SereneDBSearchDelete(duckdb::PhysicalPlan& plan,
                       std::shared_ptr<catalog::Table> table,
                       std::vector<duckdb::idx_t> pk_col_indices,
                       duckdb::idx_t estimated_cardinality);

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
  // Positions in the input chunk of the PK columns (explicit PK), or the single
  // generated-PK rowid column (no-PK tables). Same layout PlanDelete computes.
  std::vector<duckdb::idx_t> _pk_col_indices;
};

}  // namespace sdb::connector
