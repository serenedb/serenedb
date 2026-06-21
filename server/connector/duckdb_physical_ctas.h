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

#include <duckdb/execution/operator/persistent/physical_insert.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <string>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"

namespace sdb::connector {

// Owner operator for CREATE TABLE AS SELECT on SereneDB tables. It wraps a
// native duckdb::PhysicalInsert (the CTAS variant that creates and fills the
// hidden store table) and runs the whole sink under a SECOND __sdb_store
// transaction: minted with a committed snapshot, installed as a scoped override
// on the user's MetaTransaction for the pipeline, and committed independently
// in Finalize -- so the load reads committed data only and survives a user
// ROLLBACK. The catalog entry + tombstone are written at plan time; the
// tombstone is cleared after the data commits.
class SereneDBPhysicalCTAS final : public duckdb::PhysicalOperator {
 public:
  SereneDBPhysicalCTAS(duckdb::PhysicalPlan& plan,
                       duckdb::PhysicalOperator& insert, ObjectId database_id,
                       std::string database_name, std::string schema_name,
                       catalog::CreateTableOptions options, ObjectId table_id,
                       duckdb::idx_t estimated_cardinality);

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
  // Forwarded so a wrapped PhysicalBatchInsert receives its per-batch indices;
  // without it the batch sink never advances current_index and asserts.
  duckdb::SinkNextBatchType NextBatch(
    duckdb::ExecutionContext& context,
    duckdb::OperatorSinkNextBatchInput& input) const final;
  bool IsSink() const final { return true; }
  bool ParallelSink() const final { return _insert.ParallelSink(); }
  bool SinkOrderDependent() const final { return _insert.SinkOrderDependent(); }
  // BatchIndex() for PhysicalBatchInsert, NoPartitionInfo for PhysicalInsert.
  duckdb::OperatorPartitionInfo RequiredPartitionInfo() const final {
    return _insert.RequiredPartitionInfo();
  }

  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;
  bool IsSource() const final { return true; }

  std::string GetName() const final { return "SDB_CREATE_TABLE_AS"; }

 private:
  // Not in the plan tree; this operator drives its sink directly. Either a
  // duckdb::PhysicalInsert or a duckdb::PhysicalBatchInsert, chosen the same
  // way duckdb's native CTAS picks (batch for partitionable order-preserving
  // loads).
  duckdb::PhysicalOperator& _insert;
  ObjectId _database_id;
  std::string _database_name;
  std::string _schema_name;
  // The facade table is created at execution (once) with this pre-allocated id.
  catalog::CreateTableOptions _options;
  ObjectId _table_id;
};

}  // namespace sdb::connector
