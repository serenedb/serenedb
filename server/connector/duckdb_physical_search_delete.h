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

namespace sdb::catalog {

class InvertedIndex;

}  // namespace sdb::catalog
namespace sdb::connector {

// DELETE on a TableEngine::Search table, or on a view-backed index for the
// refresh's equality-delete scan road. Serial like duckdb's own delete
// sink; each chunk applies LIVE through the same remover DML uses -- table
// road: into the shard's serial iresearch trx plus the WAL delete payload;
// index road: into the index's transaction registered with the connection
// (EnsureIndexTransaction). Neither road commits here: the connection's
// commit machinery does, with its usual ticks.
class SereneDBSearchDelete final : public duckdb::PhysicalOperator {
 public:
  // Exactly one of `table` / `index` is set.
  SereneDBSearchDelete(duckdb::PhysicalPlan& plan,
                       std::shared_ptr<catalog::Table> table,
                       std::shared_ptr<const catalog::InvertedIndex> index,
                       std::vector<duckdb::idx_t> pk_col_indices,
                       duckdb::idx_t estimated_cardinality);

  bool IsSink() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const final;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;

  bool IsSource() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;

 private:
  std::shared_ptr<catalog::Table> _table;
  std::shared_ptr<const catalog::InvertedIndex> _index;
  // Positions in the input chunk of the PK columns (explicit PK), or the single
  // generated-PK rowid column (no-PK tables). Same layout PlanDelete computes.
  std::vector<duckdb::idx_t> _pk_col_indices;
};

}  // namespace sdb::connector
