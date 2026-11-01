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
#include <duckdb/common/enums/row_id_handling.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <memory>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/primary_key.h"

namespace sdb::connector {

// DELETE on a TableEngine::Search table. Single-threaded (no ParallelSink /
// local sink state): the transaction buffers each row's rowid as a removal.
class SereneDBSearchDelete final : public duckdb::PhysicalOperator {
 public:
  // `return_chunk` is RETURNING: `types` is then the table row followed by the
  // virtual columns `returning_virtual_columns` lists, in that order, and
  // `return_columns` maps each table column to the expression that reads it.
  // Otherwise the operator reports the row count and `types` is one BIGINT.
  SereneDBSearchDelete(
    duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
    duckdb::vector<duckdb::LogicalType> types,
    duckdb::idx_t estimated_cardinality, bool return_chunk,
    duckdb::vector<duckdb::idx_t> return_columns,
    const duckdb::vector<duckdb::column_t>& returning_virtual_columns,
    duckdb::RowIdHandling row_id_handling);

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
  duckdb::optional_ptr<const catalog::SearchTableEntry> _table;
  primary_key::PKColumn _rowid;
  bool _return_chunk = false;
  duckdb::vector<duckdb::idx_t> _return_columns;
  duckdb::RowIdHandling _row_id_handling;
};

}  // namespace sdb::connector
