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

#include "catalog1/entry/search_table.h"
#include "connector/primary_key.h"

namespace irs {

class IndexFieldOptions;

}  // namespace irs
namespace sdb::search {

class InvertedIndexStorage;

}  // namespace sdb::search
namespace sdb::connector {

// DELETE on a TableEngine::Search table. Single-threaded like the RocksDB
// delete operator (no ParallelSink / local sink state): each row's PK is
// encoded and (1) handed to the shard's serial iresearch trx as a removal and
// (2) recorded on the transaction as the WAL delete payload.
class SereneDBSearchDelete final : public duckdb::PhysicalOperator {
 public:
  // `return_chunk` is RETURNING: `types` is then the table row followed by its
  // row-identity virtual columns, and `return_columns` maps each table column
  // to its slot in the child's chunk. Otherwise the operator reports the row
  // count and `types` is one BIGINT.
  SereneDBSearchDelete(
    duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
    duckdb::vector<duckdb::LogicalType> types,
    duckdb::idx_t estimated_cardinality, bool return_chunk,
    duckdb::vector<duckdb::idx_t> return_columns);

  // The remove side of a REINDEX pass: DELETE FROM <index>. Removes go into
  // the index's own writer on domain ticks, and no search-table WAL is
  // written -- a died pass relaunches from the manifest-version mismatch.
  SereneDBSearchDelete(
    duckdb::PhysicalPlan& plan,
    std::shared_ptr<search::InvertedIndexStorage> storage,
    std::shared_ptr<const irs::IndexFieldOptions> field_options,
    std::vector<primary_key::PKColumn> pk_columns,
    duckdb::vector<duckdb::LogicalType> types,
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
  // The index road: DELETE FROM <index>, reachable only from a REINDEX pass.
  bool IsReindexDelete() const noexcept { return !!_index_storage; }

  template<typename GlobalState>
  duckdb::SinkResultType SinkImpl(duckdb::DataChunk& chunk,
                                  GlobalState& gstate) const;

  duckdb::optional_ptr<const catalog::SearchTableEntry> _table;
  // The PK columns as they arrive in the input chunk (explicit PK), or the
  // single generated-PK rowid column (no-PK tables).
  std::vector<primary_key::PKColumn> _pk_columns;
  bool _return_chunk = false;
  duckdb::vector<duckdb::idx_t> _return_columns;
  std::shared_ptr<search::InvertedIndexStorage> _index_storage;
  std::shared_ptr<const irs::IndexFieldOptions> _field_options;
};

}  // namespace sdb::connector
