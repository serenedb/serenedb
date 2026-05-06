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

#include "catalog/table.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "rocksdb/sst_file_writer.h"

namespace sdb::catalog {

class Sequence;

}  // namespace sdb::catalog
namespace sdb::connector {

struct SSTInsertColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

struct SSTInsertGlobalState : public duckdb::GlobalSinkState {
  duckdb::idx_t insert_count = 0;

  // SST writers -- one per data column
  std::vector<std::unique_ptr<rocksdb::SstFileWriter>> writers;
  std::vector<SSTInsertColumnMeta> columns;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;

  std::string sst_directory;
  ObjectId table_id;
  std::string table_key;

  rocksdb::DB* db = nullptr;
  rocksdb::ColumnFamilyHandle* cf = nullptr;

  std::shared_ptr<catalog::Sequence> generated_pk_seq;

  // Index writers -- created once, reused per Sink() call
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> index_writers;

  // Reusable buffers
  std::vector<std::string> row_keys;
  std::string value_buffer;
  std::vector<DuckDBSinkIndexWriter*> active_writers;
  duckdb::unique_ptr<DuckDBColumnSerializer> serializer;

  bool has_data = false;
  bool finalized = false;

  ~SSTInsertGlobalState() override;
};

class SereneDBPhysicalSSTInsert : public duckdb::PhysicalOperator {
 public:
  SereneDBPhysicalSSTInsert(duckdb::PhysicalPlan& plan,
                            std::shared_ptr<catalog::Table> table,
                            duckdb::vector<duckdb::LogicalType> types,
                            duckdb::idx_t estimated_cardinality);

  // Sink interface
  bool IsSink() const final { return true; }
  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const override;
  duckdb::SinkResultType Sink(duckdb::ExecutionContext& context,
                              duckdb::DataChunk& chunk,
                              duckdb::OperatorSinkInput& input) const final;
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const override;

  // Source interface -- returns insert count
  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const final;
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;
  bool IsSource() const final { return true; }

 protected:
  // Sets up SST writers on state for the given table. Does NOT create index
  // writers.
  static void SetupSSTState(SSTInsertGlobalState& state,
                            const catalog::Table& table);

  std::shared_ptr<catalog::Table> _table;
};

}  // namespace sdb::connector
