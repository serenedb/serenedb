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

#include "connector/duckdb_physical_sst_insert.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <filesystem>

#include "basics/assert.h"
#include "catalog/identifiers/revision_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

inline constexpr std::string_view kBulkInsertDir = "bulk_insert";

}  // namespace

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
  std::string table_key;  // [ObjectId] prefix

  rocksdb::DB* db = nullptr;
  rocksdb::ColumnFamilyHandle* cf = nullptr;

  // Index writers -- created once, reused per Sink() call
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> index_writers;

  // Reusable buffers -- allocated once, reused across Sink() calls
  std::vector<std::string> row_keys;  // [ColumnId(reserved)][ObjectId][PK]
  std::string value_buffer;
  std::vector<DuckDBSinkIndexWriter*> active_writers;
  duckdb::unique_ptr<DuckDBColumnSerializer> serializer =
    duckdb::make_uniq<DuckDBColumnSerializer>();

  bool has_data = false;
  bool finalized = false;

  ~SSTInsertGlobalState() override {
    if (!finalized && !sst_directory.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(sst_directory, ec);
    }
  }
};

struct SSTInsertSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

// --- Constructor ---

SereneDBPhysicalSSTInsert::SereneDBPhysicalSSTInsert(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(std::move(table)) {}

// --- GetGlobalSinkState ---

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalSSTInsert::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SSTInsertGlobalState>();

  auto& engine = GetServerEngine();
  state->db = engine.db();
  state->cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(state->cf);

  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);
  state->pk_columns = duckdb_primary_key::BuildPKColumns(*_table);

  // Build column metadata -- skip generated PK column
  const auto& columns = _table->Columns();
  size_t input_idx = 0;
  for (const auto& col : columns) {
    if (col.id == catalog::Column::kGeneratedPKId) {
      ++input_idx;
      continue;
    }
    state->columns.push_back(SSTInsertColumnMeta{
      .id = col.id,
      .duckdb_type = col.type,
      .input_col_idx = input_idx,
    });
    ++input_idx;
  }

  // Create SST directory
  state->sst_directory = absl::StrCat(engine.path(), "/", kBulkInsertDir, "/",
                                      RevisionId::create().id());
  std::filesystem::create_directories(state->sst_directory);

  // Configure SstFileWriter options (from sst_sink_writer.cpp)
  auto options = state->db->GetOptions(state->cf);
  options.PrepareForBulkLoad();

  rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy = nullptr;
  options.table_factory.reset(
    rocksdb::NewBlockBasedTableFactory(table_options));

  options.compression = rocksdb::kNoCompression;
  options.compression_per_level.clear();

  rocksdb::EnvOptions env;
  env.use_direct_writes = true;

  // Open one SST file per column
  state->writers.resize(state->columns.size());
  for (size_t i = 0; i < state->columns.size(); ++i) {
    state->writers[i] = std::make_unique<rocksdb::SstFileWriter>(env, options);
    auto sst_path = absl::StrCat(state->sst_directory, "/column_", i, "_.sst");
    auto status = state->writers[i]->Open(sst_path);
    if (!status.ok()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }
  }

  // Create index writers once
  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.AddRocksDBWrite();
  state->index_writers = CreateDuckDBIndexWriters<DuckDBWriteKind::Insert>(
    state->table_id, conn_ctx, *_table);

  return state;
}

// --- Sink ---

duckdb::SinkResultType SereneDBPhysicalSSTInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SSTInsertGlobalState>();

  chunk.Flatten();
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  gstate.has_data = true;

  // Build row keys with reserved ColumnId slot:
  // Layout: [ColumnId(reserved)][ObjectId][PK bytes]
  // Reuses gstate.row_keys capacity across Sink() calls.
  duckdb_primary_key::CreateBatchWithColumnSlot(
    chunk, gstate.pk_columns, gstate.table_key, gstate.row_keys);

  // Write each column to its SST file.
  // Only overwrites ColumnId in-place per column -- no string copy.
  for (size_t col = 0; col < gstate.columns.size(); ++col) {
    const auto& meta = gstate.columns[col];
    auto& writer = *gstate.writers[col];

    bool is_complex = meta.duckdb_type.id() == duckdb::LogicalTypeId::LIST ||
                      meta.duckdb_type.id() == duckdb::LogicalTypeId::MAP ||
                      meta.duckdb_type.id() == duckdb::LogicalTypeId::STRUCT;

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_utils::SetupColumnForKey(gstate.row_keys[row], meta.id);

      rocksdb::Slice slice;
      if (is_complex) {
        auto& vec = chunk.data[meta.input_col_idx];
        if (!duckdb::FlatVector::Validity(vec).RowIsValid(row)) {
          slice = {};
        } else {
          gstate.serializer->ResetForNewRow();
          if (meta.duckdb_type.id() == duckdb::LogicalTypeId::LIST) {
            gstate.serializer->WriteListValue(vec, row, meta.duckdb_type);
          } else if (meta.duckdb_type.id() == duckdb::LogicalTypeId::MAP) {
            gstate.serializer->WriteMapValue(vec, row, meta.duckdb_type);
          } else {
            gstate.serializer->WriteStructValue(vec, row, meta.duckdb_type);
          }
          slice = gstate.serializer->Finalize(gstate.value_buffer);
        }
      } else {
        slice = SerializeScalarValue(chunk.data[meta.input_col_idx], row,
                                     meta.duckdb_type, gstate.value_buffer);
      }

      auto status = writer.Put(gstate.row_keys[row], slice);
      if (!status.ok()) {
        SDB_THROW(ERROR_INTERNAL, "SST write failed: ", status.ToString());
      }
    }
  }

  // Update indexes via transaction path (different key ordering than SST)
  if (!gstate.index_writers.empty()) {
    for (auto& writer : gstate.index_writers) {
      writer->Init(num_rows, chunk);
    }

    for (const auto& meta : gstate.columns) {
      gstate.active_writers.clear();
      for (auto& writer : gstate.index_writers) {
        if (writer->SwitchColumn(meta.duckdb_type, /*have_nulls=*/true,
                                 meta.id)) {
          gstate.active_writers.push_back(writer.get());
        }
      }

      if (gstate.active_writers.empty()) {
        continue;
      }

      bool is_complex_idx =
        meta.duckdb_type.id() == duckdb::LogicalTypeId::LIST ||
        meta.duckdb_type.id() == duckdb::LogicalTypeId::MAP ||
        meta.duckdb_type.id() == duckdb::LogicalTypeId::STRUCT;

      for (duckdb::idx_t row = 0; row < num_rows; ++row) {
        key_utils::SetupColumnForKey(gstate.row_keys[row], meta.id);

        rocksdb::Slice slice;
        if (is_complex_idx) {
          auto& vec = chunk.data[meta.input_col_idx];
          if (!duckdb::FlatVector::Validity(vec).RowIsValid(row)) {
            slice = {};
          } else {
            gstate.serializer->ResetForNewRow();
            if (meta.duckdb_type.id() == duckdb::LogicalTypeId::LIST) {
              gstate.serializer->WriteListValue(vec, row, meta.duckdb_type);
            } else if (meta.duckdb_type.id() == duckdb::LogicalTypeId::MAP) {
              gstate.serializer->WriteMapValue(vec, row, meta.duckdb_type);
            } else {
              gstate.serializer->WriteStructValue(vec, row, meta.duckdb_type);
            }
            slice = gstate.serializer->Finalize(gstate.value_buffer);
          }
        } else {
          slice = SerializeScalarValue(chunk.data[meta.input_col_idx], row,
                                       meta.duckdb_type, gstate.value_buffer);
        }

        for (auto* writer : gstate.active_writers) {
          writer->Write({&slice, 1}, gstate.row_keys[row]);
        }
      }
    }

    for (auto& writer : gstate.index_writers) {
      writer->Finish();
    }
  }

  gstate.insert_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

// --- Finalize ---

duckdb::SinkFinalizeType SereneDBPhysicalSSTInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = sink_state->Cast<SSTInsertGlobalState>();

  if (!gstate.has_data) {
    gstate.finalized = true;
    std::error_code ec;
    std::filesystem::remove_all(gstate.sst_directory, ec);
    return duckdb::SinkFinalizeType::READY;
  }

  std::vector<std::string> sst_files;
  sst_files.reserve(gstate.writers.size());

  for (auto& writer : gstate.writers) {
    rocksdb::ExternalSstFileInfo file_info;
    auto status = writer->Finish(&file_info);
    if (!status.ok()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }
    sst_files.push_back(file_info.file_path);
  }

  rocksdb::IngestExternalFileOptions ingest_options;
  ingest_options.move_files = true;

  auto status =
    gstate.db->IngestExternalFile(gstate.cf, sst_files, ingest_options);
  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }

  gstate.finalized = true;
  return duckdb::SinkFinalizeType::READY;
}

// --- Source (returns insert count) ---

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalSSTInsert::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SSTInsertSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalSSTInsert::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SSTInsertSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SSTInsertGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
