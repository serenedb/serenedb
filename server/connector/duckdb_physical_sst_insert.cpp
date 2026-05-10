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
#include "basics/debugging.h"
#include "basics/system-compiler.h"
#include "catalog/object.h"
#include "catalog/sequence.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

struct SSTInsertSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

}  // namespace

SSTInsertGlobalState::~SSTInsertGlobalState() {
  if (!finalized && !sst_directory.empty()) {
    std::error_code ec;
    std::filesystem::remove_all(sst_directory, ec);
  }
}

// --- SetupSSTState: shared SST setup for both INSERT and CTAS ---

void SereneDBPhysicalSSTInsert::SetupSSTState(SSTInsertGlobalState& state,
                                              const catalog::Table& table) {
  auto& engine = GetServerEngine();
  state.db = engine.db();
  state.cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(state.cf);

  state.table_id = table.GetId();
  state.table_key = key_utils::PrepareTableKey(state.table_id);
  state.pk_columns = duckdb_primary_key::BuildPKColumns(table);

  // Build column metadata -- skip generated PK and virtual generated columns
  const auto& columns = table.Columns();
  size_t input_idx = 0;
  for (const auto& col : columns) {
    if (col.id == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state.columns.push_back(SSTInsertColumnMeta{
      .id = col.id,
      .duckdb_type = col.type,
      .input_col_idx = input_idx,
    });
    ++input_idx;
  }

  // Create SST directory
  state.sst_directory = absl::StrCat(engine.path(), "/", kBulkInsertDir, "/",
                                     catalog::NextId().id());
  std::filesystem::create_directories(state.sst_directory);

  // Configure SstFileWriter options
  auto options = state.db->GetOptions(state.cf);
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
  state.writers.resize(state.columns.size());
  for (size_t i = 0; i < state.columns.size(); ++i) {
    state.writers[i] = std::make_unique<rocksdb::SstFileWriter>(env, options);
    auto sst_path = absl::StrCat(state.sst_directory, "/column_", i, "_.sst");
    auto status = state.writers[i]->Open(sst_path);
    if (!status.ok()) {
      SDB_THROW(rocksutils::ConvertStatus(status));
    }
  }
}

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
  state->serializer = duckdb::make_uniq<DuckDBColumnSerializer>(
    duckdb::BufferAllocator::Get(context));

  SetupSSTState(*state, *_table);

  // Create index writers
  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.AddRocksDBWrite();
  state->table_shard =
    conn_ctx.EnsureCatalogSnapshot()->GetTableShard(state->table_id);
  SDB_ASSERT(state->table_shard);
  state->table_lock = std::unique_lock{state->table_shard->GetTableLock()};
  state->index_writers = CreateDuckDBIndexWriters<DuckDBWriteKind::Insert>(
    state->table_id, conn_ctx, *_table);

  state->generated_pk_seq =
    conn_ctx.EnsureCatalogSnapshot()->GetObject<catalog::Sequence>(
      _table->GetGeneratedPkSeqId());
  SDB_ASSERT(state->generated_pk_seq || !_table->PKColumns().empty());

  return state;
}

// --- Sink ---

duckdb::SinkResultType SereneDBPhysicalSSTInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SSTInsertGlobalState>();

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  gstate.has_data = true;

  // Build row keys: [ObjectId][ColumnId(reserved)][PK bytes]
  gstate.row_keys.clear();
  gstate.row_keys.reserve(num_rows);

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

  uint64_t generated_pk_base =
    gstate.generated_pk_seq
      ? gstate.generated_pk_seq->ReserveWriteUnsafe(num_rows)
      : 0;

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    duckdb_primary_key::MakeColumnKey(
      pk_formats, gstate.pk_columns, row, generated_pk_base + row,
      gstate.table_key, [](auto) {}, gstate.row_keys.emplace_back());
  }

  // Write each column to its SST file
  for (size_t col = 0; col < gstate.columns.size(); ++col) {
    const auto& meta = gstate.columns[col];

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_utils::SetupColumnForKey(gstate.row_keys[row], meta.id);
    }

    DuckDBColumnSerializer::SstWriter sst_writer{gstate.writers[col].get()};
    gstate.serializer->WriteColumn(sst_writer, chunk.data[meta.input_col_idx],
                                   meta.duckdb_type, num_rows, gstate.row_keys,
                                   {});
  }

  // Update indexes via transaction path
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

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_utils::SetupColumnForKey(gstate.row_keys[row], meta.id);
    }

    DuckDBColumnSerializer::SstWriter noop{nullptr};
    gstate.serializer->WriteColumn(noop, chunk.data[meta.input_col_idx],
                                   meta.duckdb_type, num_rows, gstate.row_keys,
                                   gstate.active_writers);
  }

  for (auto& writer : gstate.index_writers) {
    writer->Finish();
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

  // Register IResearch pending segments BEFORE the ingest so the background
  // commit thread waits for us and cannot advance _committed_tick past the
  // ingest seqno before we deliver our Commit(seq).
  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.RegisterSearchFlushes();

  rocksdb::IngestExternalFileOptions ingest_options;
  ingest_options.move_files = true;

  auto status =
    gstate.db->IngestExternalFile(gstate.cf, sst_files, ingest_options);
  if (!status.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(status));
  }
  SDB_IF_FAILURE("crash_sst_sink_after_ingest") { SDB_IMMEDIATE_ABORT(); }

  // Commit IResearch transactions with the post-ingest sequence number.
  // first_tick = post_ingest_seq (> _committed_tick) regardless of batch size.
  const uint64_t post_ingest_seq = gstate.db->GetLatestSequenceNumber();
  conn_ctx.CommitSearchTransactions(post_ingest_seq);

  if (gstate.insert_count > 0) {
    conn_ctx.UpdateNumRows(gstate.table_id,
                           static_cast<int64_t>(gstate.insert_count));
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
