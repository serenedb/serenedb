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

#include "connector/duckdb_physical_create_index.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <iostream>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/secondary_index.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_external_scan.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/duckdb_secondary_sink_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/secondary_index_shard.h"

namespace sdb::connector {
namespace {

struct InsertColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

// Build SK column mappings for secondary index backfill.
// Maps each index column to its position in the table column list.
std::vector<duckdb_secondary_key::SKColumn> BuildSKColumnsForBackfill(
  const catalog::Index& index, const catalog::Table& table) {
  const auto& columns = table.Columns();
  std::vector<duckdb_secondary_key::SKColumn> result;
  result.reserve(index.GetColumnIds().size());

  for (auto col_id : index.GetColumnIds()) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].id == col_id) {
        result.push_back(duckdb_secondary_key::SKColumn{
          .input_col_idx = i,
          .type = columns[i].type,
        });
        break;
      }
    }
  }
  return result;
}

struct CreateIndexGlobalState : public duckdb::GlobalSinkState {
  bool created = false;
  bool finalized = false;
  // TODO(mbkkt) fix this, drop by object id! Otherwise rename can break this
  ObjectId database_id;
  std::string database_name;
  std::string schema_name;
  std::string table_name;
  std::string index_name;
  catalog::ObjectType index_type = catalog::ObjectType::SecondaryIndex;

  // Column metadata (for serialization in Sink)
  ObjectId table_id;
  std::string table_key;
  std::vector<InsertColumnMeta> columns;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;

  bool is_external = false;
  duckdb::idx_t file_row_number_col_idx = 0;
  int64_t external_row_counter = 0;
  bool has_generated_pk_col = false;
  duckdb::idx_t generated_pk_col_idx = 0;

  // Index writer for the new index
  std::unique_ptr<DuckDBSinkIndexWriter> writer;

  // For inverted indexes: owned IResearch transaction
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  // Keep shard alive during backfill
  std::shared_ptr<IndexShard> index_shard;

  // Reusable buffers
  std::vector<std::string> row_keys;
  std::string value_buffer;
  duckdb::idx_t backfill_count = 0;
  duckdb::unique_ptr<DuckDBColumnSerializer> serializer;

  ~CreateIndexGlobalState() {
    search_trx.reset();
    if (created && !finalized) {
      try {
        auto& catalog = SerenedServer::Instance()
                          .getFeature<catalog::CatalogFeature>()
                          .Global();
        std::ignore = catalog.DropIndex(database_name, schema_name, index_name);
      } catch (...) {
      }
    }
  }
};

struct CreateIndexSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

}  // namespace

// --- Constructor ---

SereneDBPhysicalCreateIndex::SereneDBPhysicalCreateIndex(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  ObjectId database_id, duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
  SereneDBSchemaEntry& schema_entry, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)),
    _database_id(database_id),
    _info(std::move(info)),
    _schema_entry(schema_entry) {}

// --- GetGlobalSinkState: create index with tombstone ---

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalCreateIndex::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<CreateIndexGlobalState>();
  state->serializer = duckdb::make_uniq<DuckDBColumnSerializer>(
    duckdb::BufferAllocator::Get(context));
  state->database_id = _database_id;
  state->database_name = _schema_entry.catalog.GetName();
  state->schema_name = _schema_entry.name;
  state->table_name = _table->GetName();
  state->index_name = _info->index_name;

  auto& catalog_feature =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
  auto& catalog_impl = catalog_feature.Global();

  // Determine index type
  if (absl::EqualsIgnoreCase(_info->index_type, "inverted")) {
    state->index_type = catalog::ObjectType::InvertedIndex;
  } else {
    state->index_type = catalog::ObjectType::SecondaryIndex;
  }

  // Build CreateIndexColumn vector from parsed_expressions.
  // _info->names has ALL scan columns, not just index columns.
  // _info->parsed_expressions has the actual index column refs.
  const auto& columns = _table->Columns();
  std::vector<catalog::CreateIndexColumn> idx_columns;
  for (size_t i = 0; i < _info->parsed_expressions.size(); ++i) {
    auto& expr = _info->parsed_expressions[i];
    if (expr->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
      auto col_name = col_ref.GetColumnName();
      const catalog::Column* cat_col = nullptr;
      for (const auto& col : columns) {
        if (col.name == col_name) {
          cat_col = &col;
          break;
        }
      }
      if (!cat_col) {
        throw duckdb::CatalogException("column \"%s\" not found in table",
                                       col_name);
      }
      duckdb::case_insensitive_map_t<duckdb::Value> opclass_options;
      if (i < _info->column_opclass_options.size()) {
        opclass_options = _info->column_opclass_options[i];
      }
      idx_columns.push_back(catalog::CreateIndexColumn{
        .catalog_column = cat_col,
        .name = cat_col->name,
        .opclass = i < _info->column_opclasses.size()
                     ? _info->column_opclasses[i]
                     : std::string{},
        .opclass_options = std::move(opclass_options),
      });
    } else {
      throw duckdb::CatalogException(
        "Expression-based index columns are not supported");
    }
  }

  bool if_not_exists =
    _info->on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  Result create_result;
  if (state->index_type == catalog::ObjectType::InvertedIndex) {
    search::InvertedIndexShardOptions shard_options;
    auto it = _info->options.find("commit_interval");
    if (it != _info->options.end()) {
      shard_options.base.commit_interval_ms = it->second.GetValue<int64_t>();
    }
    it = _info->options.find("consolidation_interval");
    if (it != _info->options.end()) {
      shard_options.base.consolidation_interval_ms =
        it->second.GetValue<int64_t>();
    }
    it = _info->options.find("cleanup_interval_step");
    if (it != _info->options.end()) {
      shard_options.base.cleanup_interval_step = it->second.GetValue<int64_t>();
    }
    create_result = catalog_impl.CreateInvertedIndex(
      _database_id, _schema_entry.name, _table->GetName(), _info->index_name,
      std::move(idx_columns), shard_options, {.create_with_tombstone = true});
  } else {
    bool unique =
      (_info->constraint_type == duckdb::IndexConstraintType::UNIQUE);
    create_result = catalog_impl.CreateSecondaryIndex(
      _database_id, _schema_entry.name, _table->GetName(), _info->index_name,
      std::move(idx_columns), unique, {.create_with_tombstone = true});
  }

  if (create_result.is(ERROR_SERVER_DUPLICATE_NAME) && if_not_exists) {
    // Index already exists, nothing to do
    return state;
  }
  if (!create_result.ok()) {
    throw duckdb::CatalogException("Failed to create index: %s",
                                   create_result.errorMessage());
  }

  state->created = true;

  // Get fresh snapshot with the new index
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_index =
    snapshot->GetRelation(_database_id, _schema_entry.name, _info->index_name);
  SDB_ASSERT(catalog_index);
  auto shard = snapshot->GetIndexShard(catalog_index->GetId());
  SDB_ASSERT(shard);
  state->index_shard = shard;

  // Start background tasks for inverted indexes
  if (shard->GetType() == catalog::ObjectType::InvertedIndex) {
    auto& inverted_shard = basics::downCast<search::InvertedIndexShard>(*shard);
    inverted_shard.StartTasks();
  }

  // Set up column metadata for Sink
  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].id == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->columns.push_back(InsertColumnMeta{
      .id = columns[i].id,
      .duckdb_type = columns[i].type,
      .input_col_idx = i,
    });
  }
  state->pk_columns = duckdb_primary_key::BuildPKColumns(*_table);
  state->is_external = _table->GetTableType() == TableType::File;
  state->file_row_number_col_idx = columns.size();
  state->has_generated_pk_col =
    !state->is_external && _table->PKColumns().empty();
  state->generated_pk_col_idx = columns.size();

  // Create index writer for the new index
  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.AddRocksDBWrite();
  auto index = snapshot->GetObject<catalog::Index>(catalog_index->GetId());
  SDB_ASSERT(index);

  if (state->index_type == catalog::ObjectType::SecondaryIndex) {
    auto& sec_index = basics::downCast<const catalog::SecondaryIndex>(*index);
    auto sk_columns = BuildSKColumnsForBackfill(*index, *_table);
    auto& trx = conn_ctx.EnsureRocksDBTransaction();

    if (sec_index.IsUnique()) {
      state->writer = std::make_unique<DuckDBSecondarySinkInsertWriter<true>>(
        trx, shard->GetId(), index->GetColumnIds(), std::move(sk_columns));
    } else {
      state->writer = std::make_unique<DuckDBSecondarySinkInsertWriter<false>>(
        trx, shard->GetId(), index->GetColumnIds(), std::move(sk_columns));
    }
  } else {
    auto& inverted_shard = basics::downCast<search::InvertedIndexShard>(*shard);
    state->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
      inverted_shard.GetTransaction());
    auto& inverted_index =
      basics::downCast<const catalog::InvertedIndex>(*index);
    auto analyzer_provider = MakeAnalyzerProvider(snapshot, inverted_index);
    state->writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
      *state->search_trx, std::move(analyzer_provider), index->GetColumnIds());
  }
  return state;
}

// --- Sink: backfill existing data ---

duckdb::SinkResultType SereneDBPhysicalCreateIndex::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (!gstate.created || !gstate.writer) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  // Build row keys: [ObjectId][ColumnId(reserved)][PK bytes].
  //
  // Parquet external: PK = trailing BIGINT file_row_number column.
  // CSV / JSON external: PK = monotonic counter synthesized by the sink
  //   (the scan must be deterministic so the same counter is reproduced
  //   at query-time re-scan).
  // RocksDB: MakeColumnKey from the table's declared PK columns (or a
  //   generated pk for tables without an explicit PK).
  gstate.row_keys.clear();
  gstate.row_keys.reserve(num_rows);
  auto append_row_number_key = [&](int64_t row_number) {
    auto& key = gstate.row_keys.emplace_back();
    // Layout during construction: [ColumnId(4)][ObjectId(8)][PK(8)]
    basics::StrResize(key, sizeof(catalog::Column::Id) + sizeof(ObjectId));
    std::memcpy(key.data() + sizeof(catalog::Column::Id),
                gstate.table_key.data(), sizeof(ObjectId));
    primary_key::AppendSigned(key, row_number);
    // Final layout: [ObjectId(8)][ColumnId(4, filled later)][PK(8)].
    std::memcpy(key.data(), gstate.table_key.data(), sizeof(ObjectId));
  };
  if (gstate.is_external) {
    SDB_ASSERT(gstate.file_row_number_col_idx < chunk.ColumnCount());
    auto& rownum_vec = chunk.data[gstate.file_row_number_col_idx];
    rownum_vec.Flatten(num_rows);
    auto* rownums = duckdb::FlatVector::GetData<int64_t>(rownum_vec);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      append_row_number_key(rownums[row]);
    }
  } else if (gstate.is_external) {
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      append_row_number_key(gstate.external_row_counter++);
    }
  } else if (gstate.has_generated_pk_col) {
    SDB_ASSERT(gstate.generated_pk_col_idx < chunk.ColumnCount());
    auto& pk_vec = chunk.data[gstate.generated_pk_col_idx];
    pk_vec.Flatten(num_rows);
    auto* pks = duckdb::FlatVector::GetData<int64_t>(pk_vec);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      append_row_number_key(pks[row]);
    }
  } else {
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      duckdb_primary_key::MakeColumnKey(
        chunk, gstate.pk_columns, row, gstate.table_key, [](auto) {},
        gstate.row_keys.emplace_back());
    }
  }

  // Init writer for this batch
  gstate.writer->Init(num_rows, chunk);

  // Iterate columns -- same pattern as INSERT, but only write to the index
  DuckDBColumnSerializer::SstWriter noop{nullptr};
  for (const auto& col : gstate.columns) {
    if (col.input_col_idx >= chunk.ColumnCount()) {
      continue;
    }

    if (!gstate.writer->SwitchColumn(col.duckdb_type, /*have_nulls=*/true,
                                     col.id)) {
      continue;
    }

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_utils::SetupColumnForKey(gstate.row_keys[row], col.id);
    }

    DuckDBSinkIndexWriter* writer_ptr = gstate.writer.get();
    gstate.serializer->WriteColumn(noop, chunk.data[col.input_col_idx],
                                   col.duckdb_type, num_rows, gstate.row_keys,
                                   {&writer_ptr, 1});
  }

  gstate.writer->Finish();
  gstate.backfill_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

// --- Finalize: CommitWait + RemoveTombstone ---

duckdb::SinkFinalizeType SereneDBPhysicalCreateIndex::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (!gstate.created) {
    return duckdb::SinkFinalizeType::READY;
  }

  // For inverted indexes: flush writer, commit, then finish creation
  if (gstate.index_type == catalog::ObjectType::InvertedIndex &&
      gstate.index_shard) {
    // Close the writer and IResearch transaction so data is available for
    // commit
    gstate.writer.reset();
    gstate.search_trx.reset();

    auto& inverted_shard =
      basics::downCast<search::InvertedIndexShard>(*gstate.index_shard);
    // Synchronous commit wait
    auto future = inverted_shard.CommitWait();
    std::ignore = std::move(future).Get().Ok();
    SDB_IF_FAILURE("crash_before_finish_creation") { SDB_IMMEDIATE_ABORT(); }
    inverted_shard.FinishCreation();
  }

  SDB_IF_FAILURE("crash_before_remove_tombstone") { SDB_IMMEDIATE_ABORT(); }
  // Remove tombstone -- index is now fully built
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto r = catalog.RemoveTombstone(_database_id, gstate.schema_name,
                                   gstate.index_name);
  if (!r.ok()) {
    throw duckdb::InternalException("Failed to remove tombstone: %s",
                                    r.errorMessage());
  }
  gstate.finalized = true;

  return duckdb::SinkFinalizeType::READY;
}

// --- Source (returns CREATE INDEX tag) ---

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalCreateIndex::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<CreateIndexSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalCreateIndex::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<CreateIndexSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<CreateIndexGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.backfill_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

// --- create_plan callback ---

duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input) {
  auto& op = input.op;
  if (!op.info) {
    throw duckdb::InternalException("CreateIndexInfo is null in create_plan");
  }
  auto& table_entry = RequireBaseTable(op.table);
  auto sdb_table = table_entry.GetSereneDBTable();
  auto& sdb_catalog = table_entry.schema.catalog.Cast<SereneDBCatalog>();
  auto database_id = sdb_catalog.GetDatabaseId();
  auto& schema_entry = dynamic_cast<SereneDBSchemaEntry&>(table_entry.schema);

  auto& create_index = input.planner.Make<SereneDBPhysicalCreateIndex>(
    std::move(sdb_table), database_id, std::move(op.info), schema_entry,
    op.estimated_cardinality);
  create_index.children.push_back(input.table_scan);
  return create_index;
}

}  // namespace sdb::connector
