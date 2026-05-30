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

#include <absl/strings/match.h>

#include <atomic>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/lambda_expression.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <iostream>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/secondary_index.h"
#include "catalog/view.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/duckdb_secondary_sink_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/index_expression.hpp"
#include "connector/json_extract_names.hpp"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/search_field_name.hpp"
#include "connector/search_sink_writer.hpp"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/progress_tracker.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/secondary_index_shard.h"

namespace sdb::connector {
namespace {

struct InsertColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
  catalog::ColumnStoreMode store_mode;
};

// `chunk_columns` is the chunk-ordered list of columns the scan projects
// (state->columns). For each indexed column, find its chunk position; the
// secondary writer's BuildSK reads from chunk.data[input_col_idx].
std::vector<duckdb_secondary_key::SKColumn> BuildSKColumnsForBackfill(
  const catalog::Index& index,
  std::span<const InsertColumnMeta> chunk_columns) {
  std::vector<duckdb_secondary_key::SKColumn> result;
  result.reserve(index.GetColumnIds().size());

  for (auto col_id : index.GetColumnIds()) {
    for (size_t i = 0; i < chunk_columns.size(); ++i) {
      if (chunk_columns[i].id == col_id) {
        result.push_back(duckdb_secondary_key::SKColumn{
          .input_col_idx = i,
          .type = chunk_columns[i].duckdb_type,
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

  ObjectId table_id;
  std::string table_key;
  std::vector<InsertColumnMeta> columns;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;

  duckdb::idx_t file_row_number_col_idx = 0;
  duckdb::idx_t file_index_col_idx = 0;
  duckdb::idx_t generated_pk_col_idx = 0;
  bool is_external = false;
  bool is_glob_external = false;
  bool has_generated_pk_col = false;
  // No PK column in the chunk -- Sink synthesises a monotonic counter.
  bool is_view_synth_pk = false;
  bool is_view_rocksdb_pk = false;

  std::atomic<int64_t> view_row_counter_atomic{0};
  std::atomic<duckdb::idx_t> backfill_count_atomic{0};
  int64_t external_row_counter = 0;

  std::unique_ptr<DuckDBSinkIndexWriter> writer;
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::vector<std::string> row_keys;
  duckdb::unique_ptr<DuckDBColumnSerializer> serializer;

  std::shared_ptr<IndexShard> index_shard;
  std::shared_ptr<const catalog::Snapshot> snapshot_for_providers;
  std::shared_ptr<catalog::Index> index_for_providers;

  std::string value_buffer;

  pg::ProgressReporter* progress = nullptr;

  ~CreateIndexGlobalState() {
    search_trx.reset();
    if (created && !finalized) {
      try {
        auto& catalog = SerenedServer::Instance()
                          .getFeature<catalog::CatalogFeature>()
                          .Global();
        std::ignore =
          catalog.DropIndex(database_name, schema_name, index_name, true);
      } catch (...) {
      }
    }
  }
};

struct CreateIndexLocalState : public duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<DuckDBSinkIndexWriter> writer;
  duckdb::unique_ptr<DuckDBColumnSerializer> serializer;
  std::vector<std::string> row_keys;

  ~CreateIndexLocalState() override {
    writer.reset();
    search_trx.reset();
  }
};

struct CreateIndexSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

}  // namespace

SereneDBPhysicalCreateIndex::SereneDBPhysicalCreateIndex(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Object> relation,
  std::vector<catalog::Column> view_columns, ObjectId database_id,
  duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
  std::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions,
  SereneDBSchemaEntry& schema_entry, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _relation(std::move(relation)),
    _view_columns(std::move(view_columns)),
    _database_id(database_id),
    _info(std::move(info)),
    _bound_expressions(std::move(bound_expressions)),
    _schema_entry(schema_entry) {}

catalog::Table* SereneDBPhysicalCreateIndex::TableOrNull() const noexcept {
  if (_relation && _relation->GetType() == catalog::ObjectType::Table) {
    return static_cast<catalog::Table*>(_relation.get());
  }
  return nullptr;
}

const std::vector<catalog::Column>& SereneDBPhysicalCreateIndex::Columns()
  const noexcept {
  if (auto* t = TableOrNull()) {
    return t->Columns();
  }
  return _view_columns;
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalCreateIndex::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<CreateIndexGlobalState>();
  state->serializer = duckdb::make_uniq<DuckDBColumnSerializer>(
    duckdb::BufferAllocator::Get(context));
  state->database_id = _database_id;
  state->database_name = _schema_entry.catalog.GetName();
  state->schema_name = _schema_entry.name;
  state->table_name = std::string{_relation->GetName()};
  state->index_name = _info->index_name;

  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    state->progress = sdb_state->progress.get();
  }

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
  const auto& columns = Columns();
  std::vector<catalog::CreateIndexColumn> idx_columns;
  auto resolve_column = [&](std::string_view col_name) {
    for (const auto& col : columns) {
      if (absl::EqualsIgnoreCase(col.GetName(), col_name)) {
        return &col;
      }
    }
    return static_cast<const catalog::Column*>(nullptr);
  };

  auto make_column_ids = [&](auto&& positions) {
    return std::forward<decltype(positions)>(positions) |
           std::views::transform(
             [&](size_t pos) { return columns[pos].GetId(); }) |
           std::ranges::to<std::vector<catalog::Column::Id>>();
  };

  auto* table_for_proj = TableOrNull();
  const auto col_index_to_id =
    table_for_proj
      ? make_column_ids(BuildCreateIndexProjection(table_for_proj->Columns(),
                                                   table_for_proj->PKColumns(),
                                                   _info->column_ids))
      : make_column_ids(std::views::iota(size_t{0}, columns.size()));
  const auto relation_id =
    table_for_proj ? table_for_proj->GetId() : _relation->GetId();

  idx_columns.reserve(_info->parsed_expressions.size());
  for (size_t i = 0; i < _info->parsed_expressions.size(); ++i) {
    auto& expr = _info->parsed_expressions[i];
    std::string opclass = i < _info->column_opclasses.size()
                            ? _info->column_opclasses[i]
                            : std::string{};

    std::optional<duckdb::case_insensitive_map_t<duckdb::Value>>
      opclass_options;
    if (i < _info->column_opclass_options.size()) {
      opclass_options = _info->column_opclass_options[i];
    }

    if (expr->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
      auto col_name = col_ref.GetColumnName();
      const auto* cat_col = resolve_column(col_name);
      if (!cat_col) {
        throw duckdb::CatalogException("column \"%s\" not found in table",
                                       col_name);
      }
      idx_columns.emplace_back(catalog::CreateIndexColumn{
        .catalog_column = cat_col,
        .name = cat_col->GetName(),
        .opclass = std::move(opclass),
        .opclass_options = std::move(opclass_options),
      });
      continue;
    }

    SDB_ASSERT(i < _bound_expressions.size() && _bound_expressions[i],
               "bound expression is missing for inverted index expression");
    const auto& bound_expr = _bound_expressions[i];

    auto normalized = NormalizeBoundExpression(*bound_expr, relation_id,
                                               col_index_to_id, context);
    std::string serialized = SerializeBoundExpression(*normalized);
    auto dependent_columns = CollectDependentColumns(*normalized);
    if (dependent_columns.empty()) {
      throw duckdb::CatalogException(
        "indexed expression must reference at least one base table column");
    }
    auto return_type = normalized->GetReturnType();
    auto& indexed_column = idx_columns.emplace_back(
      nullptr, "", std::move(opclass),
      catalog::ExpressionData{
        .serialized_expr = std::move(serialized),
        .dependent_columns = std::move(dependent_columns),
        .return_type = std::move(return_type),
        .pretty_printed = expr->ToString(),
      },
      std::move(opclass_options));
    indexed_column.name = indexed_column.indexed_expr->pretty_printed;
  }

  bool if_not_exists =
    _info->on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  Result create_result;
  if (state->index_type == catalog::ObjectType::InvertedIndex) {
    auto find_with = [&](std::string_view name) -> const duckdb::Value* {
      auto it = _info->options.find(name);
      return it != _info->options.end() ? &it->second : nullptr;
    };
    auto resolve_uint = [&](std::string_view name) -> uint32_t {
      if (auto* v = find_with(name)) {
        return v->GetValue<uint32_t>();
      }
      duckdb::Value v;
      auto r = context.TryGetCurrentSetting(std::string{name}, v);
      SDB_ASSERT(r, "missing DB-level default for setting: ", name);
      return v.GetValue<uint32_t>();
    };

    catalog::InvertedIndexOptions options{
      .row_group_size = resolve_uint("row_group_size"),
      .norm_row_group_size = resolve_uint("norm_row_group_size"),
      .refresh_interval_ms = resolve_uint("refresh_interval"),
      .compaction_interval_ms = resolve_uint("compaction_interval"),
      .cleanup_interval_step = resolve_uint("cleanup_interval_step"),
    };
    if (auto* v = find_with("optimize_top_k")) {
      auto value =
        v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>();
      options.topk_scorer = catalog::ParseScorerExpression(context, value);
    }

    create_result = catalog_impl.CreateInvertedIndex(
      _database_id, _schema_entry.name, _relation->GetName(), _info->index_name,
      std::move(idx_columns), std::move(options),
      {.create_with_tombstone = true});
  } else {
    bool unique =
      (_info->constraint_type == duckdb::IndexConstraintType::UNIQUE);
    create_result = catalog_impl.CreateSecondaryIndex(
      _database_id, _schema_entry.name, _relation->GetName(), _info->index_name,
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
  if (state->progress) {
    state->progress->SetPhase(pg::create_index_progress::Phase::BuildingIndex);
  }
  auto shard = snapshot->GetIndexShard(catalog_index->GetId());
  SDB_ASSERT(shard);
  state->index_shard = shard;

  if (shard->GetType() == catalog::ObjectType::InvertedIndex) {
    auto& inverted_shard = basics::downCast<search::InvertedIndexShard>(*shard);
    // Must be set before StartTasks so the first Commit's meta_payload records
    // it.
    if (auto it = _info->options.find("_sdb_iceberg_snapshot_id");
        it != _info->options.end()) {
      inverted_shard.SetIcebergSnapshotId(it->second.GetValue<int64_t>());
    }
    inverted_shard.StartTasks();
  }

  auto* table_ptr = TableOrNull();
  state->table_id = table_ptr ? table_ptr->GetId() : _relation->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);
  // Populated only on the base-table branch; describes the chunk-order list
  // of catalog positions the scan projects. Reused below for PK chunk-index
  // resolution.
  std::vector<size_t> projection;
  if (table_ptr) {
    // Base-table backfill: BindCreateIndex narrowed the scan to index
    // columns + PK columns. Mirror that projection here so chunk positions
    // and state->columns agree. input_col_idx is the chunk position, not
    // the catalog index.
    projection = BuildCreateIndexProjection(
      table_ptr->Columns(), table_ptr->PKColumns(), _info->column_ids);
    state->columns.reserve(projection.size());
    for (size_t chunk_idx = 0; chunk_idx < projection.size(); ++chunk_idx) {
      const auto& col = columns[projection[chunk_idx]];
      state->columns.push_back(InsertColumnMeta{
        .id = col.GetId(),
        .duckdb_type = col.type,
        .input_col_idx = chunk_idx,
        .store_mode = col.store_mode,
      });
    }
  } else {
    // View-backed: chunk holds the view body's projection at catalog
    // positions; trailing positions hold virtual PK columns appended by
    // BindCreateIndex.
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == catalog::Column::kGeneratedPKId) {
        continue;
      }
      state->columns.push_back(InsertColumnMeta{
        .id = columns[i].GetId(),
        .duckdb_type = columns[i].type,
        .input_col_idx = i,
        .store_mode = columns[i].store_mode,
      });
    }
  }
  state->file_row_number_col_idx = state->columns.size();
  state->generated_pk_col_idx = state->columns.size();
  if (auto it = _info->options.find("_sdb_view_fast_path_pk");
      !table_ptr && it != _info->options.end()) {
    const auto kind = it->second.GetValue<std::string>();
    if (kind == "rocksdb_explicit_pk") {
      state->is_view_rocksdb_pk = true;
    } else if (kind == "rocksdb_rowid") {
      // Same wire format as a table-backed no-PK index.
      state->has_generated_pk_col = true;
    } else {
      state->is_external = true;
      if (kind == "file_index_plus_row_number") {
        state->is_glob_external = true;
        state->file_index_col_idx = state->columns.size();
        state->file_row_number_col_idx = state->columns.size() + 1;
      }
    }
  }
  if (table_ptr) {
    state->has_generated_pk_col = table_ptr->PKColumns().empty();
    state->is_view_synth_pk = false;
    // PK chunk positions: each PK column's chunk index is its position in
    // the projection. BuildCreateIndexProjection guarantees PK columns are
    // present in the projection when there's an explicit PK.
    if (!state->has_generated_pk_col) {
      auto projected =
        projection |
        std::views::transform(
          [&](size_t pos) -> const catalog::Column& { return columns[pos]; });
      state->pk_columns =
        duckdb_primary_key::BuildPKColumns(projected, table_ptr->PKColumns());
    }
  } else if (state->is_view_rocksdb_pk) {
    auto& view = basics::downCast<const catalog::PgSqlView>(*_relation);
    auto fp = ResolveViewFastPath(context, view);
    SDB_ASSERT(fp && fp->base_table,
               "rocksdb_explicit_pk fast-path lost ViewFastPath::base_table");
    auto base_t = fp->base_table;
    const auto& base_cols = base_t->Columns();
    const auto& pk_ids = base_t->PKColumns();
    state->pk_columns.reserve(pk_ids.size());
    duckdb::idx_t trailing_pos = state->columns.size();
    for (auto pk_id : pk_ids) {
      for (const auto& c : base_cols) {
        if (c.GetId() == pk_id) {
          state->pk_columns.push_back(duckdb_primary_key::PKColumn{
            .input_col_idx = trailing_pos,
            .type = c.type,
          });
          ++trailing_pos;
          break;
        }
      }
    }
    state->has_generated_pk_col = false;
    state->is_view_synth_pk = false;
  } else if (state->is_external) {
    state->is_view_synth_pk = false;
    state->has_generated_pk_col = false;
  } else if (state->has_generated_pk_col) {
    state->is_view_synth_pk = false;
  } else {
    state->has_generated_pk_col = false;
    state->is_view_synth_pk = true;
  }

  auto& conn_ctx = GetSereneDBContext(context);
  auto index = snapshot->GetObject<catalog::Index>(catalog_index->GetId());
  SDB_ASSERT(index);

  if (state->index_type == catalog::ObjectType::SecondaryIndex) {
    auto& sec_index = basics::downCast<const catalog::SecondaryIndex>(*index);
    auto sk_columns = BuildSKColumnsForBackfill(*index, state->columns);
    auto& trx = conn_ctx.GetRocksDBTransaction();

    if (sec_index.IsUnique()) {
      state->writer = std::make_unique<DuckDBSecondarySinkInsertWriter<true>>(
        trx, shard->GetId(), index->GetColumnIds(), std::move(sk_columns));
    } else {
      state->writer = std::make_unique<DuckDBSecondarySinkInsertWriter<false>>(
        trx, shard->GetId(), index->GetColumnIds(), std::move(sk_columns));
    }
  } else {
    state->snapshot_for_providers = snapshot;
    state->index_for_providers = index;
  }
  return state;
}

bool SereneDBPhysicalCreateIndex::ParallelSink() const {
  return _info && absl::EqualsIgnoreCase(_info->index_type, "inverted");
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBPhysicalCreateIndex::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  if (!ParallelSink()) {
    return duckdb::make_uniq<duckdb::LocalSinkState>();
  }
  auto* gstate_ptr =
    sink_state ? &sink_state->Cast<CreateIndexGlobalState>() : nullptr;
  if (!gstate_ptr || !gstate_ptr->created || !gstate_ptr->index_shard ||
      !gstate_ptr->index_for_providers) {
    return duckdb::make_uniq<duckdb::LocalSinkState>();
  }
  auto& gstate = *gstate_ptr;

  auto& inverted_shard =
    basics::downCast<search::InvertedIndexShard>(*gstate.index_shard);
  const auto& inverted_index =
    basics::downCast<const catalog::InvertedIndex>(*gstate.index_for_providers);

  auto lstate = duckdb::make_uniq<CreateIndexLocalState>();
  lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
    inverted_shard.GetTransaction());
  lstate->serializer = duckdb::make_uniq<DuckDBColumnSerializer>(
    duckdb::BufferAllocator::Get(context.client));

  auto tokenizer_provider =
    MakeTokenizerProvider(gstate.snapshot_for_providers, inverted_index);
  auto store_values_provider = MakeStoreValuesProvider(inverted_index);
  auto is_text_indexed_provider = MakeIsTextIndexedProvider(inverted_index);
  auto hnsw_info_provider = MakeHNSWInfoProvider(inverted_index);
  auto expr_tokenizer_provider = MakeExpressionTokenizerProvider(
    gstate.snapshot_for_providers, inverted_index);
  auto indexed_exprs = MakeIndexedExpressions(inverted_index, context.client);
  lstate->writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
    *lstate->search_trx, std::move(tokenizer_provider),
    gstate.index_for_providers->GetColumnIds(),
    std::move(store_values_provider), std::move(is_text_indexed_provider),
    std::move(hnsw_info_provider), std::move(expr_tokenizer_provider),
    std::move(indexed_exprs));

  return lstate;
}

duckdb::SinkResultType SereneDBPhysicalCreateIndex::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (!gstate.created) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  const bool parallel = ParallelSink();
  CreateIndexLocalState* lstate = nullptr;
  if (parallel) {
    lstate = dynamic_cast<CreateIndexLocalState*>(&input.local_state);
    if (!lstate || !lstate->writer) {
      return duckdb::SinkResultType::NEED_MORE_INPUT;
    }
  } else if (!gstate.writer) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto* writer = parallel ? lstate->writer.get() : gstate.writer.get();
  auto* serializer =
    parallel ? lstate->serializer.get() : gstate.serializer.get();
  auto& row_keys = parallel ? lstate->row_keys : gstate.row_keys;

  // Row key layout: [ObjectId][ColumnId(reserved)][PK bytes].
  row_keys.clear();
  row_keys.reserve(num_rows);
  auto append_row_number_key = [&](int64_t row_number) {
    auto& key = row_keys.emplace_back();
    basics::StrResize(key, sizeof(catalog::Column::Id) + sizeof(ObjectId));
    std::memcpy(key.data() + sizeof(catalog::Column::Id),
                gstate.table_key.data(), sizeof(ObjectId));
    primary_key::AppendSigned(key, row_number);
    std::memcpy(key.data(), gstate.table_key.data(), sizeof(ObjectId));
  };
  auto append_glob_key = [&](int64_t file_index, int64_t row_number) {
    auto& key = row_keys.emplace_back();
    basics::StrResize(key, sizeof(catalog::Column::Id) + sizeof(ObjectId));
    std::memcpy(key.data() + sizeof(catalog::Column::Id),
                gstate.table_key.data(), sizeof(ObjectId));
    primary_key::AppendSigned(key, file_index);
    primary_key::AppendSigned(key, row_number);
    std::memcpy(key.data(), gstate.table_key.data(), sizeof(ObjectId));
  };
  if (gstate.is_glob_external) {
    SDB_ASSERT(gstate.file_index_col_idx < chunk.ColumnCount());
    SDB_ASSERT(gstate.file_row_number_col_idx < chunk.ColumnCount());
    auto& fi_vec = chunk.data[gstate.file_index_col_idx];
    auto& rn_vec = chunk.data[gstate.file_row_number_col_idx];
    duckdb::UnifiedVectorFormat fi_fmt;
    duckdb::UnifiedVectorFormat rn_fmt;
    fi_vec.ToUnifiedFormat(num_rows, fi_fmt);
    rn_vec.ToUnifiedFormat(num_rows, rn_fmt);
    // file_index is UBIGINT but always non-negative -- AppendSigned is
    // bijective.
    auto* fis = duckdb::UnifiedVectorFormat::GetData<uint64_t>(fi_fmt);
    auto* rns = duckdb::UnifiedVectorFormat::GetData<int64_t>(rn_fmt);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      const auto fi_idx = fi_fmt.sel->get_index(row);
      const auto rn_idx = rn_fmt.sel->get_index(row);
      append_glob_key(static_cast<int64_t>(fis[fi_idx]), rns[rn_idx]);
    }
  } else if (gstate.is_external) {
    SDB_ASSERT(gstate.file_row_number_col_idx < chunk.ColumnCount());
    auto& rownum_vec = chunk.data[gstate.file_row_number_col_idx];
    duckdb::UnifiedVectorFormat fmt;
    rownum_vec.ToUnifiedFormat(num_rows, fmt);
    auto* rownums = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      append_row_number_key(rownums[fmt.sel->get_index(row)]);
    }
  } else if (gstate.is_view_synth_pk) {
    // View-backed: no PK column in chunk; synthesise a monotonic counter.
    const int64_t base = gstate.view_row_counter_atomic.fetch_add(
      static_cast<int64_t>(num_rows), std::memory_order_relaxed);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      append_row_number_key(base + static_cast<int64_t>(row));
    }
  } else if (gstate.has_generated_pk_col) {
    SDB_ASSERT(gstate.generated_pk_col_idx < chunk.ColumnCount());
    auto& pk_vec = chunk.data[gstate.generated_pk_col_idx];
    duckdb::UnifiedVectorFormat fmt;
    pk_vec.ToUnifiedFormat(num_rows, fmt);
    auto* pks = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      append_row_number_key(pks[fmt.sel->get_index(row)]);
    }
  } else {
    std::vector<duckdb::UnifiedVectorFormat> pk_formats;
    duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      duckdb_primary_key::MakeColumnKey(
        pk_formats, gstate.pk_columns, row, gstate.table_key, [](auto) {},
        row_keys.emplace_back());
    }
  }

  writer->Init(num_rows, chunk);

  DuckDBColumnSerializer::SstWriter noop{nullptr};
  for (const auto& col : gstate.columns) {
    if (col.input_col_idx >= chunk.ColumnCount()) {
      continue;
    }

    const ColumnDescriptor desc{col.id, col.store_mode, col.duckdb_type,
                                /*have_nulls=*/true};
    if (!writer->SwitchColumn(desc, chunk.data[col.input_col_idx], num_rows)) {
      continue;
    }

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_utils::SetupColumnForKey(row_keys[row], col.id);
    }

    DuckDBSinkIndexWriter* writer_ptr = writer;
    serializer->WriteColumn(noop, chunk.data[col.input_col_idx], num_rows,
                            row_keys, {&writer_ptr, 1}, desc);
  }

  if (auto indexed_exprs = writer->IndexedExpressions();
      !indexed_exprs.empty()) {
    auto slot_to_col_ids = gstate.columns |
                           std::views::transform(&InsertColumnMeta::id) |
                           std::ranges::to<std::vector<catalog::Column::Id>>();
    EvaluateAndWriteIndexedExpressions(
      *writer, indexed_exprs, chunk, gstate.table_id, slot_to_col_ids,
      context.client, num_rows, row_keys, *serializer);
  }

  writer->Finish();
  gstate.backfill_count_atomic.fetch_add(num_rows, std::memory_order_relaxed);
  if (gstate.progress) {
    gstate.progress->Add(pg::create_index_progress::Param::TuplesDone,
                         num_rows);
  }
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBPhysicalCreateIndex::Combine(
  duckdb::ExecutionContext& /*context*/,
  duckdb::OperatorSinkCombineInput& input) const {
  if (auto* lstate = dynamic_cast<CreateIndexLocalState*>(&input.local_state)) {
    lstate->writer.reset();
    lstate->search_trx.reset();
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBPhysicalCreateIndex::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (!gstate.created) {
    return duckdb::SinkFinalizeType::READY;
  }

  if (gstate.index_type == catalog::ObjectType::InvertedIndex &&
      gstate.index_shard) {
    if (gstate.progress) {
      gstate.progress->SetPhase(pg::create_index_progress::Phase::Committing);
    }
    gstate.writer.reset();
    gstate.search_trx.reset();

    auto& inverted_shard =
      basics::downCast<search::InvertedIndexShard>(*gstate.index_shard);
    auto future = inverted_shard.CommitWait();
    std::ignore = std::move(future).Get().Ok();
    SDB_IF_FAILURE("crash_before_finish_creation") { SDB_IMMEDIATE_ABORT(); }
    inverted_shard.FinishCreation();
  }

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::create_index_progress::Phase::Finalizing);
  }
  SDB_IF_FAILURE("crash_before_remove_tombstone") { SDB_IMMEDIATE_ABORT(); }
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
  const auto count = static_cast<int64_t>(
    gstate.backfill_count_atomic.load(std::memory_order_relaxed));
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input) {
  auto& op = input.op;
  if (!op.info) {
    throw duckdb::InternalException("CreateIndexInfo is null in create_plan");
  }

  auto* sdb_catalog = dynamic_cast<SereneDBCatalog*>(&op.table.ParentCatalog());
  if (!sdb_catalog) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot CREATE INDEX on ", op.table.ParentCatalog().GetName(),
              ".", op.table.name,
              ": its catalog differs from the current one (",
              duckdb::DatabaseManager::GetDefaultDatabase(input.context), ")"));
  }
  auto& schema_entry = op.table.ParentSchema().Cast<SereneDBSchemaEntry>();
  auto database_id = sdb_catalog->GetDatabaseId();

  std::shared_ptr<catalog::Object> relation;
  std::vector<catalog::Column> view_columns;

  if (op.table.type == duckdb::CatalogType::VIEW_ENTRY) {
    // Foreign-source view: resolve the SereneDB-catalog PgSqlView by name
    // and synthesise a column list from its bound schema.
    auto& conn_ctx = GetSereneDBContext(input.context);
    auto snapshot = conn_ctx.EnsureCatalogSnapshot();
    relation =
      snapshot->GetRelation(database_id, schema_entry.name, op.table.name);
    if (!relation || relation->GetType() != catalog::ObjectType::PgSqlView) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("view \"", op.table.name, "\" not found in SereneDB catalog"));
    }
    auto& view = basics::downCast<catalog::PgSqlView>(*relation);
    const auto& vinfo = view.GetInfo();
    // When pruning narrowed the view, build view_columns only for the
    // surviving positions. column.id keeps the original view position
    // so downstream id-based lookups stay stable.
    std::vector<size_t> view_positions;
    if (auto it = op.info->options.find("_sdb_view_kept_positions");
        it != op.info->options.end()) {
      for (const auto& v : duckdb::ListValue::GetChildren(it->second)) {
        view_positions.push_back(v.GetValue<uint64_t>());
      }
    } else {
      view_positions.reserve(vinfo.names.size());
      for (size_t i = 0; i < vinfo.names.size(); ++i) {
        view_positions.push_back(i);
      }
    }
    view_columns.reserve(view_positions.size());
    for (auto p : view_positions) {
      SDB_ASSERT(p < vinfo.names.size());
      view_columns.emplace_back(ObjectId{}, catalog::Column::Id{p},
                                vinfo.names[p], vinfo.types[p]);
      view_columns.back().SetId(catalog::Column::Id{p});
    }
  } else {
    auto& table_catalog = op.table.Cast<duckdb::TableCatalogEntry>();
    auto& table_entry = RequireBaseTable(table_catalog);
    relation = table_entry.GetSereneDBTable();
  }

  auto& create_index = input.planner.Make<SereneDBPhysicalCreateIndex>(
    std::move(relation), std::move(view_columns), database_id,
    std::move(op.info), std::move(op.unbound_expressions), schema_entry,
    op.estimated_cardinality);
  create_index.children.push_back(input.table_scan);
  return create_index;
}

}  // namespace sdb::connector
