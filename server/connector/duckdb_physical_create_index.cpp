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

#include <absl/algorithm/container.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
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
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/duckdb_secondary_sink_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/json_extract_names.hpp"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/search_sink_writer.hpp"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/secondary_index_shard.h"

namespace sdb::connector {
namespace {

// TODO(mkornaukhov) do not build path manually, see #597
bool TryLiftJsonPath(const duckdb::ParsedExpression& e, std::string& col_name,
                     std::vector<std::string>& out_path) {
  out_path.clear();
  // Reject when outtermost extraction does not return string
  if (e.GetExpressionClass() != duckdb::ExpressionClass::FUNCTION ||
      !IsJsonExtractString(
        e.Cast<duckdb::FunctionExpression>().function_name)) {
    return false;
  }

  const duckdb::ParsedExpression* cur = &e;
  while (cur->GetExpressionType() != duckdb::ExpressionType::COLUMN_REF) {
    const duckdb::ParsedExpression* key = nullptr;
    const duckdb::ParsedExpression* next_lhs = nullptr;

    switch (cur->GetExpressionClass()) {
      case duckdb::ExpressionClass::LAMBDA: {
        const auto& l = cur->Cast<duckdb::LambdaExpression>();
        if (!l.lhs || !l.expr) {
          return false;
        }
        next_lhs = l.lhs.get();
        key = l.expr.get();
        break;
      }
      case duckdb::ExpressionClass::FUNCTION: {
        const auto& f = cur->Cast<duckdb::FunctionExpression>();
        if (!IsJsonExtract(f.function_name) || f.children.size() != 2) {
          return false;
        }
        next_lhs = f.children[0].get();
        key = f.children[1].get();
        break;
      }
      default:
        return false;
    }

    if (key->GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
      return false;
    }
    const auto& key_const = key->Cast<duckdb::ConstantExpression>();
    if (key_const.value.IsNull() ||
        !AppendJsonPathKey(key_const.value, out_path)) {
      return false;
    }
    cur = next_lhs;
  }

  col_name = cur->Cast<duckdb::ColumnRefExpression>().GetColumnName();
  // We collected leaf-to-root; flip to root-to-leaf for downstream code.
  absl::c_reverse(out_path);
  return true;
}

struct InsertColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

std::vector<duckdb_secondary_key::SKColumn> BuildSKColumnsForBackfill(
  const catalog::Index& index, std::span<const catalog::Column> columns) {
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

  ObjectId table_id;
  std::string table_key;
  std::vector<InsertColumnMeta> columns;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;

  duckdb::idx_t file_row_number_col_idx = 0;
  duckdb::idx_t file_index_col_idx = 0;
  duckdb::idx_t generated_pk_col_idx = 0;
  int64_t external_row_counter = 0;
  int64_t view_row_counter = 0;
  bool is_external = false;
  bool is_glob_external = false;
  bool has_generated_pk_col = false;
  // No PK column in the chunk -- Sink synthesises a monotonic counter.
  bool is_view_synth_pk = false;
  bool is_view_rocksdb_pk = false;

  std::unique_ptr<DuckDBSinkIndexWriter> writer;
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::shared_ptr<IndexShard> index_shard;

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
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::SchemaObject> relation,
  std::vector<catalog::Column> view_columns, ObjectId database_id,
  duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
  SereneDBSchemaEntry& schema_entry, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _relation(std::move(relation)),
    _view_columns(std::move(view_columns)),
    _database_id(database_id),
    _info(std::move(info)),
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
  state->table_name = std::string{_relation->GetName()};
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
  const auto& columns = Columns();
  std::vector<catalog::CreateIndexColumn> idx_columns;
  auto resolve_column = [&](std::string_view col_name) {
    for (const auto& col : columns) {
      if (col.name == col_name) {
        return &col;
      }
    }
    return static_cast<const catalog::Column*>(nullptr);
  };

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
        .name = cat_col->name,
        .opclass = std::move(opclass),
        .opclass_options = std::move(opclass_options),
      });
      continue;
    }

    // Try to lift a JSON-path expression of the form
    // `col -> 'k1' -> 'k2' ...` or `json_extract(col, 'k')` chains.
    std::string col_name;
    std::vector<std::string> json_path;
    if (!TryLiftJsonPath(*expr, col_name, json_path) || json_path.empty()) {
      throw duckdb::CatalogException(
        "Expression-based index columns are not supported");
    }
    const auto* cat_col = resolve_column(col_name);
    if (!cat_col) {
      throw duckdb::CatalogException("column \"%s\" not found in table",
                                     col_name);
    }
    idx_columns.emplace_back(catalog::CreateIndexColumn{
      .catalog_column = cat_col,
      .name = cat_col->name,
      .opclass = std::move(opclass),
      .json_path = std::move(json_path),
      .opclass_options = std::move(opclass_options),
    });
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
    std::optional<catalog::ScorerOptions> wand_scorer;
    it = _info->options.find("optimize_top_k");
    if (it != _info->options.end()) {
      auto value = it->second.DefaultCastAs(duckdb::LogicalType::VARCHAR)
                     .GetValue<std::string>();
      wand_scorer = catalog::ParseScorerExpression(context, value);
    }
    create_result = catalog_impl.CreateInvertedIndex(
      _database_id, _schema_entry.name, _relation->GetName(), _info->index_name,
      std::move(idx_columns), shard_options, {.create_with_tombstone = true},
      std::move(wand_scorer));
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
  state->file_row_number_col_idx = columns.size();
  state->generated_pk_col_idx = columns.size();
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
        state->file_index_col_idx = columns.size();
        state->file_row_number_col_idx = columns.size() + 1;
      }
    }
  }
  if (table_ptr) {
    state->pk_columns = duckdb_primary_key::BuildPKColumns(*table_ptr);
    state->has_generated_pk_col = table_ptr->PKColumns().empty();
    state->is_view_synth_pk = false;
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
        if (c.id == pk_id) {
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
  conn_ctx.AddRocksDBWrite();
  auto index = snapshot->GetObject<catalog::Index>(catalog_index->GetId());
  SDB_ASSERT(index);

  if (state->index_type == catalog::ObjectType::SecondaryIndex) {
    auto& sec_index = basics::downCast<const catalog::SecondaryIndex>(*index);
    auto sk_columns = BuildSKColumnsForBackfill(*index, columns);
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
    auto tokenizer_provider = MakeTokenizerProvider(snapshot, inverted_index);
    auto json_paths_provider = MakeJsonPathsProvider(snapshot, inverted_index);
    state->writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
      *state->search_trx, std::move(tokenizer_provider), index->GetColumnIds(),
      std::move(json_paths_provider));
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

  // Row key layout: [ObjectId][ColumnId(reserved)][PK bytes].
  gstate.row_keys.clear();
  gstate.row_keys.reserve(num_rows);
  auto append_row_number_key = [&](int64_t row_number) {
    auto& key = gstate.row_keys.emplace_back();
    basics::StrResize(key, sizeof(catalog::Column::Id) + sizeof(ObjectId));
    std::memcpy(key.data() + sizeof(catalog::Column::Id),
                gstate.table_key.data(), sizeof(ObjectId));
    primary_key::AppendSigned(key, row_number);
    std::memcpy(key.data(), gstate.table_key.data(), sizeof(ObjectId));
  };
  auto append_glob_key = [&](int64_t file_index, int64_t row_number) {
    auto& key = gstate.row_keys.emplace_back();
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
    // View-backed: no PK column in chunk; synthesise monotonic counter.
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      append_row_number_key(gstate.view_row_counter++);
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
        gstate.row_keys.emplace_back());
    }
  }

  gstate.writer->Init(num_rows, chunk);

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

  if (gstate.index_type == catalog::ObjectType::InvertedIndex &&
      gstate.index_shard) {
    gstate.writer.reset();
    gstate.search_trx.reset();

    auto& inverted_shard =
      basics::downCast<search::InvertedIndexShard>(*gstate.index_shard);
    auto future = inverted_shard.CommitWait();
    std::ignore = std::move(future).Get().Ok();
    SDB_IF_FAILURE("crash_before_finish_creation") { SDB_IMMEDIATE_ABORT(); }
    inverted_shard.FinishCreation();
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
  auto& sdb_catalog = op.table.ParentCatalog().Cast<SereneDBCatalog>();
  // ParentSchema() comes from the parent catalog; sdb_catalog above has
  // already been validated, so the schema is necessarily one of ours.
  auto& schema_entry = op.table.ParentSchema().Cast<SereneDBSchemaEntry>();
  auto database_id = sdb_catalog.GetDatabaseId();

  std::shared_ptr<catalog::SchemaObject> relation;
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
    view_columns.reserve(vinfo.names.size());
    for (size_t i = 0; i < vinfo.names.size(); ++i) {
      view_columns.push_back(catalog::Column{
        .id = static_cast<catalog::Column::Id>(i),
        .type = vinfo.types[i],
        .name = vinfo.names[i],
      });
    }
  } else {
    auto& table_catalog = op.table.Cast<duckdb::TableCatalogEntry>();
    auto& table_entry = RequireBaseTable(table_catalog);
    relation = table_entry.GetSereneDBTable();
  }

  auto& create_index = input.planner.Make<SereneDBPhysicalCreateIndex>(
    std::move(relation), std::move(view_columns), database_id,
    std::move(op.info), schema_entry, op.estimated_cardinality);
  create_index.children.push_back(input.table_scan);
  return create_index;
}

}  // namespace sdb::connector
