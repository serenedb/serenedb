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
#include <absl/strings/match.h>

#include <atomic>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/execution/operator/scan/physical_empty_result.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/lambda_expression.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/storage/data_table.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/primary_key.hpp"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/scorer_options.h"
#include "catalog/store/store.h"
#include "catalog/view.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/index_expression.hpp"
#include "connector/inverted_index_options_util.h"
#include "connector/inverted_store_index.h"
#include "connector/search_sink_writer.hpp"
#include "connector/view_fast_path.h"
#include "connector/with_option_resolver.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/progress_registry.h"
#include "pg/sql_exception_macro.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

struct InsertColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

struct CreateIndexGlobalState : public duckdb::GlobalSinkState {
  bool created = false;
  bool finalized = false;
  ObjectId database_id;
  ObjectId index_id;
  std::string schema_name;
  std::string table_name;
  std::string index_name;
  catalog::ObjectType index_type = catalog::ObjectType::SecondaryIndex;

  ObjectId table_id;
  std::vector<InsertColumnMeta> columns;

  bool pk_term = false;
  catalog::PkColumnKind pk_column = catalog::PkColumnKind::None;
  duckdb::idx_t pk_hi_col_idx = 0;
  duckdb::idx_t pk_lo_col_idx = 0;

  std::atomic<duckdb::idx_t> backfill_count_atomic{0};

  // delete logs stuff
  std::vector<std::atomic<int64_t>> uncommitted_min_rowids;
  std::atomic<size_t> registered_sinks{0};

  std::shared_ptr<search::InvertedIndexStorage> index_storage;
  std::shared_ptr<const catalog::Snapshot> snapshot_for_providers;
  std::shared_ptr<catalog::Index> index_for_providers;

  pg::ProgressMetrics* progress = nullptr;

  ~CreateIndexGlobalState() {
    if (created && !finalized) {
      try {
        catalog::GetCatalog().DropIndexById(database_id, index_id, true);
      } catch (...) {
      }
    }
  }
};

struct CreateIndexLocalState : public duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<DuckDBSearchSinkInsertWriter> writer;
  std::vector<std::string> row_keys;
  size_t uncommitted_min_slot = std::numeric_limits<size_t>::max();

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

bool SereneDBPhysicalCreateIndex::IsDuckDBTable() const noexcept {
  auto* table = TableOrNull();
  SDB_ASSERT(table == nullptr ||
             table->GetEngine() == catalog::TableEngine::Transactional);
  return table != nullptr;
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
  state->database_id = _database_id;
  state->schema_name = _schema_entry.name.GetIdentifierName();
  state->table_name = std::string{_relation->GetName()};
  state->index_name = _info->GetIndexName().GetIdentifierName();

  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    auto& metrics = sdb_state->Progress();
    metrics.SetCommand(pg::ProgressCommand::CreateIndex);
    metrics.SetPhase(pg::progress_phase::CreateIndex::Initializing);
    pg::ProgressMetrics::Set(metrics.relid,
                             static_cast<int64_t>(_relation->GetId().id()));
    if (estimated_cardinality > 0) {
      pg::ProgressMetrics::Set(metrics.tuples_total,
                               static_cast<int64_t>(estimated_cardinality));
    }
    state->progress = &metrics;
  }

  auto& catalog_impl = catalog::GetCatalog();

  if (absl::EqualsIgnoreCase(_info->index_type, "inverted")) {
    state->index_type = catalog::ObjectType::InvertedIndex;
  } else {
    state->index_type = catalog::ObjectType::SecondaryIndex;
  }

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
      const auto& col_name = col_ref.GetColumnName().GetIdentifierName();
      const auto* cat_col = resolve_column(col_name);
      if (!cat_col) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("column \"", col_name, "\" not found in table"));
      }
      idx_columns.emplace_back(cat_col->GetName(), cat_col, std::nullopt,
                               std::move(opclass), std::move(opclass_options));
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
        ERR_MSG(
          "indexed expression must reference at least one base table column"));
    }
    auto return_type = normalized->GetReturnType();
    auto& indexed_column = idx_columns.emplace_back(
      "", nullptr,
      catalog::ExpressionData{
        .serialized_expr = std::move(serialized),
        .dependent_columns = std::move(dependent_columns),
        .return_type = std::move(return_type),
        .pretty_printed = expr->ToString(),
      },
      std::move(opclass), std::move(opclass_options));
    indexed_column.name = indexed_column.indexed_expr->pretty_printed;
  }

  bool if_not_exists =
    _info->on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  // CREATE INDEX requires ownership of the target relation; the mutation
  // enforces it and throws "must be owner of table <name>" on a non-owner.
  bool created = false;
  if (state->index_type == catalog::ObjectType::InvertedIndex) {
    auto find_with = [&](std::string_view name) -> const duckdb::Value* {
      auto it = _info->options.find(name);
      return it != _info->options.end() ? &it->second : nullptr;
    };
    // WITH values go through the same validator as ALTER INDEX SET; omitted
    // options resolve from the session settings (validated on SET).
    auto resolve_uint = [&](std::string_view name) -> uint32_t {
      if (const auto* v = find_with(name)) {
        return static_cast<uint32_t>(
          ValidateInvertedIndexOptionValue(name, *v));
      }
      return ResolveUintWithOption(context, name, nullptr);
    };
    auto resolve_ubigint = [&](std::string_view name) -> uint64_t {
      if (const auto* v = find_with(name)) {
        return ValidateInvertedIndexOptionValue(name, *v);
      }
      return ResolveUbigintWithOption(context, name, nullptr);
    };

    catalog::InvertedIndexOptions options{
      .row_group_size = resolve_uint(kRowGroupSizeSetting),
      .norm_row_group_size = resolve_uint(kNormRowGroupSizeSetting),
      .refresh_interval_ms = resolve_uint(kRefreshIntervalSetting),
      .compaction_interval_ms = resolve_uint(kCompactionIntervalSetting),
      .cleanup_interval_step = resolve_uint(kCleanupIntervalStepSetting),
      .segment_memory_max = resolve_ubigint(kSegmentMemoryMaxSetting),
      .segment_docs_max = resolve_uint(kSegmentDocsMaxSetting),
      .compaction_max_segments = resolve_uint(kCompactionMaxSegmentsSetting),
      .compaction_max_segments_bytes =
        resolve_ubigint(kCompactionMaxSegmentsBytesSetting),
      .compaction_floor_segment_bytes =
        resolve_ubigint(kCompactionFloorSegmentBytesSetting),
    };
    if (auto* v = find_with("optimize_top_k")) {
      auto value =
        v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>();
      options.topk_scorer = catalog::ParseScorerExpression(context, value);
    }
    std::string store_pk = "auto";
    if (auto* v = find_with("store_pk")) {
      store_pk = duckdb::StringUtil::Lower(
        v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>());
      if (store_pk == "true") {
        store_pk = "auto";
      } else if (store_pk == "false") {
        store_pk = "none";
      }
    }
    const bool table_backed = IsDuckDBTable();
    enum class KeyShape { Single, Two, Synth };
    auto shape = KeyShape::Synth;
    if (table_backed) {
      shape = KeyShape::Single;
    } else if (auto it = _info->options.find("_sdb_view_fast_path_pk");
               it != _info->options.end()) {
      const auto kind = it->second.GetValue<std::string>();
      shape = (kind == "file_index_plus_row_number" ||
               kind == "file_index_plus_duckdb_rowid")
                ? KeyShape::Two
                : KeyShape::Single;
    }
    options.pk_term = table_backed;
    if (store_pk == "none") {
      options.pk_term = false;
      options.pk_column = catalog::PkColumnKind::None;
    } else if (store_pk == "auto") {
      options.pk_column = shape == KeyShape::Single ? catalog::PkColumnKind::I64
                          : shape == KeyShape::Two
                            ? catalog::PkColumnKind::I64I64
                            : catalog::PkColumnKind::Unable;
    } else if (store_pk == "i64") {
      if (shape != KeyShape::Single) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("store_pk = 'i64' requires a single-part row key; this "
                  "index's key is ",
                  shape == KeyShape::Two ? "(file_index, row)" : "synthetic"));
      }
      options.pk_column = catalog::PkColumnKind::I64;
    } else if (store_pk == "i64i64") {
      if (shape != KeyShape::Two) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("store_pk = 'i64i64' requires a two-part (file_index, row) "
                  "key; this index's key is ",
                  table_backed ? "the table rowid" : "single-part"));
      }
      options.pk_column = catalog::PkColumnKind::I64I64;
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("store_pk must be one of none/auto/i64/i64i64 (or "
                "true/false), got '",
                store_pk, "'"));
    }

    state->pk_term = options.pk_term;
    state->pk_column = options.pk_column;

    created = catalog_impl.CreateInvertedIndex(
      catalog::ActingAs(context), context, _database_id,
      _schema_entry.name.GetIdentifierName(), _relation->GetName(),
      _info->GetIndexName().GetIdentifierName(), std::move(idx_columns),
      std::move(options),
      {.create_with_tombstone = true, .if_not_exists = if_not_exists});
  } else {
    bool unique =
      (_info->constraint_type == duckdb::IndexConstraintType::UNIQUE);
    created = catalog_impl.CreateSecondaryIndex(
      catalog::ActingAs(context), _database_id,
      _schema_entry.name.GetIdentifierName(), _relation->GetName(),
      _info->GetIndexName().GetIdentifierName(), std::move(idx_columns), unique,
      {.create_with_tombstone = true, .if_not_exists = if_not_exists});
  }

  if (!created) {
    // Index already exists, nothing to do
    return state;
  }

  state->created = true;

  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_index =
    snapshot->GetRelation(catalog::NoAccessCheck(), _database_id,
                          _schema_entry.name.GetIdentifierName(),
                          _info->GetIndexName().GetIdentifierName());
  SDB_ASSERT(catalog_index);
  state->index_id = catalog_index->GetId();
  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    SDB_ASSERT(!sdb_state->transaction_abort_cleanup);
    sdb_state->transaction_abort_cleanup =
      [database_id = _database_id,
       index_id = state->index_id](duckdb::MetaTransaction&) {
        catalog::GetCatalog().DropIndexById(database_id, index_id, true);
      };
  }
  if (state->progress) {
    state->progress->SetPhase(pg::progress_phase::CreateIndex::BuildingIndex);
  }
  auto inverted =
    snapshot->GetObject<catalog::InvertedIndex>(catalog_index->GetId());
  auto storage = inverted ? inverted->GetData() : nullptr;
  state->index_storage = storage;

  if (storage) {
    if (auto it = _info->options.find("_sdb_iceberg_snapshot_id");
        it != _info->options.end()) {
      storage->SetIcebergSnapshotId(it->second.GetValue<int64_t>());
    }
    storage->StartTasks();

    if (IsDuckDBTable()) {
      auto database = snapshot->GetObject<catalog::Database>(_database_id);
      SDB_ASSERT(database);
      auto store_entry = catalog::GetStoreTableEntry(
        context, database->GetName(), _schema_entry.name.GetIdentifierName(),
        _relation->GetName(), duckdb::OnEntryNotFound::THROW_EXCEPTION);
      storage->SetDeleteLogRowidEnd(store_entry->GetStorage().GetNextRowId());
      state->uncommitted_min_rowids = std::vector<std::atomic<int64_t>>(
        duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads());
    }
  }

  auto* table_ptr = TableOrNull();
  state->table_id = table_ptr ? table_ptr->GetId() : _relation->GetId();
  std::vector<size_t> projection;
  if (table_ptr) {
    projection = BuildCreateIndexProjection(
      table_ptr->Columns(), table_ptr->PKColumns(), _info->column_ids);
    state->columns.reserve(projection.size());
    for (size_t chunk_idx = 0; chunk_idx < projection.size(); ++chunk_idx) {
      const auto& col = columns[projection[chunk_idx]];
      state->columns.push_back(InsertColumnMeta{
        .id = col.GetId(),
        .duckdb_type = col.type,
        .input_col_idx = chunk_idx,
      });
    }
  } else {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == catalog::Column::kGeneratedPKId) {
        continue;
      }
      state->columns.push_back(InsertColumnMeta{
        .id = columns[i].GetId(),
        .duckdb_type = columns[i].type,
        .input_col_idx = i,
      });
    }
  }
  state->pk_hi_col_idx = state->columns.size();
  state->pk_lo_col_idx = state->columns.size() + 1;

  auto index = snapshot->GetObject<catalog::Index>(catalog_index->GetId());
  SDB_ASSERT(index);

  if (state->index_type == catalog::ObjectType::SecondaryIndex) {
    SDB_ASSERT(table_ptr);
  } else {
    state->snapshot_for_providers = snapshot;
    state->index_for_providers = index;
  }
  return state;
}

bool SereneDBPhysicalCreateIndex::ParallelSink() const {
  return _info && absl::EqualsIgnoreCase(_info->index_type, "inverted");
}

namespace {

// A sink's entry moves only when its transaction commits: a mid-build cut
// advances it to the batch that triggered the cut, the final commit retires
// it to INT64_MAX. Every move republishes begin = min over the entries.
void AdvanceUncommittedMin(CreateIndexGlobalState& gstate, size_t slot,
                           int64_t min_rowid) {
  if (slot == std::numeric_limits<size_t>::max()) {
    return;
  }
  gstate.uncommitted_min_rowids[slot].store(min_rowid,
                                            std::memory_order_release);
  const auto reg = gstate.registered_sinks.load(std::memory_order_acquire);
  SDB_ASSERT(reg <= gstate.uncommitted_min_rowids.size());
  auto begin = std::numeric_limits<int64_t>::max();
  for (size_t i = 0; i < reg; ++i) {
    auto& slot = gstate.uncommitted_min_rowids[i];
    begin = std::min(begin, slot.load(std::memory_order_acquire));
  }
  gstate.index_storage->SetDeleteLogRowidBegin(begin);
}

}  // namespace

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBPhysicalCreateIndex::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  if (!ParallelSink()) {
    return duckdb::make_uniq<duckdb::LocalSinkState>();
  }
  auto* gstate_ptr =
    sink_state ? &sink_state->Cast<CreateIndexGlobalState>() : nullptr;
  if (!gstate_ptr || !gstate_ptr->created || !gstate_ptr->index_storage ||
      !gstate_ptr->index_for_providers) {
    return duckdb::make_uniq<duckdb::LocalSinkState>();
  }
  auto& gstate = *gstate_ptr;

  auto& inverted_storage = *gstate.index_storage;
  const auto& inverted_index =
    basics::downCast<const catalog::InvertedIndex>(*gstate.index_for_providers);

  auto lstate = duckdb::make_uniq<CreateIndexLocalState>();
  lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
    inverted_storage.GetTransaction());
  lstate->search_trx->SetFieldOptions(
    basics::downCast<const catalog::InvertedIndex>(gstate.index_for_providers));

  auto tokenizer_provider =
    MakeTokenizerProvider(gstate.snapshot_for_providers, inverted_index);
  auto entry_info_provider = MakeEntryInfoProvider(inverted_index);
  auto indexed_exprs = MakeIndexedExpressions(inverted_index, context.client);
  const auto& index_options = inverted_index.GetOptions();
  lstate->writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
    *lstate->search_trx, std::move(tokenizer_provider),
    gstate.index_for_providers->GetColumns(), std::move(entry_info_provider),
    std::move(indexed_exprs),
    PkPolicy{.index_term = index_options.pk_term,
             .column = index_options.pk_column});

  if (IsDuckDBTable()) {
    auto& slot = lstate->uncommitted_min_slot;
    slot = gstate.registered_sinks.fetch_add(1, std::memory_order_relaxed);
    SDB_ASSERT(slot < gstate.uncommitted_min_rowids.size());
  }

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

  if (!ParallelSink()) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto* lstate = dynamic_cast<CreateIndexLocalState*>(&input.local_state);
  if (!lstate || !lstate->writer) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto* writer = lstate->writer.get();

  PkChunk pk;
  std::unique_ptr<duckdb::Vector> pk_scratch;
  auto& row_keys = lstate->row_keys;
  std::vector<std::string_view> key_views;
  switch (gstate.pk_column) {
    case catalog::PkColumnKind::None:
    case catalog::PkColumnKind::Unable:
      break;
    case catalog::PkColumnKind::I64:
      SDB_ASSERT(gstate.pk_hi_col_idx < chunk.ColumnCount());
      pk.column = &chunk.data[gstate.pk_hi_col_idx];
      break;
    case catalog::PkColumnKind::I64I64: {
      SDB_ASSERT(gstate.pk_lo_col_idx < chunk.ColumnCount());
      pk_scratch = std::make_unique<duckdb::Vector>(
        PkColumnType(catalog::PkColumnKind::I64I64));
      auto& entries = duckdb::StructVector::GetEntries(*pk_scratch);
      entries[0].Reinterpret(chunk.data[gstate.pk_hi_col_idx]);
      entries[1].Reference(chunk.data[gstate.pk_lo_col_idx]);
      pk.column = pk_scratch.get();
      break;
    }
  }
  if (gstate.pk_term) {
    SDB_ASSERT(gstate.pk_column == catalog::PkColumnKind::I64);
    auto& pk_vec = chunk.data[gstate.pk_hi_col_idx];
    duckdb::UnifiedVectorFormat fmt;
    pk_vec.ToUnifiedFormat(num_rows, fmt);
    auto* pks = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
    row_keys.clear();
    row_keys.reserve(num_rows);
    key_views.reserve(num_rows);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      auto& key = row_keys.emplace_back();
      primary_key::AppendSigned(key, pks[fmt.sel->get_index(row)]);
      key_views.emplace_back(key);
    }
    pk.keys = key_views;
  }

  bool committed = false;
  writer->InitImpl(num_rows, pk, &committed);

  for (const auto& col : gstate.columns) {
    if (col.input_col_idx >= chunk.ColumnCount()) {
      continue;
    }

    const ColumnDescriptor desc{col.id, col.duckdb_type};
    writer->SwitchColumn(desc, chunk.data[col.input_col_idx], num_rows);
  }

  if (auto indexed_exprs = writer->IndexedExpressions();
      !indexed_exprs.empty()) {
    auto slot_to_col_ids = gstate.columns |
                           std::views::transform(&InsertColumnMeta::id) |
                           std::ranges::to<std::vector<catalog::Column::Id>>();
    EvaluateAndWriteIndexedExpressions(*writer, indexed_exprs, chunk,
                                       gstate.table_id, slot_to_col_ids,
                                       context.client, num_rows);
  }

  writer->Finish();

  if (committed &&
      lstate->uncommitted_min_slot != std::numeric_limits<size_t>::max()) {
    duckdb::UnifiedVectorFormat fmt;
    chunk.data[gstate.pk_hi_col_idx].ToUnifiedFormat(num_rows, fmt);
    auto* rowids = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
    const auto batch_min_rowid = rowids[fmt.sel->get_index(0)];
    SDB_ASSERT(batch_min_rowid <= rowids[fmt.sel->get_index(num_rows - 1)]);
    AdvanceUncommittedMin(gstate, lstate->uncommitted_min_slot,
                          batch_min_rowid);
  }

  gstate.backfill_count_atomic.fetch_add(num_rows, std::memory_order_relaxed);
  if (gstate.progress) {
    pg::ProgressMetrics::Add(gstate.progress->tuples_processed, num_rows);
    SDB_WAIT_ON_FAILURE("pause_create_index_mid_build");
  }
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBPhysicalCreateIndex::Combine(
  duckdb::ExecutionContext& /*context*/,
  duckdb::OperatorSinkCombineInput& input) const {
  if (auto* lstate = dynamic_cast<CreateIndexLocalState*>(&input.local_state)) {
    lstate->writer.reset();
    const bool committed =
      lstate->search_trx ? lstate->search_trx->Commit() : false;
    lstate->search_trx.reset();
    if (committed) {
      // The final commit went through: nothing pending here anymore, drop
      // out of the begin computation.
      AdvanceUncommittedMin(input.global_state.Cast<CreateIndexGlobalState>(),
                            lstate->uncommitted_min_slot,
                            std::numeric_limits<int64_t>::max());
    }
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
      gstate.index_storage) {
    if (gstate.progress) {
      gstate.progress->SetPhase(pg::progress_phase::CreateIndex::Committing);
    }

    auto& inverted_storage = *gstate.index_storage;
    auto delete_log = inverted_storage.TakeDeleteLog();
    if (!delete_log.empty()) {
      // Sorted rowids encode to lexicographically sorted pk terms, so the
      // remove filter walks each segment's term dictionary sequentially.
      absl::c_sort(delete_log);
      auto trx = inverted_storage.GetTransaction();
      DuckDBSearchSinkDeleteWriter delete_writer{trx};
      delete_writer.Init(delete_log.size(), {});
      std::string key;
      for (const auto row : delete_log) {
        key.clear();
        primary_key::AppendSigned(key, row);
        delete_writer.DeleteRow(key);
      }
      delete_writer.Finish();
      const auto last_tick =
        search::TickDomain::Instance().Advance(delete_log.size() + 1);
      if (!trx.Commit(last_tick)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INTERNAL_ERROR),
          ERR_MSG("failed to replay concurrent deletes for index '",
                  gstate.index_name, "'"));
      }
    }
    inverted_storage.Refresh();
    SDB_IF_FAILURE("crash_before_finish_creation") { SDB_IMMEDIATE_ABORT(); }
    inverted_storage.FinishCreation();
  }

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateIndex::Finalizing);
  }
  SDB_IF_FAILURE("crash_before_remove_tombstone") { SDB_IMMEDIATE_ABORT(); }
  catalog::GetCatalog().RemoveTombstone(_database_id, gstate.schema_name,
                                        gstate.index_name);
  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    sdb_state->transaction_abort_cleanup = nullptr;
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
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("CreateIndexInfo is null in create_plan"));
  }

  auto* sdb_catalog = dynamic_cast<SereneDBCatalog*>(&op.table.ParentCatalog());
  if (!sdb_catalog) {
    if (op.table.type == duckdb::CatalogType::TABLE_ENTRY &&
        op.table.Cast<duckdb::TableCatalogEntry>().IsDuckTable() &&
        op.info->options.contains(InvertedStoreIndex::kIndexIdOption)) {
      auto& attach_only = input.planner.Make<duckdb::PhysicalEmptyResult>(
        input.table_scan.types, 0);
      return input.planner.CreateDefaultIndexPlan(op, attach_only);
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("cannot CREATE INDEX on ",
              op.table.ParentCatalog().GetName().GetIdentifierName(), ".",
              op.table.name.GetIdentifierName(),
              ": its catalog differs from the current one (",
              duckdb::DatabaseManager::GetDefaultDatabase(input.context)
                .GetIdentifierName(),
              ")"));
  }
  auto& schema_entry = op.table.ParentSchema().Cast<SereneDBSchemaEntry>();
  auto database_id = sdb_catalog->GetDatabaseId();

  std::shared_ptr<catalog::Object> relation;
  std::vector<catalog::Column> view_columns;

  if (op.table.type == duckdb::CatalogType::VIEW_ENTRY) {
    auto& conn_ctx = GetSereneDBContext(input.context);
    auto snapshot = conn_ctx.CatalogSnapshot();
    relation = snapshot->GetRelation(catalog::NoAccessCheck(), database_id,
                                     schema_entry.name.GetIdentifierName(),
                                     op.table.name.GetIdentifierName());
    if (!relation || relation->GetType() != catalog::ObjectType::PgSqlView) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("view \"", op.table.name.GetIdentifierName(),
                              "\" not found in SereneDB catalog"));
    }
    auto& view = basics::downCast<catalog::PgSqlView>(*relation);
    const auto& vinfo = view.GetInfo();
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
                                vinfo.names[p].GetIdentifierName(),
                                vinfo.types[p]);
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
