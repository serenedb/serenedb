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
#include <absl/strings/str_cat.h>

#include <atomic>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/primary_key.hpp"
#include "basics/system-compiler.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_schema_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/log/store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/scorer_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
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
  catalog::ColumnId id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

enum class PkShape : uint8_t {
  Single,
  Struct,
};

struct CreateIndexGlobalState : public duckdb::GlobalSinkState {
  bool created = false;
  ObjectId database_id;
  ObjectId index_id;
  std::string schema_name;
  std::string table_name;
  std::string index_name;
  bool inverted_index = false;

  ObjectId table_id;
  std::vector<InsertColumnMeta> columns;
  // Where the store WAL stood when this index was published (see the WAL
  // barrier in GetGlobalSinkState); the build covers everything below it.
  search::WalCursor backfill_wal_cursor;

  bool pk_term = false;
  catalog::PkColumnKind pk_column = catalog::PkColumnKind::None;
  PkShape pk_shape = PkShape::Single;
  duckdb::idx_t pk_base_col_idx = 0;
  duckdb::LogicalType generated_pk_type;
  std::shared_ptr<const search::FileManifest> file_manifest;

  std::atomic<duckdb::idx_t> backfill_count_atomic{0};
  // Rows at or above this rowid committed after the index was published and
  // reach it through the live commit-time feed; Sink truncates them out of
  // the backfill. INT64_MAX (no filtering) when the source has no rowids.
  int64_t backfill_rowid_end = std::numeric_limits<int64_t>::max();

  // delete logs stuff
  std::vector<std::atomic<int64_t>> uncommitted_min_rowids;
  std::atomic<size_t> registered_sinks{0};

  std::shared_ptr<search::InvertedIndexStorage> index_storage;
  std::shared_ptr<const catalog::Index> index_for_providers;

  struct Backfill {
    duckdb::AttachedDatabase* store_db = nullptr;
    duckdb::optional_ptr<duckdb::DuckTransaction> txn;
  };
  // The backfill's own store transaction (snapshot pinned at publication,
  // routed to the child scan via the meta-transaction override).
  const std::shared_ptr<Backfill> backfill = std::make_shared<Backfill>();

  pg::ProgressMetrics* progress = nullptr;
};

struct CreateIndexLocalState : public duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<DuckDBSearchSinkInsertWriter> writer;
  // Per-chunk scratch, kept at high-water mark: Sink runs once per 2048 rows,
  // so anything rebuilt on the stack there is an allocation per chunk. row_keys
  // holds its strings rather than clearing the vector, which would destroy them
  // and throw away exactly the buffers the pooling is for.
  std::vector<std::string> row_keys;
  std::vector<std::string_view> key_views;
  std::vector<FeedColumn> columns;
  std::vector<ExpressionValue> expression_values;
  duckdb::SelectionVector backfill_sel{STANDARD_VECTOR_SIZE};
  std::unique_ptr<duckdb::Vector> pk_scratch;
  size_t uncommitted_min_slot = std::numeric_limits<size_t>::max();

  ~CreateIndexLocalState() override {
    writer.reset();
    search_trx.reset();
  }
};

struct CreateIndexSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

// WITH values go through the same validator as ALTER INDEX SET; omitted options
// resolve from the session settings (validated on SET). `store_pk` is checked
// against the key shape the index will actually have.
catalog::InvertedIndexOptions ResolveInvertedIndexOptions(
  duckdb::ClientContext& context,
  const duckdb::case_insensitive_map_t<duckdb::Value>& with, bool table_backed,
  PkShape& pk_shape) {
  auto find = [&](std::string_view name) -> const duckdb::Value* {
    auto it = with.find(name);
    return it != with.end() ? &it->second : nullptr;
  };
  // WITH values go through the same validator as ALTER INDEX SET; omitted
  // options resolve from the session settings (validated on SET).
  auto resolve_uint = [&](std::string_view name) -> uint32_t {
    if (const auto* v = find(name)) {
      return static_cast<uint32_t>(ValidateInvertedIndexOptionValue(name, *v));
    }
    return ResolveUintWithOption(context, name, nullptr);
  };
  auto resolve_ubigint = [&](std::string_view name) -> uint64_t {
    if (const auto* v = find(name)) {
      return ValidateInvertedIndexOptionValue(name, *v);
    }
    return ResolveUbigintWithOption(context, name, nullptr);
  };

  // The periodic reindex is a view-only concept: on a table-backed
  // index an explicit WITH is an error, and an inherited session default
  // is dropped (never persisted, never ticks).
  if (table_backed && find(kReindexIntervalSetting)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("option \"", kReindexIntervalSetting,
                            "\" only applies to view-backed inverted indexes"));
  }
  catalog::InvertedIndexOptions options{
    .row_group_size = resolve_uint(kRowGroupSizeSetting),
    .refresh_interval_ms = resolve_uint(kRefreshIntervalSetting),
    .compaction_interval_ms = resolve_uint(kCompactionIntervalSetting),
    .cleanup_interval_step = resolve_uint(kCleanupIntervalStepSetting),
    .reindex_interval_ms =
      table_backed ? 0 : resolve_uint(kReindexIntervalSetting),
    .segment_memory_max = resolve_ubigint(kSegmentMemoryMaxSetting),
    .segment_docs_max = resolve_uint(kSegmentDocsMaxSetting),
    .compaction_max_segments = resolve_uint(kCompactionMaxSegmentsSetting),
    .compaction_max_segments_bytes =
      resolve_ubigint(kCompactionMaxSegmentsBytesSetting),
    .compaction_floor_segment_bytes =
      resolve_ubigint(kCompactionFloorSegmentBytesSetting),
  };
  if (auto* v = find("optimize_top_k")) {
    auto value =
      v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>();
    options.topk_scorer = catalog::ParseScorerExpression(context, value);
  }
  options.key_columns = KeyColumnsFromOptions(with);
  std::string store_pk = "auto";
  if (auto* v = find("store_pk")) {
    store_pk = duckdb::StringUtil::Lower(
      v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>());
    if (store_pk == "true") {
      store_pk = "auto";
    } else if (store_pk == "false") {
      store_pk = "none";
    }
  }
  bool has_pk = table_backed;
  bool file_row = false;
  pk_shape = PkShape::Single;
  if (!table_backed) {
    if (auto it = with.find("_sdb_view_fast_path_pk"); it != with.end()) {
      const auto kind = it->second.GetValue<std::string>();
      has_pk = true;
      if (kind == "external_struct_key") {
        pk_shape = PkShape::Struct;
      } else if (kind == "file_index_plus_row_number" ||
                 kind == "file_index_plus_duckdb_rowid") {
        pk_shape = PkShape::Struct;
        file_row = true;
      }
    }
  }
  bool reindex = true;
  if (auto* v = find("reindex")) {
    auto value = *v;
    if (!value.DefaultTryCastAs(duckdb::LogicalType::BOOLEAN)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("invalid value for parameter \"reindex\": \"",
                              v->ToString(), "\""));
    }
    reindex = value.GetValue<bool>();
  }

  options.pk_term = table_backed || (file_row && reindex);
  if (store_pk == "none") {
    options.pk_term = false;
    options.pk_column = catalog::PkColumnKind::None;
  } else if (store_pk == "auto") {
    options.pk_column =
      has_pk ? catalog::PkColumnKind::Has : catalog::PkColumnKind::Unable;
  } else if (store_pk == "i64") {
    if (!has_pk || pk_shape != PkShape::Single) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("store_pk = 'i64' requires a single-part row key; this "
                "index's key is ",
                !has_pk    ? "synthetic"
                : file_row ? "(file_index, row)"
                           : "a user key_columns struct"));
    }
    options.pk_column = catalog::PkColumnKind::Has;
    pk_shape = PkShape::Single;
  } else if (store_pk == "i64i64") {
    if (!file_row) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("store_pk = 'i64i64' requires a two-part (file_index, row) "
                "key; this index's key is ",
                table_backed ? "the table rowid" : "single-part"));
    }
    options.pk_column = catalog::PkColumnKind::Has;
    pk_shape = PkShape::Struct;
  } else {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("store_pk must be one of none/auto/i64/i64i64 (or "
                            "true/false), got '",
                            store_pk, "'"));
  }
  return options;
}

}  // namespace

SereneDBPhysicalCreateIndex::SereneDBPhysicalCreateIndex(
  duckdb::PhysicalPlan& plan, const duckdb::CatalogEntry& relation,
  std::vector<IndexRelationColumn> columns,
  std::vector<duckdb::LogicalIndex> pk_positions, ObjectId database_id,
  duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
  std::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions,
  duckdb::unique_ptr<duckdb::Expression> bound_where,
  catalog::SereneDBSchemaEntry& schema_entry,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _relation(relation),
    _columns(std::move(columns)),
    _pk_positions(std::move(pk_positions)),
    _database_id(database_id),
    _info(std::move(info)),
    _bound_expressions(std::move(bound_expressions)),
    _bound_where(std::move(bound_where)),
    _schema_entry(schema_entry) {
  _feeds_inverted =
    _info && absl::EqualsIgnoreCase(_info->index_type, "inverted");
}

const catalog::SereneDBTableEntry* SereneDBPhysicalCreateIndex::TableOrNull()
  const noexcept {
  return dynamic_cast<const catalog::SereneDBTableEntry*>(&_relation);
}

bool SereneDBPhysicalCreateIndex::IsDuckDBTable() const noexcept {
  const auto* table = TableOrNull();
  SDB_ASSERT(table == nullptr ||
             table->GetEngine() == catalog::TableEngine::Transactional);
  return table != nullptr;
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalCreateIndex::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<CreateIndexGlobalState>();
  state->database_id = _database_id;
  state->schema_name = _schema_entry.name.GetIdentifierName();
  state->table_name = std::string{_relation.name.GetIdentifierName()};
  state->index_name = _info->GetIndexName().GetIdentifierName();

  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    auto& metrics = sdb_state->Progress();
    metrics.SetCommand(pg::ProgressCommand::CreateIndex);
    metrics.SetPhase(pg::progress_phase::CreateIndex::Initializing);
    pg::ProgressMetrics::Set(
      metrics.relid, static_cast<int64_t>(catalog::IdOf(_relation).id()));
    if (estimated_cardinality > 0) {
      pg::ProgressMetrics::Set(metrics.tuples_total,
                               static_cast<int64_t>(estimated_cardinality));
    }
    state->progress = &metrics;
  }

  state->inverted_index = absl::EqualsIgnoreCase(_info->index_type, "inverted");

  const auto& columns = _columns;
  std::vector<catalog::CreateIndexColumn> idx_columns;
  auto resolve_column = [&](std::string_view col_name) {
    for (const auto& col : columns) {
      if (absl::EqualsIgnoreCase(col.name, col_name)) {
        return &col;
      }
    }
    return static_cast<const IndexRelationColumn*>(nullptr);
  };

  auto make_column_ids = [&](auto&& positions) {
    return std::forward<decltype(positions)>(positions) |
           std::views::transform([&](size_t pos) { return columns[pos].id; }) |
           std::ranges::to<std::vector<catalog::ColumnId>>();
  };

  const auto col_index_to_id =
    IsDuckDBTable()
      ? make_column_ids(
          BuildCreateIndexProjection(_pk_positions, _info->column_ids))
      : make_column_ids(std::views::iota(size_t{0}, columns.size()));
  const auto relation_id = catalog::IdOf(_relation);

  // Normalize + serialize a bound expression (index key or partial-index
  // predicate) into its persisted ExpressionData, keyed to stable catalog
  // column ids.
  auto make_expression_data = [&](const duckdb::Expression& bound,
                                  std::string pretty) {
    auto normalized =
      NormalizeBoundExpression(bound, relation_id, col_index_to_id, context);
    return catalog::ExpressionData{
      .serialized_expr = SerializeBoundExpression(*normalized),
      .dependent_columns = CollectDependentColumns(*normalized),
      .return_type = normalized->GetReturnType(),
      .pretty_printed = std::move(pretty),
    };
  };

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
      idx_columns.emplace_back(
        cat_col->name, catalog::IndexedColumnRef{cat_col->id, cat_col->type},
        std::nullopt, std::move(opclass), std::move(opclass_options));
      continue;
    }

    SDB_ASSERT(i < _bound_expressions.size() && _bound_expressions[i],
               "bound expression is missing for inverted index expression");
    const auto& bound_expr = _bound_expressions[i];

    auto data = make_expression_data(*bound_expr, expr->ToString());
    if (data.dependent_columns.empty()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
        ERR_MSG(
          "indexed expression must reference at least one base table column"));
    }
    auto& indexed_column =
      idx_columns.emplace_back("", std::nullopt, std::move(data),
                               std::move(opclass), std::move(opclass_options));
    indexed_column.name = indexed_column.indexed_expr->pretty_printed;
  }

  bool if_not_exists =
    _info->on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  duckdb::optional_ptr<duckdb::TableCatalogEntry> store_entry;
  if (state->inverted_index && IsDuckDBTable()) {
    store_entry = catalog::GetStoreTableEntry(
      context, _database_id, catalog::IdOf(_relation),
      duckdb::OnEntryNotFound::THROW_EXCEPTION);
  }

  // Shared, and it stays the one object: the providers below build the
  // hyperloglog and IVF columns off the per-column options, which only this
  // object answers -- a copy rebuilds them and loses them.
  std::shared_ptr<const catalog::Index> created;
  ObjectId created_id;
  if (IsReindexPass()) {
    SDB_ASSERT(state->inverted_index);
    created = catalog::FindInvertedIndex(_database_id, Info().source_index);
    if (!created) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("REINDEX: source index with id ", Info().source_index.id(),
                " vanished mid-refresh"));
    }
    created_id = created->GetId();
    state->pk_shape =
      Info().generated_pk_type.id() == duckdb::LogicalTypeId::STRUCT
        ? PkShape::Struct
        : PkShape::Single;
  } else {
    SDB_ASSERT(state->inverted_index);
    auto pk_shape = PkShape::Single;
    auto options = ResolveInvertedIndexOptions(context, _info->options,
                                               IsDuckDBTable(), pk_shape);
    state->pk_shape = pk_shape;

    catalog::ExpressionData predicate;
    if (_bound_where) {
      predicate =
        make_expression_data(*_bound_where, _info->where_clause->ToString());
    }

    created = catalog::CreateInvertedIndex(
      catalog::ActingAs(context), context, _database_id,
      _schema_entry.name.GetIdentifierName(), _relation,
      _info->GetIndexName().GetIdentifierName(), std::move(idx_columns),
      std::move(options), std::move(predicate),
      {.if_not_exists = if_not_exists, .dependencies = _info->dependencies});
    created_id = created ? created->GetId() : ObjectId{};
  }

  if (!created_id.isSet()) {
    // Index already exists, nothing to do
    return state;
  }

  state->created = true;
  state->index_id = created_id;
  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey);
      sdb_state && !IsReindexPass()) {
    SDB_ASSERT(!sdb_state->transaction_abort_cleanup);
    // Everything else an abandoned build leaves behind rolls back with the
    // statement: the entry via duckdb's undo, the live-list index and the
    // iresearch directory via SereneDBIndexEntry::Rollback. The backfill's
    // pinned store transaction is the one thing only this hook can release --
    // the MetaTransaction does not own overrides.
    sdb_state->transaction_abort_cleanup = [backfill = state->backfill](
                                             duckdb::MetaTransaction& meta,
                                             duckdb::ClientContext&) {
      if (backfill->store_db && backfill->txn) {
        meta.PopTransactionOverride(*backfill->store_db);
        backfill->store_db->GetTransactionManager().RollbackTransaction(
          *backfill->txn);
        backfill->txn = nullptr;
      }
    };
  }
  if (state->progress) {
    state->progress->SetPhase(pg::progress_phase::CreateIndex::BuildingIndex);
  }

  if (state->inverted_index) {
    const auto& options = catalog::InvertedInfo(*created).GetOptions();
    state->pk_term = options.pk_term;
    state->pk_column = options.pk_column;
  }
  state->generated_pk_type = Info().generated_pk_type;
  state->file_manifest = Info().manifest;

  auto storage = state->inverted_index ? catalog::InvertedStorageOf(
                                           &context, _database_id, created_id)
                                       : nullptr;
  if (IsReindexPass() && !storage) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("REINDEX: source index with id ",
                            Info().source_index.id(), " vanished mid-refresh"));
  }
  state->index_storage = storage;

  if (storage && !IsReindexPass()) {
    storage->StartTasks();

    if (IsDuckDBTable()) {
      SDB_ASSERT(store_entry);
      auto* table_obj = TableOrNull();
      SDB_ASSERT(table_obj);
      auto& store_storage = store_entry->GetStorage();
      duckdb::idx_t horizon = 0;
      // Built ahead of the publication point: the factory resolves catalog
      // entries, and a first touch of an attachment there starts a
      // transaction -- which must not happen under the checkpoint lock.
      auto injected = MakeInjectedInvertedIndex(
        context, store_storage, *TableOrNull()->Definition(), created, storage);
      {
        // Publication point. The exclusive checkpoint lock brackets exactly
        // {rowid assignment, commit-time index feed} of every store commit
        // (both run under the table's shared key in LocalStorage::Flush), so
        // rows below the horizon were fed to the pre-injection list and rows
        // at or above it feed the injected index. Nothing that can wait on
        // the transaction manager, the meta transaction, or the append lock
        // may run under this lock: committers hold those while blocking on
        // the shared key, so the catalog create above, the WAL barrier and
        // the snapshot pin below all stay outside.
        auto publish_lock = store_storage.GetCheckpointLock();
        // Replacing, not stacking: the store CreateIndex op this statement
        // emitted has already put its own object in the list, and two objects
        // over one storage each build a feed session, so a commit feeds the
        // rows to both and settles only the last one engaged.
        AddInjectedInvertedIndex(store_storage.GetDataTableInfo()->GetIndexes(),
                                 std::move(injected));
        horizon = store_storage.GetNextRowId();
        storage->SetDeleteLogRowidEnd(horizon);
      }
      state->backfill_rowid_end = static_cast<int64_t>(horizon);
      state->uncommitted_min_rowids = std::vector<std::atomic<int64_t>>(
        duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads());

      auto& store_db = store_entry->ParentCatalog().GetAttached();
      {
        // A committer releases the shared key before its visibility rewrite
        // but holds the WAL lock across both; passing through it here means
        // every pre-injection flush is fully visible to the snapshot below.
        auto wal_barrier = store_db.GetStorageManager().GetWALLock();
        // Indexes are injected from the catalog at attach, so this one exists
        // from the first WAL entry replay reads -- including the entries that
        // wrote the rows this build is about to index. Capture where the WAL
        // stands now, under its lock so no concurrent commit's bytes are
        // included: everything below is what the backfill covers, everything
        // above is live-fed and records its own cursor. Finalize persists it,
        // and without it the next boot replays these rows in a second time.
        state->backfill_wal_cursor =
          search::WalCursor{store_db.GetStorageManager()
                              .GetBlockManager()
                              .GetCheckpointIteration(),
                            store_db.GetStorageManager().GetWALSize()};
      }

      // The backfill's snapshot: every commit below the horizon completed
      // before the WAL barrier, everything at or above it is live-fed and
      // truncated out of the backfill by rowid in Sink. The refresh lifts
      // the snapshot above commits whose group fsync is still pending --
      // they are below the horizon, and if they never become durable the
      // whole create dies with the server anyway. Read-only; Finalize (or
      // the abort hook) pops and rolls it back.
      auto& meta = duckdb::MetaTransaction::Get(context);
      auto& store_txn_mgr = duckdb::DuckTransactionManager::Get(store_db);
      auto& backfill_txn =
        store_txn_mgr.StartTransaction(context).Cast<duckdb::DuckTransaction>();
      store_txn_mgr.RefreshCheckpointSnapshot(backfill_txn);
      backfill_txn.active_query = meta.GetActiveQuery();
      meta.PushTransactionOverride(store_db, backfill_txn);
      state->backfill->store_db = &store_db;
      state->backfill->txn = &backfill_txn;
    }
  }

  const bool is_table = IsDuckDBTable();
  state->table_id = catalog::IdOf(_relation);
  if (is_table) {
    const auto projection =
      BuildCreateIndexProjection(_pk_positions, _info->column_ids);
    state->columns.reserve(projection.size());
    for (size_t chunk_idx = 0; chunk_idx < projection.size(); ++chunk_idx) {
      const auto& col = columns[projection[chunk_idx]];
      state->columns.push_back(InsertColumnMeta{
        .id = col.id,
        .duckdb_type = col.type,
        .input_col_idx = chunk_idx,
      });
    }
  } else {
    for (size_t i = 0; i < columns.size(); ++i) {
      state->columns.push_back(InsertColumnMeta{
        .id = columns[i].id,
        .duckdb_type = columns[i].type,
        .input_col_idx = i,
      });
    }
  }
  state->pk_base_col_idx = state->columns.size();

  if (state->inverted_index) {
    state->index_for_providers = created;
  } else {
    SDB_ASSERT(is_table);
  }
  return state;
}

bool SereneDBPhysicalCreateIndex::ParallelSink() const {
  // Decided at plan time; Sink asks once per chunk, so it is not a string
  // compare each time.
  return _feeds_inverted;
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
    const auto& entry = gstate.uncommitted_min_rowids[i];
    begin = std::min(begin, entry.load(std::memory_order_acquire));
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
    catalog::InvertedInfo(*gstate.index_for_providers);

  auto lstate = duckdb::make_uniq<CreateIndexLocalState>();
  lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
    inverted_storage.GetTransaction());
  lstate->search_trx->SetFieldOptions(
    std::shared_ptr<const irs::IndexFieldOptions>{
      gstate.index_for_providers,
      &catalog::InvertedInfo(*gstate.index_for_providers)});
  auto tokenizer_provider = MakeTokenizerProvider(
    catalog::ResolveTokenizers(context.client, inverted_index), inverted_index);
  auto entry_info_provider = MakeEntryInfoProvider(inverted_index);
  const auto& index_options = inverted_index.GetOptions();
  lstate->writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
    *lstate->search_trx, std::move(tokenizer_provider),
    gstate.index_for_providers->GetColumns(), std::move(entry_info_provider),
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
  auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  if (!ParallelSink()) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  // ParallelSink() is true, so GetLocalSinkState built this type.
  auto* lstate = static_cast<CreateIndexLocalState*>(&input.local_state);
  if (!lstate->writer) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto* writer = lstate->writer.get();

  if (gstate.backfill_rowid_end != std::numeric_limits<int64_t>::max()) {
    auto& rowid_vec = chunk.data[gstate.pk_base_col_idx];
    duckdb::UnifiedVectorFormat fmt;
    rowid_vec.ToUnifiedFormat(num_rows, fmt);
    auto* rowids = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
    // Select rather than trim a suffix: a suffix trim would be equivalent only
    // while the backfill scans in rowid order and nothing below reorders, and
    // if that ever stopped holding the rows it let through would be indexed
    // twice -- silently, since duplicates are legal for this index. Selecting
    // costs the same pass and does not depend on the order.
    auto& sel = lstate->backfill_sel;
    duckdb::idx_t keep = 0;
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      if (rowids[fmt.sel->get_index(i)] < gstate.backfill_rowid_end) {
        sel.set_index(keep++, i);
      }
    }
    if (keep == 0) {
      return duckdb::SinkResultType::NEED_MORE_INPUT;
    }
    if (keep != num_rows) {
      chunk.Slice(sel, keep);
      num_rows = keep;
    }
  }

  PkChunk pk;
  auto& row_keys = lstate->row_keys;
  auto& key_views = lstate->key_views;
  key_views.clear();
  if (gstate.pk_column == catalog::PkColumnKind::Has) {
    switch (gstate.pk_shape) {
      case PkShape::Single:
        SDB_ASSERT(gstate.pk_base_col_idx < chunk.ColumnCount());
        pk.column = &chunk.data[gstate.pk_base_col_idx];
        break;
      case PkShape::Struct: {
        const auto base = gstate.pk_base_col_idx;
        // The key columns end where the pipeline's computed index expressions
        // begin -- those are appended after the scanned columns, so the chunk
        // does not end with the key (see SereneDBCreateIndexPlan).
        const auto end = HasProjectedExpressions()
                           ? _expression_slot_base.GetIndex()
                           : chunk.ColumnCount();
        SDB_ASSERT(base < end && end <= chunk.ColumnCount());
        if (!lstate->pk_scratch) {
          SDB_ASSERT(duckdb::StructType::GetChildCount(
                       gstate.generated_pk_type) == end - base);
          lstate->pk_scratch =
            std::make_unique<duckdb::Vector>(gstate.generated_pk_type);
        }
        auto& entries = duckdb::StructVector::GetEntries(*lstate->pk_scratch);
        for (duckdb::idx_t i = 0; i < entries.size(); ++i) {
          entries[i].Reference(chunk.data[base + i]);
        }
        pk.column = lstate->pk_scratch.get();
      } break;
    }
  }
  if (gstate.pk_term) {
    if (row_keys.size() < num_rows) {
      row_keys.resize(num_rows);
    }
    key_views.reserve(num_rows);
    switch (gstate.pk_shape) {
      case PkShape::Single: {
        auto& pk_vec = chunk.data[gstate.pk_base_col_idx];
        duckdb::UnifiedVectorFormat fmt;
        pk_vec.ToUnifiedFormat(num_rows, fmt);
        auto* pks = duckdb::UnifiedVectorFormat::GetData<int64_t>(fmt);
        for (duckdb::idx_t row = 0; row < num_rows; ++row) {
          auto& key = row_keys[row];
          key.clear();
          primary_key::AppendSigned(key, pks[fmt.sel->get_index(row)]);
          key_views.emplace_back(key);
        }
      } break;
      case PkShape::Struct: {
        // The glob (file, row) halves; pk_term is never set for external
        // key structs.
        SDB_ASSERT(gstate.generated_pk_type == FileIndexRowNumberStructType());
        const auto base = gstate.pk_base_col_idx;
        duckdb::UnifiedVectorFormat file_fmt;
        chunk.data[base].ToUnifiedFormat(num_rows, file_fmt);
        auto* files = duckdb::UnifiedVectorFormat::GetData<uint64_t>(file_fmt);
        duckdb::UnifiedVectorFormat row_fmt;
        chunk.data[base + 1].ToUnifiedFormat(num_rows, row_fmt);
        auto* rows = duckdb::UnifiedVectorFormat::GetData<int64_t>(row_fmt);
        for (duckdb::idx_t row = 0; row < num_rows; ++row) {
          auto& key = row_keys[row];
          key.clear();
          primary_key::AppendUnsigned(key, files[file_fmt.sel->get_index(row)]);
          primary_key::AppendSigned(key, rows[row_fmt.sel->get_index(row)]);
          key_views.emplace_back(key);
        }
      } break;
    }
    pk.keys = key_views;
  }

  auto& columns = lstate->columns;
  if (columns.empty()) {
    columns.reserve(gstate.columns.size());
    for (const auto& col : gstate.columns) {
      columns.push_back({col.input_col_idx, {col.id, col.duckdb_type}});
    }
  }

  // The pipeline's projection computed the indexed expressions into the slots
  // after the scanned columns (see SereneDBCreateIndexPlan).
  auto& expression_values = lstate->expression_values;
  expression_values.clear();
  if (HasProjectedExpressions()) {
    const auto& keys =
      catalog::InvertedInfo(*gstate.index_for_providers).ExpressionKeys();
    expression_values.reserve(keys.size());
    for (size_t k = 0; k < keys.size(); ++k) {
      const auto slot = _expression_slot_base.GetIndex() + k;
      SDB_ASSERT(slot < chunk.ColumnCount());
      // The DML feed rejects these in IndexExpressions::Execute; the build
      // path evaluates its expressions in the pipeline instead, so it has to
      // apply the same rule or a build would accept what an insert rejects.
      if (!catalog::InvertedInfo(*gstate.index_for_providers)
             .IsGeoJsonKey(keys[k])) {
        RejectJsonObjectArrayLeaves(chunk.data[slot], num_rows);
      }
      expression_values.push_back({keys[k].field_id, &chunk.data[slot]});
    }
  }

  irs::CommitOnFlush commit_on_flush{search::TickDomain::Instance().Counter()};
  FeedChunk(*writer, num_rows, pk, chunk, columns, expression_values,
            &commit_on_flush);

  if (commit_on_flush.committed &&
      lstate->uncommitted_min_slot != std::numeric_limits<size_t>::max()) {
    duckdb::UnifiedVectorFormat fmt;
    chunk.data[gstate.pk_base_col_idx].ToUnifiedFormat(num_rows, fmt);
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
    // Flush this thread's tail segment here (in parallel Combines) rather than
    // leaving it for the single-threaded Finalize refresh to write.
    bool committed = false;
    if (lstate->search_trx) {
      auto& trx = *lstate->search_trx;
      trx.RegisterFlush();
      committed = trx.FlushAndCommit(
        search::TickDomain::Instance().Advance(trx.GetQueries() + 1));
    }
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

  // The source is drained; retire the backfill's pinned snapshot.
  if (gstate.backfill->store_db && gstate.backfill->txn) {
    duckdb::MetaTransaction::Get(context).PopTransactionOverride(
      *gstate.backfill->store_db);
    gstate.backfill->store_db->GetTransactionManager().RollbackTransaction(
      *gstate.backfill->txn);
    gstate.backfill->txn = nullptr;
  }

  if (gstate.inverted_index && gstate.index_storage) {
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
      std::string key;
      FeedDeletes(delete_writer, key, delete_log.size(),
                  [&](size_t i) { return delete_log[i]; });
      trx.RegisterFlush();
      const auto last_tick =
        search::TickDomain::Instance().Advance(delete_log.size() + 1);
      if (!trx.Commit(last_tick)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INTERNAL_ERROR),
          ERR_MSG("failed to replay concurrent deletes for index '",
                  gstate.index_name, "'"));
      }
    }
    // The refresh below stamps the index payload with the cursor of the
    // highest recorded tick it makes durable. A build's ticks are unrelated to
    // commit ticks, so record the WAL position it covered at the lowest tick:
    // it means "this index already holds everything below here", whatever tick
    // the build's segments ended up with.
    if (gstate.backfill_wal_cursor.generation != 0 ||
        gstate.backfill_wal_cursor.offset != 0) {
      inverted_storage.RecordFlushCursor(irs::writer_limits::kMinTick + 1,
                                         gstate.backfill_wal_cursor);
    }
    if (gstate.file_manifest) {
      inverted_storage.SetFileManifest(gstate.file_manifest);
    }
    inverted_storage.Refresh();
    SDB_IF_FAILURE("crash_before_finish_creation") { SDB_IMMEDIATE_ABORT(); }
    inverted_storage.FinishCreation();
  }

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateIndex::Finalizing);
  }
  if (!IsReindexPass()) {
    SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
    if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
          kSereneDBClientStateKey)) {
      sdb_state->transaction_abort_cleanup = nullptr;
    }
  }
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

  auto* sdb_catalog =
    dynamic_cast<catalog::SereneDBCatalog*>(&op.table.ParentCatalog());
  if (!sdb_catalog) {
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
  auto& schema_entry =
    op.table.ParentSchema().Cast<catalog::SereneDBSchemaEntry>();
  auto database_id = sdb_catalog->GetDatabaseId();

  duckdb::optional_ptr<const duckdb::CatalogEntry> relation;
  std::vector<IndexRelationColumn> columns;
  std::vector<duckdb::LogicalIndex> pk_positions;

  if (op.table.type == duckdb::CatalogType::VIEW_ENTRY) {
    const auto schema_id = catalog::FindSchemaId(
      &input.context, database_id, schema_entry.name.GetIdentifierName());
    const auto* view =
      schema_id.isSet()
        ? catalog::Find<catalog::SereneDBViewEntry>(
            &input.context, schema_id, op.table.name.GetIdentifierName())
        : nullptr;
    if (view == nullptr) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("view \"", op.table.name.GetIdentifierName(),
                              "\" not found in SereneDB catalog"));
    }
    relation = view;
    const auto view_columns = view->GetColumnInfo();
    if (!view_columns) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("view \"", op.table.name.GetIdentifierName(),
                              "\" is not bound"));
    }
    const auto& vinfo = *view_columns;
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
    columns.reserve(view_positions.size());
    for (auto p : view_positions) {
      SDB_ASSERT(p < vinfo.names.size());
      columns.push_back({.name = vinfo.names[p].GetIdentifierName(),
                         .type = vinfo.types[p],
                         .id = catalog::ColumnId{p}});
    }
  } else {
    auto& table_catalog = op.table.Cast<duckdb::TableCatalogEntry>();
    auto& table_entry = catalog::RequireBaseTable(table_catalog);
    relation = &table_entry;
    const auto& entry_columns = table_entry.GetColumns();
    columns.reserve(entry_columns.LogicalColumnCount());
    for (const auto& column : entry_columns.Logical()) {
      columns.push_back({.name = column.Name().GetIdentifierName(),
                         .type = column.Type(),
                         .id = catalog::ColumnId{column.CatalogOid()}});
    }
    const auto pk = table_entry.GetPKColumnIndexes();
    pk_positions.assign(pk.begin(), pk.end());
  }

  duckdb::unique_ptr<duckdb::Expression> bound_where;
  if (op.info->where_clause) {
    SDB_ASSERT(!op.unbound_expressions.empty());
    bound_where = std::move(op.unbound_expressions.back());
    op.unbound_expressions.pop_back();
  }
  // Index expressions are computed by the pipeline, the way duckdb plans its
  // own CREATE INDEX (plan_create_index.cpp AddProjection): a projection over
  // the scan passes every scanned column through and appends one column per
  // indexed expression, so the build sink just reads values. The predicate is
  // already a LogicalFilter below this point.
  // From op.expressions, not op.unbound_expressions: the latter are copies
  // taken before binding resolution and still hold BoundColumnRefExpressions,
  // which no ExpressionExecutor can run. op.expressions is what the resolver
  // rewrote into chunk references, and what duckdb's own AddProjection uses.
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> projected_exprs;
  for (size_t i = 0; i < op.info->parsed_expressions.size(); ++i) {
    if (op.info->parsed_expressions[i]->GetExpressionType() ==
        duckdb::ExpressionType::COLUMN_REF) {
      continue;
    }
    SDB_ASSERT(i < op.expressions.size() && op.expressions[i]);
    projected_exprs.push_back(op.expressions[i]->Copy());
  }
  const auto scan_column_count = input.table_scan.types.size();

  auto& create_index = input.planner.Make<SereneDBPhysicalCreateIndex>(
    *relation, std::move(columns), std::move(pk_positions), database_id,
    std::move(op.info), std::move(op.unbound_expressions),
    std::move(bound_where), schema_entry, op.estimated_cardinality);
  if (projected_exprs.empty()) {
    create_index.children.push_back(input.table_scan);
    return create_index;
  }

  duckdb::vector<duckdb::LogicalType> projection_types;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> projection_exprs;
  projection_types.reserve(scan_column_count + projected_exprs.size());
  projection_exprs.reserve(scan_column_count + projected_exprs.size());
  for (duckdb::idx_t i = 0; i < scan_column_count; ++i) {
    projection_types.push_back(input.table_scan.types[i]);
    projection_exprs.push_back(
      duckdb::make_uniq<duckdb::BoundReferenceExpression>(
        input.table_scan.types[i], i));
  }
  for (auto& expr : projected_exprs) {
    projection_types.push_back(expr->GetReturnType());
    projection_exprs.push_back(std::move(expr));
  }
  auto& projection = input.planner.Make<duckdb::PhysicalProjection>(
    std::move(projection_types), std::move(projection_exprs),
    op.estimated_cardinality);
  projection.children.push_back(input.table_scan);
  create_index.children.push_back(projection);
  create_index.Cast<SereneDBPhysicalCreateIndex>().SetExpressionSlotBase(
    scan_column_count);
  return create_index;
}

}  // namespace sdb::connector
