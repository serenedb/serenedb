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
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <atomic>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <optional>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_index_utils.h"
#include "connector/index_expression.hpp"
#include "connector/inverted_index_bind.h"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "connector/search_table_backfill.h"
#include "connector/view_index_bind.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "pg/types/logical.h"
#include "query/config_variable_names.h"
#include "search/store.h"
#include "search/table_keys.h"
#include "server/utils/primary_key.h"

namespace sdb::connector {
namespace {

struct InsertColumnMeta {
  ColumnId id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

enum class PkShape : uint8_t {
  Single,
  Struct,
};

struct CreateIndexGlobalState final : public duckdb::GlobalSinkState {
  duckdb::idx_t database_id;
  duckdb::idx_t index_id;
  std::string index_name;

  duckdb::idx_t table_id;
  std::vector<InsertColumnMeta> columns;

  catalog::PkColumnKind pk_column = catalog::PkColumnKind::None;
  PkShape pk_shape = PkShape::Single;
  duckdb::idx_t pk_base_col_idx = 0;
  duckdb::LogicalType generated_pk_type;
  std::shared_ptr<const search::FileManifest> file_manifest;

  std::atomic<duckdb::idx_t> backfill_count_atomic{0};

  std::shared_ptr<search::Store> index_storage;
  catalog::IndexTokenizers tokenizers;
  // The entry's own config: the encoding handed to iresearch and the source
  // of every per-field answer the build needs.
  std::shared_ptr<const catalog::InvertedIndexConfig> config;

  std::vector<ExpressionSlot> expression_slots;

  std::optional<SearchBackfillTarget> search_backfill;
  search::Store::BuildClaim build_claim;
  std::optional<search::KeySpec> unique_key;

  pg::ProgressMetrics* progress = nullptr;
};

struct CreateIndexLocalState final : public duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<DuckDBSearchSinkInsertWriter> writer;
  // Per-chunk scratch, kept at high-water mark: Sink runs once per 2048 rows,
  // so anything rebuilt on the stack there is an allocation per chunk.
  std::vector<FeedColumn> columns;
  std::unique_ptr<duckdb::Vector> pk_scratch;
};

}  // namespace

SereneDBPhysicalCreateIndex::SereneDBPhysicalCreateIndex(
  duckdb::PhysicalPlan& plan, duckdb::CatalogEntry& relation,
  duckdb::idx_t database_id, duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions,
  duckdb::SchemaCatalogEntry& schema_entry, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _relation(relation),
    _database_id(database_id),
    _info(std::move(info)),
    _bound_expressions(std::move(bound_expressions)),
    _schema_entry(schema_entry) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalCreateIndex::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<CreateIndexGlobalState>();
  state->database_id = _database_id;
  state->index_name = _info->GetIndexName().GetIdentifierName();

  auto& metrics = ConnectionContext::Get(context).Progress();
  metrics.SetCommand(pg::ProgressCommand::CreateIndex);
  metrics.SetPhase(pg::progress_phase::CreateIndex::Initializing);
  pg::ProgressMetrics::Set(metrics.relid, static_cast<int64_t>(_relation.oid));
  if (estimated_cardinality > 0) {
    pg::ProgressMetrics::Set(metrics.tuples_total,
                             static_cast<int64_t>(estimated_cardinality));
  }
  state->progress = &metrics;

  auto resolve_column = [&](std::string_view col_name) {
    for (const auto& name : _info->names) {
      if (absl::EqualsIgnoreCase(name.GetIdentifierName(), col_name)) {
        return &name;
      }
    }
    return static_cast<const duckdb::Identifier*>(nullptr);
  };

  for (size_t i = 0; i < _info->parsed_expressions.size(); ++i) {
    auto& expr = _info->parsed_expressions[i];
    if (expr->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
      const auto& col_name = col_ref.GetColumnName().GetIdentifierName();
      if (!resolve_column(col_name)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("column \"", col_name, "\" not found in table"));
      }
      continue;
    }

    SDB_ASSERT(i < _bound_expressions.size() && _bound_expressions[i],
               "bound expression is missing for inverted index expression");
    bool references_column = false;
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundColumnRefExpression>(
      *_bound_expressions[i], [&](const duckdb::BoundColumnRefExpression&) {
        references_column = true;
      });
    if (!references_column) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_TABLE_DEFINITION),
        ERR_MSG(
          "indexed expression must reference at least one base table column"));
    }
  }

  duckdb::optional_ptr<const duckdb::IndexCatalogEntry> created;
  duckdb::idx_t created_id;
  const auto extras = Extras();
  const duckdb::LogicalType pk_type =
    extras ? extras->generated_pk_type : duckdb::LogicalType::INVALID;
  state->pk_shape =
    pk_type.id() == duckdb::LogicalTypeId::STRUCT && pk_type != pg::CTID()
      ? PkShape::Struct
      : PkShape::Single;
  if (_relation.type == duckdb::CatalogType::TABLE_ENTRY &&
      catalog::IsSearchTable(_relation) &&
      !IndexNameTaken(context, *_info, _relation)) {
    auto& search_table = _relation.Cast<catalog::SearchTableEntry>();
    search_table.Storage()->ClaimLayout(context,
                                        search_table.name.GetIdentifierName(),
                                        search::TableChange::Layout);
  }
  if (IsReindexPass()) {
    // The pass carries the index's name, resolved in the schema the statement
    // is qualified with -- the same pair every other lookup in this operator
    // uses.
    const auto source = _schema_entry.GetEntry(
      _schema_entry.GetCatalogTransaction(context),
      duckdb::CatalogType::INDEX_ENTRY, extras->source_index);
    created = source ? &source->Cast<duckdb::IndexCatalogEntry>() : nullptr;
    if (!created) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("REINDEX: source index \"",
                              extras->source_index.GetIdentifierName(),
                              "\" vanished mid-refresh"));
    }
    created_id = created->oid;
  } else {
    const auto transaction =
      _schema_entry.ParentCatalog().GetCatalogTransaction(context);
    uint32_t reindex_interval = 0;
    if (auto node = _info->options.extract(kReindexIntervalSetting)) {
      reindex_interval = node.mapped().GetValue<uint32_t>();
    }
    auto entry = _schema_entry.CreateIndex(transaction, *_info, _relation);
    if (entry) {
      auto& index_entry = entry->Cast<catalog::InvertedIndexEntry>();
      auto config = BindInvertedIndexConfig(context, index_entry, _relation,
                                            _bound_expressions, pk_type);
      if (_info->constraint_type == duckdb::IndexConstraintType::UNIQUE &&
          catalog::IsSearchTable(_relation)) {
        auto& table = _relation.Cast<catalog::SearchTableEntry>();
        std::vector<duckdb::Identifier> columns;
        for (size_t i = 0; i < _info->parsed_expressions.size(); ++i) {
          if (_info->column_opclasses[i] == "included") {
            continue;
          }
          const auto& expr = *_info->parsed_expressions[i];
          if (expr.GetExpressionType() != duckdb::ExpressionType::COLUMN_REF) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                            ERR_MSG("unique indexes on expressions are not "
                                    "supported"));
          }
          columns.push_back(
            expr.Cast<duckdb::ColumnRefExpression>().GetColumnName());
        }
        state->unique_key = search::UniqueIndexKey(
          table, static_cast<irs::field_id>(index_entry.oid), state->index_name,
          columns);
        auto unique = std::make_shared<catalog::InvertedIndexConfig>(*config);
        unique->constraint_keys.push_back(state->unique_key->key);
        config = std::move(unique);
      }
      index_entry.SetConfig(std::move(config));
      if (const auto& store = index_entry.SearchStore()) {
        state->build_claim =
          store->BeginIndexBuild(index_entry.oid, index_entry.Config());
        SearchBackfillTarget backfill;
        backfill.shard = store;
        backfill.catalog = &_relation.ParentCatalog();
        backfill.table_id = _relation.oid;
        const auto& table_columns =
          _relation.Cast<catalog::SearchTableEntry>().GetColumns();
        backfill.column_ids.reserve(table_columns.LogicalColumnCount());
        backfill.column_types.reserve(table_columns.LogicalColumnCount());
        for (const auto& column : table_columns.Logical()) {
          backfill.column_ids.emplace_back(TableColumnId(column));
          backfill.column_types.emplace_back(column.Type());
        }
        backfill.group_bytes = SearchBackfillGroupBytes(context);
        state->search_backfill = std::move(backfill);
      } else {
        const auto options = catalog::ResolveSettings(index_entry.options);
        auto storage = std::make_shared<search::Store>(
          index_entry.catalog.Cast<catalog::SereneDBCatalog>().Directory(),
          index_entry.catalog.InMemory(), false, index_entry.catalog.GetOid(),
          index_entry.oid, options, index_entry.Config()->top_k_scorer,
          /*is_new=*/true);
        storage->ApplyOptions(options);
        index_entry.AdoptStorage(storage);
        storage->SetFileManifest(extras ? extras->manifest : nullptr);
        storage->StartTasks();
        index_entry.CreateReindexJob(transaction, reindex_interval);
      }
    }
    created = entry ? &entry->Cast<duckdb::IndexCatalogEntry>() : nullptr;
    created_id = created ? created->oid : 0;
  }

  if (created_id == 0) {
    // Index already exists, nothing to do
    return state;
  }

  state->index_id = created_id;
  if (state->progress) {
    state->progress->SetPhase(pg::progress_phase::CreateIndex::BuildingIndex);
  }

  // Off the entry, which owns the definition the store merely points at.
  state->pk_column = created->Cast<catalog::InvertedIndexEntry>().Config()->pk;
  if (extras) {
    state->generated_pk_type = extras->generated_pk_type;
    state->file_manifest = extras->manifest;
  }

  auto storage = created->Cast<catalog::InvertedIndexEntry>().Storage();
  if (!storage) {
    if (IsReindexPass()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("REINDEX: source index \"",
                              extras->source_index.GetIdentifierName(),
                              "\" vanished mid-refresh"));
    }
    return state;
  }
  state->index_storage = storage;
  if (extras && extras->Pass() == ReindexPass::Rebuild) {
    storage->CommitRemoveAll();
  }

  state->table_id = _relation.oid;
  // One slot per entry of info.column_ids, in that order, then the row
  // identifier the bind appends. Position i is column_ids[i] -- nothing here
  // may reorder or widen it.
  state->columns.reserve(_info->column_ids.size());
  const auto* table = _relation.type == duckdb::CatalogType::TABLE_ENTRY
                        ? &_relation.Cast<duckdb::TableCatalogEntry>()
                        : nullptr;
  for (size_t chunk_idx = 0; chunk_idx < _info->column_ids.size();
       ++chunk_idx) {
    const auto position = _info->column_ids[chunk_idx];
    const auto id = table ? TableColumnId(table->GetColumns().GetColumn(
                              duckdb::LogicalIndex(position)))
                          : ColumnId{position};
    state->columns.emplace_back(id, _info->scan_types[chunk_idx], chunk_idx);
  }
  state->pk_base_col_idx = state->columns.size();

  auto& index_entry = created->Cast<catalog::InvertedIndexEntry>();
  state->tokenizers = index_entry.ResolveTokenizers(context);
  state->config = index_entry.Config();
  // The pipeline's projection computed the indexed expressions into the slots
  // after the scanned columns (see SereneDBCreateIndexPlan).
  if (HasProjectedExpressions()) {
    const auto& config = *state->config;
    auto slot = _expression_slot_base.GetIndex();
    for (size_t k = 0; k < config.keys.size(); ++k) {
      const auto& expr = *_bound_expressions[k];
      if (expr.GetExpressionClass() ==
          duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        continue;
      }
      const auto& key = config.keys[k];
      if (config.WritesField(key)) {
        state->expression_slots.emplace_back(key.field_id, slot);
      }
      ++slot;
    }
  }
  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBPhysicalCreateIndex::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto& gstate = sink_state->Cast<CreateIndexGlobalState>();
  if (!gstate.index_storage) {
    return duckdb::make_uniq<duckdb::LocalSinkState>();
  }

  auto& inverted_storage = *gstate.index_storage;

  auto lstate = duckdb::make_uniq<CreateIndexLocalState>();
  lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
    inverted_storage.GetTransaction(/*exclusive_segment=*/true));
  lstate->search_trx->SetFieldOptions(gstate.config);
  lstate->writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
    *lstate->search_trx,
    [&gstate](irs::field_id id) { return gstate.tokenizers.Acquire(id); },
    IndexedColumnIds(*gstate.config), MakeEntryInfoProvider(*gstate.config),
    gstate.config->pk);
  return lstate;
}

duckdb::SinkResultType SereneDBPhysicalCreateIndex::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (!gstate.index_storage) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto* lstate = &input.local_state.Cast<CreateIndexLocalState>();
  auto* writer = lstate->writer.get();

  const duckdb::Vector* pk = nullptr;
  if (gstate.pk_column == catalog::PkColumnKind::Has) {
    switch (gstate.pk_shape) {
      case PkShape::Single:
        SDB_ASSERT(gstate.pk_base_col_idx < chunk.ColumnCount());
        pk = &chunk.data[gstate.pk_base_col_idx];
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
        pk = lstate->pk_scratch.get();
      } break;
    }
  }

  auto& columns = lstate->columns;
  if (columns.empty()) {
    columns.reserve(gstate.columns.size());
    for (const auto& col : gstate.columns) {
      columns.push_back({col.input_col_idx, {col.id, col.duckdb_type}});
    }
  }

  FeedChunk(*writer, num_rows, pk, chunk, columns, gstate.expression_slots);

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
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (!gstate.index_storage) {
    return duckdb::SinkCombineResultType::FINISHED;
  }
  auto& lstate = input.local_state.Cast<CreateIndexLocalState>();
  lstate.writer.reset();
  // Flush this thread's tail segment here (in parallel Combines) rather than
  // leaving it for the single-threaded Finalize refresh to write.
  auto& trx = *lstate.search_trx;
  try {
    trx.Flush();
  } catch (const irs::SqlException& e) {
    lstate.search_trx.reset();
    THROW_SQL_ERROR(ERR_CODE(e.error().errcode),
                    ERR_MSG("failed to flush index '", gstate.index_name,
                            "': ", e.message()));
  } catch (const std::exception& e) {
    lstate.search_trx.reset();
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("failed to flush index '", gstate.index_name,
                            "': ", duckdb::ErrorData{e}.RawMessage()));
  }
  const bool committed = trx.Commit();
  lstate.search_trx.reset();
  if (!committed) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OUT_OF_MEMORY),
                    ERR_MSG("failed to commit index '", gstate.index_name,
                            "': out of memory"));
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBPhysicalCreateIndex::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (gstate.search_backfill) {
    SDB_ASSERT(gstate.search_backfill->shard->BuildInFlight());
    RunSearchTableBackfill(context, *gstate.search_backfill, gstate.progress);
    if (gstate.unique_key) {
      search::VerifyUniqueKey(
        gstate.search_backfill->shard->View(search::kAllApplied, {}),
        *gstate.unique_key);
    }
    gstate.build_claim.Finish();
    if (gstate.progress) {
      gstate.progress->SetPhase(pg::progress_phase::CreateIndex::Committing);
    }
  }
  if (!gstate.index_storage) {
    return duckdb::SinkFinalizeType::READY;
  }

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateIndex::Committing);
  }

  auto& inverted_storage = *gstate.index_storage;
  SDB_IF_FAILURE("commit_before_manifest_publish") {
    inverted_storage.Commit();
  }
  if (gstate.file_manifest) {
    inverted_storage.SetFileManifest(gstate.file_manifest);
  }
  inverted_storage.Commit();
  SDB_IF_FAILURE("crash_before_finish_creation") { SDB_IMMEDIATE_ABORT(); }

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateIndex::Finalizing);
  }
  SDB_WAIT_ON_FAILURE("pause_create_index_before_commit");
  if (!IsReindexPass()) {
    SDB_IF_FAILURE("crash_before_commit") { SDB_IMMEDIATE_ABORT(); }
  }
  return duckdb::SinkFinalizeType::READY;
}

duckdb::SourceResultType SereneDBPhysicalCreateIndex::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& gstate = sink_state->Cast<CreateIndexGlobalState>();
  chunk.SetCardinality(1);
  const auto count = static_cast<int64_t>(
    gstate.backfill_count_atomic.load(std::memory_order_relaxed));
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(count));
  return duckdb::SourceResultType::FINISHED;
}

duckdb::PhysicalOperator& SereneDBCreateIndexPlan(
  duckdb::PlanIndexInput& input) {
  auto& op = input.op;
  auto& table_catalog = op.table.ParentCatalog();
  catalog::ThrowIfReadOnly(table_catalog, "CREATE INDEX");
  if (!catalog::IsSereneDB(table_catalog)) {
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
  auto& schema_entry = op.table.ParentSchema(input.context);
  auto database_id = table_catalog.Cast<catalog::SereneDBCatalog>().GetOid();

  const auto column_ids = KeyColumnIds(op.table, op.info->column_ids);
  irs::containers::FlatHashSet<std::string> projected;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> projected_exprs;
  for (size_t i = 0; i < op.info->parsed_expressions.size(); ++i) {
    SDB_ASSERT(i < op.expressions.size() && op.expressions[i]);
    SDB_ASSERT(i < op.unbound_expressions.size());
    const auto& unbound = *op.unbound_expressions[i];
    if (unbound.GetExpressionClass() ==
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
      continue;
    }
    const auto& expr = *op.expressions[i];
    if (projected
          .emplace(NormalizedExpression(unbound, op.table.oid, column_ids,
                                        input.context))
          .second) {
      projected_exprs.emplace_back(expr.Copy());
    } else {
      projected_exprs.emplace_back(
        duckdb::make_uniq<duckdb::BoundConstantExpression>(
          duckdb::Value{expr.GetReturnType()}));
    }
  }
  const auto scan_column_count = input.table_scan.types.size();

  auto& create_index = input.planner.Make<SereneDBPhysicalCreateIndex>(
    op.table, database_id, std::move(op.info),
    std::move(op.unbound_expressions), schema_entry, op.estimated_cardinality);
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
