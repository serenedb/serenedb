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
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/execution/operator/projection/physical_projection.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
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
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <duckdb/transaction/undo_buffer.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <optional>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/index_expression.hpp"
#include "connector/inverted_index_bind.h"
#include "connector/inverted_store_index.h"
#include "connector/pg_logical_types.h"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "connector/search_table_backfill.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "pg/sql_utils.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"
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
  std::string schema_name;
  std::string table_name;
  std::string index_name;

  duckdb::idx_t table_id;
  std::vector<InsertColumnMeta> columns;

  bool pk_term = false;
  connector::PkColumnKind pk_column = connector::PkColumnKind::None;
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
  catalog::IndexTokenizers tokenizers;
  // The entry's own config: the encoding handed to iresearch and the source
  // of every per-field answer the build needs.
  std::shared_ptr<const catalog::InvertedIndexConfig> config;

  std::optional<SearchBackfillTarget> search_backfill;

  pg::ProgressMetrics* progress = nullptr;
};

struct CreateIndexLocalState final : public duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<DuckDBSearchSinkInsertWriter> writer;
  // Per-chunk scratch, kept at high-water mark: Sink runs once per 2048 rows,
  // so anything rebuilt on the stack there is an allocation per chunk. row_keys
  // holds its strings rather than clearing the vector, which would destroy them
  // and throw away exactly the buffers the pooling is for.
  std::vector<std::string> row_keys;
  std::vector<duckdb::string_t> key_views;
  std::vector<FeedColumn> columns;
  std::vector<ExpressionValue> expression_values;
  duckdb::SelectionVector backfill_sel{STANDARD_VECTOR_SIZE};
  std::unique_ptr<duckdb::Vector> pk_scratch;
  size_t uncommitted_min_slot = std::numeric_limits<size_t>::max();
};

}  // namespace

SereneDBPhysicalCreateIndex::SereneDBPhysicalCreateIndex(
  duckdb::PhysicalPlan& plan, duckdb::CatalogEntry& relation,
  duckdb::idx_t database_id, duckdb::unique_ptr<duckdb::CreateIndexInfo> info,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> bound_expressions,
  duckdb::DuckSchemaEntry& schema_entry, duckdb::idx_t estimated_cardinality)
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
  state->schema_name = _schema_entry.name.GetIdentifierName();
  state->table_name = std::string{_relation.name.GetIdentifierName()};
  state->index_name = _info->GetIndexName().GetIdentifierName();

  if (auto sdb_state = context.registered_state->Get<SereneDBClientState>(
        kSereneDBClientStateKey)) {
    auto& metrics = sdb_state->Progress();
    metrics.SetCommand(pg::ProgressCommand::CreateIndex);
    metrics.SetPhase(pg::progress_phase::CreateIndex::Initializing);
    pg::ProgressMetrics::Set(metrics.relid,
                             static_cast<int64_t>(_relation.oid));
    if (estimated_cardinality > 0) {
      pg::ProgressMetrics::Set(metrics.tuples_total,
                               static_cast<int64_t>(estimated_cardinality));
    }
    state->progress = &metrics;
  }

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

  // Shared, and it stays the one object: the providers below build the
  // hyperloglog and IVF columns off the per-column options, which only this
  // object answers -- a copy rebuilds them and loses them.
  duckdb::optional_ptr<const duckdb::IndexCatalogEntry> created;
  duckdb::idx_t created_id;
  const auto extras = Extras();
  const duckdb::LogicalType pk_type =
    extras ? extras->generated_pk_type : duckdb::LogicalType::INVALID;
  state->pk_shape =
    pk_type.id() == duckdb::LogicalTypeId::STRUCT && pk_type != pg::CTID()
      ? PkShape::Struct
      : PkShape::Single;
  if (IsReindexPass()) {
    // The pass carries the index's name, resolved in the schema the statement
    // is qualified with -- the same pair every other lookup in this operator
    // uses.
    created = duckdb::Catalog::GetEntry<duckdb::DuckIndexEntry>(
                context,
                duckdb::QualifiedName{_relation.ParentCatalog().GetName(),
                                      _schema_entry.name, extras->source_index},
                duckdb::OnEntryNotFound::RETURN_NULL)
                .get();
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
    auto entry = _schema_entry.CreateIndex(transaction, *_info, _relation);
    if (entry) {
      auto& index_entry = entry->Cast<catalog::InvertedIndexEntry>();
      index_entry.SetConfig(BindInvertedIndexConfig(
        context, index_entry, _relation, _bound_expressions, pk_type));
      if (const auto& store = index_entry.SearchStore()) {
        store->MergeIndexConfig(index_entry.oid, index_entry.Config());
        SearchBackfillTarget backfill;
        backfill.shard = store;
        backfill.catalog = &_relation.ParentCatalog();
        backfill.table_id = _relation.oid;
        const auto& table_columns =
          _relation.Cast<catalog::SearchTableEntry>().GetColumns();
        backfill.column_ids.reserve(table_columns.LogicalColumnCount());
        backfill.column_types.reserve(table_columns.LogicalColumnCount());
        for (const auto& column : table_columns.Logical()) {
          backfill.column_ids.emplace_back(column.Oid());
          backfill.column_types.push_back(column.Type());
        }
        backfill.group_bytes = uint64_t{1} << 30;
        duckdb::Value group_bytes;
        if (context.TryGetCurrentSetting(
              std::string{kSearchBackfillGroupBytesSetting}, group_bytes) &&
            !group_bytes.IsNull()) {
          backfill.group_bytes = group_bytes.GetValue<uint64_t>();
        }
        state->search_backfill = std::move(backfill);
      } else {
        const auto published = PublishInvertedIndex(
          context, index_entry, _relation, _bound_expressions);
        published.storage->SetFileManifest(extras ? extras->manifest : nullptr);
        published.storage->StartTasks();
        if (IsDuckDBTable()) {
          published.storage->SetDeleteLogRowidEnd(published.rowid_horizon);
          state->backfill_rowid_end =
            static_cast<int64_t>(published.rowid_horizon);
          state->uncommitted_min_rowids = std::vector<std::atomic<int64_t>>(
            duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads());
          auto& store_db = _relation.ParentCatalog().GetAttached();
          auto& store_txn = duckdb::DuckTransaction::Get(context, store_db);
          const auto undo = store_txn.GetUndoProperties();
          if (!undo.has_updates && !undo.has_deletes) {
            duckdb::DuckTransactionManager::Get(store_db)
              .RefreshCheckpointSnapshot(store_txn);
          }
        }
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
  const auto policy = created->Cast<catalog::InvertedIndexEntry>().Config()->pk;
  state->pk_term = policy.index_term;
  state->pk_column = policy.column;
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

  state->table_id = _relation.oid;
  // One slot per entry of info.column_ids, in that order, then the row
  // identifier the bind appends. Position i is column_ids[i] -- nothing here
  // may reorder or widen it.
  state->columns.reserve(_info->column_ids.size());
  for (size_t chunk_idx = 0; chunk_idx < _info->column_ids.size();
       ++chunk_idx) {
    state->columns.push_back(InsertColumnMeta{
      .id = _info->column_ids[chunk_idx],
      .duckdb_type = _info->scan_types[chunk_idx],
      .input_col_idx = chunk_idx,
    });
  }
  state->pk_base_col_idx = state->columns.size();

  auto& index_entry = created->Cast<catalog::InvertedIndexEntry>();
  state->tokenizers = index_entry.ResolveTokenizers(context);
  state->config = index_entry.Config();
  return state;
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
  auto& gstate = sink_state->Cast<CreateIndexGlobalState>();
  if (!gstate.index_storage) {
    return duckdb::make_uniq<duckdb::LocalSinkState>();
  }

  auto& inverted_storage = *gstate.index_storage;

  auto lstate = duckdb::make_uniq<CreateIndexLocalState>();
  lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
    inverted_storage.GetTransaction());
  lstate->search_trx->SetFieldOptions(gstate.config);
  lstate->writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
    *lstate->search_trx,
    [&gstate](irs::field_id id) { return gstate.tokenizers.Acquire(id); },
    IndexedColumnIds(*gstate.config), MakeEntryInfoProvider(*gstate.config),
    gstate.config->pk);

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
  if (!gstate.index_storage) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto* lstate = &input.local_state.Cast<CreateIndexLocalState>();
  auto* writer = lstate->writer.get();

  if (gstate.backfill_rowid_end != std::numeric_limits<int64_t>::max()) {
    auto& rowid_vec = chunk.data[gstate.pk_base_col_idx];
    auto rowids = rowid_vec.Values<int64_t>();
    auto& sel = lstate->backfill_sel;
    duckdb::idx_t keep = 0;
    for (duckdb::idx_t i = 0; i < num_rows; ++i) {
      if (rowids[i].GetValueUnsafe() < gstate.backfill_rowid_end) {
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
  if (gstate.pk_column == connector::PkColumnKind::Has) {
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
        auto pks = pk_vec.Values<int64_t>();
        for (duckdb::idx_t row = 0; row < num_rows; ++row) {
          auto& key = row_keys[row];
          key.clear();
          primary_key::AppendSigned(key, pks[row].GetValueUnsafe());
          key_views.emplace_back(key.data(), static_cast<uint32_t>(key.size()));
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
          key_views.emplace_back(key.data(), static_cast<uint32_t>(key.size()));
        }
      } break;
    }
    pk.key_terms = key_views;
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
    const auto& config = *gstate.config;
    const auto keys = std::span{config.keys};
    const auto& unbound = _bound_expressions;
    expression_values.reserve(keys.size());
    auto slot = _expression_slot_base.GetIndex();
    for (size_t k = 0; k < keys.size(); ++k) {
      if (k < unbound.size() && unbound[k]->GetExpressionClass() ==
                                  duckdb::ExpressionClass::BOUND_COLUMN_REF) {
        continue;
      }
      SDB_ASSERT(slot < chunk.ColumnCount());
      const auto* entry = config.FindEntry(keys[k].field_id);
      if (!entry || entry->IsTokenized()) {
        RejectJsonObjectArrayLeaves(chunk.data[slot], num_rows);
      }
      expression_values.push_back({keys[k].field_id, &chunk.data[slot]});
      ++slot;
    }
  }

  irs::CommitOnFlush commit_on_flush{search::TickDomain::Instance().Counter()};
  FeedChunk(*writer, num_rows, pk, chunk, columns, expression_values,
            &commit_on_flush);

  if (commit_on_flush.committed &&
      lstate->uncommitted_min_slot != std::numeric_limits<size_t>::max()) {
    auto rowids = chunk.data[gstate.pk_base_col_idx].Values<int64_t>();
    const auto batch_min_rowid = rowids[0].GetValueUnsafe();
    SDB_ASSERT(batch_min_rowid <= rowids[num_rows - 1].GetValueUnsafe());
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
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (!gstate.index_storage) {
    return duckdb::SinkCombineResultType::FINISHED;
  }
  auto& lstate = input.local_state.Cast<CreateIndexLocalState>();
  lstate.writer.reset();
  // Flush this thread's tail segment here (in parallel Combines) rather than
  // leaving it for the single-threaded Finalize refresh to write.
  auto& trx = *lstate.search_trx;
  trx.RegisterFlush();
  const bool committed = trx.FlushAndCommit(
    search::TickDomain::Instance().Next(trx.GetQueries() + 1));
  lstate.search_trx.reset();
  if (committed) {
    // The final commit went through: nothing pending here anymore, drop
    // out of the begin computation.
    AdvanceUncommittedMin(gstate, lstate.uncommitted_min_slot,
                          std::numeric_limits<int64_t>::max());
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBPhysicalCreateIndex::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<CreateIndexGlobalState>();
  if (gstate.search_backfill) {
    RunSearchTableBackfill(context, *gstate.search_backfill, gstate.progress);
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
      search::TickDomain::Instance().Next(delete_log.size() + 1);
    if (!trx.Commit(last_tick)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("failed to replay concurrent deletes for index '",
                              gstate.index_name, "'"));
    }
  }
  SDB_IF_FAILURE("refresh_before_manifest_publish") {
    inverted_storage.Refresh();
  }
  if (gstate.file_manifest) {
    inverted_storage.SetFileManifest(gstate.file_manifest);
  }
  inverted_storage.Refresh();
  SDB_IF_FAILURE("crash_before_finish_creation") { SDB_IMMEDIATE_ABORT(); }
  inverted_storage.FinishCreation();

  if (gstate.progress) {
    gstate.progress->SetPhase(pg::progress_phase::CreateIndex::Finalizing);
  }
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
  if (table_catalog.GetCatalogType() !=
      catalog::SereneDBCatalog::kStorageType) {
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
    op.table.ParentSchema(input.context).Cast<duckdb::DuckSchemaEntry>();
  auto database_id = table_catalog.Cast<catalog::SereneDBCatalog>().GetOid();

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
