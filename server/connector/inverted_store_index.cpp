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

#include "connector/inverted_store_index.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <iterator>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/primary_key.hpp"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/index_expression.hpp"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

ObjectId OptionId(const duckdb::case_insensitive_map_t<duckdb::Value>& options,
                  const char* key) {
  auto it = options.find(key);
  SDB_ENSURE(it != options.end(), "store index is missing the ", key,
             " option");
  return ObjectId{it->second.GetValue<uint64_t>()};
}

struct InvertedStoreBuildGlobalState final : duckdb::IndexBuildGlobalState {
  duckdb::unique_ptr<InvertedStoreIndex> index;
};

struct InvertedStoreBuildLocalState final : duckdb::IndexBuildLocalState {};

}  // namespace
namespace {

std::string RowIdKey(duckdb::row_t row) {
  std::string key;
  primary_key::AppendSigned(key, static_cast<int64_t>(row));
  return key;
}

struct FilteredChunk {
  duckdb::DataChunk chunk;
  duckdb::Vector row_ids;
  duckdb::idx_t count = 0;

  explicit FilteredChunk(const duckdb::Vector& rows)
    : row_ids(rows.GetType(), nullptr, 0) {}
};

duckdb::unique_ptr<FilteredChunk> CalcFilter(
  const duckdb::Expression* predicate, duckdb::DataChunk& chunk,
  duckdb::Vector& row_ids, ObjectId table_id,
  std::span<const catalog::Column::Id> col_ids,
  duckdb::ClientContext& context) {
  if (!predicate) {
    return nullptr;
  }
  SDB_ASSERT(predicate->GetReturnType() == duckdb::LogicalType::BOOLEAN);
  auto result =
    EvaluateExprOverChunk(*predicate, chunk, table_id, col_ids, context);
  const auto total = chunk.size();
  duckdb::UnifiedVectorFormat fmt;
  result.ToUnifiedFormat(total, fmt);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);
  duckdb::SelectionVector sel(total);
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i < total; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (fmt.validity.RowIsValid(idx) && values[idx]) {
      sel.set_index(kept++, i);
    }
  }
  if (kept == total) {
    return nullptr;
  }
  auto filtered = duckdb::make_uniq<FilteredChunk>(row_ids);
  filtered->count = kept;
  if (kept != 0) {
    filtered->chunk.InitializeEmpty(chunk.GetTypes());
    filtered->chunk.Reference(chunk);
    filtered->chunk.Slice(sel, kept);
    filtered->row_ids.Slice(row_ids, sel, kept);
  }
  return filtered;
}

}  // namespace

struct InvertedStoreIndex::ReplaySession {
  std::shared_ptr<search::InvertedIndexStorage> storage;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<const catalog::Table> table;
  std::vector<duckdb::idx_t> ref_positions;
  std::vector<catalog::Column::Id> ref_col_ids;
  std::vector<catalog::Column::Id> all_col_ids;
  duckdb::unique_ptr<duckdb::Expression> predicate_expr;
  duckdb::Connection expr_conn;
  irs::IndexWriter::Transaction trx;
  std::unique_ptr<DuckDBSearchSinkInsertWriter> insert_writer;
  DuckDBSearchSinkDeleteWriter delete_writer;
  uint64_t total_ops = 0;
  uint64_t durable_offset = 0;

  ReplaySession(std::shared_ptr<search::InvertedIndexStorage> storage_in,
                std::shared_ptr<const catalog::Snapshot> snapshot_in,
                std::shared_ptr<const catalog::InvertedIndex> index_in,
                std::shared_ptr<const catalog::Table> table_in,
                duckdb::DatabaseInstance& db)
    : storage{std::move(storage_in)},
      snapshot{std::move(snapshot_in)},
      index{std::move(index_in)},
      table{std::move(table_in)},
      expr_conn{db},
      trx{storage->GetTransaction()},
      delete_writer{trx} {
    expr_conn.BeginTransaction();
    trx.SetFieldOptions(index);
  }
};

InvertedStoreIndex::InvertedStoreIndex(
  const std::string& name, duckdb::TableIOManager& io,
  const duckdb::vector<duckdb::column_t>& column_ids,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
  duckdb::AttachedDatabase& db, ObjectId table_id, ObjectId index_id)
  : BoundIndex(duckdb::Identifier{name}, kTypeName,
               duckdb::IndexConstraintType::NONE, column_ids, io, exprs, db),
    _table_id{table_id},
    _index_id{index_id} {}

InvertedStoreIndex::~InvertedStoreIndex() = default;

InvertedStoreIndex::ReplaySession& InvertedStoreIndex::EnsureReplaySession() {
  if (_replay) {
    return *_replay;
  }
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  SDB_ENSURE(snapshot, "inverted index replay: no catalog snapshot");
  auto inverted = snapshot->GetObject<catalog::InvertedIndex>(_index_id);
  auto table = snapshot->GetObject<catalog::Table>(_table_id);
  SDB_ENSURE(inverted && table, "inverted index replay: catalog objects for ",
             _index_id.id(), " missing");
  auto storage = inverted->GetData();
  SDB_ENSURE(storage, "inverted index replay: storage ", _index_id.id(),
             " missing");
  const search::WalCursor cursor = storage->GetRecoveryWalCursor();
  uint64_t durable_offset = 0;
  auto& block_manager = db.GetStorageManager().GetBlockManager();
  if (cursor.generation == block_manager.GetCheckpointIteration()) {
    durable_offset = cursor.offset;
  }
  _replay = std::make_unique<ReplaySession>(
    std::move(storage), std::move(snapshot), std::move(inverted),
    std::move(table), db.GetDatabase());
  _replay->durable_offset = durable_offset;
  return *_replay;
}

void InvertedStoreIndex::OnReplayRange(duckdb::idx_t commit_offset) {
  _replay_commit_offset = commit_offset;
}

void InvertedStoreIndex::ReplayAppend(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto& session = EnsureReplaySession();
  if (_replay_commit_offset != 0 &&
      _replay_commit_offset < session.durable_offset) {
    return;
  }
  auto& inverted = *session.index;
  if (!session.insert_writer) {
    session.all_col_ids = TableChunkColumnIds(*session.table);
    for (duckdb::idx_t pos = 0;
         pos < chunk.ColumnCount() && pos < session.all_col_ids.size(); ++pos) {
      if (!inverted.ReferencesColumn(session.all_col_ids[pos])) {
        continue;
      }
      session.ref_positions.push_back(pos);
      session.ref_col_ids.push_back(session.all_col_ids[pos]);
    }
    if (inverted.Predicate()) {
      session.predicate_expr = DeserializeBoundExpression(
        inverted.Predicate()->serialized_expr, *session.expr_conn.context);
    }
    session.insert_writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
      session.trx, MakeTokenizerProvider(session.snapshot, inverted),
      inverted.GetColumns(), MakeEntryInfoProvider(inverted),
      MakeIndexedExpressions(inverted, *session.expr_conn.context),
      PkPolicy{.index_term = inverted.GetOptions().pk_term,
               .column = inverted.GetOptions().pk_column});
  }

  auto filtered =
    CalcFilter(session.predicate_expr.get(), chunk, row_ids, _table_id,
               session.all_col_ids, *session.expr_conn.context);
  if (filtered && filtered->count == 0) {
    session.trx.AdvanceQueries(1);
    ++session.total_ops;
    return;
  }
  auto& feed_chunk = filtered ? filtered->chunk : chunk;
  auto& feed_rows = filtered ? filtered->row_ids : row_ids;
  count = filtered ? filtered->count : count;

  duckdb::UnifiedVectorFormat row_fmt;
  feed_rows.ToUnifiedFormat(count, row_fmt);
  std::vector<std::string> keys(count);
  std::vector<std::string_view> key_views(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    keys[i] = RowIdKey(row);
    key_views[i] = keys[i];
  }

  auto& ins = *session.insert_writer;
  ins.Init(count, PkChunk{.keys = key_views, .column = &feed_rows});
  for (duckdb::idx_t k = 0; k < session.ref_positions.size(); ++k) {
    auto col_id = session.ref_col_ids[k];
    const auto* col = session.table->ColumnById(col_id);
    if (!col) {
      continue;
    }
    const ColumnDescriptor desc{col_id, col->type};
    ins.SwitchColumn(desc, feed_chunk.data[session.ref_positions[k]], count);
  }
  if (auto indexed_exprs = ins.IndexedExpressions(); !indexed_exprs.empty()) {
    EvaluateAndWriteIndexedExpressions(ins, indexed_exprs, feed_chunk,
                                       _table_id, session.ref_col_ids,
                                       *session.expr_conn.context, count);
  }
  ins.Finish();
  session.trx.AdvanceQueries(1);
  ++session.total_ops;
}

void InvertedStoreIndex::ReplayDelete(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto& session = EnsureReplaySession();
  if (_replay_commit_offset != 0 &&
      _replay_commit_offset < session.durable_offset) {
    return;
  }
  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  session.delete_writer.Init(count, {});
  std::string key;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    key.clear();
    primary_key::AppendSigned(key, static_cast<int64_t>(row));
    session.delete_writer.DeleteRow(key);
  }
  session.delete_writer.Finish();
  ++session.total_ops;
}

void InvertedStoreIndex::FinishReplay() {
  if (!_replay) {
    return;
  }
  auto& session = *_replay;
  if (session.total_ops == 0) {
    session.expr_conn.Rollback();
    _replay.reset();
    return;
  }
  const auto last_tick =
    search::TickDomain::Instance().Advance(session.total_ops);
  auto& sm = db.GetStorageManager();
  session.storage->RecordFlushCursor(
    last_tick, search::WalCursor{sm.GetBlockManager().GetCheckpointIteration(),
                                 sm.GetWALSize()});
  SDB_ENSURE(session.trx.Commit(last_tick),
             "inverted index replay: commit failed for index ", _index_id.id());
  session.expr_conn.Rollback();
  _replay.reset();
}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    ReplayAppend(chunk, row_ids);
    return {};
  }
  auto snapshot = conn->CatalogSnapshot();
  auto table = snapshot->GetObject<catalog::Table>(_table_id);
  if (!table) {
    return {};
  }
  return AppendRows(*conn, chunk, row_ids, TableChunkColumnIds(*table));
}

duckdb::ErrorData InvertedStoreIndex::AppendRows(
  ConnectionContext& conn, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
  std::span<const catalog::Column::Id> chunk_column_ids) {
  if (chunk.size() == 0) {
    return {};
  }
  duckdb::Connection expr_conn(*conn.GetClientContext().db);
  expr_conn.BeginTransaction();
  absl::Cleanup rollback_expr_conn = [&] { expr_conn.Rollback(); };
  const catalog::ExpressionData* predicate = nullptr;
  auto writer = CreateInvertedIndexWriter<DuckDBWriteKind::Insert>(
    _table_id, _index_id, conn, expr_conn.context.get(), &predicate);
  if (!writer) {
    return {};
  }
  auto snapshot = conn.CatalogSnapshot();
  auto table = snapshot->GetObject<catalog::Table>(_table_id);
  if (!table) {
    return {};
  }

  duckdb::unique_ptr<duckdb::Expression> pred;
  if (predicate) {
    pred = DeserializeBoundExpression(predicate->serialized_expr,
                                      *expr_conn.context);
  }
  auto filtered = CalcFilter(pred.get(), chunk, row_ids, _table_id,
                             chunk_column_ids, *expr_conn.context);
  if (filtered && filtered->count == 0) {
    return {};
  }
  auto& feed_chunk = filtered ? filtered->chunk : chunk;
  auto& feed_rows = filtered ? filtered->row_ids : row_ids;
  const auto count = filtered ? filtered->count : chunk.size();

  duckdb::UnifiedVectorFormat row_fmt;
  feed_rows.ToUnifiedFormat(count, row_fmt);
  std::vector<std::string> keys(count);
  std::vector<std::string_view> key_views(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    keys[i] = RowIdKey(row);
    key_views[i] = keys[i];
  }

  writer->Init(count, PkChunk{.keys = key_views, .column = &feed_rows});
  for (duckdb::idx_t pos = 0;
       pos < feed_chunk.ColumnCount() && pos < chunk_column_ids.size(); ++pos) {
    auto col_id = chunk_column_ids[pos];
    const auto* col = table->ColumnById(col_id);
    if (!col) {
      continue;
    }
    const ColumnDescriptor desc{col_id, col->type};
    writer->SwitchColumn(desc, feed_chunk.data[pos], count);
  }
  if (auto indexed_exprs = writer->IndexedExpressions();
      !indexed_exprs.empty()) {
    EvaluateAndWriteIndexedExpressions(*writer, indexed_exprs, feed_chunk,
                                       _table_id, chunk_column_ids,
                                       *expr_conn.context, count);
  }
  writer->Finish();
  conn.RegisterSearchFlush();
  return {};
}

std::vector<catalog::Column::Id> InvertedStoreIndex::TableChunkColumnIds(
  const catalog::Table& table) {
  std::vector<catalog::Column::Id> ids;
  for (const auto& col : table.Columns()) {
    if (col.GetId() != catalog::Column::kGeneratedPKId) {
      ids.push_back(col.GetId());
    }
  }
  return ids;
}

duckdb::ErrorData InvertedStoreIndex::Append(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
}

duckdb::ErrorData InvertedStoreIndex::Insert(duckdb::IndexLock&,
                                             duckdb::DataChunk& chunk,
                                             duckdb::Vector& row_ids) {
  return AppendImpl(chunk, row_ids);
}

void InvertedStoreIndex::Delete(duckdb::IndexLock&, duckdb::DataChunk& chunk,
                                duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    ReplayDelete(chunk, row_ids);
    return;
  }
  std::shared_ptr<search::InvertedIndexStorage> storage;
  if (auto inverted =
        conn->CatalogSnapshot()->GetObject<catalog::InvertedIndex>(_index_id)) {
    storage = inverted->GetData();
  }
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  auto write_rows = [&](size_t n, auto&& row_at) {
    auto writer = CreateInvertedIndexWriter<DuckDBWriteKind::Delete>(
      _table_id, _index_id, *conn);
    if (!writer) {
      return;
    }
    writer->Init(n, {});
    std::string key;
    for (size_t i = 0; i < n; ++i) {
      key.clear();
      primary_key::AppendSigned(key, row_at(i));
      writer->DeleteRow(key);
    }
    writer->Finish();
    conn->RegisterSearchFlush();
  };
  if (storage && storage->IsDeleteLogOpen()) {
    const auto log_begin = storage->DeleteLogRowidBegin();
    const auto log_end = storage->DeleteLogRowidEnd();
    std::vector<int64_t> native;
    std::vector<int64_t> logged;
    native.reserve(count);
    logged.reserve(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const int64_t row = data[fmt.sel->get_index(i)];
      (row < log_begin || row >= log_end ? native : logged).push_back(row);
    }
    if (!logged.empty() && !storage->AppendDeleteLog(std::move(logged))) {
      absl::c_move(logged, std::back_inserter(native));
    }
    if (!native.empty()) {
      write_rows(native.size(), [&](size_t i) { return native[i]; });
    }
    return;
  }
  write_rows(count, [&](size_t i) { return data[fmt.sel->get_index(i)]; });
}

idx_t InvertedStoreIndex::TryDelete(
  duckdb::IndexLock& l, duckdb::DataChunk& chunk, duckdb::Vector& row_ids,
  duckdb::optional_ptr<duckdb::SelectionVector> deleted_sel,
  duckdb::optional_ptr<duckdb::SelectionVector>) {
  Delete(l, chunk, row_ids);
  if (deleted_sel) {
    for (duckdb::idx_t i = 0; i < chunk.size(); ++i) {
      deleted_sel->set_index(i, i);
    }
  }
  return chunk.size();
}

std::string InvertedStoreIndex::ToString(duckdb::IndexLock&, bool) {
  return "inverted store index";
}

duckdb::IndexStorageInfo InvertedStoreIndex::MakeStorageInfo() const {
  duckdb::IndexStorageInfo info{name};
  info.allocator_infos.emplace_back();
  info.options[kTableIdOption] = duckdb::Value::UBIGINT(_table_id.id());
  info.options[kIndexIdOption] = duckdb::Value::UBIGINT(_index_id.id());
  return info;
}

void InvertedStoreIndex::CheckpointBarrier() const {
  auto* catalog = catalog::TryGetCatalog();
  if (!catalog) {
    THROW_SQL_ERROR(
      ERR_MSG("inverted index ", _index_id.id(),
              ": catalog is shut down, cannot verify index durability; "
              "refusing to checkpoint (WAL retained for replay)"));
  }
  auto snapshot = catalog->GetCatalogSnapshot();
  SDB_ASSERT(snapshot);
  auto inverted = snapshot->GetObject<catalog::InvertedIndex>(_index_id);
  if (!inverted) {
    return;
  }
  auto storage = inverted->GetData();
  if (!storage) {
    return;
  }
  SDB_ENSURE(!storage->IsOutOfSync(), "inverted index ", _index_id.id(),
             " is out of sync with its store table; refusing to checkpoint "
             "(WAL retained for replay; REINDEX to clear)");
  storage->CheckpointRefresh();
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext, const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  CheckpointBarrier();
  return MakeStorageInfo();
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToWAL(
  const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  return MakeStorageInfo();
}

std::string InvertedStoreIndex::GetConstraintViolationMessage(
  duckdb::VerifyExistenceType, idx_t, duckdb::DataChunk&) {
  return "inverted store index constraint violation";
}

void AttachInvertedStoreIndexCallbacks(duckdb::IndexType& type) {
  type.defer_implicit_bind = true;
  type.build_bind = [](duckdb::IndexBuildBindInput&)
    -> duckdb::unique_ptr<duckdb::IndexBuildBindData> { return nullptr; };
  type.build_global_init = [](duckdb::IndexBuildInitGlobalStateInput& input)
    -> duckdb::unique_ptr<duckdb::IndexBuildGlobalState> {
    auto state = duckdb::make_uniq<InvertedStoreBuildGlobalState>();
    state->index = duckdb::make_uniq<InvertedStoreIndex>(
      input.info.GetIndexName().GetIdentifierName(),
      duckdb::TableIOManager::Get(input.table.GetStorage()), input.storage_ids,
      input.expressions, input.table.GetStorage().db,
      OptionId(input.info.options, InvertedStoreIndex::kTableIdOption),
      OptionId(input.info.options, InvertedStoreIndex::kIndexIdOption));
    return std::move(state);
  };
  type.build_local_init = [](duckdb::IndexBuildInitLocalStateInput&)
    -> duckdb::unique_ptr<duckdb::IndexBuildLocalState> {
    return duckdb::make_uniq<InvertedStoreBuildLocalState>();
  };
  type.build_sink = [](duckdb::IndexBuildSinkInput&, duckdb::DataChunk&,
                       duckdb::DataChunk&) {};
  type.build_combine = [](duckdb::IndexBuildCombineInput&) {};
  type.build_finalize = [](duckdb::IndexBuildFinalizeInput& input)
    -> duckdb::unique_ptr<duckdb::BoundIndex> {
    auto& gstate = input.global_state.Cast<InvertedStoreBuildGlobalState>();
    return std::move(gstate.index);
  };
  type.create_instance = [](duckdb::CreateIndexInput& input)
    -> duckdb::unique_ptr<duckdb::BoundIndex> {
    return duckdb::make_uniq<InvertedStoreIndex>(
      input.name.GetIdentifierName(), input.table_io_manager, input.column_ids,
      input.unbound_expressions, input.db,
      OptionId(input.storage_info.options, InvertedStoreIndex::kTableIdOption),
      OptionId(input.storage_info.options, InvertedStoreIndex::kIndexIdOption));
  };
}

}  // namespace sdb::connector
