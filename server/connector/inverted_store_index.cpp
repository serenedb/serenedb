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

#include <absl/cleanup/cleanup.h>

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/primary_key.hpp"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

std::string RowIdKey(duckdb::row_t row) {
  std::string key;
  primary_key::AppendSigned(key, static_cast<int64_t>(row));
  return key;
}

}  // namespace

struct InvertedStoreIndex::ReplaySession {
  std::shared_ptr<search::InvertedIndexStorage> storage;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<const catalog::Table> table;
  std::vector<duckdb::idx_t> ref_positions;
  std::vector<catalog::Column::Id> ref_col_ids;
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

// The store-WAL byte offset of the entry currently replaying (stamped by the
// replayer per WAL entry). Operations strictly below the storage's durable
// cursor are already in the segments and are skipped; the op exactly at the
// cursor is the first un-durable one and is streamed. 0 = unknown, don't skip.
duckdb::idx_t InvertedStoreIndex::ReplayCommitOffset() const {
  return duckdb::DuckTransactionManager::Get(db).GetReplayCommitOffset();
}

void InvertedStoreIndex::ReplayAppend(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto& session = EnsureReplaySession();
  const auto commit_offset = ReplayCommitOffset();
  if (commit_offset != 0 && commit_offset < session.durable_offset) {
    return;
  }
  auto& inverted = *session.index;
  if (!session.insert_writer) {
    auto chunk_column_ids = TableChunkColumnIds(*session.table);
    for (duckdb::idx_t pos = 0;
         pos < chunk.ColumnCount() && pos < chunk_column_ids.size(); ++pos) {
      if (!inverted.ReferencesColumn(chunk_column_ids[pos])) {
        continue;
      }
      session.ref_positions.push_back(pos);
      session.ref_col_ids.push_back(chunk_column_ids[pos]);
    }
    session.insert_writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
      session.trx, MakeTokenizerProvider(session.snapshot, inverted),
      inverted.GetColumns(), MakeEntryInfoProvider(inverted),
      MakeIndexedExpressions(inverted, *session.expr_conn.context),
      PkPolicy{.index_term = inverted.GetOptions().pk_term,
               .column = inverted.GetOptions().pk_column});
  }

  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  std::vector<std::string> keys(count);
  std::vector<std::string_view> key_views(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    keys[i] = RowIdKey(row);
    key_views[i] = keys[i];
  }

  auto& ins = *session.insert_writer;
  ins.Init(count, PkChunk{.keys = key_views, .column = &row_ids});
  for (duckdb::idx_t k = 0; k < session.ref_positions.size(); ++k) {
    auto col_id = session.ref_col_ids[k];
    const auto* col = session.table->ColumnById(col_id);
    if (!col) {
      continue;
    }
    const ColumnDescriptor desc{col_id, col->type};
    ins.SwitchColumn(desc, chunk.data[session.ref_positions[k]], count);
  }
  if (auto indexed_exprs = ins.IndexedExpressions(); !indexed_exprs.empty()) {
    EvaluateAndWriteIndexedExpressions(ins, indexed_exprs, chunk, _table_id,
                                       session.ref_col_ids,
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
  const auto commit_offset = ReplayCommitOffset();
  if (commit_offset != 0 && commit_offset < session.durable_offset) {
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
  const auto count = chunk.size();
  if (count == 0) {
    return {};
  }
  duckdb::Connection expr_conn(*conn.GetClientContext().db);
  expr_conn.BeginTransaction();
  absl::Cleanup rollback_expr_conn = [&] { expr_conn.Rollback(); };
  auto writer = CreateInvertedIndexWriter<DuckDBWriteKind::Insert>(
    _table_id, _index_id, conn, expr_conn.context.get());
  if (!writer) {
    return {};
  }
  auto snapshot = conn.CatalogSnapshot();
  auto table = snapshot->GetObject<catalog::Table>(_table_id);
  if (!table) {
    return {};
  }

  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  std::vector<std::string> keys(count);
  std::vector<std::string_view> key_views(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    keys[i] = RowIdKey(row);
    key_views[i] = keys[i];
  }

  writer->Init(count, PkChunk{.keys = key_views, .column = &row_ids});
  for (duckdb::idx_t pos = 0;
       pos < chunk.ColumnCount() && pos < chunk_column_ids.size(); ++pos) {
    auto col_id = chunk_column_ids[pos];
    const auto* col = table->ColumnById(col_id);
    if (!col) {
      continue;
    }
    const ColumnDescriptor desc{col_id, col->type};
    writer->SwitchColumn(desc, chunk.data[pos], count);
  }
  if (auto indexed_exprs = writer->IndexedExpressions();
      !indexed_exprs.empty()) {
    EvaluateAndWriteIndexedExpressions(*writer, indexed_exprs, chunk, _table_id,
                                       chunk_column_ids, *expr_conn.context,
                                       count);
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
  auto writer = CreateInvertedIndexWriter<DuckDBWriteKind::Delete>(
    _table_id, _index_id, *conn);
  if (!writer) {
    return;
  }
  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  writer->Init(count, {});
  std::string key;
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    key.clear();
    primary_key::AppendSigned(key, static_cast<int64_t>(row));
    writer->DeleteRow(key);
  }
  writer->Finish();
  conn->RegisterSearchFlush();
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

void InvertedStoreIndex::CheckpointBarrier() {
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

std::string InvertedStoreIndex::GetConstraintViolationMessage(
  duckdb::VerifyExistenceType, idx_t, duckdb::DataChunk&) {
  return "inverted store index constraint violation";
}

duckdb::unique_ptr<InvertedStoreIndex> MakeInjectedInvertedIndex(
  duckdb::DataTable& storage, const catalog::Table& table,
  const catalog::InvertedIndex& inverted) {
  duckdb::vector<duckdb::column_t> column_ids;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> exprs;
  const auto& defs = storage.Columns();
  // Indexed columns plus indexed-expression dependencies, mirroring the
  // referenced set so duckdb's column tracking (DROP COLUMN dependency
  // checks) sees exactly what the index reads.
  for (const auto col_id : inverted.GetReferencedColumns()) {
    const auto name = catalog::StoreColumnName(col_id);
    for (duckdb::idx_t i = 0; i < defs.size(); ++i) {
      if (defs[i].Name().GetIdentifierName() == name) {
        exprs.push_back(duckdb::make_uniq<duckdb::BoundReferenceExpression>(
          defs[i].Type(), i));
        column_ids.push_back(i);
        break;
      }
    }
  }
  return duckdb::make_uniq<InvertedStoreIndex>(
    catalog::StoreIndexName(inverted.GetId()),
    duckdb::TableIOManager::Get(storage), column_ids, exprs, storage.db,
    table.GetId(), inverted.GetId());
}

void InjectExternalIndexes(duckdb::DataTable& storage) {
  if (storage.db.GetName().GetIdentifierName() != catalog::kStoreDatabaseName) {
    return;
  }
  auto* catalog = catalog::TryGetCatalog();
  if (!catalog) {
    return;
  }
  const auto table_id = catalog::ParseStoreId(
    't', storage.GetDataTableInfo()->GetTableName().GetIdentifierName());
  if (!table_id) {
    return;
  }
  auto snapshot = catalog->GetCatalogSnapshot();
  if (!snapshot) {
    return;
  }
  auto table = snapshot->GetObject<catalog::Table>(*table_id);
  if (!table) {
    // Constructive DDL creates the physical table before the catalog append,
    // so a fresh CREATE TABLE lands here with no definitions yet.
    return;
  }
  auto& list = storage.GetDataTableInfo()->GetIndexes();
  for (const auto& index : snapshot->GetIndexesByRelation(*table_id)) {
    if (!index || index->GetType() != catalog::ObjectType::InvertedIndex ||
        index->Tombstoned()) {
      continue;
    }
    const auto& inverted =
      basics::downCast<const catalog::InvertedIndex>(*index);
    list.AddIndex(MakeInjectedInvertedIndex(storage, *table, inverted));
  }
}

}  // namespace sdb::connector
