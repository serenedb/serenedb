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
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

ObjectId OptionId(const duckdb::case_insensitive_map_t<duckdb::Value>& options,
                  const char* key) {
  auto it = options.find(key);
  SDB_ENSURE(it != options.end(), ERROR_INTERNAL, "store index is missing the ",
             key, " option");
  return ObjectId{it->second.GetValue<uint64_t>()};
}

struct InvertedStoreBuildGlobalState final : duckdb::IndexBuildGlobalState {
  duckdb::unique_ptr<InvertedStoreIndex> index;
};

struct InvertedStoreBuildLocalState final : duckdb::IndexBuildLocalState {};

}  // namespace
namespace {

std::string RowIdKey(ObjectId table_id, duckdb::row_t row) {
  auto key = key_utils::PrepareColumnKey(table_id, catalog::Column::Id{0});
  primary_key::AppendSigned(key, static_cast<int64_t>(row));
  return key;
}

}  // namespace

// Accumulates the WAL-replay delta for one ApplyBufferedReplays pass, then
// applies it in FinishReplay as TWO sequential iresearch transactions:
//   T1 removes every touched rowid (clearing any posting iresearch already
//      made durable ahead of the last checkpoint), then
//   T2 inserts the buffered rows whose rowid was not subsequently deleted.
// The remove and insert MUST be in separate transactions: removing then
// re-inserting the same pk in one iresearch transaction suppresses the
// insert. `final_insert`/`touched` capture last-op-wins per rowid so the
// delta is correct under arbitrary insert/delete ordering AND under TRUNCATE
// (which frees a rowid and lets a later insert reuse it). Resolved from the
// GLOBAL catalog snapshot -- WAL replay / index bind run with no connection.
struct InvertedStoreIndex::ReplaySession {
  std::shared_ptr<search::InvertedIndexStorage> storage;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<const catalog::Table> table;
  // ApplyBufferedReplays only populates the index's referenced columns in the
  // replay chunk; the rest are empty. These are the positions/catalog-ids we
  // buffer and feed (computed on the first ReplayAppend).
  std::vector<duckdb::idx_t> ref_positions;
  std::vector<catalog::Column::Id> ref_col_ids;
  // Scratch connection for indexed-expression deserialization/evaluation.
  duckdb::Connection expr_conn;
  // Buffered insert rows: the referenced columns plus a trailing ROW_TYPE
  // rowid column, in WAL order. Lazily created on the first ReplayAppend.
  std::unique_ptr<duckdb::ColumnDataCollection> insert_buffer;
  // Running count of rows appended to insert_buffer (global buffer index).
  duckdb::idx_t buffered_rows = 0;
  // Store-WAL byte offset (within the current generation) up to which the
  // storage is already durable: replay ops at/below this are skipped. 0 when
  // the persisted cursor is from a different generation or absent (replay all).
  uint64_t durable_offset = 0;
  // rowid -> global buffer index of its LAST insert. A delete erases the
  // entry; a later insert overwrites it. After replay this holds exactly the
  // rowids that should be present, each mapped to the buffer row carrying its
  // final content -- correct even when TRUNCATE frees and reuses a rowid.
  absl::flat_hash_map<duckdb::row_t, duckdb::idx_t> final_insert;
  // Every rowid the delta inserted or deleted; T1 clears all of them.
  absl::flat_hash_set<duckdb::row_t> touched;

  ReplaySession(std::shared_ptr<search::InvertedIndexStorage> storage_in,
                std::shared_ptr<const catalog::Snapshot> snapshot_in,
                std::shared_ptr<const catalog::InvertedIndex> index_in,
                std::shared_ptr<const catalog::Table> table_in,
                duckdb::DatabaseInstance& db)
    : storage{std::move(storage_in)},
      snapshot{std::move(snapshot_in)},
      index{std::move(index_in)},
      table{std::move(table_in)},
      expr_conn{db} {
    expr_conn.BeginTransaction();
  }
};

InvertedStoreIndex::InvertedStoreIndex(
  const std::string& name, duckdb::TableIOManager& io,
  const duckdb::vector<duckdb::column_t>& column_ids,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs,
  duckdb::AttachedDatabase& db, ObjectId table_id, ObjectId index_id)
  : BoundIndex(name, kTypeName, duckdb::IndexConstraintType::NONE, column_ids,
               io, exprs, db),
    _table_id{table_id},
    _index_id{index_id} {}

InvertedStoreIndex::~InvertedStoreIndex() = default;

InvertedStoreIndex::ReplaySession& InvertedStoreIndex::EnsureReplaySession() {
  if (_replay) {
    return *_replay;
  }
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  SDB_ENSURE(snapshot, ERROR_INTERNAL,
             "inverted index replay: no catalog snapshot");
  auto inverted = snapshot->GetObject<catalog::InvertedIndex>(_index_id);
  auto table = snapshot->GetObject<catalog::Table>(_table_id);
  SDB_ENSURE(inverted && table, ERROR_INTERNAL,
             "inverted index replay: catalog objects for ", _index_id.id(),
             " missing");
  auto storage = inverted->GetData();
  SDB_ENSURE(storage, ERROR_INTERNAL, "inverted index replay: storage ",
             _index_id.id(), " missing");
  // Resolve the durable cursor for this generation: the persisted (gen,offset)
  // is comparable to the replay offsets only within the same WAL generation.
  // A different/absent generation => replay the whole post-checkpoint delta.
  const uint64_t cursor = storage->GetRecoveryWalCursor();
  uint64_t durable_offset = 0;
  auto& block_manager = db.GetStorageManager().GetBlockManager();
  if (search::WalCursorGeneration(cursor) ==
      block_manager.GetCheckpointIteration()) {
    durable_offset = search::WalCursorOffset(cursor);
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
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto& session = EnsureReplaySession();
  // Skip operations the storage already made durable before the crash: this
  // range's WAL offset is at/below the persisted cursor. Bounds recovery to
  // the un-durable tail. (The boundary overlap left by the conservative cursor
  // is absorbed idempotently by FinishReplay's delete-then-insert.)
  if (_replay_commit_offset != 0 &&
      _replay_commit_offset <= session.durable_offset) {
    return;
  }
  if (!session.insert_buffer) {
    // The replay chunk has one slot per non-generated-PK column but only the
    // index's referenced columns are populated. Buffer exactly those.
    auto chunk_column_ids = TableChunkColumnIds(*session.table);
    absl::flat_hash_set<catalog::Column::Id> referenced;
    for (auto id : session.index->GetReferencedColumnIds()) {
      referenced.insert(id);
    }
    duckdb::vector<duckdb::LogicalType> types;
    for (duckdb::idx_t pos = 0;
         pos < chunk.ColumnCount() && pos < chunk_column_ids.size(); ++pos) {
      if (!referenced.contains(chunk_column_ids[pos])) {
        continue;
      }
      session.ref_positions.push_back(pos);
      session.ref_col_ids.push_back(chunk_column_ids[pos]);
      types.push_back(chunk.data[pos].GetType());
    }
    types.push_back(duckdb::LogicalType::ROW_TYPE);
    session.insert_buffer = std::make_unique<duckdb::ColumnDataCollection>(
      duckdb::Allocator::DefaultAllocator(), std::move(types));
  }
  // Record last-insert-wins per rowid, then stash the chunk + rowids; T2 in
  // FinishReplay feeds the surviving rows. (ColumnDataCollection copies, so
  // the source chunk may be reused after Append.)
  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    session.final_insert[row] = session.buffered_rows + i;
    session.touched.insert(row);
  }

  // Copy the referenced columns (+ rowid) into an owned chunk; the source
  // vectors are sliced/dictionary and reused on the next replay range, so
  // ColumnDataCollection::Append needs materialized buffers.
  duckdb::DataChunk buffered;
  buffered.Initialize(duckdb::Allocator::DefaultAllocator(),
                      session.insert_buffer->Types());
  for (duckdb::idx_t k = 0; k < session.ref_positions.size(); ++k) {
    duckdb::VectorOperations::Copy(chunk.data[session.ref_positions[k]],
                                   buffered.data[k], count, 0, 0);
  }
  duckdb::VectorOperations::Copy(
    row_ids, buffered.data[session.ref_positions.size()], count, 0, 0);
  buffered.SetCardinality(count);
  session.insert_buffer->Append(buffered);
  session.buffered_rows += count;
}

void InvertedStoreIndex::ReplayDelete(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto& session = EnsureReplaySession();
  // Skip operations already durable before the crash (see ReplayAppend).
  if (_replay_commit_offset != 0 &&
      _replay_commit_offset <= session.durable_offset) {
    return;
  }
  duckdb::UnifiedVectorFormat row_fmt;
  row_ids.ToUnifiedFormat(count, row_fmt);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
      row_fmt)[row_fmt.sel->get_index(i)];
    session.final_insert.erase(row);
    session.touched.insert(row);
  }
}

void InvertedStoreIndex::FinishReplay() {
  if (!_replay) {
    return;
  }
  auto& session = *_replay;
  const auto rowid_col =
    session.insert_buffer ? session.insert_buffer->ColumnCount() - 1 : 0;

  // T1: remove every touched rowid (inserted and/or deleted in the delta),
  // clearing any posting iresearch already made durable past the checkpoint.
  {
    auto t1 = session.storage->GetTransaction();
    DuckDBSearchSinkDeleteWriter del{t1};
    del.Init(1, duckdb::DataChunk{});
    std::string key;
    for (auto row : session.touched) {
      key.clear();
      primary_key::AppendSigned(key, static_cast<int64_t>(row));
      del.DeleteRow(key);
    }
    del.Finish();
    const auto ordinal = search::TickDomain::Instance().Advance(1);
    SDB_ENSURE(t1.Commit(ordinal), ERROR_INTERNAL,
               "inverted index replay: delete commit failed for index ",
               _index_id.id());
  }

  // T2: insert each rowid's final content. A buffer row is emitted only if it
  // is that rowid's LAST insert (final_insert) -- this drops rows superseded
  // by a later delete or, after TRUNCATE, by a later insert reusing the rowid.
  if (session.insert_buffer && !session.final_insert.empty()) {
    auto t2 = session.storage->GetTransaction();
    auto& inverted = *session.index;
    DuckDBSearchSinkInsertWriter ins{
      t2, MakeTokenizerProvider(session.snapshot, inverted),
      inverted.GetColumnIds(), MakeEntryInfoProvider(inverted),
      MakeIndexedExpressions(inverted, *session.expr_conn.context)};
    const auto& columns = session.table->Columns();

    duckdb::ColumnDataScanState scan;
    session.insert_buffer->InitializeScan(scan);
    duckdb::DataChunk chunk;
    session.insert_buffer->InitializeScanChunk(chunk);
    duckdb::idx_t global_index = 0;
    while (session.insert_buffer->Scan(scan, chunk)) {
      auto count = chunk.size();
      const auto base = global_index;
      global_index += count;
      duckdb::UnifiedVectorFormat row_fmt;
      chunk.data[rowid_col].ToUnifiedFormat(count, row_fmt);
      // Keep only rows that are their rowid's final insert.
      duckdb::SelectionVector sel(count);
      duckdb::idx_t kept = 0;
      std::vector<std::string> keys;
      std::vector<std::string_view> key_views;
      keys.reserve(count);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        auto row = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(
          row_fmt)[row_fmt.sel->get_index(i)];
        auto it = session.final_insert.find(row);
        if (it == session.final_insert.end() || it->second != base + i) {
          continue;
        }
        sel.set_index(kept++, i);
        keys.push_back(RowIdKey(_table_id, row));
      }
      if (kept == 0) {
        continue;
      }
      if (kept != count) {
        chunk.Slice(sel, kept);
        count = kept;
      }
      key_views.reserve(keys.size());
      for (const auto& k : keys) {
        key_views.emplace_back(k);
      }
      ins.Init(count, chunk);
      // Buffer columns are exactly the referenced columns, in ref_col_ids
      // order (rowid is the trailing column).
      for (duckdb::idx_t pos = 0; pos < rowid_col; ++pos) {
        auto col_id = session.ref_col_ids[pos];
        auto it = std::ranges::find_if(
          columns, [&](const auto& c) { return c.GetId() == col_id; });
        if (it == columns.end()) {
          continue;
        }
        const ColumnDescriptor desc{col_id, it->type};
        ins.SwitchColumn(desc, chunk.data[pos], key_views, count);
      }
      if (auto indexed_exprs = ins.IndexedExpressions();
          !indexed_exprs.empty()) {
        EvaluateAndWriteIndexedExpressions(
          ins, indexed_exprs, chunk, _table_id, session.ref_col_ids,
          *session.expr_conn.context, count, keys);
      }
      ins.Finish();
    }
    const auto ordinal = search::TickDomain::Instance().Advance(1);
    SDB_ENSURE(t2.Commit(ordinal), ERROR_INTERNAL,
               "inverted index replay: insert commit failed for index ",
               _index_id.id());
  }

  session.expr_conn.Rollback();
  _replay.reset();
}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    // No committing connection => duckdb is replaying buffered WAL inserts
    // into a freshly-bound index. Feed the delta into the storage (idempotent
    // delete-then-insert); FinishReplay commits the batch.
    ReplayAppend(chunk, row_ids);
    return {};
  }
  auto snapshot = conn->EnsureCatalogSnapshot();
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
  // Commit-time appends run after the duckdb transaction context detached;
  // indexed-expression deserialization and evaluation ride a scratch
  // transaction that must stay alive until the writer finishes.
  duckdb::Connection expr_conn(*conn.GetClientContext().db);
  expr_conn.BeginTransaction();
  absl::Cleanup rollback_expr_conn = [&] { expr_conn.Rollback(); };
  auto writer = CreateInvertedIndexWriter<DuckDBWriteKind::Insert>(
    _table_id, _index_id, conn, expr_conn.context.get());
  if (!writer) {
    return {};
  }
  auto snapshot = conn.EnsureCatalogSnapshot();
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
    keys[i] = key_utils::PrepareColumnKey(_table_id, catalog::Column::Id{0});
    primary_key::AppendSigned(keys[i], static_cast<int64_t>(row));
    key_views[i] = keys[i];
  }

  writer->Init(count, chunk);
  for (duckdb::idx_t pos = 0;
       pos < chunk.ColumnCount() && pos < chunk_column_ids.size(); ++pos) {
    auto col_id = chunk_column_ids[pos];
    auto it = std::ranges::find_if(
      table->Columns(), [&](const auto& c) { return c.GetId() == col_id; });
    if (it == table->Columns().end()) {
      continue;
    }
    const ColumnDescriptor desc{col_id, it->type};
    writer->SwitchColumn(desc, chunk.data[pos], key_views, count);
  }
  if (auto indexed_exprs = writer->IndexedExpressions();
      !indexed_exprs.empty()) {
    EvaluateAndWriteIndexedExpressions(*writer, indexed_exprs, chunk, _table_id,
                                       chunk_column_ids, *expr_conn.context,
                                       count, keys);
  }
  writer->Finish();
  // Pin this staged batch into the iresearch flush context before the store
  // WAL bytes are written, so a concurrent refresh's durable cursor can't skip
  // it on recovery (see Transaction::RegisterSearchFlush).
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
    // Buffered WAL-replay delete into a freshly-bound index.
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
  writer->Init(count, chunk);
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

duckdb::IndexStorageInfo InvertedStoreIndex::MakeStorageInfo() const {
  // Postings live in the iresearch storage; one empty allocator entry keeps
  // the info IsValid() for WAL/checkpoint round-trips.
  duckdb::IndexStorageInfo info{name};
  info.allocator_infos.emplace_back();
  info.options[kTableIdOption] = duckdb::Value::UBIGINT(_table_id.id());
  info.options[kIndexIdOption] = duckdb::Value::UBIGINT(_index_id.id());
  return info;
}

void InvertedStoreIndex::CheckpointBarrier() const {
  // The checkpoint is about to truncate the store WAL, so the iresearch storage
  // must be durable up to this point first -- otherwise a crash drops
  // [index-durable, checkpoint) from the index (the rows survive in the
  // checkpointed table but are gone from the index, with no WAL to replay).
  // If a prior commit's index leg failed (out-of-sync), refuse: throw so the
  // checkpoint is skipped/aborted and the WAL is retained -- a restart replays
  // the delta, an explicit REINDEX clears it. Resolved from the GLOBAL snapshot
  // (checkpoint runs with no connection).
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  if (!snapshot) {
    return;
  }
  // The index's storage is bound at CREATE INDEX before this point; a null
  // lookup means a checkpoint racing the CREATE INDEX that defines us, so both
  // the catalog existence gate and the storage handle non-asserting
  // early-return.
  auto inverted = snapshot->GetObject<catalog::InvertedIndex>(_index_id);
  if (!inverted) {
    return;
  }
  auto storage = inverted->GetData();
  if (!storage) {
    return;
  }
  SDB_ENSURE(!storage->IsOutOfSync(), ERROR_INTERNAL, "inverted index ",
             _index_id.id(),
             " is out of sync with its store table; refusing to checkpoint "
             "(WAL retained for replay; REINDEX to clear)");
  storage->Refresh();
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext, const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  CheckpointBarrier();
  return MakeStorageInfo();
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToWAL(
  const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  // WAL-resident CREATE INDEX: the build already produced durable iresearch
  // segments, and the index isn't in the global snapshot yet -- no barrier.
  return MakeStorageInfo();
}

std::string InvertedStoreIndex::GetConstraintViolationMessage(
  duckdb::VerifyExistenceType, idx_t, duckdb::DataChunk&) {
  return "inverted store index constraint violation";
}

void AttachInvertedStoreIndexCallbacks(duckdb::IndexType& type) {
  // Never bind this index implicitly: its data lives in iresearch and its bind
  // needs the serenedb catalog + index storage, which load after the store DB's
  // WAL replay. It is bound only by InitInvertedIndexes (an explicit by-name
  // bind) once those are ready. This keeps an ALTER-driven rebuild during WAL
  // replay from binding it too early (queries use IRESEARCH_SCAN, not this
  // index).
  type.defer_implicit_bind = true;
  type.build_bind = [](duckdb::IndexBuildBindInput&)
    -> duckdb::unique_ptr<duckdb::IndexBuildBindData> { return nullptr; };
  type.build_global_init = [](duckdb::IndexBuildInitGlobalStateInput& input)
    -> duckdb::unique_ptr<duckdb::IndexBuildGlobalState> {
    auto state = duckdb::make_uniq<InvertedStoreBuildGlobalState>();
    state->index = duckdb::make_uniq<InvertedStoreIndex>(
      input.info.index_name,
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
  // The initial build is fed by the facade CREATE INDEX operator (which
  // scans with rowid keys after the catalog objects exist); the store-side
  // pipeline only constructs the BoundIndex for DML maintenance.
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
      input.name, input.table_io_manager, input.column_ids,
      input.unbound_expressions, input.db,
      OptionId(input.storage_info.options, InvertedStoreIndex::kTableIdOption),
      OptionId(input.storage_info.options, InvertedStoreIndex::kIndexIdOption));
  };
}

}  // namespace sdb::connector
