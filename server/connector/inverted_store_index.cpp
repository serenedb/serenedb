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
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/append_state.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <deque>
#include <iterator>
#include <mutex>
#include <string>
#include <thread>
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

// Replay fan-out: the replay thread never tokenizes -- it scatters each
// batch's rows by rowid across N partitions (one rowid always lands on one
// partition, so per-key op order -- incl. rowid reuse and truncate -- is the
// dispatch order) and partitions drain on the duckdb task scheduler: at most
// one drain task per partition at a time, so each writer transaction stays
// single-threaded, the writer's supported model. Partitions never commit
// mid-replay (segment flushes bound memory; tickless commits are unsafe on
// pooled segments); FinishReplay drains the executor and commits each
// transaction on its own ascending TickDomain range.
struct InvertedStoreIndex::ReplaySession {
  struct Batch {
    bool is_delete = false;
    duckdb::DataChunk chunk;
    std::vector<int64_t> rowids;
  };

  struct Partition {
    explicit Partition(ReplaySession& session_in, duckdb::DatabaseInstance& db)
      : session{&session_in},
        expr_conn{db},
        trx{session_in.storage->GetTransaction()} {
      trx.SetFieldOptions(session_in.index);
    }

    // The un-committed transaction aborts in its own destructor.
    ~Partition() { Teardown(); }

    void Drain() {
      for (;;) {
        std::unique_ptr<Batch> batch;
        {
          std::lock_guard lock{mu};
          if (queue.empty()) {
            running = false;
            return;
          }
          batch = std::move(queue.front());
          queue.pop_front();
        }
        if (batch->is_delete) {
          Delete(*batch);
        } else {
          Insert(*batch);
        }
      }
    }

    void Insert(Batch& batch) {
      auto& s = *session;
      const auto count = batch.rowids.size();
      if (!insert_writer) {
        expr_conn.BeginTransaction();
        expr_txn_open = true;
        insert_writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
          trx, MakeTokenizerProvider(s.snapshot, *s.index),
          s.index->GetColumns(), MakeEntryInfoProvider(*s.index),
          MakeIndexedExpressions(*s.index, *expr_conn.context),
          PkPolicy{.index_term = s.index->GetOptions().pk_term,
                   .column = s.index->GetOptions().pk_column});
      }
      std::vector<std::string> keys(count);
      std::vector<std::string_view> key_views(count);
      for (size_t i = 0; i < count; ++i) {
        keys[i] = RowIdKey(batch.rowids[i]);
        key_views[i] = keys[i];
      }
      duckdb::Vector rowid_vec{
        duckdb::LogicalType::ROW_TYPE,
        reinterpret_cast<duckdb::data_ptr_t>(batch.rowids.data()),
        count};

      auto& ins = *insert_writer;
      ins.Init(count, PkChunk{.keys = key_views, .column = &rowid_vec});
      for (duckdb::idx_t k = 0; k < batch.chunk.ColumnCount(); ++k) {
        const ColumnDescriptor desc{s.ref_col_ids[k], s.ref_types[k]};
        ins.SwitchColumn(desc, batch.chunk.data[k], count);
      }
      if (auto indexed_exprs = ins.IndexedExpressions();
          !indexed_exprs.empty()) {
        EvaluateAndWriteIndexedExpressions(
          ins, indexed_exprs, batch.chunk, s.table->GetId(), s.ref_col_ids,
          *expr_conn.context, count);
      }
      ins.Finish();
      trx.AdvanceQueries(1);
      ++ops;
    }

    void Delete(Batch& batch) {
      if (!delete_writer) {
        delete_writer = std::make_unique<DuckDBSearchSinkDeleteWriter>(trx);
      }
      delete_writer->Init(batch.rowids.size(), {});
      std::string key;
      for (const auto rowid : batch.rowids) {
        key.clear();
        primary_key::AppendSigned(key, rowid);
        delete_writer->DeleteRow(key);
      }
      delete_writer->Finish();
      ++ops;
    }

    void Teardown() {
      if (expr_txn_open) {
        expr_conn.Rollback();
        expr_txn_open = false;
      }
    }

    ReplaySession* session;
    duckdb::Connection expr_conn;
    irs::IndexWriter::Transaction trx;
    std::unique_ptr<DuckDBSearchSinkInsertWriter> insert_writer;
    std::unique_ptr<DuckDBSearchSinkDeleteWriter> delete_writer;
    bool expr_txn_open = false;
    uint64_t ops = 0;
    uint64_t last_tick = 0;

    std::mutex mu;
    std::deque<std::unique_ptr<Batch>> queue;
    bool running = false;
  };

  struct DrainTask final : duckdb::BaseExecutorTask {
    DrainTask(duckdb::TaskExecutor& executor_in, Partition& partition_in)
      : BaseExecutorTask{executor_in}, partition{partition_in} {}

    void ExecuteTask() override { partition.Drain(); }

    std::string TaskType() const override { return "InvertedReplayDrain"; }

    Partition& partition;
  };

  static constexpr size_t kMaxQueuedBatches = 4;

  std::shared_ptr<search::InvertedIndexStorage> storage;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<const catalog::Table> table;
  std::vector<duckdb::idx_t> ref_positions;
  std::vector<catalog::Column::Id> ref_col_ids;
  duckdb::vector<duckdb::LogicalType> ref_types;
  duckdb::DatabaseInstance& instance;
  duckdb::TaskExecutor executor;
  std::vector<std::unique_ptr<Partition>> partitions;
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
      instance{db},
      executor{duckdb::TaskScheduler::GetScheduler(db)} {
    const auto chunk_column_ids = TableChunkColumnIds(*table);
    for (duckdb::idx_t pos = 0; pos < chunk_column_ids.size(); ++pos) {
      if (!index->ReferencesColumn(chunk_column_ids[pos])) {
        continue;
      }
      const auto* col = table->ColumnById(chunk_column_ids[pos]);
      if (!col) {
        continue;
      }
      ref_positions.push_back(pos);
      ref_col_ids.push_back(chunk_column_ids[pos]);
      ref_types.push_back(col->type);
    }
    partitions.resize(std::clamp<duckdb::idx_t>(
      duckdb::TaskScheduler::GetScheduler(db).NumberOfThreads(), 1, 8));
  }

  // Any teardown path (attach failure destroying the catalog under a live
  // session) must wait out in-flight drain tasks before the partitions die.
  ~ReplaySession() {
    try {
      executor.WorkOnTasks();
    } catch (...) {
    }
  }

  // Scatter one replayed batch: per-partition rowid lists (and for inserts a
  // per-partition owned copy of the referenced rows).
  void Dispatch(bool is_delete, duckdb::DataChunk& chunk,
                duckdb::Vector& row_ids, duckdb::idx_t count) {
    if (executor.HasError()) {
      return;  // FinishReplay rethrows
    }
    duckdb::UnifiedVectorFormat row_fmt;
    row_ids.ToUnifiedFormat(count, row_fmt);
    const auto* rows =
      duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(row_fmt);

    const auto n = partitions.size();
    std::vector<std::vector<int64_t>> rowids(n);
    std::vector<duckdb::SelectionVector> sels;
    std::vector<duckdb::idx_t> counts(n, 0);
    if (!is_delete) {
      sels.reserve(n);
      for (size_t w = 0; w < n; ++w) {
        sels.emplace_back(count);
      }
    }
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const auto row =
        static_cast<int64_t>(rows[row_fmt.sel->get_index(i)]);
      const auto w = static_cast<uint64_t>(row) % n;
      rowids[w].push_back(row);
      if (!is_delete) {
        sels[w].set_index(counts[w]++, i);
      }
    }
    for (size_t w = 0; w < n; ++w) {
      if (rowids[w].empty()) {
        continue;
      }
      auto batch = std::make_unique<Batch>();
      batch->is_delete = is_delete;
      batch->rowids = std::move(rowids[w]);
      if (!is_delete) {
        // Only the referenced columns: replay chunks leave the rest
        // unmaterialized, and the compact layout maps slot k exactly to
        // ref_col_ids[k] for the writer and the expression evaluator.
        batch->chunk.Initialize(duckdb::Allocator::DefaultAllocator(),
                                ref_types, counts[w]);
        for (size_t k = 0; k < ref_positions.size(); ++k) {
          duckdb::VectorOperations::Copy(chunk.data[ref_positions[k]],
                                         batch->chunk.data[k], sels[w],
                                         counts[w], 0, 0);
        }
        batch->chunk.SetCardinality(counts[w]);
      }
      auto& slot = partitions[w];
      if (!slot) {
        slot = std::make_unique<Partition>(*this, instance);
      }
      Push(*slot, std::move(batch));
    }
  }

  // Bounded queue; instead of sleeping when full, help run already-scheduled
  // drain tasks (a full partition's drainer may be sitting in the scheduler
  // queue behind others -- helping guarantees progress even on a busy pool).
  void Push(Partition& partition, std::unique_ptr<Batch> batch) {
    for (;;) {
      if (executor.HasError()) {
        return;  // drainers bailed out; FinishReplay rethrows
      }
      {
        std::lock_guard lock{partition.mu};
        if (partition.queue.size() < kMaxQueuedBatches) {
          partition.queue.push_back(std::move(batch));
          if (partition.running) {
            return;
          }
          partition.running = true;
          break;
        }
      }
      duckdb::shared_ptr<duckdb::Task> task;
      if (executor.GetTask(task)) {
        task->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
        task.reset();
      } else {
        std::this_thread::yield();
      }
    }
    executor.ScheduleTask(duckdb::make_uniq<DrainTask>(executor, partition));
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
  session.Dispatch(/*is_delete=*/false, chunk, row_ids, count);
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
  session.Dispatch(/*is_delete=*/true, chunk, row_ids, count);
}

void InvertedStoreIndex::FinishReplay() {
  if (!_replay) {
    return;
  }
  auto& session = *_replay;
  const absl::Cleanup reset = [&] { _replay.reset(); };
  session.executor.WorkOnTasks();
  uint64_t total_ops = 0;
  for (auto& partition : session.partitions) {
    if (partition) {
      total_ops += partition->ops;
    }
  }
  if (total_ops == 0) {
    return;
  }
  // One ascending tick range per partition; the cursor records the final tick
  // before any commit so a mid-commit crash re-replays rather than skips.
  auto& tick_domain = search::TickDomain::Instance();
  uint64_t final_tick = 0;
  for (auto& partition : session.partitions) {
    if (partition && partition->ops != 0) {
      partition->last_tick = tick_domain.Advance(partition->ops);
      final_tick = partition->last_tick;
    }
  }
  auto& sm = db.GetStorageManager();
  session.storage->RecordFlushCursor(
    final_tick, search::WalCursor{sm.GetBlockManager().GetCheckpointIteration(),
                                  sm.GetWALSize()});
  for (auto& partition : session.partitions) {
    if (!partition || partition->ops == 0) {
      continue;
    }
    partition->insert_writer.reset();
    partition->delete_writer.reset();
    SDB_ENSURE(partition->trx.Commit(partition->last_tick),
               "inverted index replay: commit failed for index ",
               _index_id.id());
  }
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
