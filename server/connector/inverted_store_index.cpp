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
#include <duckdb/main/config.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <atomic>
#include <numeric>
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
#include "query/config_variable_names.h"
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

// Replay fan-out: the WAL reader dispatches one task per replayed chunk,
// referencing the chunk shared by duckdb's replayer -- nothing is copied.
// Tasks tokenize in parallel, each into its own writer transaction; a cheap
// per-index retirement step commits finished tasks strictly in dispatch
// order, so tick order == WAL order (per-rowid correctness incl. rowid
// reuse), per-segment ticks stay monotone, and the pending state is a WAL
// prefix at every moment -- a background refresh mid-feed durably banks the
// finished prefix with a matching cursor point.
struct InvertedStoreIndex::ReplaySession : public query::ParallelInvertedFeed {
  // One scan for a ROW_GROUP_DATA entry, shared by every index of the table:
  // the first range job to run scans (and, with siblings attached, stores the
  // chunks); the rest consume.
  struct RangeShare {
    std::once_flag once;
    std::vector<std::pair<duckdb::shared_ptr<duckdb::DataChunk>, int64_t>>
      chunks;
  };

  struct Job {
    Job(ReplaySession& session_in, bool is_delete_in,
         duckdb::shared_ptr<duckdb::DataChunk> chunk_in,
         std::vector<int64_t> rowids_in, uint64_t wal_offset_in)
      : session{session_in},
        is_delete{is_delete_in},
        chunk{std::move(chunk_in)},
        rowids{std::move(rowids_in)},
        wal_offset{wal_offset_in},
        trx{session_in.storage->GetTransaction()} {
      trx.SetFieldOptions(session_in.index);
    }

    Job(ReplaySession& session_in, duckdb::DataTable& table,
        duckdb::row_t row_start_in, duckdb::idx_t count_in,
        duckdb::shared_ptr<RangeShare> share_in, uint64_t wal_offset_in)
      : session{session_in},
        is_delete{false},
        wal_offset{wal_offset_in},
        trx{session_in.storage->GetTransaction()},
        range_table{&table},
        row_start{row_start_in},
        range_count{count_in},
        share{std::move(share_in)} {
      trx.SetFieldOptions(session_in.index);
    }

    ReplaySession& session;
    const bool is_delete;
    duckdb::shared_ptr<duckdb::DataChunk> chunk;
    std::vector<int64_t> rowids;
    const uint64_t wal_offset;
    irs::IndexWriter::Transaction trx;
    duckdb::DataTable* range_table = nullptr;
    duckdb::row_t row_start = 0;
    duckdb::idx_t range_count = 0;
    duckdb::shared_ptr<RangeShare> share;
    uint64_t query_count = 1;
    std::atomic<bool> done{false};
    // Recovery jobs self-retire on completion (WAL-ordered tick commit); live
    // jobs are committed together at CommitSearch, so they only mark done.
    bool self_retire = true;
  };

  struct RunTask final : duckdb::BaseExecutorTask {
    RunTask(duckdb::TaskExecutor& executor_in, Job& job_in)
      : BaseExecutorTask{executor_in}, job{job_in} {}

    void ExecuteTask() override { job.session.Run(job); }

    std::string TaskType() const override { return "InvertedReplayChunk"; }

    Job& job;
  };

  // Everything a task needs beyond its transaction: expression binding is
  // per-connection and writer construction is not per-chunk cheap, so tasks
  // check bundles out of a pool that never exceeds the prefetch depth.
  struct Bundle {
    Bundle(ReplaySession& session, irs::IndexWriter::Transaction& trx) {
      // Live feeds tokenize as pure index work: expressions are pre-computed
      // on the committing thread (see DispatchLive) and fed via SwitchExpression,
      // so the workers open no duckdb connection/transaction (which would race
      // the in-flight store commit). Recovery keeps a connection: it evaluates
      // expressions inline and its range jobs scan the store table.
      std::vector<IndexedExpression> exprs;
      if (!session.live_mode) {
        expr_conn.emplace(session.instance);
        expr_conn->BeginTransaction();
        exprs = MakeIndexedExpressions(*session.index, *expr_conn->context);
      }
      insert_writer = std::make_unique<DuckDBSearchSinkInsertWriter>(
        trx, MakeTokenizerProvider(session.snapshot, *session.index),
        session.index->GetColumns(), MakeEntryInfoProvider(*session.index),
        std::move(exprs),
        PkPolicy{.index_term = session.index->GetOptions().pk_term,
                 .column = session.index->GetOptions().pk_column});
      delete_writer = std::make_unique<DuckDBSearchSinkDeleteWriter>(trx);
    }

    ~Bundle() {
      if (expr_conn) {
        expr_conn->Rollback();
      }
    }

    std::optional<duckdb::Connection> expr_conn;
    std::unique_ptr<DuckDBSearchSinkInsertWriter> insert_writer;
    std::unique_ptr<DuckDBSearchSinkDeleteWriter> delete_writer;
  };

  // Flush cadence during replay: keeps file writes on the workers and the
  // serial flush residue at the final writer commit small, without ending
  // segments (ticks are stamped later, at retirement).
  static constexpr size_t kReplayFlushBytes = size_t{32} << 20;

  std::shared_ptr<search::InvertedIndexStorage> storage;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<const catalog::Table> table;
  std::vector<catalog::Column::Id> chunk_column_ids;
  std::vector<FeedColumn> ref_columns;
  duckdb::DatabaseInstance& instance;
  duckdb::AttachedDatabase& attached;
  duckdb::TaskExecutor executor;
  uint64_t generation = 0;
  size_t depth = 1;
  uint64_t durable_offset = 0;

  std::mutex retire_mu;
  std::deque<std::unique_ptr<Job>> window;
  std::deque<Job*> deferred;
  std::deque<std::pair<uint64_t, uint64_t>> pending_cursors;
  uint64_t committed_below = 0;
  std::atomic<uint64_t> scheduled{0};
  std::atomic<uint64_t> retired{0};

  // Live commit-window feed (persistent session reused after recovery): jobs
  // tokenize in parallel into their own segments; CommitSearch commits them
  // all at the commit's tick. Only touched on the committing thread + its
  // workers; commits are serialized DB-wide by the store WAL lock.
  std::vector<std::unique_ptr<Job>> live_jobs;
  std::atomic<uint64_t> live_scheduled{0};
  std::atomic<uint64_t> live_done{0};
  // A worker tokenization error surfaces at drain, after the store commit is
  // already durable; the index is marked out-of-sync (rebuilt on boot) rather
  // than throwing out of the noexcept commit path.
  bool live_failed = false;

  // Indexed-expression evaluation for the live feed. The workers must not open
  // duckdb connections (that races the in-flight store commit), so one shared
  // eval connection + transaction is opened on the committing thread and the
  // expressions bound once; the workers evaluate them in parallel over that
  // shared context (duckdb's own parallel-projection pattern: per-thread
  // ExpressionExecutors, one shared ClientContext).
  bool live_mode = false;
  std::optional<duckdb::Connection> live_expr_conn;
  bool live_expr_txn_open = false;
  std::vector<IndexedExpression> live_exprs;

  std::mutex bundle_mu;
  std::vector<std::unique_ptr<Bundle>> bundles;

  ReplaySession(std::shared_ptr<search::InvertedIndexStorage> storage_in,
                std::shared_ptr<const catalog::Snapshot> snapshot_in,
                std::shared_ptr<const catalog::InvertedIndex> index_in,
                std::shared_ptr<const catalog::Table> table_in,
                duckdb::AttachedDatabase& attached_in)
    : storage{std::move(storage_in)},
      snapshot{std::move(snapshot_in)},
      index{std::move(index_in)},
      table{std::move(table_in)},
      instance{attached_in.GetDatabase()},
      attached{attached_in},
      executor{duckdb::TaskScheduler::GetScheduler(instance)} {
    chunk_column_ids = TableChunkColumnIds(*table);
    for (duckdb::idx_t pos = 0; pos < chunk_column_ids.size(); ++pos) {
      if (!index->ReferencesColumn(chunk_column_ids[pos])) {
        continue;
      }
      const auto* col = table->ColumnById(chunk_column_ids[pos]);
      if (!col) {
        continue;
      }
      ref_columns.push_back({pos, {chunk_column_ids[pos], col->type}});
    }
    depth = ConfiguredReplayDepth(instance);
    if (depth == 0) {
      const auto threads = static_cast<size_t>(
        duckdb::TaskScheduler::GetScheduler(instance).NumberOfThreads());
      depth = 4 * std::max<size_t>(1, threads);
    }
    depth = std::clamp<size_t>(depth, 1, 1024);
  }

  // Any teardown path (attach failure destroying the catalog under a live
  // session) must wait out in-flight tasks before the members die.
  ~ReplaySession() {
    try {
      executor.WorkOnTasks();
    } catch (...) {
    }
  }

  static size_t ConfiguredReplayDepth(duckdb::DatabaseInstance& db) {
    auto& config = duckdb::DBConfig::GetConfig(db);
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    const auto index = config.TryGetSettingIndex(
      std::string{kRecoveryReplayDepthSetting}, option);
    if (!index.IsValid()) {
      return 0;
    }
    duckdb::Value value;
    if (!config.user_settings.TryGetSetting(index.GetIndex(), value) ||
        value.IsNull()) {
      return 0;
    }
    return value.GetValue<uint32_t>();
  }

  void Dispatch(bool is_delete, duckdb::shared_ptr<duckdb::DataChunk> chunk,
                std::vector<int64_t> rowids, uint64_t wal_offset) {
    if (executor.HasError()) {
      return;  // FinishReplay rethrows
    }
    auto job = std::make_unique<Job>(*this, is_delete, std::move(chunk),
                                     std::move(rowids), wal_offset);
    auto* raw = job.get();
    {
      std::lock_guard lock{retire_mu};
      // Entries replay in ascending offset order and each transaction commits
      // before the next entry is read, so everything strictly below the entry
      // being dispatched is committed.
      committed_below = std::max(committed_below, wal_offset);
      FlushCursorsLocked();
      ScheduleReadyLocked();
      window.push_back(std::move(job));
    }
    scheduled.fetch_add(1, std::memory_order_relaxed);
    executor.ScheduleTask(duckdb::make_uniq<RunTask>(executor, *raw));
    Backpressure();
  }

  // A range job scans the merged table itself, so it may only run once its
  // entry's transaction has committed; until then it sits reserved (ordering)
  // but unscheduled (it holds no data, so it is exempt from the window bound).
  void DispatchRange(duckdb::DataTable& table, duckdb::row_t row_start,
                     duckdb::idx_t count, uint64_t wal_offset,
                     duckdb::shared_ptr<void>& share) {
    if (executor.HasError()) {
      return;  // FinishReplay rethrows
    }
    if (!share) {
      share = duckdb::make_shared_ptr<RangeShare>();
    }
    auto job = std::make_unique<Job>(
      *this, table, row_start, count,
      duckdb::shared_ptr<RangeShare>(share,
                                     static_cast<RangeShare*>(share.get())),
      wal_offset);
    auto* raw = job.get();
    {
      std::lock_guard lock{retire_mu};
      committed_below = std::max(committed_below, wal_offset);
      FlushCursorsLocked();
      ScheduleReadyLocked();
      window.push_back(std::move(job));
      deferred.push_back(raw);
    }
    Backpressure();
  }

  void ScheduleReadyLocked() {
    while (!deferred.empty() &&
           deferred.front()->wal_offset < committed_below) {
      scheduled.fetch_add(1, std::memory_order_relaxed);
      executor.ScheduleTask(
        duckdb::make_uniq<RunTask>(executor, *deferred.front()));
      deferred.pop_front();
    }
  }

  // Prefetch window over scheduled-but-unretired jobs: never wait on the job
  // just dispatched; when full, help run scheduled tasks instead of sleeping.
  void Backpressure() {
    while (scheduled.load(std::memory_order_relaxed) -
             retired.load(std::memory_order_acquire) >=
           depth) {
      if (executor.HasError()) {
        return;
      }
      duckdb::shared_ptr<duckdb::Task> help;
      if (executor.GetTask(help)) {
        help->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
        help.reset();
      } else {
        std::this_thread::yield();
      }
    }
  }

  void Run(Job& task) {
    auto* bundle = AcquireBundle(task.trx);
    const absl::Cleanup release = [&] { ReleaseBundle(bundle); };
    if (task.range_table != nullptr) {
      RunRange(*bundle, task);
    } else if (task.is_delete) {
      Delete(*bundle, task);
    } else {
      Insert(*bundle, task);
    }
    task.done.store(true, std::memory_order_release);
    if (task.self_retire) {
      Retire();
    } else {
      live_done.fetch_add(1, std::memory_order_release);
    }
  }

  void RunRange(Bundle& bundle, Job& task) {
    bool scanned = false;
    std::call_once(task.share->once, [&] {
      scanned = true;
      ScanRange(bundle, task);
    });
    if (scanned) {
      return;
    }
    uint64_t queries = 0;
    for (auto& [chunk, first_row] : task.share->chunks) {
      FeedRows(bundle, task, *chunk, first_row);
      ++queries;
    }
    task.query_count = std::max<uint64_t>(queries, 1);
  }

  void ScanRange(Bundle& bundle, Job& task) {
    auto& transaction =
      duckdb::DuckTransaction::Get(*bundle.expr_conn->context, attached);
    // Siblings attached to the share consume stored copies; alone, tokenize
    // straight out of the scan callback and store nothing.
    const bool store_copies = task.share.use_count() > 1;
    uint64_t queries = 0;
    int64_t row = static_cast<int64_t>(task.row_start);
    task.range_table->ScanTableSegment(
      transaction, static_cast<duckdb::idx_t>(task.row_start),
      task.range_count, [&](duckdb::DataChunk& chunk) {
        FeedRows(bundle, task, chunk, row);
        if (store_copies) {
          auto owned = duckdb::make_shared_ptr<duckdb::DataChunk>();
          owned->Initialize(duckdb::Allocator::DefaultAllocator(),
                            chunk.GetTypes());
          chunk.Copy(*owned);
          task.share->chunks.emplace_back(std::move(owned), row);
        }
        row += static_cast<int64_t>(chunk.size());
        ++queries;
      });
    task.query_count = std::max<uint64_t>(queries, 1);
  }

  // Feed one contiguous-rowid chunk (full physical column layout) into the
  // task's transaction.
  void FeedRows(Bundle& bundle, Job& task, duckdb::DataChunk& chunk,
                int64_t first_row) {
    const auto count = chunk.size();
    std::vector<int64_t> rowids(count);
    std::iota(rowids.begin(), rowids.end(), first_row);
    std::vector<std::string> keys(count);
    std::vector<std::string_view> key_views(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      keys[i] = RowIdKey(rowids[i]);
      key_views[i] = keys[i];
    }
    duckdb::Vector rowid_vec{
      duckdb::LogicalType::ROW_TYPE,
      reinterpret_cast<duckdb::data_ptr_t>(rowids.data()), count};

    FeedChunk(*bundle.insert_writer, count,
              PkChunk{.keys = key_views, .column = &rowid_vec}, chunk,
              ref_columns, table->GetId(), chunk_column_ids,
              &*bundle.expr_conn->context);
    task.trx.AdvanceQueries(1);
    if (task.trx.ActiveMemory() >= kReplayFlushBytes) {
      task.trx.Flush();
    }
  }

  Bundle* AcquireBundle(irs::IndexWriter::Transaction& trx) {
    std::unique_ptr<Bundle> bundle;
    {
      std::lock_guard lock{bundle_mu};
      if (!bundles.empty()) {
        bundle = std::move(bundles.back());
        bundles.pop_back();
      }
    }
    if (!bundle) {
      bundle = std::make_unique<Bundle>(*this, trx);
    }
    bundle->insert_writer->SetTransaction(trx);
    bundle->delete_writer->SetTransaction(trx);
    return bundle.release();
  }

  void ReleaseBundle(Bundle* bundle) {
    std::lock_guard lock{bundle_mu};
    bundles.emplace_back(bundle);
  }

  void Insert(Bundle& bundle, Job& task) {
    const auto count = task.rowids.size();
    std::vector<std::string> keys(count);
    std::vector<std::string_view> key_views(count);
    for (size_t i = 0; i < count; ++i) {
      keys[i] = RowIdKey(task.rowids[i]);
      key_views[i] = keys[i];
    }
    duckdb::Vector rowid_vec{
      duckdb::LogicalType::ROW_TYPE,
      reinterpret_cast<duckdb::data_ptr_t>(task.rowids.data()), count};

    // Recovery evaluates its writer's expressions against the bundle's own
    // connection; live workers evaluate the session's shared expressions in
    // parallel over the shared eval context (no per-worker connection).
    if (task.self_retire) {
      FeedChunk(*bundle.insert_writer, count,
                PkChunk{.keys = key_views, .column = &rowid_vec}, *task.chunk,
                ref_columns, table->GetId(), chunk_column_ids,
                &*bundle.expr_conn->context);
    } else {
      FeedChunk(*bundle.insert_writer, count,
                PkChunk{.keys = key_views, .column = &rowid_vec}, *task.chunk,
                ref_columns, table->GetId(), chunk_column_ids,
                live_expr_conn ? &*live_expr_conn->context : nullptr,
                live_exprs);
    }
    task.trx.AdvanceQueries(1);
    if (task.trx.ActiveMemory() >= kReplayFlushBytes) {
      task.trx.Flush();
    }
  }

  void Delete(Bundle& bundle, Job& task) {
    auto& del = *bundle.delete_writer;
    del.Init(task.rowids.size(), {});
    std::string key;
    for (const auto rowid : task.rowids) {
      key.clear();
      primary_key::AppendSigned(key, rowid);
      del.DeleteRow(key);
    }
    del.Finish();
  }

  // Commit finished tasks strictly in dispatch order: ticks allocated here
  // are WAL-ordered, per-segment ticks stay monotone, and the pending state
  // is always a WAL prefix. Retirement is eager (a WAL v2/v3 entry is a
  // checksummed whole-transaction block, so a torn tail throws before any of
  // its chunks dispatch), but cursor points -- and with them the frontier a
  // mid-replay refresh may commit durably -- only advance once the entry's
  // transaction has committed.
  void Retire() {
    std::lock_guard lock{retire_mu};
    while (!window.empty()) {
      auto& head = *window.front();
      if (!head.done.load(std::memory_order_acquire)) {
        break;
      }
      const auto tick = search::TickDomain::Instance().Advance(head.query_count);
      SDB_ENSURE(head.trx.Commit(tick),
                 "inverted index replay: commit failed for index ",
                 index->GetId().id());
      pending_cursors.emplace_back(tick, head.wal_offset);
      window.pop_front();
      retired.fetch_add(1, std::memory_order_release);
    }
    FlushCursorsLocked();
  }

  void FlushCursorsLocked() {
    while (!pending_cursors.empty() &&
           pending_cursors.front().second < committed_below) {
      const auto [tick, offset] = pending_cursors.front();
      pending_cursors.pop_front();
      storage->RecordFlushCursor(tick,
                                 search::WalCursor{generation, offset + 1});
      storage->SetRecoveryFrontierTick(tick);
    }
  }

  // End of replay: everything below the success offset committed; flush the
  // remaining cursor points so the final refresh commits the whole feed.
  void FinishRetire(uint64_t success_offset) {
    Retire();
    std::lock_guard lock{retire_mu};
    SDB_ASSERT(window.empty());
    SDB_ASSERT(deferred.empty());
    committed_below = std::max(committed_below, success_offset + 1);
    FlushCursorsLocked();
    SDB_ASSERT(pending_cursors.empty());
  }

  // --- Live commit-window feed (post-recovery reuse of the pool/executor) ---

  // Bind the indexed expressions once, lazily, on the committing thread. The
  // eval needs a transaction, so a private connection is opened here (safe on
  // the committing thread -- the inline path does the same; only a worker
  // opening one would race the store commit). The transaction stays open for
  // the commit and is rolled back in ResetLive.
  void EnsureLiveExprs() {
    if (index->ExpressionKeys().empty() || !live_exprs.empty()) {
      if (!live_expr_txn_open && !index->ExpressionKeys().empty()) {
        live_expr_conn->BeginTransaction();
        live_expr_txn_open = true;
      }
      return;
    }
    if (!live_expr_conn) {
      live_expr_conn.emplace(instance);
    }
    live_expr_conn->BeginTransaction();
    live_expr_txn_open = true;
    live_exprs = MakeIndexedExpressions(*index, *live_expr_conn->context);
  }

  // Deep-copy the committing chunk (its vectors reference scan block pins that
  // the feed loop recycles) and dispatch a tokenize job into its own segment.
  // Only the indexed columns are populated in the commit-time table chunk, so
  // copy exactly those (ref_columns positions). Indexed expressions are
  // evaluated on the workers in parallel over the session's shared eval
  // context (opened here on the committing thread by EnsureLiveExprs), so no
  // worker opens its own connection.
  void DispatchLive(duckdb::DataChunk& source, std::vector<int64_t> rowids) {
    const auto count = source.size();
    EnsureLiveExprs();
    auto owned = duckdb::make_shared_ptr<duckdb::DataChunk>();
    owned->Initialize(duckdb::Allocator::DefaultAllocator(), source.GetTypes());
    for (const auto& col : ref_columns) {
      duckdb::VectorOperations::Copy(source.data[col.slot],
                                     owned->data[col.slot], count, 0, 0);
    }
    owned->SetCardinality(count);
    auto job = std::make_unique<Job>(*this, /*is_delete=*/false,
                                     std::move(owned), std::move(rowids),
                                     /*wal_offset=*/0);
    job->self_retire = false;
    auto* raw = job.get();
    live_jobs.push_back(std::move(job));
    live_scheduled.fetch_add(1, std::memory_order_relaxed);
    executor.ScheduleTask(duckdb::make_uniq<RunTask>(executor, *raw));
    BackpressureLive();
  }

  void BackpressureLive() {
    while (live_scheduled.load(std::memory_order_relaxed) -
             live_done.load(std::memory_order_acquire) >=
           depth) {
      if (executor.HasError()) {
        return;
      }
      duckdb::shared_ptr<duckdb::Task> help;
      if (executor.GetTask(help)) {
        help->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
        help.reset();
      } else {
        std::this_thread::yield();
      }
    }
  }

  // Drain every scheduled live job. A worker error is captured (not rethrown):
  // the store commit is already durable here, so the index is marked
  // out-of-sync at finish rather than aborting the noexcept commit.
  void DrainLive() {
    try {
      executor.WorkOnTasks();
    } catch (const std::exception&) {
      live_failed = true;
    }
  }

  bool HasLiveJobs() const { return !live_jobs.empty(); }

  // Phase 1 of the live commit, BEFORE the tick is allocated: finish
  // tokenization and pin every segment onto the flush context. RegisterFlush
  // must precede TickDomain::Advance -- otherwise a refresh whose tick
  // snapshot lands between the Advance and the pin could advance its committed
  // tick past an unpinned segment (lost insert / FlushPending assert).
  // Returns the max per-segment query count for tick-range sizing.
  uint64_t Prepare() override {
    DrainLive();
    if (live_failed) {
      return 0;
    }
    uint64_t queries = 0;
    for (auto& job : live_jobs) {
      job->trx.RegisterFlush();
      queries = std::max(queries, job->trx.GetQueries());
    }
    return queries;
  }

  // Phase 2, AFTER the tick is allocated: record the durable cursor (before
  // the segments become flushable) then commit every segment at the commit
  // tick. Mirrors the inline path's record-cursor-then-commit ordering. On a
  // tokenization failure the segments are dropped and the index marked
  // out-of-sync (rebuilt on boot) -- the store commit is already durable.
  void Finish(uint64_t last_tick,
              std::optional<search::WalCursor> cursor) override {
    if (live_failed) {
      if (storage) {
        storage->MarkOutOfSync();
      }
      for (auto& job : live_jobs) {
        job->trx.Abort();
      }
      ResetLive();
      return;
    }
    if (storage && cursor) {
      storage->RecordFlushCursor(last_tick, *cursor);
    }
    for (auto& job : live_jobs) {
      SDB_ENSURE(job->trx.Commit(last_tick),
                 "inverted index live feed: commit failed for index ",
                 index->GetId().id());
    }
    ResetLive();
  }

  void Abort() override {
    DrainLive();
    for (auto& job : live_jobs) {
      job->trx.Abort();
    }
    ResetLive();
  }

  void ResetLive() {
    live_jobs.clear();
    live_scheduled.store(0, std::memory_order_relaxed);
    live_done.store(0, std::memory_order_relaxed);
    live_failed = false;
    if (live_expr_txn_open) {
      live_expr_conn->Rollback();
      live_expr_txn_open = false;
    }
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

InvertedStoreIndex::ReplaySession& InvertedStoreIndex::EnsureReplaySession(
  bool live) {
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
    std::move(table), db);
  _replay->durable_offset = durable_offset;
  _replay->generation = block_manager.GetCheckpointIteration();
  // Set once, before any job is dispatched, so tokenizing workers read a
  // stable value (a per-chunk write would race their Bundle construction).
  _replay->live_mode = live;
  return *_replay;
}

namespace {

std::vector<int64_t> ExtractRowIds(duckdb::Vector& row_ids,
                                   duckdb::idx_t count) {
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* rows = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  std::vector<int64_t> out;
  out.reserve(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    out.push_back(static_cast<int64_t>(rows[fmt.sel->get_index(i)]));
  }
  return out;
}

}  // namespace

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
  auto& session = EnsureReplaySession(/*live=*/false);
  const auto commit_offset = ReplayCommitOffset();
  if (commit_offset != 0 && commit_offset < session.durable_offset) {
    return;
  }
  auto shared = duckdb::DuckTransactionManager::Get(db).GetReplayChunk();
  SDB_ENSURE(shared.get() == &chunk,
             "inverted index replay: append chunk is not the replayer's "
             "shared chunk");
  session.Dispatch(/*is_delete=*/false, std::move(shared),
                   ExtractRowIds(row_ids, count), commit_offset);
}

void InvertedStoreIndex::ReplayDelete(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  const auto count = chunk.size();
  if (count == 0) {
    return;
  }
  auto& session = EnsureReplaySession(/*live=*/false);
  const auto commit_offset = ReplayCommitOffset();
  if (commit_offset != 0 && commit_offset < session.durable_offset) {
    return;
  }
  session.Dispatch(/*is_delete=*/true, nullptr, ExtractRowIds(row_ids, count),
                   commit_offset);
}

void InvertedStoreIndex::ReplayAppendRange(duckdb::DataTable& table,
                                           duckdb::row_t row_start,
                                           duckdb::idx_t count,
                                           duckdb::shared_ptr<void>& share) {
  if (count == 0) {
    return;
  }
  auto& session = EnsureReplaySession(/*live=*/false);
  const auto commit_offset = ReplayCommitOffset();
  if (commit_offset != 0 && commit_offset < session.durable_offset) {
    return;
  }
  session.DispatchRange(table, row_start, count, commit_offset, share);
}

void InvertedStoreIndex::FinishReplay() {
  if (!_replay) {
    return;
  }
  auto& session = *_replay;
  const absl::Cleanup reset = [&] { _replay.reset(); };
  const auto success_offset =
    duckdb::DuckTransactionManager::Get(db).GetReplaySuccessOffset();
  {
    std::lock_guard lock{session.retire_mu};
    session.committed_below =
      std::max(session.committed_below, success_offset + 1);
    session.ScheduleReadyLocked();
  }
  session.executor.WorkOnTasks();
  session.FinishRetire(success_offset);
}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    ReplayAppend(chunk, row_ids);
    return {};
  }
  const auto count = chunk.size();
  if (count == 0) {
    return {};
  }
  // Past the fan-out gate a bulk commit tokenizes on workers instead of inline
  // on the committing thread; small commits stay on the inline path below.
  if (conn->ClassifyInvertedFeed(_index_id) ==
      query::Transaction::InvertedFeedMode::Parallel) {
    auto& session = EnsureReplaySession(/*live=*/true);
    conn->EngageParallelInvertedFeed(_index_id, &session);
    session.DispatchLive(chunk, ExtractRowIds(row_ids, count));
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

  std::vector<FeedColumn> columns;
  columns.reserve(chunk.ColumnCount());
  for (duckdb::idx_t pos = 0;
       pos < chunk.ColumnCount() && pos < chunk_column_ids.size(); ++pos) {
    auto col_id = chunk_column_ids[pos];
    const auto* col = table->ColumnById(col_id);
    if (!col) {
      continue;
    }
    columns.push_back({pos, {col_id, col->type}});
  }
  FeedChunk(*writer, count, PkChunk{.keys = key_views, .column = &row_ids},
            chunk, columns, _table_id, chunk_column_ids, *expr_conn.context);
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
