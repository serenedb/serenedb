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

#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/execution/index/unbound_index.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <iterator>
#include <mutex>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/log.h"
#include "basics/primary_key.hpp"
#include "catalog1/catalog.h"
#include "catalog1/entry/inverted_index.h"
#include "catalog1/entry/tokenizer.h"
#include "catalog1/scorer_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/search_sink_writer.hpp"
#include "connector/term_dict.h"
#include "connector/view_fast_path.h"
#include "pg/connection_context.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

// The index entry one id names in the database holding it, or null when no
// entry there carries it -- an online CREATE INDEX feeds a concurrent writer
// before its own transaction has committed, so a miss is ordinary.
duckdb::optional_ptr<const duckdb::IndexCatalogEntry> FindIndexEntry(
  duckdb::ClientContext* context, duckdb::AttachedDatabase& db,
  duckdb::idx_t id) {
  const auto found = db.GetCatalog()
                       .Cast<catalog::SereneDBCatalog>()
                       .FindIn<duckdb::DuckIndexEntry>(context, id);
  return found ? &found->Cast<duckdb::IndexCatalogEntry>() : nullptr;
}

constexpr const char* kIndexIdOption = "sdb_index_id";

duckdb::idx_t IdOption(const duckdb::case_insensitive_map_t<duckdb::Value>& o,
                       const char* key) {
  const auto it = o.find(key);
  if (it == o.end() || it->second.IsNull()) {
    return 0;
  }
  return it->second.GetValue<uint64_t>();
}

duckdb::IndexStorageInfo StorageRecord(const InvertedStoreIndex& index) {
  duckdb::IndexStorageInfo info{index.name};
  info.options[kIndexIdOption] = duckdb::Value::UBIGINT(index.IndexId());
  return info;
}

// Per-worker evaluation of an index's expressions. The bound expressions and
// their chunk offsets come from duckdb (BoundIndex), so this owns nothing but
// an executor and the result chunk: no binding, no context, no shared state --
// every feed worker constructs one and runs it on its own batch in parallel.
class IndexExpressions {
 public:
  IndexExpressions(
    duckdb::ClientContext& context,
    std::shared_ptr<const catalog::InvertedIndexConfig> config,
    const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& exprs)
    : _executor{context},
      _config{std::move(config)},
      _has_predicate{exprs.size() > _config->keys.size()} {
    const auto keys = std::span{_config->keys};
    _feeds.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      _feeds.push_back(absl::c_none_of(
        keys.first(i), [&](const catalog::InvertedIndexKey& earlier) {
          return earlier.field_id == keys[i].field_id;
        }));
    }
    if (exprs.empty()) {
      return;
    }
    duckdb::vector<duckdb::LogicalType> types;
    types.reserve(exprs.size());
    for (const auto& expr : exprs) {
      _executor.AddExpression(*expr);
      types.push_back(expr->GetReturnType());
    }
    _results.Initialize(duckdb::Allocator::DefaultAllocator(), types);
  }

  bool Empty() const noexcept { return _results.ColumnCount() == 0; }

  // Evaluates every expression over `chunk` once.
  void Execute(duckdb::DataChunk& chunk) {
    _results.Reset();
    _executor.Execute(chunk, _results);
    for (size_t i = 0; i < _config->keys.size(); ++i) {
      // A whole-value analyzer is handed the object as it stands, so the
      // leaf rule does not apply to its key.
      const auto* entry = _config->FindEntry(_config->keys[i].field_id);
      if (entry && entry->whole_value) {
        continue;
      }
      RejectJsonObjectArrayLeaves(_results.data[i], chunk.size());
    }
  }

  // The value vector feeding `field_ids()[i]`.
  duckdb::Vector& Value(size_t i) noexcept { return _results.data[i]; }
  std::span<const catalog::InvertedIndexKey> Fields() const noexcept {
    return _config->keys;
  }
  // False for a later key of a column listed twice: its field is written by
  // the first one.
  bool Feeds(size_t i) const noexcept { return _feeds[i]; }

  // The partial-index predicate's result (null when the index is not partial).
  duckdb::Vector* Predicate() noexcept {
    return _has_predicate ? &_results.data[_results.ColumnCount() - 1]
                          : nullptr;
  }

 private:
  duckdb::ExpressionExecutor _executor;
  duckdb::DataChunk _results;
  std::shared_ptr<const catalog::InvertedIndexConfig> _config;
  std::vector<bool> _feeds;
  bool _has_predicate = false;
};

// Per-worker scratch for the feed body. Held by the pooled Bundle so a bulk
// commit reuses the buffers (and the key strings' capacity) across batches
// instead of reallocating them per batch per index.
struct FeedScratch {
  // Rowids of a scanned range, which run contiguously from the range's first
  // row -- generated in place rather than materialized into a side buffer.
  std::vector<std::string> keys;
  std::vector<std::string_view> key_views;
  std::string delete_key;
  std::vector<duckdb::Vector> sliced;
  std::vector<ExpressionValue> values;
  // Partial-index selection, reused: the vector keeps its capacity and the
  // chunk its vector array, so a filtered batch costs no allocation.
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  duckdb::DataChunk filtered_chunk;
  duckdb::Vector filtered_rowids{duckdb::LogicalType::ROW_TYPE};
  // A worker's view of its slice of the borrowed chunk, reused across chunks.
  duckdb::DataChunk slice_chunk;
  duckdb::Vector slice_rowids{duckdb::LogicalType::ROW_TYPE};
};

// Rows the partial-index predicate keeps, selected into `scratch.sel`. Returns
// `total` when everything passes (or the index is not partial), so the caller
// feeds the batch untouched.
duckdb::idx_t SelectRows(duckdb::Vector* predicate, duckdb::idx_t total,
                         FeedScratch& scratch) {
  if (!predicate) {
    return total;
  }
  duckdb::UnifiedVectorFormat fmt;
  predicate->ToUnifiedFormat(total, fmt);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i < total; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (fmt.validity.RowIsValid(idx) && values[idx]) {
      scratch.sel.set_index(kept++, i);
    }
  }
  return kept;
}

// Feed one batch through the predicate: evaluate the index's expressions,
// drop the rows the predicate rejects, and tokenize the rest. Shared by the
// parallel feed and the inline commit path so the filtering, key building and
// value slicing exist once. Returns the number of rows fed (0 when the
// predicate rejected the whole batch).
duckdb::idx_t FeedFilteredChunk(
  DuckDBSinkIndexWriter& writer, IndexExpressions& exprs,
  duckdb::DataChunk& chunk, duckdb::Vector& rowid_vec, duckdb::idx_t scanned,
  std::span<const FeedColumn> columns, FeedScratch& scratch) {
  if (!exprs.Empty()) {
    exprs.Execute(chunk);
  }
  const auto count = SelectRows(exprs.Predicate(), scanned, scratch);
  if (count == 0) {
    return 0;
  }
  const bool filtered = count != scanned;
  auto* feed_chunk = &chunk;
  auto* feed_rows = &rowid_vec;
  if (filtered) {
    // Sized once: the scratch is pooled and reused across every batch this
    // writer feeds, and InitializeEmpty only accepts an empty chunk.
    if (scratch.filtered_chunk.ColumnCount() == 0) {
      scratch.filtered_chunk.InitializeEmpty(chunk.GetTypes());
    }
    scratch.filtered_chunk.Reference(chunk);
    scratch.filtered_chunk.Slice(scratch.sel, count);
    scratch.filtered_rowids.Slice(rowid_vec, scratch.sel, count);
    feed_chunk = &scratch.filtered_chunk;
    feed_rows = &scratch.filtered_rowids;
  }

  duckdb::UnifiedVectorFormat row_fmt;
  feed_rows->ToUnifiedFormat(count, row_fmt);
  const auto* row_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(row_fmt);
  auto& keys = scratch.keys;
  auto& key_views = scratch.key_views;
  keys.resize(count);
  key_views.resize(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    keys[i].clear();
    primary_key::AppendSigned(keys[i], row_data[row_fmt.sel->get_index(i)]);
    key_views[i] = keys[i];
  }

  // Expression values were computed over the unfiltered batch, so a filtered
  // feed slices them the same way its columns were sliced.
  const auto fields = exprs.Fields();
  auto& sliced = scratch.sliced;
  auto& values = scratch.values;
  sliced.clear();
  values.clear();
  values.reserve(fields.size());
  if (filtered) {
    sliced.reserve(fields.size());
  }
  for (size_t i = 0; i < fields.size(); ++i) {
    if (!exprs.Feeds(i)) {
      continue;
    }
    auto& raw = exprs.Value(i);
    if (!filtered) {
      values.push_back({fields[i].field_id, &raw});
      continue;
    }
    sliced.emplace_back(raw.GetType(), nullptr, 0);
    sliced.back().Slice(raw, scratch.sel, count);
    values.push_back({fields[i].field_id, &sliced.back()});
  }

  FeedChunk(writer, count, PkChunk{.keys = key_views, .column = feed_rows},
            *feed_chunk, columns, values);
  return count;
}

}  // namespace

// The per-index worker machinery: the task executor, the pooled
// writer/expression kits, and the tokenize bodies. Knows nothing about commit
// ticks -- those belong to whoever owns the entries.
struct FeedPool {
  // Per-worker feed kit, pooled because neither the writers nor the executor
  // are per-chunk cheap. The bound expressions are shared and read-only; only
  // the executor and its result chunk are per-worker.
  struct Bundle {
    Bundle(FeedPool& pool, irs::IndexWriter::Transaction& trx)
      : insert_writer{pool.MakeInsertWriter(trx)},
        delete_writer{std::make_unique<DuckDBSearchSinkDeleteWriter>(trx)},
        exprs{std::make_unique<IndexExpressions>(
          *pool.expr_context, pool.config, pool.owner.Expressions())} {}

    std::unique_ptr<DuckDBSearchSinkInsertWriter> insert_writer;
    std::unique_ptr<DuckDBSearchSinkDeleteWriter> delete_writer;
    std::unique_ptr<IndexExpressions> exprs;
    FeedScratch scratch;
  };

  std::unique_ptr<DuckDBSearchSinkInsertWriter> MakeInsertWriter(
    irs::IndexWriter::Transaction& trx) {
    return std::make_unique<DuckDBSearchSinkInsertWriter>(
      trx, [this](irs::field_id id) { return tokenizers.Acquire(id); },
      IndexedColumnIds(*config), MakeEntryInfoProvider(*config), config->pk);
  }

  irs::IndexWriter::Transaction NewTransaction() {
    auto trx = storage->GetTransaction();
    trx.SetFieldOptions(config);
    return trx;
  }

  // Flush cadence of every feed through FeedEvaluated: keeps file writes on
  // the workers and the serial flush residue at the final writer commit small,
  // without ending segments (ticks are stamped later, at commit).
  static constexpr size_t kFlushBytes = size_t{32} << 20;

  std::shared_ptr<search::InvertedIndexStorage> storage;
  catalog::IndexTokenizers tokenizers;
  // The configuration this pool's writers encode against, held by shared
  // ownership: it IS the irs::IndexFieldOptions iresearch takes, and an ALTER
  // landing mid-commit swaps the index's copy without disturbing this one.
  std::shared_ptr<const InvertedIndexConfig> config;
  duckdb::DatabaseInstance& instance;
  // duckdb has no context-free ExpressionExecutor, and a feed worker has no
  // connection of its own. One transaction-less context per pool, shared by
  // every worker's executor: it is read for its allocator and settings only,
  // never to reach the catalog.
  duckdb::shared_ptr<duckdb::ClientContext> expr_context;
  // The index owning this feed; workers build their executors from its
  // duckdb-bound expressions. It outlives the session.
  const InvertedStoreIndex& owner;
  duckdb::TaskExecutor executor;

  std::mutex bundle_mu;
  std::vector<std::unique_ptr<Bundle>> bundles;

  FeedPool(std::shared_ptr<search::InvertedIndexStorage> storage_in,
           catalog::IndexTokenizers tokenizers_in,
           std::shared_ptr<const InvertedIndexConfig> config_in,
           duckdb::AttachedDatabase& attached_in,
           const InvertedStoreIndex& owner_in)
    : storage{std::move(storage_in)},
      tokenizers{std::move(tokenizers_in)},
      config{std::move(config_in)},
      instance{attached_in.GetDatabase()},
      expr_context{duckdb::make_shared_ptr<duckdb::ClientContext>(
        instance.shared_from_this())},
      owner{owner_in},
      executor{duckdb::TaskScheduler::GetScheduler(instance)} {}

  // The feed body every worker runs. Binds nothing and touches no context.
  void FeedEvaluated(DuckDBSearchSinkInsertWriter& writer,
                     IndexExpressions& exprs, FeedScratch& scratch,
                     irs::IndexWriter::Transaction& trx,
                     duckdb::DataChunk& chunk, duckdb::Vector& rowid_vec,
                     duckdb::idx_t scanned) {
    FeedFilteredChunk(writer, exprs, chunk, rowid_vec, scanned, {}, scratch);
    trx.AdvanceQueries(1);
    if (trx.ActiveMemory() >= kFlushBytes) {
      trx.Flush();
    }
  }

  // Borrows a kit for the duration of a scope. The pool hands them back on
  // destruction, so a throwing feed cannot leak one.
  class BundleLease {
   public:
    BundleLease(FeedPool& pool, irs::IndexWriter::Transaction& trx)
      : _pool{pool}, _bundle{pool.AcquireBundle(trx)} {}
    ~BundleLease() { _pool.ReleaseBundle(std::move(_bundle)); }
    BundleLease(const BundleLease&) = delete;
    BundleLease& operator=(const BundleLease&) = delete;

    Bundle& operator*() const noexcept { return *_bundle; }
    Bundle* operator->() const noexcept { return _bundle.get(); }

   private:
    FeedPool& _pool;
    std::unique_ptr<Bundle> _bundle;
  };

  std::unique_ptr<Bundle> AcquireBundle(irs::IndexWriter::Transaction& trx) {
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
    return bundle;
  }

  void ReleaseBundle(std::unique_ptr<Bundle> bundle) {
    std::lock_guard lock{bundle_mu};
    bundles.push_back(std::move(bundle));
  }

  // Takes the rows through an accessor rather than a span: the live path reads
  // them straight out of duckdb's (possibly selection-vectored) rowid vector.
  template<typename RowAt>
  void FeedDeletes(Bundle& bundle, size_t count, RowAt&& row_at) {
    connector::FeedDeletes(*bundle.delete_writer, bundle.scratch.delete_key,
                           count, std::forward<RowAt>(row_at));
  }
};

// One transaction per sub-range, filled by the slice tasks themselves and
// committed together at the commit's tick.
struct Entry {
  Entry(FeedPool& pool, size_t subranges) {
    trxs.reserve(subranges);
    for (size_t i = 0; i < subranges; ++i) {
      trxs.emplace_back(pool.NewTransaction());
    }
  }

  Entry(const Entry&) = delete;
  Entry& operator=(const Entry&) = delete;

  std::vector<irs::IndexWriter::Transaction> trxs;
};

// The committing thread owns the scan and feeds every native index inline, so a
// chunk is only ours for the duration of the call. Tokenizing it is the slow
// part, so split it across workers and join before returning: the work goes
// parallel without the chunk ever outliving the call that lent it. Slicing
// takes the source by const reference, so every worker may view the same chunk
// at once.
struct SliceTask final : duckdb::BaseExecutorTask {
  SliceTask(duckdb::TaskExecutor& executor_in, FeedPool& pool_in,
            irs::IndexWriter::Transaction& trx_in,
            const duckdb::DataChunk& chunk_in, const duckdb::Vector& row_ids_in,
            duckdb::idx_t begin_in, duckdb::idx_t end_in)
    : BaseExecutorTask{executor_in},
      pool{pool_in},
      trx{trx_in},
      chunk{chunk_in},
      row_ids{row_ids_in},
      begin{begin_in},
      end{end_in} {}

  void ExecuteTask() override {
    const FeedPool::BundleLease bundle{pool, trx};
    auto& scratch = bundle->scratch;
    // Slice() writes into this chunk's existing vectors, so shape it once.
    if (scratch.slice_chunk.ColumnCount() == 0) {
      scratch.slice_chunk.InitializeEmpty(chunk.GetTypes());
    }
    // Per column rather than DataChunk::Slice: when a native index shares the
    // table, duckdb hands us a table-shaped chunk in which only the indexed
    // columns are referenced and the rest are placeholders with no buffer at
    // all. Slicing one of those dereferences nothing.
    for (duckdb::idx_t c = 0; c < chunk.ColumnCount(); ++c) {
      if (chunk.data[c].GetBufferRef()) {
        scratch.slice_chunk.data[c].Slice(chunk.data[c], begin, end);
      }
    }
    scratch.slice_chunk.SetCardinality(end - begin);
    scratch.slice_rowids.Slice(row_ids, begin, end);
    pool.FeedEvaluated(*bundle->insert_writer, *bundle->exprs, scratch, trx,
                       scratch.slice_chunk, scratch.slice_rowids, end - begin);
  }

  std::string TaskType() const override { return "InvertedChunkSlice"; }

  FeedPool& pool;
  irs::IndexWriter::Transaction& trx;
  const duckdb::DataChunk& chunk;
  const duckdb::Vector& row_ids;
  const duckdb::idx_t begin;
  const duckdb::idx_t end;
};

// The commit-window feed: entries tokenize in parallel into their own segments
// and CommitSearch commits them all at the
// commit's tick. No WAL ordering to keep -- one tick covers the whole set.
// Only touched on the committing thread and its workers; commits are
// serialized DB-wide by the store WAL lock.
struct LiveFeed {
  explicit LiveFeed(FeedPool& pool_in) : pool{pool_in} {}

  FeedPool& pool;
  // The transactions this commit staged. Every one is filled synchronously --
  // slices join before the append returns -- so there is nothing in flight by
  // the time the commit asks for them.
  std::vector<std::unique_ptr<Entry>> jobs;

  // The committing thread's own leg. Two operations cannot go to a worker: a
  // chunk append (duckdb hands over a buffer it recycles, so nothing may
  // outlive the call) and a delete (one filter to build, cheaper than the task
  // that would carry it). Both stage into this one transaction, which commits
  // with the parallel jobs at the same tick -- so there is a single commit
  // protocol, not an inline one and a parallel one.
  std::optional<irs::IndexWriter::Transaction> inline_trx;
  std::unique_ptr<FeedPool::Bundle> inline_bundle;

  FeedPool::Bundle& Inline() {
    if (!inline_trx) {
      inline_trx.emplace(pool.NewTransaction());
      inline_bundle = pool.AcquireBundle(*inline_trx);
    }
    return *inline_bundle;
  }

  // Rows per slice. 64 puts a 2048-row chunk on every core (the count is capped
  // at the thread count); larger slices measurably leave cores idle, and a
  // chunk too small to fill one slice is fed whole on the calling thread
  // instead.
  static constexpr duckdb::idx_t kMinSliceRows = 64;

  // One transaction per slice, for the whole commit window: the entry is
  // created once and every chunk feeds slice k into the same transaction k.
  Entry* slice_entry = nullptr;
  size_t slice_count = 0;

  // Tokenize one borrowed chunk in parallel and join before returning, so the
  // producer may reuse it the moment this call ends. RegisterFlush happens here
  // rather than at Prepare because this runs before the store WAL bytes are
  // written, and a background refresh computing its durable cursor in between
  // must already see these segments.
  void FeedChunkParallel(duckdb::DataChunk& chunk, duckdb::Vector& row_ids) {
    const auto count = chunk.size();
    if (slice_count == 0) {
      const auto threads = static_cast<size_t>(
        duckdb::TaskScheduler::GetScheduler(pool.instance).NumberOfThreads());
      slice_count = std::clamp<size_t>(count / kMinSliceRows, 1,
                                       std::max<size_t>(1, threads));
      slice_entry = CreateRangeEntry(slice_count);
    }
    // The tail chunk can be far smaller than the first: use only as many slices
    // as it can fill, always slice k into transaction k.
    const auto slices =
      std::clamp<size_t>(count / kMinSliceRows, 1, slice_count);
    if (slices == 1) {
      auto& trx = slice_entry->trxs[0];
      const FeedPool::BundleLease bundle{pool, trx};
      pool.FeedEvaluated(*bundle->insert_writer, *bundle->exprs,
                         bundle->scratch, trx, chunk, row_ids, count);
      trx.RegisterFlush();
      return;
    }
    // Its own executor, not the pool's: a TaskExecutor never clears its error
    // state, so a shared one would make a single failed chunk bail out every
    // later commit's slices without tokenizing and rethrow the stale error.
    duckdb::TaskExecutor executor{
      duckdb::TaskScheduler::GetScheduler(pool.instance)};
    for (size_t k = 0; k < slices; ++k) {
      const auto begin = (k * count) / slices;
      const auto end = ((k + 1) * count) / slices;
      if (end <= begin) {
        continue;
      }
      executor.ScheduleTask(duckdb::make_uniq<SliceTask>(
        executor, pool, slice_entry->trxs[k], chunk, row_ids, begin, end));
    }
    executor.WorkOnTasks();
    for (size_t k = 0; k < slices; ++k) {
      slice_entry->trxs[k].RegisterFlush();
    }
  }

  template<typename RowAt>
  void FeedDeletesInline(size_t count, RowAt&& row_at) {
    auto& bundle = Inline();
    pool.FeedDeletes(bundle, count, std::forward<RowAt>(row_at));
    inline_trx->RegisterFlush();
  }

  void ReleaseInline() {
    if (inline_bundle) {
      pool.ReleaseBundle(std::move(inline_bundle));
    }
    inline_trx.reset();
  }

  // Phase 1 of the live commit, BEFORE the tick is allocated: finish
  // tokenization and pin every segment onto the flush context. RegisterFlush
  // must precede TickDomain::Advance -- otherwise a refresh whose tick
  // snapshot lands between the Advance and the pin could advance its committed
  // tick past an unpinned segment (lost insert / FlushPending assert).
  // Returns the max per-segment query count for tick-range sizing.
  uint64_t Prepare() {
    uint64_t queries = 0;
    ForEachTransaction([&](irs::IndexWriter::Transaction& trx) {
      trx.RegisterFlush();
      queries = std::max(queries, trx.GetQueries());
    });
    return queries;
  }

  // Phase 2, AFTER the tick is allocated: record the durable cursor (before
  // the segments become flushable) then commit every segment at the commit
  // tick. A commit that fails leaves the index marked out-of-sync (rebuilt on
  // boot) rather than throwing: the store commit is already durable, and this
  // runs inside a noexcept commit, so throwing would take the process down.
  void Finish(uint64_t last_tick, std::optional<search::WalCursor> cursor) {
    auto& storage = pool.storage;
    if (storage && cursor) {
      storage->RecordFlushCursor(last_tick, *cursor);
    }
    ForEachTransaction([&](irs::IndexWriter::Transaction& trx) {
      if (trx.Commit(last_tick)) {
        return;
      }
      SDB_ERROR(SEARCH, "inverted index live feed: commit failed for index '",
                pool.owner.IndexId(), "' at tick ", last_tick,
                "; the index will be rebuilt from the store on next boot");
      if (storage) {
        storage->MarkOutOfSync();
      }
    });
    Reset();
  }

  void Abort() {
    ForEachTransaction([](irs::IndexWriter::Transaction& trx) { trx.Abort(); });
    Reset();
  }

  // Every transaction this commit staged: the sliced ones and, when the
  // committing thread fed a whole chunk or a delete itself, the inline one.
  template<typename Fn>
  void ForEachTransaction(Fn&& fn) {
    for (auto& job : jobs) {
      for (auto& trx : job->trxs) {
        fn(trx);
      }
    }
    if (inline_trx) {
      fn(*inline_trx);
    }
  }

  // One transaction per sub-range of a parallel append scan, filled by the scan
  // jobs themselves (see FeedChunkParallel) rather than by a worker task, and
  // committed with everything else at the commit tick.
  Entry* CreateRangeEntry(size_t subranges) {
    auto job = std::make_unique<Entry>(pool, subranges);
    auto* raw = job.get();
    jobs.push_back(std::move(job));
    return raw;
  }

  void Reset() {
    jobs.clear();
    slice_entry = nullptr;
    slice_count = 0;
    ReleaseInline();
  }

  // Nothing staged, so the pool behind it may be rebuilt. True between commits:
  // Commit and Abort both Reset.
  bool Idle() const noexcept { return jobs.empty() && !inline_trx; }
};

// One index's feed: the shared worker pool and the live commit window.
struct InvertedFeedSession {
  InvertedFeedSession(std::shared_ptr<search::InvertedIndexStorage> storage,
                      catalog::IndexTokenizers tokenizers,
                      std::shared_ptr<const InvertedIndexConfig> config,
                      duckdb::AttachedDatabase& attached,
                      const InvertedStoreIndex& owner)
    : pool{std::move(storage), std::move(tokenizers), std::move(config),
           attached, owner},
      live{pool} {}

  // Any teardown path (attach failure destroying the catalog under a live
  // session) must wait out in-flight tasks before the entries they reference
  // die -- so it happens here, ahead of member destruction, not in ~FeedPool.
  ~InvertedFeedSession() {
    try {
      pool.executor.WorkOnTasks();
    } catch (...) {
    }
  }

  FeedPool pool;
  LiveFeed live;
};

// The commit-time entry points query::Transaction drives. Free functions rather
// than a virtual interface: the session is a connector type the transaction can
// name but not complete, and there was only ever one implementation.
uint64_t PrepareInvertedFeed(InvertedFeedSession& feed) {
  return feed.live.Prepare();
}

void FinishInvertedFeed(InvertedFeedSession& feed, uint64_t last_tick,
                        std::optional<search::WalCursor> cursor) {
  feed.live.Finish(last_tick, cursor);
}

void AbortInvertedFeed(InvertedFeedSession& feed) { feed.live.Abort(); }

InvertedStoreIndex::InvertedStoreIndex(
  duckdb::CreateIndexInput& input, duckdb::idx_t index_id,
  std::shared_ptr<search::InvertedIndexStorage> storage,
  std::shared_ptr<const InvertedIndexConfig> config,
  catalog::IndexTokenizers tokenizers)
  : BoundIndex(input.name, kTypeName, input.constraint_type, input.column_ids,
               input.table_io_manager, input.unbound_expressions, input.db),
    _index_id{index_id},
    _storage{std::move(storage)},
    _config{std::move(config)},
    _tokenizers{std::move(tokenizers)} {
  SDB_ASSERT(_config);
}

std::shared_ptr<InvertedFeedSession>
InvertedStoreIndex::EnsureInvertedFeedSession() {
  auto* committing = CurrentCommittingContext();
  SDB_ASSERT(committing);
  // A commit feeds one session per index. The entry can be republished under
  // it -- an online build finishing, an ALTER INDEX committing -- and
  // rebuilding the session then would strand the segments this commit has
  // already staged in the one it is replacing.
  if (auto engaged = committing->InvertedFeed(_index_id)) {
    return engaged;
  }
  // The configuration is decoded once, when the index object is built, and a
  // reshape replaces the whole object -- so a pool that is still here was
  // built against the config that is still there and never needs rebuilding.
  // ALTER INDEX ... SET applies through InvertedIndexStorage::ApplyOptions
  // instead, which reconfigures the live writer and the maintenance loops.
  if (!_feed) {
    SDB_ENSURE(_storage, "inverted index feed: storage ", _index_id,
               " missing");
    _feed = std::make_shared<InvertedFeedSession>(_storage, _tokenizers,
                                                  _config, db, *this);
  }
  return _feed;
}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  auto* conn = CurrentCommittingContext();
  if (conn == nullptr || chunk.size() == 0) {
    return {};
  }
  const auto& engaged = EnsureInvertedFeedSession();
  conn->EngageInvertedFeed(_index_id, engaged);
  engaged->live.FeedChunkParallel(chunk, row_ids);
  return {};
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
    return;
  }
  const auto& engaged = EnsureInvertedFeedSession();
  auto& session = *engaged;
  if (!_config->pk.index_term) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("inverted index \"", name.GetIdentifierName(),
              "\" was created WITH (store_pk = 'none') and does not "
              "index row PKs: DELETE/UPDATE cannot maintain it; drop "
              "the index first or recreate it without store_pk = "
              "'none'"));
  }
  conn->EngageInvertedFeed(_index_id, engaged);
  const auto& storage = session.pool.storage;
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  if (storage->IsDeleteLogOpen()) {
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
    // Reads like a use-after-move and is not: AppendDeleteLog only takes the
    // vector when it accepts it, and returns false without touching it once the
    // log is latched. Then these rows are past publication and delete natively.
    if (!logged.empty() && !storage->AppendDeleteLog(std::move(logged))) {
      absl::c_move(logged, std::back_inserter(native));
    }
    if (!native.empty()) {
      session.live.FeedDeletesInline(native.size(),
                                     [&](size_t i) { return native[i]; });
    }
    return;
  }
  session.live.FeedDeletesInline(
    count, [&](size_t i) { return data[fmt.sel->get_index(i)]; });
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

std::string InvertedStoreIndex::GetConstraintViolationMessage(
  duckdb::VerifyExistenceType, idx_t, duckdb::DataChunk&) {
  return "inverted store index constraint violation";
}

duckdb::unique_ptr<duckdb::BoundIndex> InvertedStoreIndex::Create(
  duckdb::CreateIndexInput& input) {
  // Everything this needs is in the record duckdb read back: the id names the
  // entry, and the entry says the rest. No injection pass and no held
  // definition -- the registry builds the index the way it builds an ART.
  const auto& record = input.storage_info.options;
  const auto index_id = IdOption(record, kIndexIdOption);
  const auto entry = FindIndexEntry(&input.context, input.db, index_id);
  SDB_ENSURE(entry, "inverted index: catalog entry for ", index_id, " missing");
  // A rebind (an ALTER-driven table rebuild, a re-bind after replay) must not
  // open a second writer over the same directory, so it adopts the storage the
  // index already registered under this name is holding.
  std::shared_ptr<search::InvertedIndexStorage> storage;
  auto& indexes = entry->Cast<duckdb::DuckIndexEntry>().GetDataTableInfo();
  for (auto& index : indexes.GetIndexes().Indexes()) {
    if (index.IsBound() && index.GetIndexName() == input.name &&
        index.GetIndexType() == std::string{kTypeName}) {
      storage = index.Cast<InvertedStoreIndex>().Storage();
      break;
    }
  }
  const auto& index_entry = entry->Cast<catalog::InvertedIndexEntry>();
  return duckdb::make_uniq<InvertedStoreIndex>(
    input, index_id, std::move(storage), index_entry.Config(),
    index_entry.ResolveTokenizers(input.context));
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext, const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  return StorageRecord(*this);
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToWAL(
  const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  return StorageRecord(*this);
}

duckdb::IndexType InvertedStoreIndex::GetInvertedIndexType() {
  duckdb::IndexType type;
  type.name = kTypeName;
  type.create_instance = &InvertedStoreIndex::Create;
  type.create_plan = &SereneDBCreateIndexPlan;
  type.defer_implicit_bind = true;
  return type;
}

std::shared_ptr<search::InvertedIndexStorage> PublishInvertedIndex(
  duckdb::ClientContext& context, catalog::InvertedIndexEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs) {
  const auto& options = entry.Config()->settings;
  auto storage = search::InvertedIndexStorage::Create(
    entry.catalog.GetOid(), entry.schema.oid, relation.oid, entry.oid, options,
    entry.TopKScorer(context), /*is_new=*/true);
  storage->ApplyOptions(options);
  entry.AdoptStorage(storage);
  auto* table = dynamic_cast<duckdb::DuckTableEntry*>(&relation);
  if (table == nullptr) {
    return storage;
  }
  auto& data = table->GetStorage();
  duckdb::CreateIndexInput input{
    context,      duckdb::TableIOManager::Get(data),
    data.db,      entry.index_constraint_type,
    entry.name,   entry.column_ids,
    bound_exprs,  duckdb::IndexStorageInfo{entry.name},
    entry.options};
  data.GetDataTableInfo()->GetIndexes().AddIndex(
    duckdb::make_uniq<InvertedStoreIndex>(input, entry.oid, storage,
                                          entry.Config(),
                                          entry.ResolveTokenizers(context)));
  return storage;
}

}  // namespace sdb::connector
