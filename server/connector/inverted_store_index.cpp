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
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <deque>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iterator>
#include <span>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/inverted_index.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/index_expression.hpp"
#include "connector/primary_key.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/scorer_options.h"
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

constexpr const char* kIndexIdOption = "catalog_oid";

duckdb::idx_t IdOption(const duckdb::case_insensitive_map_t<duckdb::Value>& o,
                       const char* key) {
  const auto* value = catalog::FindOption(o, key);
  if (!value || value->IsNull()) {
    return 0;
  }
  return value->GetValue<uint64_t>();
}

duckdb::IndexStorageInfo StorageRecord(const InvertedStoreIndex& index) {
  duckdb::IndexStorageInfo info{index.name};
  info.options[kIndexIdOption] = duckdb::Value::UBIGINT(index.IndexId());
  return info;
}

duckdb::idx_t SelectRows(duckdb::Vector& predicate, duckdb::idx_t total,
                         duckdb::SelectionVector& sel) {
  duckdb::UnifiedVectorFormat fmt;
  predicate.ToUnifiedFormat(total, fmt);
  const auto* values = duckdb::UnifiedVectorFormat::GetData<bool>(fmt);
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i < total; ++i) {
    const auto idx = fmt.sel->get_index(i);
    if (fmt.validity.RowIsValid(idx) && values[idx]) {
      sel.set_index(kept++, i);
    }
  }
  return kept;
}

size_t ReplayDepth(duckdb::DatabaseInstance& db) {
  auto& config = duckdb::DBConfig::GetConfig(db);
  duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
  const auto index =
    config.TryGetSettingIndex(std::string{kRecoveryReplayDepthSetting}, option);
  duckdb::Value value;
  size_t depth = 0;
  if (index.IsValid() &&
      config.user_settings.TryGetSetting(index.GetIndex(), value) &&
      !value.IsNull()) {
    depth = value.GetValue<uint32_t>();
  }
  if (depth == 0) {
    depth = 4 * std::max<size_t>(
                  1, duckdb::TaskScheduler::GetScheduler(db).NumberOfThreads());
  }
  return std::clamp<size_t>(depth, 1, 1024);
}

std::unique_ptr<DuckDBSearchSinkInsertWriter> MakeInsertWriter(
  irs::IndexWriter::Transaction& trx, const InvertedIndexConfig& config,
  TokenizerProvider tokenizers) {
  return std::make_unique<DuckDBSearchSinkInsertWriter>(
    trx, std::move(tokenizers), IndexedColumnIds(config),
    MakeEntryInfoProvider(config), config.pk);
}

TokenizerProvider BoundTokenizers(catalog::IndexTokenizers::Bound tokenizers) {
  return [tokenizers = std::move(tokenizers)](irs::field_id id) mutable {
    const auto it = tokenizers.find(id);
    return it == tokenizers.end() ? catalog::ColumnTokenizer{}
                                  : std::move(it->second);
  };
}

constexpr duckdb::idx_t kMinSlotRows = 8 * STANDARD_VECTOR_SIZE;
constexpr size_t kLiveFeedDepth = 2;

}  // namespace

struct InvertedStoreIndex::ReplayOp {
  ReplayOp(bool insert, duckdb::Vector& source, duckdb::idx_t count)
    : rows{duckdb::LogicalType::ROW_TYPE, std::max<duckdb::idx_t>(count, 1)},
      count{count},
      insert{insert} {
    duckdb::VectorOperations::Copy(source, rows, count, 0, 0);
    duckdb::FlatVector::SetSize(rows, count);
  }

  duckdb::DataChunk results;
  duckdb::Vector rows;
  duckdb::idx_t count;
  bool insert;
};

struct InvertedStoreIndex::FeedQueue {
  FeedQueue(DuckDBSinkIndexWriter& insert_writer,
            DuckDBSinkIndexWriter* delete_writer,
            irs::IndexWriter::Transaction& trx, size_t depth)
    : insert_writer{insert_writer},
      delete_writer{delete_writer},
      trx{trx},
      depth{depth} {}

  void Clear() {
    absl::MutexLock lock{&mutex};
    ops.clear();
  }

  bool NotFull() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) {
    return ops.size() < depth;
  }

  DuckDBSinkIndexWriter& insert_writer;
  DuckDBSinkIndexWriter* delete_writer;
  irs::IndexWriter::Transaction& trx;
  const size_t depth;
  absl::Mutex mutex;
  std::deque<std::unique_ptr<ReplayOp>> ops ABSL_GUARDED_BY(mutex);
  bool running ABSL_GUARDED_BY(mutex) = false;
};

struct InvertedStoreIndex::ReplaySession {
  ReplaySession(InvertedStoreIndex& index,
                catalog::IndexTokenizers::Bound tokenizers)
    : trx{index.NewTransaction()},
      insert_writer{MakeInsertWriter(trx, *index._config,
                                     BoundTokenizers(std::move(tokenizers)))},
      delete_writer{trx},
      executor{duckdb::TaskScheduler::GetScheduler(index.db.GetDatabase())},
      queue{*insert_writer, &delete_writer, trx,
            ReplayDepth(index.db.GetDatabase())} {
    const auto cursor = index._storage->GetRecoveryWalCursor();
    const auto& block_manager = index.db.GetStorageManager().GetBlockManager();
    if (cursor.generation == block_manager.GetCheckpointIteration()) {
      durable_offset = cursor.offset;
    }
  }

  ~ReplaySession() {
    queue.Clear();
    try {
      executor.WorkOnTasks();
    } catch (...) {
    }
  }

  irs::IndexWriter::Transaction trx;
  std::unique_ptr<DuckDBSearchSinkInsertWriter> insert_writer;
  DuckDBSearchSinkDeleteWriter delete_writer;
  uint64_t durable_offset = 0;
  duckdb::TaskExecutor executor;
  FeedQueue queue;
};

struct InvertedStoreIndex::LiveFeed {
  explicit LiveFeed(duckdb::DatabaseInstance& db)
    : executor{duckdb::TaskScheduler::GetScheduler(db)} {}

  ~LiveFeed() {
    for (auto& queue : queues) {
      queue.Clear();
    }
    try {
      executor.WorkOnTasks();
    } catch (...) {
    }
  }

  duckdb::TaskExecutor executor;
  std::deque<FeedQueue> queues;
  size_t next = 0;
};

struct InvertedStoreIndex::FeedTask final : duckdb::BaseExecutorTask {
  FeedTask(duckdb::TaskExecutor& executor, InvertedStoreIndex& index,
           FeedQueue& queue)
    : BaseExecutorTask{executor}, index{index}, queue{queue} {}

  void ExecuteTask() final {
    try {
      while (true) {
        std::unique_ptr<ReplayOp> op;
        {
          absl::MutexLock lock{&queue.mutex};
          if (queue.ops.empty()) {
            queue.running = false;
            return;
          }
          op = std::move(queue.ops.front());
          queue.ops.pop_front();
        }
        index.Apply(queue, *op);
      }
    } catch (...) {
      absl::MutexLock lock{&queue.mutex};
      queue.ops.clear();
      queue.running = false;
      throw;
    }
  }

  std::string TaskType() const final { return "InvertedIndexFeed"; }

  InvertedStoreIndex& index;
  FeedQueue& queue;
};

InvertedStoreIndex::InvertedStoreIndex(
  duckdb::CreateIndexInput& input, duckdb::idx_t index_id,
  std::shared_ptr<search::InvertedIndexStorage> storage,
  std::shared_ptr<const InvertedIndexConfig> config,
  catalog::IndexTokenizers tokenizers, bool has_predicate)
  : BoundIndex(input.name, kTypeName, input.constraint_type, input.column_ids,
               input.table_io_manager, input.unbound_expressions, input.db),
    _index_id{index_id},
    _storage{std::move(storage)},
    _config{std::move(config)},
    _tokenizers{std::move(tokenizers)},
    _has_predicate{has_predicate} {
  SDB_ASSERT(_config);
}

InvertedStoreIndex::~InvertedStoreIndex() = default;

irs::IndexWriter::Transaction InvertedStoreIndex::NewTransaction() {
  auto trx = _storage->GetTransaction();
  trx.SetFieldOptions(_config);
  return trx;
}

duckdb::idx_t InvertedStoreIndex::Evaluate(duckdb::DataChunk& chunk,
                                           duckdb::Vector& row_ids,
                                           duckdb::DataChunk& results,
                                           duckdb::Vector& rows) {
  const auto total = chunk.size();
  rows.Reference(row_ids);
  if (bound_expressions.empty()) {
    return total;
  }
  const auto keys = std::span{_config->keys};
  results.Initialize(duckdb::Allocator::DefaultAllocator(), logical_types);
  ExecuteExpressions(chunk, results);
  for (size_t i = 0; i < keys.size(); ++i) {
    const auto* entry = _config->FindEntry(keys[i].field_id);
    if (!entry || entry->IsTokenized()) {
      RejectJsonObjectArrayLeaves(results.data[i], total);
    }
  }
  if (!_has_predicate) {
    return total;
  }
  duckdb::SelectionVector sel{total};
  const auto count = SelectRows(results.data.back(), total, sel);
  if (count != total) {
    rows.Slice(row_ids, sel, count);
    results.Slice(sel, count);
  }
  return count;
}

void InvertedStoreIndex::Feed(DuckDBSinkIndexWriter& writer,
                              irs::IndexWriter::Transaction& trx,
                              duckdb::DataChunk& results, duckdb::Vector& rows,
                              duckdb::idx_t count) {
  if (count != 0) {
    const auto keys = std::span{_config->keys};
    duckdb::UnifiedVectorFormat row_fmt;
    rows.ToUnifiedFormat(count, row_fmt);
    const auto* row_data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(row_fmt);
    std::vector<duckdb::string_t> key_views(count);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      key_views[i] =
        primary_key::SignedKeyTerm(row_data[row_fmt.sel->get_index(i)]);
    }
    std::vector<ExpressionValue> values;
    values.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      const auto first = absl::c_none_of(
        keys.first(i), [&](const catalog::InvertedIndexKey& earlier) {
          return earlier.field_id == keys[i].field_id;
        });
      if (first) {
        values.emplace_back(keys[i].field_id, &results.data[i]);
      }
    }
    FeedChunk(writer, count, PkChunk{.key_terms = key_views, .column = &rows},
              results, {}, values);
  }
  trx.AdvanceQueries(1);
}

InvertedStoreIndex::ReplaySession* InvertedStoreIndex::ReplaySessionForEntry() {
  SDB_ENSURE(_replay, "inverted index ", _index_id,
             ": append outside a commit and a replay");
  const auto offset =
    duckdb::DuckTransactionManager::Get(db).GetReplayCommitOffset();
  if (offset != 0 && offset < _replay->durable_offset) {
    return nullptr;
  }
  return _replay.get();
}

void InvertedStoreIndex::Enqueue(duckdb::TaskExecutor& executor,
                                 FeedQueue& queue,
                                 std::unique_ptr<ReplayOp> op) {
  if (executor.HasError()) {
    return;
  }
  bool schedule = false;
  {
    absl::MutexLock lock{&queue.mutex};
    queue.ops.push_back(std::move(op));
    schedule = !std::exchange(queue.running, true);
    if (!schedule && queue.NotFull()) {
      return;
    }
  }
  if (schedule) {
    executor.ScheduleTask(duckdb::make_uniq<FeedTask>(executor, *this, queue));
  }
  while (true) {
    {
      absl::MutexLock lock{&queue.mutex};
      if (queue.NotFull()) {
        return;
      }
    }
    duckdb::shared_ptr<duckdb::Task> task;
    if (!executor.GetTask(task)) {
      break;
    }
    task->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
  }
  absl::MutexLock lock{&queue.mutex};
  queue.mutex.Await(absl::Condition(&queue, &FeedQueue::NotFull));
}

void InvertedStoreIndex::Apply(FeedQueue& queue, ReplayOp& op) {
  if (op.insert) {
    Feed(queue.insert_writer, queue.trx, op.results, op.rows, op.count);
    return;
  }
  SDB_ASSERT(queue.delete_writer);
  const auto* data = duckdb::FlatVector::GetData<duckdb::row_t>(op.rows);
  std::string key;
  FeedDeletes(*queue.delete_writer, key, op.count,
              [&](size_t i) { return data[i]; });
}

std::unique_ptr<InvertedStoreIndex::ReplayOp> InvertedStoreIndex::CopyInsert(
  duckdb::DataChunk& results, duckdb::Vector& rows, duckdb::idx_t count) {
  auto op = std::make_unique<ReplayOp>(true, rows, count);
  if (results.ColumnCount() != 0) {
    op->results.Initialize(duckdb::Allocator::DefaultAllocator(),
                           results.GetTypes(),
                           std::max<duckdb::idx_t>(count, 1));
    results.Copy(op->results);
  }
  return op;
}

void InvertedStoreIndex::ReplayAppend(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  if (!ReplaySessionForEntry()) {
    return;
  }
  duckdb::DataChunk results;
  duckdb::Vector rows{duckdb::LogicalType::ROW_TYPE, nullptr, 0};
  const auto count = Evaluate(chunk, row_ids, results, rows);
  Enqueue(_replay->executor, _replay->queue, CopyInsert(results, rows, count));
}

void InvertedStoreIndex::ReplayDelete(duckdb::DataChunk& chunk,
                                      duckdb::Vector& row_ids) {
  if (!ReplaySessionForEntry()) {
    return;
  }
  Enqueue(_replay->executor, _replay->queue,
          std::make_unique<ReplayOp>(false, row_ids, chunk.size()));
}

void InvertedStoreIndex::FinishReplay() {
  if (!_replay) {
    return;
  }
  auto& session = *_replay;
  session.executor.WorkOnTasks();
  if (session.trx.GetQueries() != 0) {
    session.trx.RegisterFlush();
    const auto last_tick =
      search::TickDomain::Instance().Next(session.trx.GetQueries() + 1);
    auto& storage_manager = db.GetStorageManager();
    _storage->RecordFlushCursor(
      last_tick, search::WalCursor{
                   storage_manager.GetBlockManager().GetCheckpointIteration(),
                   storage_manager.GetWALSize()});
    SDB_ENSURE(session.trx.Commit(last_tick),
               "inverted index replay: commit failed for index ", _index_id);
  }
  _replay.reset();
}

duckdb::ErrorData InvertedStoreIndex::AppendImpl(duckdb::DataChunk& chunk,
                                                 duckdb::Vector& row_ids) {
  if (chunk.size() == 0) {
    return {};
  }
  auto* conn = CurrentCommittingContext();
  if (!conn) {
    ReplayAppend(chunk, row_ids);
    return {};
  }
  duckdb::DataChunk results;
  duckdb::Vector rows{duckdb::LogicalType::ROW_TYPE, nullptr, 0};
  const auto count = Evaluate(chunk, row_ids, results, rows);
  const auto slots = conn->IndexSlots(_index_id);
  duckdb::idx_t prepared = 0;
  while (prepared < slots.size() && slots[prepared].writer) {
    ++prepared;
  }
  if (prepared == 0) {
    auto& context = conn->GetClientContext();
    auto& trx = conn->EnsureIndexTransaction(_index_id, _storage, _config);
    const auto writer = MakeInsertWriter(trx, *_config, [&](irs::field_id id) {
      return _tokenizers.Acquire(id, context);
    });
    Feed(*writer, trx, results, rows, count);
    conn->RegisterSearchFlush();
    return {};
  }
  if (prepared == 1) {
    Feed(*slots[0].writer, *slots[0].transaction, results, rows, count);
  } else if (count != 0) {
    if (!_live) {
      _live = std::make_unique<LiveFeed>(db.GetDatabase());
      for (duckdb::idx_t k = 0; k < prepared; ++k) {
        _live->queues.emplace_back(*slots[k].writer, nullptr,
                                   *slots[k].transaction, kLiveFeedDepth);
      }
    }
    auto& queue = _live->queues[_live->next++ % _live->queues.size()];
    Enqueue(_live->executor, queue, CopyInsert(results, rows, count));
  }
  conn->RegisterSearchFlush();
  return {};
}

duckdb::ErrorData InvertedStoreIndex::FinishAppend() {
  if (!_live) {
    return {};
  }
  const auto live = std::move(_live);
  try {
    live->executor.WorkOnTasks();
  } catch (const std::exception& e) {
    return duckdb::ErrorData{e};
  }
  return {};
}

void InvertedStoreIndex::PrepareFeed(query::Transaction& transaction,
                                     duckdb::ClientContext& context,
                                     duckdb::idx_t rows) {
  const auto threads = duckdb::TaskScheduler::QueryThreads(context);
  const auto slots = std::clamp<duckdb::idx_t>(
    rows / kMinSlotRows, 1, std::max<duckdb::idx_t>(1, threads));
  for (duckdb::idx_t k = 0; k < slots; ++k) {
    auto& slot = transaction.EnsureIndexSlot(_index_id, _storage, _config, k);
    if (!slot.writer) {
      slot.writer =
        MakeInsertWriter(*slot.transaction, *_config,
                         BoundTokenizers(_tokenizers.AcquireAll(context)));
    }
  }
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
  if (!_config->pk.index_term) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("inverted index \"", name.GetIdentifierName(),
              "\" was created WITH (store_pk = 'none') and does not "
              "index row PKs: DELETE/UPDATE cannot maintain it; drop "
              "the index first or recreate it without store_pk = "
              "'none'"));
  }
  auto& trx = conn->EnsureIndexTransaction(_index_id, _storage, _config);
  const auto remove = [&](size_t n, auto&& row_at) {
    DuckDBSearchSinkDeleteWriter writer{trx};
    std::string key;
    FeedDeletes(writer, key, n, row_at);
  };
  duckdb::UnifiedVectorFormat fmt;
  row_ids.ToUnifiedFormat(count, fmt);
  const auto* data = duckdb::UnifiedVectorFormat::GetData<duckdb::row_t>(fmt);
  if (_storage->IsDeleteLogOpen()) {
    const auto log_begin = _storage->DeleteLogRowidBegin();
    const auto log_end = _storage->DeleteLogRowidEnd();
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
    if (!logged.empty() && !_storage->AppendDeleteLog(std::move(logged))) {
      absl::c_move(logged, std::back_inserter(native));
    }
    if (!native.empty()) {
      remove(native.size(), [&](size_t i) { return native[i]; });
    }
  } else {
    remove(count, [&](size_t i) { return data[fmt.sel->get_index(i)]; });
  }
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

duckdb::unique_ptr<duckdb::BoundIndex> InvertedStoreIndex::Create(
  duckdb::CreateIndexInput& input) {
  // Everything this needs is in the record duckdb read back: the id names the
  // entry, and the entry says the rest. No injection pass and no held
  // definition -- the registry builds the index the way it builds an ART.
  const auto& record = input.storage_info.options;
  const auto index_id = IdOption(record, kIndexIdOption);
  const auto entry = FindIndexEntry(&input.context, input.db, index_id);
  SDB_ENSURE(entry, "inverted index: catalog entry for ", index_id, " missing");
  const auto& index_entry = entry->Cast<catalog::InvertedIndexEntry>();
  SDB_ENSURE(index_entry.Storage());
  auto index = duckdb::make_uniq<InvertedStoreIndex>(
    input, index_id, index_entry.Storage(), index_entry.Config(),
    index_entry.ResolveTokenizers(input.context),
    static_cast<bool>(index_entry.where_clause));
  index->_replay = std::make_unique<ReplaySession>(
    *index, index->_tokenizers.AcquireAll(input.context));
  return std::move(index);
}

duckdb::IndexStorageInfo InvertedStoreIndex::SerializeToDisk(
  duckdb::QueryContext, const duckdb::case_insensitive_map_t<duckdb::Value>&) {
  SDB_ENSURE(!_storage->IsOutOfSync(), "inverted index ", _index_id,
             " is out of sync with its store table; refusing to checkpoint");
  _storage->CheckpointRefresh();
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
  type.remaps_columns = true;
  return type;
}

PublishedInvertedIndex PublishInvertedIndex(
  duckdb::ClientContext& context, catalog::InvertedIndexEntry& entry,
  duckdb::CatalogEntry& relation,
  const duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& bound_exprs) {
  const auto options = catalog::ResolveSettings(entry.options);
  catalog::ClusterOf(context).LogArtifact(
    duckdb::CatalogType::INDEX_ENTRY, entry.catalog.GetOid(), entry.oid,
    {search::InvertedIndexStorage::GetPath(entry.catalog.GetOid(),
                                           entry.ParentSchemaOid(),
                                           relation.oid, entry.oid)},
    false);
  auto storage = search::InvertedIndexStorage::Create(
    entry.catalog.GetOid(), entry.ParentSchemaOid(), relation.oid, entry.oid,
    options, entry.Config()->top_k_scorer, /*is_new=*/true);
  storage->ApplyOptions(options);
  entry.AdoptStorage(storage);
  if (relation.type != duckdb::CatalogType::TABLE_ENTRY ||
      !relation.Cast<duckdb::TableCatalogEntry>().IsDuckTable()) {
    return {std::move(storage), 0};
  }
  auto& data = relation.Cast<duckdb::DuckTableEntry>().GetStorage();
  duckdb::CreateIndexInput input{
    context,      duckdb::TableIOManager::Get(data),
    data.db,      entry.index_constraint_type,
    entry.name,   entry.column_ids,
    bound_exprs,  duckdb::IndexStorageInfo{entry.name},
    entry.options};
  auto index = duckdb::make_uniq<InvertedStoreIndex>(
    input, entry.oid, storage, entry.Config(), entry.ResolveTokenizers(context),
    static_cast<bool>(entry.where_clause));
  auto publish_lock = data.GetCheckpointLock();
  data.GetDataTableInfo()->GetIndexes().AddIndex(std::move(index));
  return {std::move(storage), data.GetNextRowId()};
}

}  // namespace sdb::connector
