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

#include "search/wal_recovery.h"

#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <iresearch/index/index_writer.hpp>
#include <limits>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/search_sink_writer.hpp"
#include "rest_server/serened_single.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::search {
namespace {

constexpr size_t kFlushThreshold = 2048;

struct RowState {
  // Empty cell = column not seen in this WAL window (flushed as null).
  std::vector<std::string> cells;
  bool has_put = false;
  bool deleted = false;
};

struct ShardState {
  std::shared_ptr<InvertedIndexShard> shard;
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<const catalog::Table> table;
  ObjectId table_object_id;
  Tick start_tick = 0;
  Tick end_tick = 0;

  std::vector<catalog::Column::Id> indexed_column_ids;
  containers::FlatHashMap<catalog::Column::Id, size_t> col2index;

  struct IndexedColumn {
    catalog::Column::Id id;
    duckdb::LogicalType type;
  };
  std::vector<IndexedColumn> indexed_columns;

  containers::FlatHashMap<std::string, RowState> pk2state;
  std::vector<std::string> ordered_pks;

  // Lazy: only opened on first matching WAL entry, so shards with no entries
  // in the scanned range never touch iresearch.
  std::optional<irs::IndexWriter::Transaction> trx;
  std::optional<connector::SearchSinkInsertBaseImpl> insert_sink;
  std::optional<connector::SearchSinkDeleteBaseImpl> delete_sink;

  // [ObjectId(8 BE)][ColumnId(8 BE)] prefix; pk gets appended then
  // truncated per cell during flush.
  std::string full_key_prefix;

  uint64_t total_inserted = 0;
  uint64_t total_deleted = 0;
};

bool ResolveShardMetadata(ShardState& s, const catalog::Snapshot& snapshot) {
  auto index_obj =
    snapshot.template GetObject<catalog::Index>(s.shard->GetIndexId());
  if (!index_obj) {
    return false;
  }
  auto inverted =
    std::dynamic_pointer_cast<const catalog::InvertedIndex>(index_obj);
  if (!inverted) {
    return false;
  }
  auto table =
    snapshot.template GetObject<catalog::Table>(index_obj->GetRelationId());
  if (!table) {
    return false;
  }

  auto column_ids = index_obj->GetColumnIds();
  if (column_ids.empty()) {
    return false;
  }

  s.indexed_column_ids.assign(column_ids.begin(), column_ids.end());
  s.col2index.reserve(s.indexed_column_ids.size());
  for (size_t i = 0; i < s.indexed_column_ids.size(); ++i) {
    s.col2index.emplace(s.indexed_column_ids[i], i);
  }

  s.indexed_columns.reserve(s.indexed_column_ids.size());
  const auto& table_columns = table->Columns();
  for (auto col_id : s.indexed_column_ids) {
    const catalog::Column* found = nullptr;
    for (const auto& col : table_columns) {
      if (col.id == col_id) {
        found = &col;
        break;
      }
    }
    if (!found) {
      return false;
    }
    s.indexed_columns.push_back({col_id, found->type});
  }

  s.index = std::move(inverted);
  s.table = std::move(table);
  s.table_object_id = s.table->GetId();

  s.full_key_prefix.resize(sizeof(ObjectId) + sizeof(catalog::Column::Id));
  absl::big_endian::Store64(s.full_key_prefix.data(), s.table_object_id.id());
  return true;
}

void EnsureTrxOpen(ShardState& s,
                   const std::shared_ptr<const catalog::Snapshot>& snapshot) {
  if (s.trx.has_value()) {
    return;
  }
  s.trx.emplace(s.shard->GetTransaction());
  auto analyzer_provider = connector::MakeAnalyzerProvider(snapshot, *s.index);
  s.insert_sink.emplace(*s.trx, std::move(analyzer_provider),
                        s.indexed_column_ids);
  s.delete_sink.emplace(*s.trx);
}

void FlushShard(ShardState& s,
                const std::shared_ptr<const catalog::Snapshot>& snapshot) {
  if (s.ordered_pks.empty()) {
    return;
  }

  std::vector<std::pair<std::string_view, const RowState*>> insert_entries;
  std::vector<std::string_view> delete_only_pks;
  insert_entries.reserve(s.ordered_pks.size());

  for (const auto& pk : s.ordered_pks) {
    auto it = s.pk2state.find(pk);
    SDB_ASSERT(it != s.pk2state.end());
    const auto& state = it->second;
    if (state.deleted) {
      delete_only_pks.emplace_back(pk);
    } else if (state.has_put) {
      insert_entries.emplace_back(std::string_view{pk}, &state);
    }
  }

  if (insert_entries.empty() && delete_only_pks.empty()) {
    s.pk2state.clear();
    s.ordered_pks.clear();
    return;
  }

  EnsureTrxOpen(s, snapshot);

  // Put-affected pks get a remove first to clear stale fields from the old
  // doc — mirrors SearchSinkUpdateWriter.
  const size_t delete_batch_size =
    insert_entries.size() + delete_only_pks.size();
  s.delete_sink->InitImpl(delete_batch_size);
  for (const auto& [pk, _] : insert_entries) {
    s.delete_sink->DeleteRowImpl(pk);
  }
  for (auto pk : delete_only_pks) {
    s.delete_sink->DeleteRowImpl(pk);
  }
  s.delete_sink->FinishImpl();
  s.total_deleted += delete_only_pks.size();

  if (!insert_entries.empty()) {
    s.insert_sink->InitImpl(insert_entries.size());
    auto& key_buffer = s.full_key_prefix;
    for (size_t col_idx = 0; col_idx < s.indexed_columns.size(); ++col_idx) {
      const auto& col = s.indexed_columns[col_idx];
      if (!s.insert_sink->SwitchColumnImpl(col.type, /*have_nulls=*/true,
                                           col.id)) {
        continue;
      }
      absl::big_endian::Store(key_buffer.data() + sizeof(ObjectId), col.id);
      for (const auto& [pk, state] : insert_entries) {
        const auto& cell = state->cells[col_idx];

        key_buffer.resize(sizeof(ObjectId) + sizeof(catalog::Column::Id));
        key_buffer.append(pk.data(), pk.size());

        rocksdb::Slice cell_slice{cell.data(), cell.size()};
        std::array<rocksdb::Slice, 1> slices{cell_slice};
        s.insert_sink->WriteImpl(slices, key_buffer);
      }
    }
    s.insert_sink->FinishImpl();
    s.total_inserted += insert_entries.size();
  }

  s.pk2state.clear();
  s.ordered_pks.clear();
}

class FanoutBatchHandler final : public rocksdb::WriteBatch::Handler {
 public:
  FanoutBatchHandler(
    containers::FlatHashMap<ObjectId, std::vector<ShardState*>>& table2shards,
    uint32_t default_cf_id, rocksdb::SequenceNumber batch_sequence)
    : _table2shards{table2shards},
      _default_cf_id{default_cf_id},
      _batch_sequence{batch_sequence} {}

  rocksdb::Status PutCF(uint32_t cf_id, const rocksdb::Slice& key,
                        const rocksdb::Slice& value) override {
    ForEachMatchingShard(
      cf_id, key, [&](ShardState& s, size_t col_idx, std::string_view pk) {
        auto& state = s.pk2state[std::string{pk}];
        if (state.cells.empty()) {
          state.cells.resize(s.indexed_columns.size());
          s.ordered_pks.emplace_back(pk);
        }
        state.cells[col_idx].assign(value.data(), value.size());
        state.has_put = true;
        state.deleted = false;
      });
    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteCF(uint32_t cf_id, const rocksdb::Slice& key) override {
    ForEachMatchingShard(
      cf_id, key, [&](ShardState& s, size_t /*col_idx*/, std::string_view pk) {
        auto& state = s.pk2state[std::string{pk}];
        if (state.cells.empty()) {
          state.cells.resize(s.indexed_columns.size());
          s.ordered_pks.emplace_back(pk);
        }
        state.deleted = true;
      });
    return rocksdb::Status::OK();
  }

  rocksdb::Status SingleDeleteCF(uint32_t cf_id,
                                 const rocksdb::Slice& key) override {
    return DeleteCF(cf_id, key);
  }

  rocksdb::Status DeleteRangeCF(uint32_t, const rocksdb::Slice&,
                                const rocksdb::Slice&) override {
    return rocksdb::Status::OK();
  }

  void LogData(const rocksdb::Slice&) override {}
  rocksdb::Status MarkNoop(bool) override { return rocksdb::Status::OK(); }
  rocksdb::Status MarkBeginPrepare(bool) override {
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkEndPrepare(const rocksdb::Slice&) override {
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkCommit(const rocksdb::Slice&) override {
    return rocksdb::Status::OK();
  }
  rocksdb::Status MarkRollback(const rocksdb::Slice&) override {
    return rocksdb::Status::OK();
  }

 private:
  template<typename Fn>
  void ForEachMatchingShard(uint32_t cf_id, const rocksdb::Slice& key,
                            Fn&& fn) {
    if (cf_id != _default_cf_id) {
      return;
    }
    constexpr size_t kPrefix = sizeof(ObjectId) + sizeof(catalog::Column::Id);
    if (key.size() <= kPrefix) {
      return;
    }
    ObjectId key_object_id{absl::big_endian::Load64(key.data())};
    auto table_it = _table2shards.find(key_object_id);
    if (table_it == _table2shards.end()) {
      return;
    }

    catalog::Column::Id col_id = absl::big_endian::Load<catalog::Column::Id>(
      key.data() + sizeof(ObjectId));
    std::string_view pk{key.data() + kPrefix, key.size() - kPrefix};

    for (auto* s : table_it->second) {
      // start_tick is exclusive: shard already has everything up to and
      // including start_tick in its persisted payload.
      if (_batch_sequence <= s->start_tick) {
        continue;
      }
      auto col_it = s->col2index.find(col_id);
      if (col_it == s->col2index.end()) {
        continue;
      }
      fn(*s, col_it->second, pk);
    }
  }

  containers::FlatHashMap<ObjectId, std::vector<ShardState*>>& _table2shards;
  uint32_t _default_cf_id;
  rocksdb::SequenceNumber _batch_sequence;
};

}  // namespace

void WalRecovery::Run() {
  auto& server = SerenedServer::Instance();
  auto& engine = server.getFeature<EngineFeature>().engine();
  auto& rdb = basics::downCast<RocksDBEngineCatalog>(engine);
  SDB_ASSERT(!rdb.inRecovery());
  const Tick end_tick = rdb.recoveryTick();

  auto& catalog_feature = server.getFeature<catalog::CatalogFeature>();
  auto snapshot = catalog_feature.Global().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);

  // Walk the catalog and collect every inverted-index shard whose persisted
  // tick lags the engine's recovered tick. Caught-up shards are promoted by
  // their own InitPostRecovery callback, so we only deal with lagging ones.
  std::vector<ShardState> shards;
  Tick min_start_tick = std::numeric_limits<Tick>::max();
  for (const auto& database : snapshot->GetDatabases()) {
    for (const auto& schema : snapshot->GetSchemas(database->GetId())) {
      for (const auto& idx :
           snapshot->GetIndexes(database->GetId(), schema->GetName())) {
        if (idx->GetType() != catalog::ObjectType::InvertedIndex) {
          continue;
        }
        auto raw_shard = snapshot->GetIndexShard(idx->GetId());
        auto inv_shard =
          std::dynamic_pointer_cast<InvertedIndexShard>(raw_shard);
        if (!inv_shard) {
          continue;
        }
        const Tick persisted = inv_shard->GetRecoveryTick();
        if (persisted >= end_tick) {
          continue;
        }
        ShardState state;
        state.shard = std::move(inv_shard);
        state.start_tick = persisted;
        state.end_tick = end_tick;
        if (!ResolveShardMetadata(state, *snapshot)) {
          SDB_FATAL("xxxxx", Logger::SEARCH,
                    "WAL recovery: could not resolve catalog metadata for "
                    "inverted index '",
                    state.shard->GetId().id(), "'");
        }
        min_start_tick = std::min(min_start_tick, state.start_tick);
        shards.push_back(std::move(state));
      }
    }
  }

  if (shards.empty()) {
    return;
  }

  containers::FlatHashMap<ObjectId, std::vector<ShardState*>> table2shards;
  for (auto& s : shards) {
    table2shards[s.table_object_id].push_back(&s);
  }

  auto* db = rdb.db();
  SDB_ASSERT(db);
  auto* default_cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(default_cf);
  const uint32_t default_cf_id = default_cf->GetID();

  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  {
    auto s =
      db->GetUpdatesSince(min_start_tick, &iter,
                          rocksdb::TransactionLogIterator::ReadOptions(true));
    if (!s.ok()) {
      SDB_WARN("xxxxx", Logger::SEARCH,
               "WAL recovery: failed to open WAL iterator from tick ",
               min_start_tick, ": ", s.ToString(), " — promoting ",
               shards.size(), " shard(s) without replay");
      for (auto& s_entry : shards) {
        s_entry.shard->FinishCreation();
        s_entry.shard->StartTasks();
      }
      return;
    }
  }

  auto maybe_flush_large_shards = [&]() {
    for (auto& s : shards) {
      if (s.ordered_pks.size() >= kFlushThreshold) {
        FlushShard(s, snapshot);
      }
    }
  };

  bool replay_failed = false;
  while (iter && iter->Valid()) {
    auto status = iter->status();
    if (!status.ok()) {
      SDB_WARN("xxxxx", Logger::SEARCH,
               "WAL recovery: iterator error: ", status.ToString());
      replay_failed = true;
      break;
    }

    auto batch = iter->GetBatch();
    if (batch.sequence > end_tick) {
      break;
    }

    FanoutBatchHandler handler{table2shards, default_cf_id, batch.sequence};
    status = batch.writeBatchPtr->Iterate(&handler);
    if (!status.ok()) {
      SDB_WARN("xxxxx", Logger::SEARCH,
               "WAL recovery: batch iterate failed at seq ", batch.sequence,
               ": ", status.ToString());
      replay_failed = true;
      break;
    }

    maybe_flush_large_shards();
    iter->Next();
  }

  if (replay_failed) {
    // Discard partial trx contents so the persisted tick doesn't advance
    // and the next restart retries the same range.
    for (auto& s : shards) {
      if (s.trx.has_value()) {
        s.delete_sink.reset();
        s.insert_sink.reset();
        s.trx->Reset();
        s.trx.reset();
      }
      s.shard->FinishCreation();
      s.shard->StartTasks();
    }
    return;
  }

  for (auto& s : shards) {
    FlushShard(s, snapshot);
  }

  // Destruct trx before CommitUnsafe so pending inserts reach the
  // IndexWriter — otherwise Commit takes the "no changes" fast-path and
  // skips meta_payload_provider, which is what persists the recovery tick.
  for (auto& s : shards) {
    s.delete_sink.reset();
    s.insert_sink.reset();
    s.trx.reset();

    s.shard->SetRecoveredTick(s.end_tick);

    CommitResult code = CommitResult::Undefined;
    auto r = s.shard->CommitUnsafe(/*wait=*/true, nullptr, code);
    if (!r.res.ok()) {
      SDB_WARN("xxxxx", Logger::SEARCH,
               "WAL recovery: commit failed for index '", s.shard->GetId().id(),
               "': ", r.res.errorMessage());
      continue;
    }
    SDB_INFO(
      "xxxxx", Logger::SEARCH, "WAL recovery: index '", s.shard->GetId().id(),
      "' replayed (", s.start_tick, ", ", s.end_tick,
      "], inserted=", s.total_inserted, ", deleted=", s.total_deleted,
      ", commit_code=", static_cast<int>(code), ", commit_time_ms=", r.time_ms);
  }

  for (auto& s : shards) {
    s.shard->FinishCreation();
    s.shard->StartTasks();
  }
}

}  // namespace sdb::search
