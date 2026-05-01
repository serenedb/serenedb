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
#include <chrono>
#include <iresearch/index/index_writer.hpp>
#include <limits>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"
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

constexpr size_t kFlushThreshold = 1 << 16;

// Last-op-wins per pk. Invalid is the default after lazy creation; the next
// PutCF/DeleteCF transitions it. Put after Delete and Delete after Put both
// just overwrite the field -- no priority logic needed at flush time.
enum class RowOp : uint8_t {
  Invalid,
  Put,
  Delete,
};

struct Row {
  // empty view = column not seen
  std::vector<std::string_view> indexed_cols;
  std::string_view full_key;
  RowOp op = RowOp::Invalid;
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

  // pk -> state. Keys are string_view into live_batches; valid until flush.
  containers::FlatHashMap<std::string_view, Row> pk2row;

  // Lazy: only opened on first matching WAL entry, so shards with no entries
  // in the scanned range never touch iresearch.
  std::optional<irs::IndexWriter::Transaction> trx;
  std::optional<connector::SearchSinkInsertBaseImpl> insert_sink;
  std::optional<connector::SearchSinkDeleteBaseImpl> delete_sink;

  // [ObjectId(8 BE)][ColumnId(8 BE)] prefix; pk gets appended then
  // truncated per col during flush.
  std::string full_key_prefix;

  uint64_t total_inserted = 0;
  uint64_t total_deleted = 0;

  Row& GetRow(std::string_view pk) {
    auto [it, inserted] = pk2row.try_emplace(pk);
    if (inserted) {
      it->second.indexed_cols.resize(indexed_columns.size());
    }
    return it->second;
  }

  static auto GetComparator() {
    return [](const ShardState* a, const ShardState* b) {
      return a->start_tick < b->start_tick;
    };
  }
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
                const std::shared_ptr<const catalog::Snapshot>& snapshot,
                rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf) {
  if (s.pk2row.empty()) {
    return;
  }

  irs::Finally _ = [&] noexcept { s.pk2row.clear(); };

  // Materialise put-affected rows for the column-major insert phase. The pk
  // is the suffix of full_key after the [ObjectId][ColumnId] prefix, so we
  // don't need to keep it alongside the Row pointer.
  std::vector<const Row*> insert_entries;
  insert_entries.reserve(s.pk2row.size());
  for (const auto& [_pk, row] : s.pk2row) {
    switch (row.op) {
      case RowOp::Put:
        insert_entries.push_back(&row);
        break;
      case RowOp::Delete:
        break;
      case RowOp::Invalid:
        SDB_UNREACHABLE();
    }
  }

  EnsureTrxOpen(s, snapshot);

  // Put-affected pks get a remove first to mirror SearchSinkUpdateWriter
  // (clear stale fields from the prior doc).
  s.delete_sink->InitImpl(s.pk2row.size());
  for (const auto& [pk, _row] : s.pk2row) {
    s.delete_sink->DeleteRowImpl(pk);
  }
  s.delete_sink->FinishImpl();
  s.total_deleted += s.pk2row.size() - insert_entries.size();

  if (!insert_entries.empty()) {
    auto& sink = *s.insert_sink;
    sink.InitImpl(insert_entries.size());
    auto& get_key_buffer = s.full_key_prefix;
    rocksdb::ReadOptions read_opts;
    rocksdb::PinnableSlice value_buffer;
    for (size_t col_idx = 0; col_idx < s.indexed_columns.size(); ++col_idx) {
      const auto& col = s.indexed_columns[col_idx];
      const bool switched = sink.SwitchColumnImpl(col.type, true, col.id);
      SDB_ASSERT(switched);
      absl::big_endian::Store(get_key_buffer.data() + sizeof(ObjectId), col.id);
      static constexpr auto kPrefix =
        sizeof(ObjectId) + sizeof(catalog::Column::Id);
      for (const auto* row : insert_entries) {
        std::string_view cell = row->indexed_cols[col_idx];
        if (!cell.empty()) {
          rocksdb::Slice slice{cell.data(), cell.size()};
          sink.WriteImpl(std::span{&slice, 1}, row->full_key);
        } else {
          // This is a very bad code that exists because we don't support PATCH
          // queries for inverted index. It happens when we update a column that
          // is indexed but not included in the UPDATE statement: the new value
          // is missing from the WAL window, so we have to fetch it from the db
          // to be able to write it to iresearch.
          std::string_view pk = row->full_key.substr(kPrefix);
          get_key_buffer.resize(kPrefix);
          get_key_buffer.append(pk.data(), pk.size());
          value_buffer.Reset();
          auto status = db.Get(read_opts, &cf, get_key_buffer, &value_buffer);
          SDB_FATAL_IF("xxxxx", Logger::SEARCH, !status.ok(),
                       "WAL recovery: rocksdb Get failed for index '",
                       s.shard->GetId().id(), "' col=", col.id, ": ",
                       status.ToString());
          rocksdb::Slice slice{value_buffer.data(), value_buffer.size()};
          sink.WriteImpl(std::span{&slice, 1}, row->full_key);
        }
      }
    }
    sink.FinishImpl();
  }

  s.total_inserted += insert_entries.size();
}

struct PerTableShards {
  // shards[0..started) -- the prefix of shards whose start tick < curr tick.
  size_t started = 0;
  std::vector<ShardState*> shards;
};

using TableToShards = containers::FlatHashMap<ObjectId, PerTableShards>;

class FanoutBatchHandler final : public rocksdb::WriteBatch::Handler {
 public:
  FanoutBatchHandler(TableToShards& table2shards, uint32_t default_cf_id,
                     rocksdb::SequenceNumber batch_sequence)
    : _table2shards{table2shards},
      _default_cf_id{default_cf_id},
      _batch_sequence{batch_sequence} {}

  rocksdb::Status PutCF(uint32_t cf_id, const rocksdb::Slice& key,
                        const rocksdb::Slice& value) override {
    ForEachMatchingShard(cf_id, key, [&](Row& row, size_t col_idx) {
      row.indexed_cols[col_idx] = {value.data(), value.size()};
      row.op = RowOp::Put;
    });
    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteCF(uint32_t cf_id, const rocksdb::Slice& key) override {
    ForEachMatchingShard(
      cf_id, key, [&](Row& row, size_t col_idx) { row.op = RowOp::Delete; });
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
    static constexpr auto kPrefix =
      sizeof(ObjectId) + sizeof(catalog::Column::Id);
    if (key.size() <= kPrefix) {
      return;
    }
    ObjectId id{absl::big_endian::Load64(key.data())};
    auto table_it = _table2shards.find(id);
    if (table_it == _table2shards.end()) {
      return;
    }

    auto& [started, shards] = table_it->second;
    SDB_ASSERT(std::ranges::is_sorted(shards, ShardState::GetComparator()));
    // start_tick is exclusive: a shard already has everything up.
    while (started < shards.size() &&
           shards[started]->start_tick < _batch_sequence) {
      ++started;
    }
    if (started == 0) {
      return;
    }

    const auto col_id = absl::big_endian::Load<catalog::Column::Id>(
      key.data() + sizeof(ObjectId));
    std::string_view pk{key.data() + kPrefix, key.size() - kPrefix};
    std::string_view full_key{key.data(), key.size()};
    for (auto* s : std::span{shards.data(), started}) {
      auto col_it = s->col2index.find(col_id);
      if (col_it == s->col2index.end()) {
        // unindexed column
        continue;
      }
      auto& row = s->GetRow(pk);
      row.full_key = full_key;
      fn(row, col_it->second);
    }
  }

  TableToShards& _table2shards;
  uint32_t _default_cf_id;
  rocksdb::SequenceNumber _batch_sequence;
};

}  // namespace

void WalRecovery::Run() {
  auto recovery_begin = std::chrono::steady_clock::now();

  auto& server = SerenedServer::Instance();
  auto& engine = server.getFeature<EngineFeature>().engine();
  auto& rdb = basics::downCast<RocksDBEngineCatalog>(engine);
  SDB_ASSERT(!rdb.inRecovery());
  const Tick end_tick = rdb.recoveryTick();

  auto& catalog_feature = server.getFeature<catalog::CatalogFeature>();
  auto snapshot = catalog_feature.Global().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);

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
        SDB_FATAL_IF("xxxxx", Logger::SEARCH,
                     !ResolveShardMetadata(state, *snapshot),
                     "WAL recovery: could not resolve catalog metadata for "
                     "inverted index '",
                     state.shard->GetId().id(), "'");
        min_start_tick = std::min(min_start_tick, state.start_tick);
        shards.push_back(std::move(state));
      }
    }
  }

  if (shards.empty()) {
    return;
  }

  TableToShards table2shards;
  for (auto& s : shards) {
    table2shards[s.table_object_id].shards.push_back(&s);
  }
  for (auto& [_, per_table] : table2shards) {
    std::ranges::sort(per_table.shards, ShardState::GetComparator());
  }

  auto* db = rdb.db();
  SDB_ASSERT(db);
  auto* default_cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(default_cf);
  const uint32_t default_cf_id = default_cf->GetID();

  SDB_INFO("xxxxx", Logger::SEARCH,
           "Starting WAL recovery, to skip it use the flag: "
           "\"--search.skip-wal-recovery\"");

  std::unique_ptr<rocksdb::TransactionLogIterator> iter;

  auto s = db->GetUpdatesSince(
    min_start_tick, &iter, rocksdb::TransactionLogIterator::ReadOptions(true));
  SDB_FATAL_IF("xxxxx", Logger::SEARCH, !s.ok(),
               "WAL recovery: failed to open WAL iterator from tick ",
               min_start_tick, ": ", s.ToString());
  SDB_ASSERT(iter);

  std::vector<rocksdb::BatchResult> live_batches;
  auto flush_all = [&]() {
    for (auto& s : shards) {
      FlushShard(s, snapshot, *db, *default_cf);
    }
    live_batches.clear();
  };

  while (iter->Valid()) {
    auto status = iter->status();
    SDB_FATAL_IF("xxxxx", Logger::SEARCH, !status.ok(),
                 "WAL recovery: iterator error: ", status.ToString());

    auto batch = iter->GetBatch();
    if (batch.sequence > end_tick) {
      break;
    }

    FanoutBatchHandler handler{table2shards, default_cf_id, batch.sequence};
    status = batch.writeBatchPtr->Iterate(&handler);
    SDB_FATAL_IF("xxxxx", Logger::SEARCH, !status.ok(),
                 "WAL recovery: batch iterate failed at seq ", batch.sequence,
                 ": ", status.ToString());

    live_batches.emplace_back(std::move(batch));

    const auto need_flush = absl::c_any_of(shards, [&](const ShardState& s) {
      return s.pk2row.size() >= kFlushThreshold;
    });
    if (need_flush) {
      flush_all();
    }

    iter->Next();
  }

  auto status = iter->status();
  SDB_FATAL_IF(
    "xxxxx", Logger::SEARCH, !iter->status().ok(),
    "WAL recovery: iterator error after last batch: ", status.ToString());

  flush_all();

  for (auto& s : shards) {
    s.delete_sink.reset();
    s.insert_sink.reset();
    s.trx.reset();

    s.shard->SetRecoveredTick(s.end_tick);

    CommitResult code = CommitResult::Undefined;
    auto r = s.shard->CommitUnsafe(/*wait=*/true, nullptr, code);
    SDB_FATAL_IF("xxxxx", Logger::SEARCH, !r.res.ok(),
                 "WAL recovery: commit failed for index '",
                 s.shard->GetId().id(), "': ", r.res.errorMessage());
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

  uint64_t recovery_time_ms =
    std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - recovery_begin)
      .count();
  SDB_INFO("xxxxx", Logger::SEARCH, "WAL recovery: completed in ",
           recovery_time_ms, " ms, indexes=", shards.size());
}

}  // namespace sdb::search
