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

#include <absl/algorithm/container.h>
#include <absl/base/internal/endian.h>
#include <absl/cleanup/cleanup.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/write_batch.h>

#include <algorithm>
#include <chrono>
#include <iresearch/index/index_writer.hpp>
#include <limits>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/indexonly_marker.h"
#include "connector/key_utils.hpp"
#include "connector/search_sink_writer.hpp"
#include "query/duckdb_engine.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::search {
namespace {

constexpr size_t kFlushThreshold = 1 << 15;

constexpr size_t kKeyPrefixSize =
  sizeof(ObjectId) + sizeof(catalog::Column::Id);

constexpr std::string_view kSkipHint =
  ". To skip WAL recovery and proceed with stale index content, restart "
  "with --search.skip-wal-recovery (data loss for the unreplayed delta).";

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

  std::vector<catalog::Column::Id> indexed_column_ids;
  containers::FlatHashMap<catalog::Column::Id, size_t> col2index;

  struct IndexedColumn {
    catalog::Column::Id id;
    duckdb::LogicalType type;
    catalog::ColumnStoreMode store_mode;
  };
  std::vector<IndexedColumn> indexed_columns;

  containers::FlatHashMap<std::string_view, Row> pk2row;

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

  static auto ByStartTick() {
    return [](const ShardState* a, const ShardState* b) {
      return a->start_tick < b->start_tick;
    };
  }
};

bool ResolveShardMetadata(ShardState& s, const catalog::Snapshot& snapshot) {
  auto inverted =
    snapshot.template GetObject<catalog::InvertedIndex>(s.shard->GetIndexId());
  if (!inverted) {
    return false;
  }
  auto table =
    snapshot.template GetObject<catalog::Table>(inverted->GetRelationId());
  if (!table) {
    return false;
  }

  auto column_ids = inverted->GetReferencedColumnIds();
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
    auto it = absl::c_find_if(table_columns, [col_id](const auto& col) {
      return col.GetId() == col_id;
    });
    if (it == table_columns.end()) {
      return false;
    }
    s.indexed_columns.emplace_back(col_id, it->type, it->store_mode);
  }

  s.index = std::move(inverted);
  s.table = std::move(table);
  s.table_object_id = s.table->GetId();

  s.full_key_prefix.resize(kKeyPrefixSize);
  absl::big_endian::Store64(s.full_key_prefix.data(), s.table_object_id.id());
  return true;
}

void FlushShard(ShardState& s,
                const std::shared_ptr<const catalog::Snapshot>& snapshot,
                rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf,
                Tick last_tick) {
  if (s.pk2row.empty()) {
    return;
  }

  irs::Finally _ = [&] noexcept { s.pk2row.clear(); };

  std::vector<const Row*> insert_entries;
  insert_entries.reserve(s.pk2row.size());
  for (const auto& [_, row] : s.pk2row) {
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

  auto trx = s.shard->GetTransaction();
  auto tokenizer_provider =
    connector::MakeTokenizerProvider(snapshot, *s.index);
  auto store_values_provider = connector::MakeStoreValuesProvider(*s.index);
  auto is_text_indexed_provider =
    connector::MakeIsTextIndexedProvider(*s.index);
  auto hnsw_info_provider = connector::MakeHNSWInfoProvider(*s.index);
  // Private read-only txn: expression deserialise + execute need one.
  auto conn = query::DuckDBEngine::Instance().CreateConnection();
  conn->BeginTransaction();
  irs::Finally rollback_on_exit = [&]() noexcept {
    try {
      conn->Rollback();
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  };
  auto& client_context = *conn->context;
  auto expr_tokenizer_provider =
    connector::MakeExpressionTokenizerProvider(snapshot, *s.index);
  auto indexed_exprs =
    connector::MakeIndexedExpressions(*s.index, client_context);
  auto direct_column_ids = s.index->GetColumnIds();
  connector::DuckDBSearchSinkInsertWriter insert_sink{
    trx,
    std::move(tokenizer_provider),
    direct_column_ids,
    std::move(store_values_provider),
    std::move(is_text_indexed_provider),
    std::move(hnsw_info_provider),
    std::move(expr_tokenizer_provider),
    std::move(indexed_exprs),
  };
  connector::SearchSinkDeleteBaseImpl delete_sink{trx};

  delete_sink.InitImpl(s.pk2row.size());
  for (const auto& [pk, _] : s.pk2row) {
    delete_sink.DeleteRowImpl(pk);
  }
  delete_sink.FinishImpl();
  s.total_deleted += s.pk2row.size() - insert_entries.size();

  if (!insert_entries.empty()) {
    auto& sink = insert_sink;
    sink.InitImpl(insert_entries.size());
    auto& get_key_buffer = s.full_key_prefix;
    rocksdb::ReadOptions read_opts;
    rocksdb::PinnableSlice value_buffer;
    auto resolve_cell = [&](const Row& row,
                            size_t col_idx) -> std::string_view {
      std::string_view cell = row.indexed_cols[col_idx];
      if (!cell.empty()) {
        return cell;
      }
      std::string_view pk = row.full_key.substr(kKeyPrefixSize);
      get_key_buffer.resize(kKeyPrefixSize);
      get_key_buffer.append(pk.data(), pk.size());
      value_buffer.Reset();
      auto status = db.Get(read_opts, &cf, get_key_buffer, &value_buffer);
      SDB_FATAL_IF(
        SEARCH, !status.ok(), "WAL recovery: rocksdb Get failed for index '",
        s.shard->GetId().id(), "' col=", s.indexed_columns[col_idx].id, ": ",
        status.ToString(), kSkipHint);
      return std::string_view{value_buffer.data(), value_buffer.size()};
    };
    for (size_t col_idx = 0; col_idx < s.indexed_columns.size(); ++col_idx) {
      const auto& col = s.indexed_columns[col_idx];
      absl::big_endian::Store(get_key_buffer.data() + sizeof(ObjectId), col.id);
      const connector::ColumnDescriptor desc{col.id, col.store_mode, col.type,
                                             /*have_nulls=*/true};
      const auto* info = s.index->FindColumnInfo(col.id);
      const bool wants_cs = info != nullptr && info->store_values;
      const bool has_hnsw = info && info->hnsw_config;
      // HNSW columns have wants_cs=false in the catalog (the hnsw opclass
      // setter does both); still feed cs via the typed-batch path.
      const bool wants_cs_batch = wants_cs || has_hnsw;
      const auto count = static_cast<duckdb::idx_t>(insert_entries.size());
      if (wants_cs_batch) {
        bool switched_for_inverted = false;
        for (duckdb::idx_t emitted = 0; emitted < count;) {
          const auto chunk =
            std::min<duckdb::idx_t>(count - emitted, STANDARD_VECTOR_SIZE);
          duckdb::Vector vec{col.type, chunk};
          duckdb::FlatVector::ValidityMutable(vec).SetAllValid(chunk);
          for (duckdb::idx_t i = 0; i < chunk; ++i) {
            connector::DeserializeValueIntoDuckDB(
              resolve_cell(*insert_entries[emitted + i], col_idx), vec,
              col.type, i);
          }
          if (emitted == 0) {
            switched_for_inverted = sink.SwitchColumnImpl(desc, vec, chunk);
          } else {
            sink.AppendCsContinuation(vec, chunk, emitted);
          }
          emitted += chunk;
        }
        if (!switched_for_inverted) {
          continue;
        }
      } else {
        duckdb::Vector unused{col.type, nullptr, 0};
        // Dep-only columns: skip per-row inverted write; chunk feeds expr eval.
        if (!sink.SwitchColumnImpl(desc, unused, /*count=*/0)) {
          continue;
        }
      }
      for (const auto* row : insert_entries) {
        std::string_view cell = resolve_cell(*row, col_idx);
        rocksdb::Slice slice{cell.data(), cell.size()};
        sink.WriteImpl(std::span{&slice, 1}, row->full_key);
      }
    }

    auto indexed_exprs = insert_sink.IndexedExpressions();
    if (!indexed_exprs.empty()) {
      duckdb::vector<duckdb::LogicalType> types;
      std::vector<catalog::Column::Id> slot_to_col_id;
      types.reserve(s.indexed_columns.size());
      slot_to_col_id.reserve(s.indexed_columns.size());
      for (const auto& col : s.indexed_columns) {
        types.push_back(col.type);
        slot_to_col_id.push_back(col.id);
      }
      connector::DuckDBColumnSerializer serializer{
        duckdb::Allocator::DefaultAllocator()};
      std::vector<std::string> row_keys;
      const size_t total = insert_entries.size();
      for (size_t off = 0; off < total;) {
        const auto count = std::min<size_t>(total - off, STANDARD_VECTOR_SIZE);
        duckdb::DataChunk chunk;
        chunk.Initialize(duckdb::Allocator::DefaultAllocator(), types, count);
        chunk.SetCardinality(count);
        for (size_t slot = 0; slot < s.indexed_columns.size(); ++slot) {
          const auto& col = s.indexed_columns[slot];
          absl::big_endian::Store(get_key_buffer.data() + sizeof(ObjectId),
                                  col.id);
          auto& vec = chunk.data[slot];
          duckdb::FlatVector::ValidityMutable(vec).SetAllValid(count);
          for (size_t i = 0; i < count; ++i) {
            connector::DeserializeValueIntoDuckDB(
              resolve_cell(*insert_entries[off + i], slot), vec, col.type, i);
          }
        }
        row_keys.clear();
        row_keys.reserve(count);
        for (size_t i = 0; i < count; ++i) {
          row_keys.emplace_back(insert_entries[off + i]->full_key);
        }
        connector::EvaluateAndWriteIndexedExpressions(
          insert_sink, indexed_exprs, chunk, s.table_object_id, slot_to_col_id,
          client_context, count, row_keys, serializer);
        off += count;
      }
    }
    sink.FinishImpl();
  }

  s.total_inserted += insert_entries.size();

  SDB_ASSERT(last_tick != 0);
  const bool committed = trx.Commit(last_tick);
  SDB_FATAL_IF(SEARCH, !committed,
               "WAL recovery: iresearch trx Commit failed for index '",
               s.shard->GetId().id(), "' last_tick=", last_tick, kSkipHint);
}

struct PerTableShards {
  // shards[0..started) -- the prefix of shards whose start tick < curr tick.
  size_t started = 0;
  std::vector<ShardState*> shards;
};

using TableToShards = containers::FlatHashMap<ObjectId, PerTableShards>;

class WalBatchReplay final : public rocksdb::WriteBatch::Handler {
 public:
  WalBatchReplay(TableToShards& table2shards, uint32_t default_cf_id,
                 rocksdb::SequenceNumber batch_sequence, size_t flush_threshold)
    : _table2shards{table2shards},
      _default_cf_id{default_cf_id},
      _batch_sequence{batch_sequence},
      _flush_threshold{flush_threshold} {}

  rocksdb::Status PutCF(uint32_t cf_id, const rocksdb::Slice& key,
                        const rocksdb::Slice& value) final {
    ForEachMatchingShard(
      cf_id, key, [&](ShardState& s, Row& row, size_t col_idx) {
        if (row.indexed_cols.empty()) {
          row.indexed_cols.resize(s.indexed_columns.size());
        }
        row.indexed_cols[col_idx] = {value.data(), value.size()};
        row.op = RowOp::Put;
      });
    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteCF(uint32_t cf_id, const rocksdb::Slice& key) final {
    ForEachMatchingShard(cf_id, key,
                         [&](ShardState& /*s*/, Row& row, size_t /*col_idx*/) {
                           row.indexed_cols.clear();
                           row.op = RowOp::Delete;
                         });
    return rocksdb::Status::OK();
  }

  rocksdb::Status SingleDeleteCF(uint32_t cf_id,
                                 const rocksdb::Slice& key) final {
    return DeleteCF(cf_id, key);
  }

  rocksdb::Status MergeCF(uint32_t cf_id, const rocksdb::Slice& /*key*/,
                          const rocksdb::Slice& /*value*/) final {
    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteRangeCF(uint32_t cf_id, const rocksdb::Slice& begin,
                                const rocksdb::Slice& end) final {
    if (cf_id != _default_cf_id) {
      return rocksdb::Status::OK();
    }

    ObjectId id{absl::big_endian::Load64(begin.data())};
    auto table_it = _table2shards.find(id);
    if (table_it == _table2shards.end()) {
      // DROP table
      return rocksdb::Status::OK();
    }

    // we expect that this is TRUNCATE case.
    SDB_ASSERT(begin.size() == sizeof(ObjectId));
    SDB_ASSERT(end.size() == sizeof(ObjectId));
    SDB_ASSERT(absl::big_endian::Load64(end.data()) == id.id() + 1);

    auto& shards = table_it->second.shards;
    for (auto* s : shards) {
      if (s->start_tick >= _batch_sequence) {
        break;
      }
      s->pk2row.clear();
      auto guard = s->shard->TruncateBegin();
      s->shard->TruncateCommit(std::move(guard), _batch_sequence, nullptr);
    }
    return rocksdb::Status::OK();
  }

  void LogData(const rocksdb::Slice& blob) final {
    // Replay sdb_indexonly markers; unrecognised blobs (different magic)
    // are silently ignored.
    namespace iom = connector::indexonly_marker;
    auto decoded = iom::Decode(blob);
    if (!decoded) {
      return;
    }
    switch (decoded->kind) {
      case iom::MarkerKind::CP:
        PutCF(_default_cf_id,
              rocksdb::Slice{decoded->key.data(), decoded->key.size()},
              rocksdb::Slice{decoded->value.data(), decoded->value.size()});
        return;
      case iom::MarkerKind::RD:
        ApplyMarkerRowDelete(decoded->key);
        return;
      case iom::MarkerKind::Unknown:
        SDB_UNREACHABLE();
    }
  }

  rocksdb::Status MarkNoop(bool) final { return rocksdb::Status::OK(); }

  rocksdb::Status MarkBeginPrepare(bool) final {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "WAL recovery: MarkBeginPrepare unexpected (no 2PC)");
  }
  rocksdb::Status MarkEndPrepare(const rocksdb::Slice&) final {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "WAL recovery: MarkEndPrepare unexpected (no 2PC)");
  }
  rocksdb::Status MarkCommit(const rocksdb::Slice&) final {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "WAL recovery: MarkCommit unexpected (no 2PC)");
  }
  rocksdb::Status MarkRollback(const rocksdb::Slice&) final {
    SDB_ASSERT(false);
    return rocksdb::Status::InvalidArgument(
      "WAL recovery: MarkRollback unexpected (no 2PC)");
  }

  bool NeedsFlush() const noexcept { return _needs_flush; }

 private:
  // Row delete fires for every shard on the table; the col_id portion of
  // `key` is ignored because the inverted index deletes by PK.
  void ApplyMarkerRowDelete(std::string_view key) {
    if (key.size() < kKeyPrefixSize) {
      return;
    }
    ObjectId id{absl::big_endian::Load64(key.data())};
    auto table_it = _table2shards.find(id);
    if (table_it == _table2shards.end()) {
      return;
    }
    std::string_view pk{key.data() + kKeyPrefixSize,
                        key.size() - kKeyPrefixSize};
    std::string_view full_key{key.data(), key.size()};

    auto& [started, shards] = table_it->second;
    for (size_t i = 0; i < shards.size(); ++i) {
      auto* s = shards[i];
      if (i >= started) {
        if (s->start_tick >= _batch_sequence) {
          break;
        }
        started = i + 1;
      }
      auto& row = s->GetRow(pk);
      row.full_key = full_key;
      row.indexed_cols.clear();
      row.op = RowOp::Delete;
      _needs_flush = s->pk2row.size() >= _flush_threshold;
    }
  }

  template<typename Fn>
  void ForEachMatchingShard(uint32_t cf_id, const rocksdb::Slice& key,
                            Fn&& fn) {
    if (cf_id != _default_cf_id) {
      return;
    }
    // We have an ugly hack that writes an empty slice to keep ticks aligned
    // (transaction.cpp: _rocksdb_transaction->Delete(rocksdb::Slice{})).
    if (key.size() < kKeyPrefixSize) {
      return;
    }
    ObjectId id{absl::big_endian::Load64(key.data())};
    auto table_it = _table2shards.find(id);
    if (table_it == _table2shards.end()) {
      return;
    }

    const auto col_id = absl::big_endian::Load<catalog::Column::Id>(
      key.data() + sizeof(ObjectId));
    std::string_view pk{key.data() + kKeyPrefixSize,
                        key.size() - kKeyPrefixSize};
    std::string_view full_key{key.data(), key.size()};

    auto& [started, shards] = table_it->second;
    for (size_t i = 0; i < shards.size(); ++i) {
      auto* s = shards[i];
      if (i >= started) {
        if (s->start_tick >= _batch_sequence) {
          break;
        }
        started = i + 1;
      }
      auto col_it = s->col2index.find(col_id);
      if (col_it == s->col2index.end()) {
        // unindexed column
        continue;
      }
      auto& row = s->GetRow(pk);
      row.full_key = full_key;
      fn(*s, row, col_it->second);
      _needs_flush = s->pk2row.size() >= _flush_threshold;
    }
  }

  TableToShards& _table2shards;
  uint32_t _default_cf_id;
  rocksdb::SequenceNumber _batch_sequence;
  size_t _flush_threshold;
  bool _needs_flush = false;
};

void RunWalRecovery(std::vector<ShardState>& shards,
                    const std::shared_ptr<const catalog::Snapshot>& snapshot,
                    rocksdb::DB& db, Tick min_start_tick, Tick end_tick) {
  TableToShards table2shards;
  for (auto& s : shards) {
    table2shards[s.table_object_id].shards.emplace_back(&s);
  }
  for (auto& [_, shards] : table2shards) {
    absl::c_sort(shards.shards, ShardState::ByStartTick());
  }

  auto* default_cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(default_cf);
  const uint32_t default_cf_id = default_cf->GetID();

  SDB_INFO(SEARCH,
           "Starting WAL recovery, to skip it use the flag: "
           "\"--search.skip-wal-recovery\"");

  std::unique_ptr<rocksdb::TransactionLogIterator> iter;
  rocksdb::TransactionLogIterator::ReadOptions opts{true};
  auto s = db.GetUpdatesSince(min_start_tick, &iter, opts);
  SDB_FATAL_IF(SEARCH, !s.ok(),
               "WAL recovery: failed to open WAL iterator from tick ",
               min_start_tick, ": ", s.ToString(), kSkipHint);
  SDB_ASSERT(iter);

  std::vector<rocksdb::BatchResult> live_batches;
  Tick latest_seq = 0;
  auto flush_all = [&] {
    for (auto& shard : shards) {
      FlushShard(shard, snapshot, db, *default_cf, latest_seq);
    }
    live_batches.clear();
  };

  while (iter->Valid()) {
    auto status = iter->status();
    SDB_FATAL_IF(SEARCH, !status.ok(),
                 "WAL recovery: iterator error: ", status.ToString(),
                 kSkipHint);

    auto batch = iter->GetBatch();
    latest_seq = batch.sequence;
    WalBatchReplay handler{table2shards, default_cf_id, batch.sequence,
                           kFlushThreshold};
    status = batch.writeBatchPtr->Iterate(&handler);
    SDB_FATAL_IF(SEARCH, !status.ok(),
                 "WAL recovery: batch iterate failed at seq ", batch.sequence,
                 ": ", status.ToString(), kSkipHint);

    live_batches.emplace_back(std::move(batch));

    if (handler.NeedsFlush()) {
      flush_all();
    }

    iter->Next();
  }

  auto status = iter->status();
  SDB_FATAL_IF(SEARCH, !iter->status().ok(),
               "WAL recovery: iterator error after last batch: ",
               status.ToString(), kSkipHint);

  flush_all();

  std::vector<yaclib::Future<>> commits;
  commits.reserve(shards.size());
  for (auto& shard : shards) {
    SDB_ASSERT(shard.start_tick < end_tick);
    commits.emplace_back(shard.shard->CommitWait());
  }
  yaclib::Wait(commits.begin(), commits.end());

  for (auto& shard : shards) {
    SDB_INFO_IF(SEARCH, shard.total_deleted > 0 || shard.total_inserted > 0,
                "WAL recovery: index '", shard.shard->GetId().id(),
                "' replayed (", shard.start_tick, ", ", end_tick,
                "], inserted=", shard.total_inserted,
                ", deleted=", shard.total_deleted);
  }
}

}  // namespace

void InitInvertedIndexes(bool skip_wal_recovery) {
  auto begin = std::chrono::steady_clock::now();

  auto& engine = EngineFeature::instance().engine();
  auto& rdb = basics::downCast<RocksDBEngineCatalog>(engine);
  SDB_ASSERT(!rdb.inRecovery());
  const Tick end_tick = rdb.recoveryTick();

  auto& catalog_feature = catalog::CatalogFeature::instance();
  auto snapshot = catalog_feature.Global().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);

  std::vector<ShardState> recovery_shards;
  Tick min_start_tick = std::numeric_limits<Tick>::max();
  for (const auto& database : snapshot->GetDatabases()) {
    for (const auto& schema : snapshot->GetSchemas(database->GetId())) {
      for (const auto& idx :
           snapshot->GetIndexes(database->GetId(), schema->GetName())) {
        if (idx->GetType() != catalog::ObjectType::InvertedIndex) {
          continue;
        }
        auto inv_shard = basics::downCast<InvertedIndexShard>(
          snapshot->GetIndexShard(idx->GetId()));
        SDB_ASSERT(inv_shard);
        const Tick persisted = inv_shard->GetRecoveryTick();
        if (persisted > end_tick) {
          SDB_WARN(SEARCH, "Inverted index '", inv_shard->GetId().id(),
                   "' is recovered at tick ", persisted,
                   " greater than storage engine tick ", end_tick,
                   ", it seems WAL tail was lost and index is out of sync");
        }
        inv_shard->StartTasks();

        // View-backed indexes are static -- the underlying view's data
        // doesn't change at runtime, so there's nothing to replay from WAL.
        const auto relation = snapshot->GetObject(idx->GetRelationId());
        const bool is_view_backed =
          relation && relation->GetType() == catalog::ObjectType::PgSqlView;

        if (skip_wal_recovery || is_view_backed || persisted >= end_tick) {
          inv_shard->FinishCreation();
          continue;
        }

        inv_shard->StartRecovery();
        ShardState state;
        state.shard = std::move(inv_shard);
        state.start_tick = persisted;
        SDB_FATAL_IF(SEARCH, !ResolveShardMetadata(state, *snapshot),
                     "WAL recovery: could not resolve catalog metadata for "
                     "inverted index '",
                     state.shard->GetId().id(), "'", kSkipHint);
        min_start_tick = std::min(min_start_tick, persisted);
        recovery_shards.push_back(std::move(state));
      }
    }
  }

  irs::Finally finish_recovering = [&] noexcept {
    for (auto& s : recovery_shards) {
      s.shard->FinishCreation();
    }
  };

  if (recovery_shards.empty()) {
    return;
  }

  auto* db = rdb.db();
  SDB_ASSERT(db);
  RunWalRecovery(recovery_shards, snapshot, *db, min_start_tick, end_tick);

  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "WAL recovery: completed in ",
           absl::FormatDuration(duration),
           ", indexes=", recovery_shards.size());
}

}  // namespace sdb::search
