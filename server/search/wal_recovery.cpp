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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/time/time.h>

#include <chrono>
#include <iresearch/index/index_writer.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/duckdb_engine.h"
#include "basics/errors.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/inverted_index.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_search_sink_writer.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/search_sink_writer.hpp"
#include "search/inverted_index_shard.h"
#include "search/tick_domain.h"

namespace sdb::search {
namespace {

constexpr std::string_view kSkipHint =
  ". To skip the index rebuild and proceed with stale index content, restart "
  "with --server_skip_search_recovery (data loss for the missed delta).";

struct ShardState {
  std::shared_ptr<InvertedIndexShard> shard;
  std::shared_ptr<const catalog::InvertedIndex> index;
  std::shared_ptr<const catalog::Table> table;
  ObjectId table_object_id;

  // Store-table coordinates for rebuild-from-store recovery.
  std::string database_name;
  std::string schema_name;

  struct IndexedColumn {
    catalog::Column::Id id;
    duckdb::LogicalType type;
  };
  std::vector<IndexedColumn> indexed_columns;

  uint64_t total_inserted = 0;
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

  s.indexed_columns.reserve(column_ids.size());
  const auto& table_columns = table->Columns();
  for (auto col_id : column_ids) {
    auto it = absl::c_find_if(table_columns, [col_id](const auto& col) {
      return col.GetId() == col_id;
    });
    if (it == table_columns.end()) {
      return false;
    }
    s.indexed_columns.emplace_back(col_id, it->type);
  }

  s.index = std::move(inverted);
  s.table = std::move(table);
  s.table_object_id = s.table->GetId();
  return true;
}

// Re-feed an inverted index over a store table from the table itself: the
// tick domain restarts at boot, so the persisted tick cannot prove the
// index saw every committed row. Truncate, scan the store table, append
// every row keyed by its native rowid, and commit one tick above the
// truncate point.
void RebuildStoreShard(
  ShardState& s, const std::shared_ptr<const catalog::Snapshot>& snapshot) {
  const auto end_tick = TickDomain::Instance().Advance(2);
  s.shard->TruncateCommit({}, end_tick - 1, nullptr);

  auto trx = s.shard->GetTransaction();
  auto tokenizer_provider =
    connector::MakeTokenizerProvider(snapshot, *s.index);
  auto entry_info_provider = connector::MakeEntryInfoProvider(*s.index);
  auto conn = sdb::DuckDBEngine::Instance().CreateConnection();
  conn->BeginTransaction();
  irs::Finally rollback_on_exit = [&]() noexcept {
    try {
      conn->Rollback();
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  };
  auto& client_context = *conn->context;
  auto indexed_exprs =
    connector::MakeIndexedExpressions(*s.index, client_context);
  auto direct_column_ids = s.index->GetColumnIds();
  connector::DuckDBSearchSinkInsertWriter insert_sink{
    trx,
    std::move(tokenizer_provider),
    direct_column_ids,
    std::move(entry_info_provider),
    std::move(indexed_exprs),
  };

  std::string select_list = "rowid";
  duckdb::vector<duckdb::LogicalType> col_types;
  std::vector<catalog::Column::Id> slot_to_col_id;
  const auto& table_columns = s.table->Columns();
  for (const auto& col : s.indexed_columns) {
    auto it = absl::c_find_if(
      table_columns, [&](const auto& c) { return c.GetId() == col.id; });
    SDB_ASSERT(it != table_columns.end());
    absl::StrAppend(&select_list, ", \"",
                    absl::StrReplaceAll(it->GetName(), {{"\"", "\"\""}}), "\"");
    col_types.push_back(col.type);
    slot_to_col_id.push_back(col.id);
  }
  const auto store_name =
    catalog::StoreTableName(s.database_name, s.schema_name, s.table->GetName());
  auto result = conn->Query(absl::StrCat("SELECT ", select_list, " FROM \"",
                                         catalog::kStoreDatabaseName,
                                         "\".main.\"", store_name, "\""));
  SDB_FATAL_IF(SEARCH, result->HasError(), "store rebuild: scan of \"",
               store_name, "\" failed: ", result->GetError(), kSkipHint);

  std::vector<std::string> keys;
  std::vector<std::string_view> key_views;
  auto exprs = insert_sink.IndexedExpressions();
  while (auto chunk = result->Fetch()) {
    const auto count = chunk->size();
    if (count == 0) {
      continue;
    }
    chunk->Flatten();
    keys.clear();
    keys.reserve(count);
    key_views.clear();
    key_views.reserve(count);
    const auto* rowids =
      duckdb::FlatVector::GetData<duckdb::row_t>(chunk->data[0]);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      auto& key = keys.emplace_back(connector::key_utils::PrepareColumnKey(
        s.table_object_id, catalog::Column::Id{0}));
      connector::primary_key::AppendSigned(key,
                                           static_cast<int64_t>(rowids[i]));
      key_views.emplace_back(key);
    }
    insert_sink.Init(count, *chunk);
    for (size_t slot = 0; slot < s.indexed_columns.size(); ++slot) {
      const connector::ColumnDescriptor desc{s.indexed_columns[slot].id,
                                             s.indexed_columns[slot].type};
      insert_sink.SwitchColumn(desc, chunk->data[slot + 1], key_views, count);
    }
    if (!exprs.empty()) {
      duckdb::DataChunk cols_chunk;
      cols_chunk.InitializeEmpty(col_types);
      for (size_t slot = 0; slot < col_types.size(); ++slot) {
        cols_chunk.data[slot].Reference(chunk->data[slot + 1]);
      }
      cols_chunk.SetCardinality(count);
      connector::EvaluateAndWriteIndexedExpressions(
        insert_sink, exprs, cols_chunk, s.table_object_id, slot_to_col_id,
        client_context, count, keys);
    }
    insert_sink.Finish();
    s.total_inserted += count;
  }

  const bool committed = trx.Commit(end_tick);
  SDB_FATAL_IF(SEARCH, !committed,
               "store rebuild: iresearch trx Commit failed for index '",
               s.shard->GetId().id(), "' tick=", end_tick, kSkipHint);
}

}  // namespace

void InitInvertedIndexes(bool skip_wal_recovery) {
  auto begin = std::chrono::steady_clock::now();

  auto& catalog_feature = catalog::CatalogFeature::instance();
  auto snapshot = catalog_feature.Global().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);

  // Seed the tick domain above every persisted shard tick first, so all
  // ticks minted from here on (including the rebuild commits below) stay
  // monotonic across restarts.
  std::vector<ShardState> rebuild_shards;
  std::vector<std::shared_ptr<InvertedIndexShard>> static_shards;
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
        TickDomain::Instance().SeedAtLeast(inv_shard->GetRecoveryTick());
        inv_shard->StartTasks();

        // View-backed indexes are static -- the underlying view's data
        // doesn't change at runtime, so the persisted index is current.
        const auto relation = snapshot->GetObject(idx->GetRelationId());
        const bool table_backed =
          relation && relation->GetType() == catalog::ObjectType::Table;
        if (skip_wal_recovery || !table_backed) {
          static_shards.push_back(std::move(inv_shard));
          continue;
        }

        inv_shard->StartRecovery();
        ShardState state;
        state.shard = std::move(inv_shard);
        state.database_name = database->GetName();
        state.schema_name = schema->GetName();
        SDB_FATAL_IF(SEARCH, !ResolveShardMetadata(state, *snapshot),
                     "store rebuild: could not resolve catalog metadata for "
                     "inverted index '",
                     state.shard->GetId().id(), "'", kSkipHint);
        rebuild_shards.push_back(std::move(state));
      }
    }
  }

  irs::Finally finish_recovering = [&] noexcept {
    for (auto& shard : static_shards) {
      shard->FinishCreation();
    }
    for (auto& s : rebuild_shards) {
      s.shard->FinishCreation();
    }
  };

  if (rebuild_shards.empty()) {
    return;
  }

  for (auto& s : rebuild_shards) {
    RebuildStoreShard(s, snapshot);
  }
  std::vector<yaclib::Future<>> commits;
  commits.reserve(rebuild_shards.size());
  for (auto& s : rebuild_shards) {
    commits.emplace_back(s.shard->CommitWait());
  }
  yaclib::Wait(commits.begin(), commits.end());
  for (auto& s : rebuild_shards) {
    SDB_INFO(SEARCH, "store rebuild: index '", s.shard->GetId().id(),
             "' rebuilt with ", s.total_inserted, " rows");
  }

  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: completed in ",
           absl::FormatDuration(duration),
           ", indexes=", rebuild_shards.size() + static_shards.size());
}

}  // namespace sdb::search
