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

#include "search/search_table_recovery.h"

#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/index/index_writer.hpp>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_primary_key.h"
#include "connector/key_utils.hpp"
#include "connector/search_sink_writer.hpp"
#include "search/search_db_wal.h"
#include "search/search_table_shard.h"
#include "storage_engine/search_engine.h"
#include "storage_engine/table_shard.h"

namespace sdb::search {

void RunSearchTableRecovery(bool skip_wal_recovery) {
  if (skip_wal_recovery) {
    return;
  }
  auto begin = std::chrono::steady_clock::now();
  auto& catalog_feature = catalog::CatalogFeature::instance();
  auto snapshot = catalog_feature.Global().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);
  auto& engine = GetSearchEngine();

  // Per-shard replay metadata, built once from the catalog table (mirrors the
  // operator's GetGlobalSinkState so the recovered key matches the written
  // one).
  struct ShardInfo {
    std::shared_ptr<TableShard> shard;  // keeps the shard alive
    SearchTableShard* search = nullptr;
    uint64_t schema_id = 0;
    std::vector<catalog::Column::Id> column_ids;
    std::vector<connector::duckdb_primary_key::PKColumn> pk_columns;
    std::string table_key;
    bool uses_generated_pk = false;
  };
  // Per-shard replay context: one open iresearch trx + sink, accumulated across
  // all of the shard's records. In a node map so the sink's trx reference is
  // stable across inserts.
  struct ReplayCtx {
    irs::IndexWriter::Transaction trx;
    std::unique_ptr<connector::SearchSinkInsertBaseImpl> sink;
    uint64_t max_tick = 0;
  };

  size_t recovered_shards = 0;
  for (const auto& database : snapshot->GetDatabases()) {
    const ObjectId db_id = database->GetId();
    containers::NodeHashMap<uint64_t, ShardInfo> shards;  // table_id -> info
    for (const auto& schema : snapshot->GetSchemas(db_id)) {
      for (const auto& table : snapshot->GetTables(db_id, schema->GetName())) {
        auto ts = snapshot->GetTableShard(table->GetId());
        if (!ts || ts->GetStorage() != catalog::StorageKind::kSearch) {
          continue;
        }
        ShardInfo info;
        info.search = &basics::downCast<SearchTableShard>(*ts);
        info.shard = std::move(ts);
        info.schema_id = schema->GetId().id();
        for (const auto& col : table->Columns()) {
          if (col.GetId() == catalog::Column::kGeneratedPKId) {
            continue;
          }
          info.column_ids.push_back(col.GetId());
        }
        info.pk_columns = connector::duckdb_primary_key::BuildPKColumns(*table);
        info.table_key = connector::key_utils::PrepareTableKey(table->GetId());
        info.uses_generated_pk = table->PKColumns().empty();
        shards.emplace(table->GetId().id(), std::move(info));
      }
    }
    if (shards.empty()) {
      continue;
    }

    auto& wal = engine.GetDbWal(db_id);
    containers::NodeHashMap<uint64_t, ReplayCtx>
      ctxs;  // table_id -> ctx (stable)

    auto exists_of = [&](uint64_t schema_id, uint64_t table_id) {
      auto it = shards.find(table_id);
      return it != shards.end() && it->second.schema_id == schema_id;
    };
    auto committed_of = [&](uint64_t table_id) -> uint64_t {
      auto it = shards.find(table_id);
      return it != shards.end() ? it->second.search->CommittedTick()
                                : std::numeric_limits<uint64_t>::max();
    };
    auto replay = [&](uint64_t tick, uint64_t /*schema_id*/, uint64_t table_id,
                      uint64_t pk_base, duckdb::DataChunk& chunk) {
      auto& info = shards.at(table_id);
      auto [cit, inserted] = ctxs.try_emplace(table_id);
      auto& ctx = cit->second;
      if (inserted) {
        ctx.trx = info.search->GetTransaction();
        ctx.sink =
          connector::MakeSearchTableInsertSink(ctx.trx, info.column_ids);
      }
      connector::WriteChunkToSearchSink(*ctx.sink, chunk, info.column_ids,
                                        info.pk_columns, info.table_key,
                                        info.uses_generated_pk, pk_base);
      ctx.max_tick = std::max(ctx.max_tick, tick);
    };

    wal.Recover(exists_of, committed_of, replay);

    // Finalize each replayed shard. Outside Recover() (its _append_mu is
    // released), so Commit()->OnShardCommit's locking + GC are safe.
    for (auto& [table_id, ctx] : ctxs) {
      ctx.sink.reset();              // release the Document before committing
      ctx.trx.Commit(ctx.max_tick);  // stage the replayed docs at the max tick
      auto& info = shards.at(table_id);
      info.search->Commit();  // RefreshCommit + OnShardCommit (publish + GC)
      info.search->SyncNumRowsFromIndex();
      ++recovered_shards;
    }
  }

  if (recovered_shards > 0) {
    const auto duration =
      absl::FromChrono(std::chrono::steady_clock::now() - begin);
    SDB_INFO(SEARCH, "Search-table WAL recovery: completed in ",
             absl::FormatDuration(duration), ", shards=", recovered_shards);
  }
}

}  // namespace sdb::search
