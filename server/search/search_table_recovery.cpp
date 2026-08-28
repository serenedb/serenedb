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

#include <absl/cleanup/cleanup.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <algorithm>
#include <chrono>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/connection.hpp>
#include <iresearch/index/index_writer.hpp>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/node_hash_map.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/search_sink_writer.hpp"
#include "search/search_db_wal.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

void RunSearchTableRecovery(bool skip_wal_recovery) {
  if (skip_wal_recovery) {
    return;
  }
  auto begin = std::chrono::steady_clock::now();
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);
  auto& engine = GetSearchEngine();

  // A dedicated connection whose ClientContext drives indexed-expression
  // evaluation for replayed rows (the WAL stores raw columns; expressions must
  // be recomputed). Rolled back at the end -- it never writes anything.
  duckdb::Connection expr_conn(DuckDBEngine::Instance().instance());
  expr_conn.BeginTransaction();
  absl::Cleanup rollback_expr_conn = [&] { expr_conn.Rollback(); };
  auto& expr_context = *expr_conn.context;

  // Per-shard replay metadata, built once from the catalog table so the
  // recovered key matches the written one.
  struct ShardInfo {
    std::shared_ptr<SearchTable> shard;  // keeps the table store alive
    SearchTable* search = nullptr;
    std::vector<catalog::Column::Id> column_ids;
  };
  // Per-shard replay context: one open iresearch trx accumulated across all of
  // the shard's records, with an insert sink and a delete sink that share it.
  // Ops replay in manifest order into this single trx; iresearch's `_queries`
  // cursor reproduces the original insert/delete ordering. Kept in a node map
  // so the sinks' trx reference stays stable.
  struct ReplayCtx {
    irs::IndexWriter::Transaction trx;
    std::unique_ptr<connector::SearchSinkInsertBaseImpl> insert_sink;
    std::unique_ptr<connector::SearchSinkDeleteBaseImpl> delete_sink;
    uint64_t max_tick = 0;
    // Segments to re-attach, each with the query count at its manifest
    // position; the adopt tick needs the final count (see the finalize loop).
    struct PendingAdopt {
      std::string meta_file;
      std::string codec;
      uint64_t queries_before;
    };
    std::vector<PendingAdopt> adopts;
  };

  size_t recovered_shards = 0;
  for (const auto& database : snapshot->GetDatabases()) {
    const ObjectId db_id = database->GetId();
    containers::NodeHashMap<ObjectId, ShardInfo> shards;
    for (const auto& schema : snapshot->GetSchemas(db_id)) {
      for (const auto& table : snapshot->GetTables(db_id, schema->GetName())) {
        if (table->GetEngine() != catalog::TableEngine::Search) {
          continue;  // Transactional table: no Search-engine store to recover.
        }
        auto search = table->GetData();  // asserts the store is bound
        ShardInfo info;
        info.search = search.get();
        info.shard = std::move(search);
        for (const auto& col : table->Columns()) {
          if (col.GetId() == catalog::Column::kGeneratedPKId) {
            continue;
          }
          info.column_ids.push_back(col.GetId());
        }
        shards.emplace(table->GetId(), std::move(info));
      }
    }
    if (shards.empty()) {
      continue;
    }

    auto& wal = engine.GetDbWal(db_id);
    containers::NodeHashMap<ObjectId, ReplayCtx> ctxs;
    auto exists_of = [&](ObjectId table_id) {
      return shards.find(table_id) != shards.end();
    };
    auto committed_of = [&](ObjectId table_id) -> uint64_t {
      auto it = shards.find(table_id);
      return it != shards.end() ? it->second.search->CommittedTick()
                                : std::numeric_limits<uint64_t>::max();
    };
    auto ensure_ctx = [&](ObjectId table_id) -> ReplayCtx& {
      auto [cit, inserted] = ctxs.try_emplace(table_id);
      auto& ctx = cit->second;
      auto& info = shards.at(table_id);
      if (inserted) {
        ctx.trx = info.search->GetTransaction();
      }

      if (!ctx.insert_sink) {
        ctx.insert_sink = connector::MakeSearchTableInsertSink(
          ctx.trx, *info.shard, snapshot, expr_context);
        ctx.delete_sink =
          std::make_unique<connector::SearchSinkDeleteBaseImpl>(ctx.trx);
      }
      return ctx;
    };
    auto replay = [&](uint64_t tick, ObjectId table_id, uint64_t pk_base,
                      duckdb::DataChunk& chunk) {
      auto& info = shards.at(table_id);
      auto& ctx = ensure_ctx(table_id);
      connector::WriteChunkToSearchSink(*ctx.insert_sink, chunk,
                                        info.column_ids, pk_base, table_id,
                                        expr_context);
      ctx.max_tick = std::max(ctx.max_tick, tick);
    };
    // Each DELETE op replays as one removal batch on the shared trx; feeding it
    // in manifest order keeps the `_queries` ordering vs surrounding inserts.
    auto replay_delete = [&](uint64_t tick, ObjectId table_id,
                             std::span<const std::string_view> pks) {
      if (pks.empty()) {
        return;
      }
      auto& ctx = ensure_ctx(table_id);
      ctx.delete_sink->InitImpl(pks.size());
      for (auto pk : pks) {
        ctx.delete_sink->DeleteRowImpl(pk);
      }
      ctx.delete_sink->FinishImpl();
      ctx.max_tick = std::max(ctx.max_tick, tick);
    };
    // TRUNCATE wipes the shard as of `tick`. Clear rolls back the open trx
    // (discarding any pre-truncate replayed inserts -- superseded by the
    // truncate) and drops on-disk published data <= tick; drop the sinks first
    // so nothing pins the trx, then start a fresh trx. Post-truncate ops (in
    // later records) lazily rebuild the sinks via ensure_ctx; if the truncate
    // is last, Finalize commits the empty trx so the cleared state publishes.
    auto replay_truncate = [&](uint64_t tick, ObjectId table_id) {
      auto& info = shards.at(table_id);
      auto& ctx = ensure_ctx(table_id);
      ctx.insert_sink.reset();
      ctx.delete_sink.reset();
      info.search->Clear(tick);
      ctx.trx = info.search->GetTransaction();
      ctx.max_tick = std::max(ctx.max_tick, tick);
    };
    // Re-attach the files the crashed process already flushed instead of
    // re-indexing their rows. Only stashed here: the tick they adopt at needs
    // the final query count, so the manifest position is all we can capture.
    auto replay_adopt = [&](uint64_t tick, ObjectId table_id,
                            const SearchDbWal::SegmentRef& ref) {
      auto& ctx = ensure_ctx(table_id);
      ctx.adopts.push_back({ref.meta_file, ref.codec, ctx.trx.GetQueries()});
      ctx.max_tick = std::max(ctx.max_tick, tick);
    };
    wal.Recover(exists_of, committed_of, replay, replay_delete, replay_truncate,
                replay_adopt);

    // Finalize each replayed shard outside Recover() so Commit()'s locking + GC
    // are safe.
    for (auto& [table_id, ctx] : ctxs) {
      // Release the insert Document (and the delete filter) before committing.
      ctx.insert_sink.reset();
      ctx.delete_sink.reset();
      auto& info = shards.at(table_id);

      // Adopt in this transaction's tick space, not at the record's tick: the
      // commit rebases removal #k to `max_tick - queries + k`, so a segment
      // reached after `m` removals belongs at `max_tick - queries + m`.
      const uint64_t queries = ctx.trx.GetQueries();
      SDB_FATAL_IF(SEARCH, ctx.max_tick <= queries,
                   "search-table WAL recovery: tick ", ctx.max_tick,
                   " cannot cover ", queries, " removals for table ",
                   table_id.id());
      const uint64_t first_tick = ctx.max_tick - queries;
      for (const auto& pending : ctx.adopts) {
        const uint64_t tick = first_tick + pending.queries_before;
        // A durable record claims these documents: failing to reopen them is
        // data loss, not something to skip.
        const bool adopted =
          info.search->AdoptSegment(pending.meta_file, pending.codec, tick);
        SDB_FATAL_IF(SEARCH, !adopted,
                     "search-table WAL recovery: failed to adopt segment '",
                     pending.meta_file, "' for table ", table_id.id(),
                     " tick=", tick);
      }

      // A failed commit during replay leaves the index inconsistent with the
      // durable WAL it was rebuilt from -- unrecoverable, so crash.
      const bool committed = ctx.trx.Commit(ctx.max_tick);
      SDB_FATAL_IF(SEARCH, !committed,
                   "search-table WAL recovery: iresearch trx Commit failed for "
                   "table ",
                   table_id.id(), " tick=", ctx.max_tick);
      info.search->Commit();
      ++recovered_shards;
    }

    // Advance every shard -- including ones with no replayed records -- to the
    // recovered max tick, so an idle shard doesn't pin this database WAL's GC
    // floor after recovery. FinishRecovery is per-shard for the same reason:
    // one that adopted nothing still has to reclaim what the crash left behind.
    const uint64_t db_max_tick = wal.CurrentTick();
    for (const auto& entry : shards) {
      wal.OnShardCommit(entry.first, db_max_tick);
      entry.second.search->FinishRecovery();
    }
  }

  if (recovered_shards > 0) {
    const auto duration =
      absl::FromChrono(std::chrono::steady_clock::now() - begin);
    SDB_INFO(SEARCH, "Search-table WAL recovery: completed in ",
             absl::FormatDuration(duration), ", shards=", recovered_shards);
  }
}

void StartSearchTableMaintenance() {
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);
  for (const auto& database : snapshot->GetDatabases()) {
    const ObjectId db_id = database->GetId();
    for (const auto& schema : snapshot->GetSchemas(db_id)) {
      for (const auto& table : snapshot->GetTables(db_id, schema->GetName())) {
        if (table->GetEngine() != catalog::TableEngine::Search) {
          continue;
        }
        table->GetData()->StartTasks();  // GetData asserts the store is bound
      }
    }
  }
}

}  // namespace sdb::search
