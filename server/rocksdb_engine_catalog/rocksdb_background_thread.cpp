////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "rocksdb_background_thread.h"

#include <absl/time/clock.h>
#include <absl/time/time.h>

#include "basics/logger/logger.h"
#include "basics/thread_id.h"
#include "catalog/catalog.h"
#include "rest_server/flush_feature.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_settings_manager.h"
#include "storage_engine/table_shard.h"

using namespace sdb;

RocksDBBackgroundThread::RocksDBBackgroundThread(RocksDBEngineCatalog& engine,
                                                 double interval)
  : _engine(engine), _interval(interval) {}

RocksDBBackgroundThread::~RocksDBBackgroundThread() {
  beginShutdown();
  if (_thread.joinable()) {
    _thread.join();
  }
}

void RocksDBBackgroundThread::start() {
  _thread = std::jthread{[this] {
    InitThread("RocksDBThread");
    try {
      run();
    } catch (const std::exception& ex) {
      SDB_WARN(STORAGE,
               "caught exception in rocksdb background thread: ", ex.what());
    } catch (...) {
      SDB_WARN(STORAGE, "caught unknown exception in rocksdb background");
    }
  }};
}

void RocksDBBackgroundThread::beginShutdown() {
  absl::MutexLock guard{&_condition.mutex};
  if (_stopping) {
    return;
  }
  _stopping = true;
  _condition.cv.notify_all();
}

void RocksDBBackgroundThread::SyncStats() {
  SDB_TRACE(STORAGE, "syncing RocksDB settings statistics");
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  for (auto& db : snapshot->GetDatabases()) {
    for (auto& schema : snapshot->GetSchemas(db->GetId())) {
      catalog::VisitTableShards(
        *snapshot, db->GetId(), schema->GetName(), [&](auto& shard) mutable {
          if (!shard) {
            SDB_WARN(STORAGE, "table shard is null");
            return;
          }
          auto r = _engine.SyncTableShard(*shard);
          if (!r.ok()) {
            SDB_WARN(STORAGE, "unable to update settings for table '",
                     shard->GetName(), "': ", r.errorMessage());
          }
        });
    }
  }
}

void RocksDBBackgroundThread::run() {
  auto& flush_feature = FlushFeature::instance();

  const double start_time = absl::ToDoubleSeconds(absl::Now() - absl::UnixEpoch());
  uint64_t runs_until_sync_forced = 1;
  constexpr uint64_t kMaxRunsUntilSyncForced = 5;

  while (true) {
    {
      absl::MutexLock guard{&_condition.mutex};
      if (_stopping) {
        break;
      }
      _condition.cv.WaitWithTimeout(&_condition.mutex,
                                    absl::Seconds(_interval));
      if (_stopping) {
        break;
      }
    }

    if (_engine.inRecovery()) {
      continue;
    }

    SDB_IF_FAILURE("RocksDBBackgroundThread::run") { continue; }

    try {
      flush_feature.releaseUnusedTicks();

      // it is important that we wrap the sync operation inside a
      // try..catch of its own, because we still want the following
      // garbage collection operations to be carried out even if
      // the sync fails.
      try {
        // forceSync will effectively be true for the initial run that
        // will happen when the recovery has finished. that way we
        // can quickly push forward the WAL lower bound value after
        // the recovery
        bool force_sync = false;

        // force a sync after at most x iterations (or initial run)
        if (runs_until_sync_forced > 0 && --runs_until_sync_forced == 0) {
          SDB_ASSERT(runs_until_sync_forced == 0);
          force_sync = true;
        }

        SDB_IF_FAILURE("BuilderIndex::purgeWal") { force_sync = true; }

        SDB_TRACE(STORAGE, "running ", (force_sync ? "forced " : ""),
                  "background settings sync");

        double start = absl::ToDoubleSeconds(absl::Now() - absl::UnixEpoch());
        auto sync_res = _engine.settingsManager()->sync(force_sync);
        SyncStats();
        double end = absl::ToDoubleSeconds(absl::Now() - absl::UnixEpoch());

        if (!sync_res) {
          SDB_WARN(STORAGE, "background settings sync failed: ",
                   sync_res.error().errorMessage());
        } else if (*sync_res) {
          // reset our counter
          runs_until_sync_forced = kMaxRunsUntilSyncForced;
        }

        if (end - start > 5.0) {
          SDB_WARN(STORAGE, "slow background settings sync took: ",
                   absl::StrFormat("%.6f", end - start), " s");
        } else if (end - start > 0.75) {
          SDB_DEBUG(STORAGE, "slow background settings sync took: ",
                    absl::StrFormat("%.6f", end - start), " s");
        }
      } catch (const std::exception& ex) {
        SDB_WARN(
          STORAGE,
          "caught exception in rocksdb background sync operation: ", ex.what());
      }

      const uint64_t latest_seq_no = _engine.db()->GetLatestSequenceNumber();
      const auto earliest_seq_needed =
        _engine.settingsManager()->earliestSeqNeeded();

      uint64_t min_tick = latest_seq_no;

      if (earliest_seq_needed < min_tick) {
        min_tick = earliest_seq_needed;
      }

      SDB_DEBUG(STORAGE, "latest seq number: ", latest_seq_no,
                ", earliest seq needed: ", earliest_seq_needed);

      try {
        _engine.flushOpenFilesIfRequired();
      } catch (...) {
        // whatever happens here, we don't want it to block/skip any of
        // the following operations
      }

      bool can_prune = absl::ToDoubleSeconds(absl::Now() - absl::UnixEpoch()) >=
                       start_time + _engine.pruneWaitTimeInitial();
      SDB_IF_FAILURE("BuilderIndex::purgeWal") { can_prune = true; }

      // only start pruning of obsolete WAL files a few minutes after
      // server start. if we start pruning too early, replication followers
      // will not have a chance to reconnect to a restarted leader in
      // time so the leader may purge WAL files that replication followers
      // would still like to peek into
      if (can_prune) {
        // determine which WAL files can be pruned
        _engine.determinePrunableWalFiles(min_tick);
        // and then prune them when they expired
        _engine.pruneWalFiles();
      } else {
        // WAL file pruning not (yet) enabled. this will be the case the
        // first few minutes after the instance startup.
        // only keep track of which WAL files exist and what the lower
        // bound sequence number is
        _engine.determineWalFilesInitial();
      }

    } catch (const std::exception& ex) {
      SDB_WARN(STORAGE,
               "caught exception in rocksdb background thread: ", ex.what());
    } catch (...) {
      SDB_WARN(STORAGE, "caught unknown exception in rocksdb background");
    }
  }

  // final write on shutdown
  auto sync_res = _engine.settingsManager()->sync(/*force*/ true);
  if (!sync_res) {
    SDB_WARN(STORAGE, "caught exception during final RocksDB sync operation: ",
             sync_res.error().errorMessage());
  }
  // Final stats sync after settings sync, so the catalog-iteration race
  // with the run-loop is gone (this used to run from beginShutdown()).
  SyncStats();
}
