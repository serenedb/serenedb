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

#include <atomic>

#include "app/app_server.h"
#include "basics/logger/logger.h"
#include "basics/system-functions.h"
#include "metrics/gauge_builder.h"
#include "metrics/metrics_feature.h"
#include "rest_server/flush_feature.h"
#include "rest_server/serened_single.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_settings_manager.h"

#ifdef SDB_CLUSTER
#include "replication/replication_clients.h"
#include "replication/replication_feature.h"
#include "rocksdb_engine/rocksdb_dump_manager.h"
#include "rocksdb_engine/rocksdb_replication_manager.h"
#endif

using namespace sdb;

DECLARE_GAUGE(rocksdb_wal_released_tick_replication, uint64_t,
              "Released tick for RocksDB WAL deletion (replication-induced)");

RocksDBBackgroundThread::RocksDBBackgroundThread(RocksDBEngineCatalog& engine,
                                                 double interval)
  : Thread(SerenedServer::Instance(), "RocksDBThread"),
    _engine(engine),
    _interval(interval),
    _metrics_wal_released_tick_replication(
      SerenedServer::Instance().getFeature<metrics::MetricsFeature>().add(
        rocksdb_wal_released_tick_replication{})) {}

RocksDBBackgroundThread::~RocksDBBackgroundThread() { shutdown(); }

void RocksDBBackgroundThread::beginShutdown() {
  Thread::beginShutdown();

  // wake up the thread that may be waiting in run()
  absl::MutexLock guard{&_condition.mutex};
  _condition.cv.notify_all();
}

void RocksDBBackgroundThread::run() {
  auto& flush_feature = SerenedServer::Instance().getFeature<FlushFeature>();

  const double start_time = utilities::GetMicrotime();
  uint64_t runs_until_sync_forced = 1;
  constexpr uint64_t kMaxRunsUntilSyncForced = 5;

  while (!isStopping()) {
    {
      absl::MutexLock guard{&_condition.mutex};
      _condition.cv.WaitWithTimeout(&_condition.mutex,
                                    absl::Seconds(_interval));
    }

    if (_engine.inRecovery()) {
      continue;
    }

    SDB_IF_FAILURE("RocksDBBackgroundThread::run") { continue; }

    try {
      if (!isStopping()) {
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

          SDB_TRACE("xxxxx", Logger::ENGINES, "running ",
                    (force_sync ? "forced " : ""), "background settings sync");

          double start = utilities::GetMicrotime();
          auto sync_res = _engine.settingsManager()->sync(force_sync);
          double end = utilities::GetMicrotime();

          if (!sync_res) {
            SDB_WARN("xxxxx", Logger::ENGINES,
                     "background settings sync failed: ",
                     sync_res.error().errorMessage());
          } else if (*sync_res) {
            // reset our counter
            runs_until_sync_forced = kMaxRunsUntilSyncForced;
          }

          if (end - start > 5.0) {
            SDB_WARN("xxxxx", Logger::ENGINES,
                     "slow background settings sync took: ",
                     absl::StrFormat("%.6f", end - start), " s");
          } else if (end - start > 0.75) {
            SDB_DEBUG("xxxxx", Logger::ENGINES,
                      "slow background settings sync took: ",
                      absl::StrFormat("%.6f", end - start), " s");
          }
        } catch (const std::exception& ex) {
          SDB_WARN("xxxxx", Logger::ENGINES,
                   "caught exception in rocksdb background sync operation: ",
                   ex.what());
        }
      }

      bool force = isStopping();
#ifdef SDB_CLUSTER
      _engine.replicationManager()->garbageCollect(force);
      _engine.dumpManager()->garbageCollect(force);
#endif

      if (!force) {
        try {
          // this only schedules tree rebuilds, but the actual rebuilds are
          // performed by async tasks in the scheduler.
          _engine.processTreeRebuilds();
        } catch (const std::exception& ex) {
          SDB_WARN("xxxxx", Logger::ENGINES,
                   "caught exception during tree rebuilding: ", ex.what());
        }
      }

      const uint64_t latest_seq_no = _engine.db()->GetLatestSequenceNumber();
      const auto earliest_seq_needed =
        _engine.settingsManager()->earliestSeqNeeded();

      uint64_t min_tick = latest_seq_no;

      if (earliest_seq_needed < min_tick) {
        min_tick = earliest_seq_needed;
      }

      uint64_t min_tick_for_replication = latest_seq_no;

#ifdef SDB_CLUSTER
      for (auto* clients : GetAllReplicationClients()) {
        // lowestServedValue will return the lowest of the lastServedTick
        // values stored, or UINT64_MAX if no clients are registered
        min_tick_for_replication =
          std::min(min_tick_for_replication, clients->lowestServedValue());
      }
#endif
      min_tick = std::min(min_tick, min_tick_for_replication);

      _metrics_wal_released_tick_replication.store(min_tick_for_replication,
                                                   std::memory_order_relaxed);

      SDB_DEBUG("xxxxx", Logger::ENGINES, "latest seq number: ", latest_seq_no,
                ", earliest seq needed: ", earliest_seq_needed,
                ", min tick for replication: ", min_tick_for_replication);

      try {
        _engine.flushOpenFilesIfRequired();
      } catch (...) {
        // whatever happens here, we don't want it to block/skip any of
        // the following operations
      }

      bool can_prune = utilities::GetMicrotime() >=
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

      if (!isStopping()) {
        _engine.processCompactions();
      }
    } catch (const std::exception& ex) {
      SDB_WARN("xxxxx", Logger::ENGINES,
               "caught exception in rocksdb background thread: ", ex.what());
    } catch (...) {
      SDB_WARN("xxxxx", Logger::ENGINES,
               "caught unknown exception in rocksdb background");
    }
  }

  // final write on shutdown
  auto sync_res = _engine.settingsManager()->sync(/*force*/ true);
  if (!sync_res) {
    SDB_WARN("xxxxx", Logger::ENGINES,
             "caught exception during final RocksDB sync operation: ",
             sync_res.error().errorMessage());
  }
}
