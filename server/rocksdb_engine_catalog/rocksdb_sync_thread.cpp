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

#include "rocksdb_sync_thread.h"

#include <rocksdb/status.h>
#include <rocksdb/utilities/transaction_db.h>

#include "app/app_server.h"
#include "basics/logger/logger.h"
#include "rest_server/serened_single.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

using namespace sdb;

RocksDBSyncThread::RocksDBSyncThread(RocksDBEngineCatalog& engine,
                                     std::chrono::milliseconds interval,
                                     std::chrono::milliseconds delay_threshold)
  : Thread(SerenedServer::Instance(), "RocksDBSync"),
    _engine(engine),
    _interval(interval),
    _last_sync_time(std::chrono::steady_clock::now()),
    _last_sequence_number(0),
    _delay_threshold(delay_threshold) {}

RocksDBSyncThread::~RocksDBSyncThread() { shutdown(); }

Result RocksDBSyncThread::syncWal() {
  // note the following line in RocksDB documentation (rocksdb/db.h):
  // > Currently only works if allow_mmap_writes = false in Options.
  SDB_ASSERT(!_engine.rocksDBOptions().allow_mmap_writes);

  auto db = _engine.db()->GetBaseDB();

  // set time of last syncing under the lock
  const auto now = std::chrono::steady_clock::now();
  const auto last_sequence_number = db->GetLatestSequenceNumber();

  // actual syncing is done without holding the lock
  auto result = sync(db);

  if (result.ok()) {
    absl::MutexLock guard{&_condition.mutex};

    if (now > _last_sync_time) {
      // update last sync time...
      _last_sync_time = now;
    }

    if (last_sequence_number > _last_sequence_number) {
      // update last sequence number
      _last_sequence_number = last_sequence_number;
    }
  }

  return result;
}

Result RocksDBSyncThread::sync(rocksdb::DB* db) {
  SDB_TRACE("xxxxx", Logger::ENGINES, "syncing RocksDB WAL");

  rocksdb::Status status = db->SyncWAL();
  if (!status.ok()) {
    return rocksutils::ConvertStatus(status);
  }
  return {};
}

void RocksDBSyncThread::beginShutdown() {
  Thread::beginShutdown();

  // wake up the thread that may be waiting in run()
  absl::MutexLock guard{&_condition.mutex};
  _condition.cv.notify_all();
}

void RocksDBSyncThread::run() {
  auto db = _engine.db()->GetBaseDB();

  SDB_TRACE("xxxxx", Logger::ENGINES,
            "starting RocksDB sync thread with interval ", _interval.count(),
            " milliseconds");

  while (!isStopping()) {
    try {
      const auto now = std::chrono::steady_clock::now();

      rocksdb::SequenceNumber last_sequence_number;
      rocksdb::SequenceNumber previous_last_sequence_number;
      std::chrono::time_point<std::chrono::steady_clock> last_sync_time;
      std::chrono::time_point<std::chrono::steady_clock>
        previous_last_sync_time;

      {
        // wait for time to elapse, and after that update last sync time
        absl::MutexLock guard{&_condition.mutex};

        previous_last_sequence_number = _last_sequence_number;
        previous_last_sync_time = _last_sync_time;
        const auto end = _last_sync_time + _interval;
        if (end > now) {
          _condition.cv.WaitWithTimeout(&_condition.mutex,
                                        absl::FromChrono(end - now));
        }

        if (_last_sync_time > previous_last_sync_time) {
          // somebody else outside this thread has called sync...
          continue;
        }

        last_sync_time = std::chrono::steady_clock::now();
        last_sequence_number = db->GetLatestSequenceNumber();

        if (last_sequence_number == previous_last_sequence_number) {
          // nothing to sync, so don't cause unnecessary load.
          // still update our lastSyncTime to now, so we don't run into warnings
          // later with syncs being reported as delayed
          _last_sync_time = last_sync_time;
          continue;
        }
      }

      {
        if (_delay_threshold.count() > 0 &&
            (last_sync_time - previous_last_sync_time) > _delay_threshold) {
          SDB_INFO("xxxxx", Logger::ENGINES,
                   "last RocksDB WAL sync happened longer ago than configured "
                   "threshold. ",
                   "last sync happened ",
                   (std::chrono::duration_cast<std::chrono::milliseconds>(
                      last_sync_time - previous_last_sync_time))
                     .count(),
                   " ms ago, threshold value: ", _delay_threshold.count(),
                   " ms");
        }
      }

      Result res = this->sync(db);

      if (res.ok()) {
        // success case
        absl::MutexLock guard{&_condition.mutex};

        if (last_sequence_number > _last_sequence_number) {
          // bump last sequence number we have synced
          _last_sequence_number = last_sequence_number;
        }
        if (last_sync_time > _last_sync_time) {
          _last_sync_time = last_sync_time;
        }
      } else {
        // could not sync... in this case, don't advance our last
        // sync time and last synced sequence number
        SDB_ERROR("xxxxx", Logger::ENGINES,
                  "could not sync RocksDB WAL: ", res.errorMessage());
      }
    } catch (const std::exception& ex) {
      SDB_ERROR("xxxxx", Logger::ENGINES,
                "caught exception in RocksDBSyncThread: ", ex.what());
    } catch (...) {
      SDB_ERROR("xxxxx", Logger::ENGINES,
                "caught unknown exception in RocksDBSyncThread");
    }
  }
}
