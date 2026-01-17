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

#pragma once

#include <absl/synchronization/mutex.h>
#include <rocksdb/types.h>

#include <chrono>

#include "basics/common.h"
#include "basics/condition_variable.h"
#include "basics/result.h"
#include "basics/thread.h"

namespace rocksdb {
class DB;
}

namespace sdb {

class RocksDBEngineCatalog;

class RocksDBSyncThread final : public Thread {
 public:
  RocksDBSyncThread(RocksDBEngineCatalog& engine,
                    std::chrono::milliseconds interval,
                    std::chrono::milliseconds delay_threshold);

  ~RocksDBSyncThread() final;

  void beginShutdown() final;

  /// updates last sync time and calls the synchronization
  /// this is the preferred method to call when trying to avoid redundant
  /// syncs by foreground work and the background sync thread
  Result syncWal();

  /// unconditionally syncs the RocksDB WAL, static variant
  static Result sync(rocksdb::DB* db);

 private:
  void run() final;

  RocksDBEngineCatalog& _engine;

  /// the sync interval
  const std::chrono::milliseconds _interval;

  /// last time we synced the RocksDB WAL
  std::chrono::time_point<std::chrono::steady_clock> _last_sync_time;

  /// the last definitely synced RocksDB WAL sequence number
  rocksdb::SequenceNumber _last_sequence_number;

  /// threshold for self-observation of WAL disk syncs.
  /// if the last WAL sync happened longer ago than this configured
  /// threshold, a warning will be logged on every invocation of the
  /// sync thread
  const std::chrono::milliseconds _delay_threshold;

  /// protects _last_sync_time and _last_sequence_number
  struct {
    absl::CondVar cv;
    absl::Mutex mutex;
  } _condition;
};
}  // namespace sdb
