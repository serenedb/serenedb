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

#include <thread>

namespace sdb {

class RocksDBEngineCatalog;

class RocksDBBackgroundThread {
 public:
  RocksDBBackgroundThread(RocksDBEngineCatalog& eng, double interval);
  ~RocksDBBackgroundThread();

  // start the background thread.
  void start();

  // request the thread to stop and notify it; idempotent.
  void beginShutdown();

 private:
  void run();

  RocksDBEngineCatalog& _engine;

  /// interval in which we will run
  const double _interval;

  /// condition variable for heartbeat; also guards _stopping.
  struct {
    absl::CondVar cv;
    absl::Mutex mutex;
  } _condition;

  bool _stopping{false};

  // Declared last so it joins first on destruction.
  std::jthread _thread;
};

}  // namespace sdb
