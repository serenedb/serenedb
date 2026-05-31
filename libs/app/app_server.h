////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include "basics/assert.h"

namespace sdb::app {

// Lifecycle entry points: CLI parsing + signal-driven shutdown wait.
// RunServer drives each feature's start/stop directly. The class is
// retained only because a handful of subsystems (RestHandler, Scheduler,
// RocksDBMetricsListener, ...) accept an `AppServer&` as a pass-through
// context token; their references resolve back to Instance().
class AppServer {
 public:
  static AppServer& Instance() noexcept {
    SDB_ASSERT(gInstance);
    return *gInstance;
  }

  AppServer() {
    SDB_ASSERT(gInstance == nullptr, "AppServer is a singleton");
    gInstance = this;
  }
  ~AppServer() { gInstance = nullptr; }

  AppServer(const AppServer&) = delete;
  AppServer& operator=(const AppServer&) = delete;

  void parseOptions(int argc, char* argv[]);

  // Block until BeginShutdown() (signal handler) wakes the eventfd.
  void wait();

 private:
  inline static AppServer* gInstance = nullptr;
};

}  // namespace sdb::app
