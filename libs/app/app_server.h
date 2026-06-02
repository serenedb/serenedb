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

  void wait();

 private:
  inline static AppServer* gInstance = nullptr;
};

}  // namespace sdb::app
