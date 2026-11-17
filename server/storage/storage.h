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

#include <string>

#include "runtime/runtime.h"
#include "search/engine.h"
#include "server/utils/lockfile.h"

namespace sdb {

struct StorageConfig {
  std::string directory;
  std::string hba_config;
  bool default_database = true;
};

StorageConfig ReadStorageConfig();

class Storage final {
 public:
  Storage(Runtime& runtime, const StorageConfig& config);
  ~Storage();

 private:
  bool Lock(const std::string& directory);
  void Close() noexcept;

  Runtime& _runtime;
  FileLock _lock;
  search::SearchEngine _search;
};

}  // namespace sdb
