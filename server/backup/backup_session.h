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

#pragma once

#include <absl/synchronization/mutex.h>

#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <vector>

namespace duckdb {

class AttachedDatabase;
class SingleFileBlockManager;
class StorageLockKey;

}  // namespace duckdb
namespace sdb::backup {

class BackupSession final {
 public:
  static BackupSession& Instance();

  void Begin();
  void End() noexcept;
  bool IsActive();

 private:
  absl::Mutex _mutex;
  bool _active ABSL_GUARDED_BY(_mutex) = false;
  std::vector<duckdb::shared_ptr<duckdb::AttachedDatabase>> _pins
    ABSL_GUARDED_BY(_mutex);
  std::vector<duckdb::unique_ptr<duckdb::StorageLockKey>> _checkpoint_keys
    ABSL_GUARDED_BY(_mutex);
  std::vector<duckdb::SingleFileBlockManager*> _block_managers
    ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::backup
