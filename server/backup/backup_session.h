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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class AttachedDatabase;
class SingleFileBlockManager;
class StorageLockKey;

}  // namespace duckdb
namespace sdb::backup {

inline constexpr std::string_view kBackupDir = "backup";
inline constexpr std::string_view kCatalogLink = "catalog.wal";

struct DatabaseCut {
  duckdb::idx_t oid = 0;
  uint64_t checkpoint_iteration = 0;
  duckdb::idx_t wal_size = 0;
  duckdb::idx_t used_size = 0;
  std::optional<uint64_t> search_tick;
};

struct StoreCut {
  duckdb::idx_t database_oid = 0;
  duckdb::idx_t oid = 0;
  uint64_t generation = 0;
};

struct Cut {
  std::filesystem::path catalog_link;
  duckdb::idx_t catalog_log_size = 0;
  std::vector<DatabaseCut> databases;
  std::vector<StoreCut> stores;
};

struct BackupOwner {
  const void* session = nullptr;
  int32_t pid = 0;
};

struct FinishedBackup {
  uint64_t lsn = 0;
  std::string label_file;
};

class BackupSession final {
 public:
  static BackupSession& Instance();

  uint64_t Begin(BackupOwner owner, std::string_view label,
                 absl::FunctionRef<bool()> cancelled);
  FinishedBackup Finish(const void* session);
  void AbortIfOwnedBy(const void* session) noexcept;
  void End() noexcept;
  bool IsActive();

 private:
  void Stop() noexcept ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  absl::Mutex _mutex;
  bool _active ABSL_GUARDED_BY(_mutex) = false;
  std::vector<duckdb::shared_ptr<duckdb::AttachedDatabase>> _pins
    ABSL_GUARDED_BY(_mutex);
  std::vector<duckdb::unique_ptr<duckdb::StorageLockKey>> _checkpoint_keys
    ABSL_GUARDED_BY(_mutex);
  std::vector<duckdb::SingleFileBlockManager*> _block_managers
    ABSL_GUARDED_BY(_mutex);
  std::filesystem::path _backup_dir ABSL_GUARDED_BY(_mutex);
  BackupOwner _owner ABSL_GUARDED_BY(_mutex);
  std::string _label_file ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::backup
