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

#include <cstdint>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

#include "pg/progress_registry.h"
#include "utils/drain_gate.h"

namespace sdb::backup {

inline constexpr std::string_view kBackupDir = "backup";

struct StoreCut {
  duckdb::idx_t database_oid = 0;
  duckdb::idx_t oid = 0;
};

struct Cut {
  std::filesystem::path log_link;
  uint64_t log_size = 0;
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

  uint64_t Begin(duckdb::ClientContext& context, BackupOwner owner,
                 std::string_view label, pg::ProgressMetrics& progress);
  FinishedBackup Finish(const void* session);
  void AbortIfOwnedBy(const void* session) noexcept;
  void End() noexcept;

 private:
  void Stop() noexcept ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  absl::Mutex _mutex;
  bool _active ABSL_GUARDED_BY(_mutex) = false;
  utils::DrainGate::Closed _checkpoints ABSL_GUARDED_BY(_mutex);
  utils::DrainGate::Closed _maintenance ABSL_GUARDED_BY(_mutex);
  std::vector<duckdb::shared_ptr<duckdb::AttachedDatabase>> _pins
    ABSL_GUARDED_BY(_mutex);
  std::filesystem::path _backup_dir ABSL_GUARDED_BY(_mutex);
  BackupOwner _owner ABSL_GUARDED_BY(_mutex);
  std::string _label_file ABSL_GUARDED_BY(_mutex);
};

}  // namespace sdb::backup
