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
#include <deque>
#include <duckdb/catalog/job_schedule.hpp>
#include <duckdb/catalog/schema_info.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/identifier.hpp>
#include <duckdb/common/optional_ptr.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <memory>
#include <string>
#include <vector>
#include <yaclib/algo/wait_group.hpp>

namespace duckdb {

class ClientContext;
class SQLStatement;

}  // namespace duckdb
namespace sdb::catalog {

class JobCatalogEntry;

}  // namespace sdb::catalog
namespace sdb {

void VerifySchedule(const duckdb::JobSchedule& schedule);

struct JobRunRecord {
  duckdb::idx_t database_oid = 0;
  duckdb::Identifier catalog;
  duckdb::Identifier schema;
  duckdb::Identifier name;
  bool manual = false;
  duckdb::timestamp_t start;
  duckdb::timestamp_t finish;
  bool success = false;
  std::string error;
};

struct JobStatus {
  duckdb::JobSchedule schedule;
  bool suspended = false;
  uint32_t running = 0;
  duckdb::timestamp_t next_run;
  uint64_t run_count = 0;
  uint64_t failure_count = 0;
  JobRunRecord last_run;
};

struct JobDefinition {
  duckdb::idx_t database_oid = 0;
  duckdb::Identifier catalog;
  duckdb::shared_ptr<duckdb::SchemaInfo> schema;
  duckdb::Identifier name;
  duckdb::idx_t owner = 0;
  std::shared_ptr<duckdb::SQLStatement> body;
};

struct JobState {
  absl::Mutex mutex;
  JobDefinition definition;
  JobStatus status;
  uint64_t timer = 0;
  bool dropped = false;
  std::vector<duckdb::shared_ptr<duckdb::ClientContext>> contexts;
};

class JobScheduler final {
 public:
  static constexpr size_t kDefaultHistorySize = 1024;

  static JobScheduler* Instance() noexcept { return gInstance; }

  JobScheduler() { gInstance = this; }
  ~JobScheduler() { gInstance = nullptr; }

  void Start();
  void Schedule(catalog::JobCatalogEntry& job);
  void Execute(duckdb::ClientContext& caller, catalog::JobCatalogEntry& job);
  std::vector<JobRunRecord> GetHistory();
  void SetHistorySize(size_t size);
  void Stop();

 private:
  void Run(std::shared_ptr<JobState> state, uint64_t timer);
  duckdb::ErrorData RunBody(std::shared_ptr<JobState> state, JobDefinition job,
                            duckdb::optional_ptr<duckdb::ClientContext> caller,
                            uint64_t timer);

  inline static JobScheduler* gInstance = nullptr;

  yaclib::WaitGroup<> _runs{1};
  absl::Mutex _mutex;
  std::deque<JobRunRecord> _history;
  size_t _history_size = kDefaultHistorySize;
};

}  // namespace sdb
