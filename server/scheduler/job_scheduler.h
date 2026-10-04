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

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <deque>
#include <duckdb/catalog/job_schedule.hpp>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace duckdb {

class ClientContext;
class SQLStatement;

}  // namespace duckdb
namespace sdb::catalog {

class JobCatalogEntry;

}  // namespace sdb::catalog
namespace sdb {

class BackgroundScheduler;

struct JobRunRecord {
  duckdb::idx_t database_oid = 0;
  std::string catalog;
  std::string schema;
  std::string name;
  bool manual = false;
  duckdb::timestamp_t start;
  duckdb::timestamp_t finish;
  bool success = false;
  std::string error;
};

struct JobStatus {
  duckdb::JobSchedule schedule;
  bool suspended = false;
  bool running = false;
  duckdb::timestamp_t next_run;
  uint64_t run_count = 0;
  uint64_t failure_count = 0;
  JobRunRecord last_run;
};

class JobScheduler final {
 public:
  static JobScheduler* Instance() noexcept { return gInstance; }

  explicit JobScheduler(BackgroundScheduler& background);
  ~JobScheduler();

  void Start();
  void Schedule(catalog::JobCatalogEntry& job);
  void Drop(catalog::JobCatalogEntry& job);
  void DropDatabase(duckdb::idx_t database_oid);
  void Execute(catalog::JobCatalogEntry& job);
  bool TryGetStatus(catalog::JobCatalogEntry& job, JobStatus& result);
  std::vector<JobRunRecord> GetHistory();
  void Stop();

 private:
  using Key = std::pair<duckdb::idx_t, duckdb::idx_t>;

  struct Definition {
    Key key;
    std::string catalog;
    std::string schema;
    std::string name;
    duckdb::idx_t owner = 0;
    std::shared_ptr<duckdb::SQLStatement> body;
  };

  struct Job {
    Definition definition;
    JobStatus status;
    uint64_t epoch = 0;
    duckdb::shared_ptr<duckdb::ClientContext> context;
  };

  static Key KeyOf(catalog::JobCatalogEntry& job);
  static Definition DefinitionOf(catalog::JobCatalogEntry& job);

  void Run(Key key, uint64_t epoch);
  duckdb::ErrorData RunBody(std::unique_lock<absl::Mutex>& guard,
                            Definition job, bool manual);

  inline static JobScheduler* gInstance = nullptr;

  BackgroundScheduler& _background;
  absl::Mutex _mutex;
  absl::CondVar _idle;
  bool _stopped = false;
  uint64_t _in_flight = 0;
  uint64_t _last_epoch = 0;
  absl::flat_hash_map<Key, Job> _jobs;
  std::deque<JobRunRecord> _history;
};

}  // namespace sdb
