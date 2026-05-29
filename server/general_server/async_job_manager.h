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

#include <vector>

#include "basics/common.h"
#include "basics/containers/node_hash_map.h"
#include "basics/read_write_lock.h"
#include "basics/result.h"
#include "rest/general_response.h"
#include "utils/exec_context.h"

namespace sdb::rest {

class RestHandler;

struct AsyncJobResult {
  enum Status {
    kJobUndefined,
    kJobPending,
    kJobDone,
  };
  using IdType = uint64_t;

  AsyncJobResult() = default;
  AsyncJobResult(IdType job_id, Status status,
                 std::shared_ptr<RestHandler> handler);

  IdType job_id = 0;
  std::unique_ptr<GeneralResponse> response;
  double stamp = 0.0;
  Status status = kJobUndefined;
  std::shared_ptr<RestHandler> handler;
};

/// Manages responses which will be fetched later by clients.
class AsyncJobManager {
 public:
  AsyncJobManager(const AsyncJobManager&) = delete;
  AsyncJobManager& operator=(const AsyncJobManager&) = delete;

  using JobList =
    containers::NodeHashMap<AsyncJobResult::IdType,
                            std::pair<std::string, AsyncJobResult>>;

  AsyncJobManager() = default;

  ~AsyncJobManager() {
    // remove all results that haven't been fetched
    deleteJobs(ExecContext::superuser());
  }

  std::unique_ptr<GeneralResponse> getJobResult(const ExecContext& exec,
                                                AsyncJobResult::IdType id,
                                                AsyncJobResult::Status& status,
                                                bool remove_from_list);
  bool deleteJobResult(const ExecContext& exec, AsyncJobResult::IdType id);
  void deleteJobs(const ExecContext& exec);
  void deleteExpiredJobResults(const ExecContext& exec, double stamp);

  /// cancel and delete a specific job
  Result cancelJob(const ExecContext& exec, AsyncJobResult::IdType id);

  /// cancel and delete all pending / done jobs
  Result clearAllJobs();

  std::vector<AsyncJobResult::IdType> pending(const ExecContext& exec,
                                              size_t max_count);
  std::vector<AsyncJobResult::IdType> done(const ExecContext& exec,
                                           size_t max_count);
  std::vector<AsyncJobResult::IdType> byStatus(const ExecContext& exec,
                                               AsyncJobResult::Status,
                                               size_t max_count);
  void initAsyncJob(std::shared_ptr<RestHandler>);
  void finishAsyncJob(RestHandler*);
  std::pair<uint64_t, uint64_t> getNrPendingAndDone();

 private:
  absl::Mutex _lock;
  JobList _jobs;
};

}  // namespace sdb::rest
