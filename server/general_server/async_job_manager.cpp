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

#include "async_job_manager.h"

#include <absl/strings/str_cat.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include "basics/errors.h"
#include "basics/log.h"
#include "general_server/rest_handler.h"
#include "general_server/state.h"
#include "rest/general_response.h"
#include "utils/exec_context.h"

namespace sdb::rest {
namespace {

bool Authorized(
  const ExecContext& exec,
  const std::pair<std::string, sdb::rest::AsyncJobResult>& user_result) {
  return exec.isSuperuser() || exec.user() == user_result.first;
}

}  // namespace

AsyncJobResult::AsyncJobResult(IdType job_id, Status status,
                               std::shared_ptr<RestHandler> handler)
  : job_id{job_id},
    stamp{absl::ToDoubleSeconds(absl::Now() - absl::UnixEpoch())},
    status{status},
    handler{std::move(handler)} {}

std::unique_ptr<GeneralResponse> AsyncJobManager::getJobResult(
  const ExecContext& exec, AsyncJobResult::IdType id,
  AsyncJobResult::Status& status, bool remove_from_list) {
  absl::WriterMutexLock write_locker{&_lock};
  auto it = _jobs.find(id);
  if (it == _jobs.end() || !Authorized(exec, it->second)) {
    status = AsyncJobResult::kJobUndefined;
    return nullptr;
  }
  status = it->second.second.status;
  if (status == AsyncJobResult::kJobPending || !remove_from_list) {
    return nullptr;
  }
  auto response = std::move(it->second.second.response);
  _jobs.erase(it);
  return response;
}

bool AsyncJobManager::deleteJobResult(const ExecContext& exec,
                                      AsyncJobResult::IdType id) {
  absl::WriterMutexLock write_locker{&_lock};
  auto it = _jobs.find(id);
  if (it == _jobs.end() || !Authorized(exec, it->second)) {
    return false;
  }
  _jobs.erase(it);
  return true;
}

void AsyncJobManager::deleteJobs(const ExecContext& exec) {
  absl::WriterMutexLock write_locker{&_lock};
  absl::erase_if(_jobs,
                 [&](const auto& job) { return Authorized(exec, job.second); });
}

void AsyncJobManager::deleteExpiredJobResults(const ExecContext& exec,
                                              double stamp) {
  absl::WriterMutexLock write_locker{&_lock};
  absl::erase_if(_jobs, [&](const auto& job) {
    return Authorized(exec, job.second) && job.second.second.stamp < stamp;
  });
}

Result AsyncJobManager::cancelJob(const ExecContext& exec,
                                  AsyncJobResult::IdType id) {
  absl::WriterMutexLock write_locker{&_lock};
  auto it = _jobs.find(id);
  if (it == _jobs.end() || !Authorized(exec, it->second)) {
    return {ERROR_HTTP_NOT_FOUND, "could not find job (", id,
            ") in AsyncJobManager during cancel operation"};
  }
  if (auto& handler = it->second.second.handler) {
    handler->cancel();
    // handlers running async tasks use shared_ptr to keep alive
    handler = nullptr;
  }
  return {};
}

Result AsyncJobManager::clearAllJobs() {
  Result r;
  absl::WriterMutexLock write_locker{&_lock};
  for (auto& [_, user_result] : _jobs) {
    if (auto& handler = user_result.second.handler) {
      handler->cancel();
    }
  }
  _jobs.clear();
  return r;
}

std::vector<AsyncJobResult::IdType> AsyncJobManager::pending(
  const ExecContext& exec, size_t max_count) {
  return byStatus(exec, AsyncJobResult::kJobPending, max_count);
}

std::vector<AsyncJobResult::IdType> AsyncJobManager::done(
  const ExecContext& exec, size_t max_count) {
  return byStatus(exec, AsyncJobResult::kJobDone, max_count);
}

std::vector<AsyncJobResult::IdType> AsyncJobManager::byStatus(
  const ExecContext& exec, AsyncJobResult::Status status, size_t max_count) {
  std::vector<AsyncJobResult::IdType> jobs;
  absl::ReaderMutexLock read_locker{&_lock};
  for (const auto& [id, user_result] : _jobs) {
    if (status == user_result.second.status && Authorized(exec, user_result)) {
      jobs.emplace_back(id);
      if (jobs.size() == max_count) {
        break;
      }
    }
  }
  return jobs;
}

std::pair<uint64_t, uint64_t> AsyncJobManager::getNrPendingAndDone() {
  uint64_t pending = 0;
  uint64_t done = 0;
  absl::ReaderMutexLock read_locker{&_lock};
  for (const auto& [_, user_result] : _jobs) {
    if (user_result.second.status == AsyncJobResult::kJobPending) {
      ++pending;
    } else if (user_result.second.status == AsyncJobResult::kJobDone) {
      ++done;
    }
  }
  return {pending, done};
}

void AsyncJobManager::initAsyncJob(std::shared_ptr<RestHandler> handler) {
  handler->assignHandlerId();
  auto id = handler->handlerId();
  std::string user = handler->request()->user();
  AsyncJobResult result{id, AsyncJobResult::kJobPending, std::move(handler)};

  absl::WriterMutexLock write_locker{&_lock};
  _jobs.try_emplace(id, std::move(user), std::move(result));
}

void AsyncJobManager::finishAsyncJob(RestHandler* handler) {
  auto id = handler->handlerId();
  auto response = handler->stealResponse();
  absl::WriterMutexLock write_locker{&_lock};
  auto it = _jobs.find(id);
  if (it == _jobs.end()) {
    return;  // job is already deleted
  }
  it->second.second.response = std::move(response);
  it->second.second.status = AsyncJobResult::kJobDone;
  it->second.second.stamp =
    absl::ToDoubleSeconds(absl::Now() - absl::UnixEpoch());
}

}  // namespace sdb::rest
