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

#include "scheduler_feature.h"

#include <absl/flags/flag.h>

#include "app/app_server.h"
#include "basics/log.h"
#include "basics/number_of_cores.h"
#include "general_server/scheduler.h"
#include "general_server/signal_handling.h"

ABSL_FLAG(uint64_t, server_io_threads, 0,
          "IO threads for HTTP and pg-wire connections "
          "(0 = max(1, cpu_count / 4)).");
ABSL_FLAG(uint64_t, server_request_threads, 0,
          "Maximum worker threads for the SchedulerFeature pool (the legacy "
          "ArangoDB scheduler used for HTTP/REST work; distinct from "
          "server_io_threads and server_cpu_threads). 0 = auto-detect "
          "(max(32, 2 * cpu_count)).");
ABSL_FLAG(uint64_t, server_maximal_queue_size, 4096,
          "Maximum FIFO queue depth per priority in the SchedulerFeature "
          "scheduler; controls backpressure for the HTTP/REST work queue.");
ABSL_FLAG(uint64_t, server_cpu_threads, 0,
          "Executor pool size at process start. 0 = let DuckDB "
          "auto-detect from cpu_count. The SQL-level `SET threads = N` "
          "continues to win at runtime.");

namespace sdb {
namespace {

/// return the default number of threads to use (upper bound)
size_t DefaultNumberOfThreads() {
  // use two times the number of hardware threads as the default
  size_t result = number_of_cores::GetValue() * 2;
  // but only if higher than 64. otherwise use a default minimum value of 32
  if (result < 32) {
    result = 32;
  }
  return result;
}

}  // namespace

SchedulerFeature::SchedulerFeature()
  : _nr_maximal_threads(absl::GetFlag(FLAGS_server_request_threads)),
    _queue_size(absl::GetFlag(FLAGS_server_maximal_queue_size)) {
  if (_nr_maximal_threads == 0) {
    _nr_maximal_threads = DefaultNumberOfThreads();
  }
  gInstance = this;
}

SchedulerFeature::~SchedulerFeature() { gInstance = nullptr; }

void SchedulerFeature::start() {
  SDB_ASSERT(4 <= _nr_minimal_threads);
  SDB_ASSERT(_nr_minimal_threads <= _nr_maximal_threads);
  SDB_ASSERT(_queue_size > 0);

  auto sched = std::make_unique<Scheduler>(
    app::AppServer::Instance(), _nr_minimal_threads, _nr_maximal_threads,
    _queue_size, _fifo1_size, _fifo2_size, _fifo3_size,
    _unavailability_queue_fill_grade);

  gScheduler = sched.get();

  _scheduler = std::move(sched);

  _scheduler->start();
  SDB_DEBUG(STARTUP, "scheduler has started");

  // Install signal handlers now that the scheduler is up (SIGHUP queues
  // onto it).
  signal_handling::Install();
}

void SchedulerFeature::stop() {
  if (!_scheduler) {
    return;
  }
  signal_handling::Shutdown();
  _scheduler->shutdown();
  gScheduler = nullptr;
  _scheduler.reset();
}

}  // namespace sdb
