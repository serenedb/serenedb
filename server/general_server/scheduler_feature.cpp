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

#include "app/app_server.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "general_server/scheduler.h"
#include "general_server/signal_handling.h"
#include "metrics/metrics_feature.h"

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
  : _metrics_feature(sdb::metrics::GetMetrics()) {
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
    _unavailability_queue_fill_grade, _metrics_feature);

  gScheduler = sched.get();

  _scheduler = std::move(sched);

  _scheduler->start();
  SDB_DEBUG(STARTUP, "scheduler has started");

  // Install signal handlers now that the scheduler is up (SIGHUP queues
  // onto it).
  signal_handling::Install();
}

void SchedulerFeature::stop() {
  signal_handling::Shutdown();
  _scheduler->shutdown();
  gScheduler = nullptr;
  // TSAN: setting this global pointer to nullptr at shutdown is a
  // synchronization point that the sanitizer doesn't always pick up via
  // implicit happens-before edges. We use std::atomic_ref at the read
  // sites to silence it.
  _scheduler.reset();
}

}  // namespace sdb
