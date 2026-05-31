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
  : _scheduler(nullptr), _metrics_feature(sdb::metrics::GetMetrics()) {
  const auto n = number_of_cores::GetValue();

  SDB_DEBUG(GENERAL, "Detected number of processors: ", n);

  SDB_ASSERT(n > 0);
  if (_nr_maximal_threads == 0) {
    _nr_maximal_threads = DefaultNumberOfThreads();
  }

  if (_nr_minimal_threads < 4) {
    SDB_WARN(GENERAL, "--server.minimal-threads (", _nr_minimal_threads,
             ") must be at least 4");
    _nr_minimal_threads = 4;
  }

  if (_nr_minimal_threads >= _nr_maximal_threads) {
    SDB_WARN(GENERAL, "--server.maximal-threads (", _nr_maximal_threads,
             ") should be at least ", (_nr_minimal_threads + 1),
             ", raising it");
    _nr_maximal_threads = _nr_minimal_threads;
  }

  if (_queue_size == 0) {
    // Note that this is way smaller than the default of 4096!
    SDB_ASSERT(_nr_maximal_threads > 0);
    _queue_size = _nr_maximal_threads * 8;
    SDB_ASSERT(_queue_size > 0);
  }

  if (_fifo1_size < 1) {
    _fifo1_size = 1;
  }

  if (_fifo2_size < 1) {
    _fifo2_size = 1;
  }

  SDB_ASSERT(_queue_size > 0);
  gInstance = this;
}

SchedulerFeature::~SchedulerFeature() { gInstance = nullptr; }

void SchedulerFeature::start() {
  SDB_ASSERT(4 <= _nr_minimal_threads);
  SDB_ASSERT(_nr_minimal_threads <= _nr_maximal_threads);
  SDB_ASSERT(_queue_size > 0);

  auto sched = std::make_unique<Scheduler>(
    SerenedServer::Instance(), _nr_minimal_threads, _nr_maximal_threads,
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
