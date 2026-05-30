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

#include <atomic>
#include <chrono>
#include <limits>
#include <thread>

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/asio_ns.h"
#include "basics/lifecycle.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/signals.h"
#include "basics/system-functions.h"
#include "general_server/scheduler.h"
#include "general_server/state.h"
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

// atomic flag to track shutdown requests
std::atomic_bool gReceivedShutdownRequest{false};

// id of process that will not be used to send SIGHUP requests
constexpr pid_t kProcessIdUnspecified{std::numeric_limits<pid_t>::min()};

static_assert(kProcessIdUnspecified != 0, "minimum pid number must be != 0");

// id of process that requested a log rotation via SIGHUP
std::atomic<pid_t> gProcessIdRequestingLogRotate{kProcessIdUnspecified};

}  // namespace

struct SchedulerFeature::AsioHandler {
  std::shared_ptr<asio_ns::signal_set> exit_signals;
  std::shared_ptr<asio_ns::signal_set> hangup_signals;
};

SchedulerFeature::SchedulerFeature()
  : _scheduler(nullptr),
    _metrics_feature(sdb::metrics::GetMetrics()),
    _asio_handler(std::make_unique<AsioHandler>()) {
  gInstance = this;
}

SchedulerFeature::~SchedulerFeature() { gInstance = nullptr; }

void SchedulerFeature::validateOptions() {
  const auto n = number_of_cores::GetValue();

  SDB_DEBUG(GENERAL, "Detected number of processors: ", n);

  SDB_ASSERT(n > 0);
  if (_nr_maximal_threads == 0) {
    _nr_maximal_threads = DefaultNumberOfThreads();
  }

  if (_nr_minimal_threads < 4) {
    SDB_WARN(GENERAL, "--server.minimal-threads (",
             _nr_minimal_threads, ") must be at least 4");
    _nr_minimal_threads = 4;
  }

  if (_ongoing_low_priority_multiplier < 1.0) {
    SDB_WARN(GENERAL,
             "--server.ongoing-low-priority-multiplier (",
             _ongoing_low_priority_multiplier,
             ") is less than 1.0, setting to default (4.0)");
    _ongoing_low_priority_multiplier = 4.0;
  }

  if (_nr_minimal_threads >= _nr_maximal_threads) {
    SDB_WARN(GENERAL, "--server.maximal-threads (",
             _nr_maximal_threads, ") should be at least ",
             (_nr_minimal_threads + 1), ", raising it");
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
}

void SchedulerFeature::prepare() {
  SDB_ASSERT(4 <= _nr_minimal_threads);
  SDB_ASSERT(_nr_minimal_threads <= _nr_maximal_threads);
  SDB_ASSERT(_queue_size > 0);

  // single and coordinator are trottle users requests
  uint64_t ongoing_low_priority_limit =
    ServerState::instance()->IsDBServer() || ServerState::instance()->IsAgent()
      ? 0
      : static_cast<uint64_t>(_ongoing_low_priority_multiplier *
                              _nr_maximal_threads);

  auto sched = std::make_unique<Scheduler>(
    SerenedServer::Instance(), _nr_minimal_threads, _nr_maximal_threads,
    _queue_size,
    _fifo1_size, _fifo2_size, _fifo3_size, ongoing_low_priority_limit,
    _unavailability_queue_fill_grade, _metrics_feature);

  gScheduler = sched.get();

  _scheduler = std::move(sched);
}

void SchedulerFeature::start() {
  signalStuffInit();

  bool ok = _scheduler->start();
  if (!ok) {
    SDB_FATAL(GENERAL, "the scheduler cannot be started");
  }
  SDB_DEBUG(STARTUP, "scheduler has started");

  // Install the SIGINT handler now that the scheduler is up.
  buildControlCHandler();
}

void SchedulerFeature::stop() {
  // shutdown user jobs again, in case new ones appear
  signalStuffDeinit();
  _scheduler->shutdown();
}

void SchedulerFeature::unprepare() {
  gScheduler = nullptr;
  // This is to please the TSAN sanitizer: On shutdown, we set this global
  // pointer to nullptr. Other threads read the pointer, but the logic of
  // ApplicationFeatures should ensure that nobody will read the pointer
  // out after the SchedulerFeature has run its unprepare method.
  // Sometimes the TSAN sanitizer cannot recognize this indirect
  // synchronization and complains about reads that have happened before
  // this write here, but are not officially inter-thread synchronized.
  // We use the atomic reference here and in these places to silence TSAN.
  // std::atomic_ref<Scheduler*> schedulerRef{SCHEDULER};
  // schedulerRef.store(nullptr, std::memory_order_relaxed);
  _scheduler.reset();
}

uint64_t SchedulerFeature::maximalThreads() const noexcept {
  return _nr_maximal_threads;
}

// ---------------------------------------------------------------------------
// Signal Handler stuff - no body knows what this has to do with scheduling
// ---------------------------------------------------------------------------

void SchedulerFeature::signalStuffInit() {
  signals::MaskAllSignalsServer();

  struct sigaction action;
  memset(&action, 0, sizeof(action));
  sigfillset(&action.sa_mask);

  // ignore broken pipes
  action.sa_handler = SIG_IGN;

  int res = sigaction(SIGPIPE, &action, nullptr);

  if (res < 0) {
    SDB_ERROR(GENERAL,
              "cannot initialize signal handler for SIGPIPE");
  }

  buildHangupHandler();
}

void SchedulerFeature::signalStuffDeinit() {
  // cancel signals
  if (_asio_handler->exit_signals != nullptr) {
    auto exit_signals = _asio_handler->exit_signals;
    _asio_handler->exit_signals.reset();
    exit_signals->cancel();
  }

  if (_asio_handler->hangup_signals != nullptr) {
    _asio_handler->hangup_signals->cancel();
    _asio_handler->hangup_signals.reset();
  }
}

extern "C" void CExitHandler(int signal, siginfo_t* info, void*) {
  if (signal == SIGQUIT || signal == SIGTERM || signal == SIGINT) {
    if (!gReceivedShutdownRequest.exchange(true)) {
      SDB_INFO(GENERAL, signals::Name(signal),
               " received (sender pid ", (info ? info->si_pid : 0),
               "), beginning shut down sequence");
      lifecycle::gCtrlC.store(true);
    } else {
      SDB_FATAL(GENERAL, signals::Name(signal),
                " received during shutdown sequence (sender pid ", info->si_pid,
                "), terminating!");
    }
  }
}

extern "C" void CHangupHandler(int signal, siginfo_t* info, void*) {
  SDB_ASSERT(signal == SIGHUP);

  // id of process that issued the SIGHUP.
  // if we don't have any information about the issuing process, we
  // assume a pid of 0.
  pid_t process_id_requesting = info ? info->si_pid : 0;
  // note that we need to be able to tell pid 0 and the "unspecified"
  // process id apart.
  static_assert(kProcessIdUnspecified != 0, "unspecified pid should be != 0");

  // the expected process id that we want to see
  pid_t process_id_expected = kProcessIdUnspecified;

  // only set log rotate request if we don't have one queued already. this
  // prevents duplicate execution of log rotate requests.
  // if the CAS fails, it doesn't matter, because it means that a log rotate
  // request was already queued
  if (!gProcessIdRequestingLogRotate.compare_exchange_strong(
        process_id_expected, process_id_requesting)) {
    // already a log rotate request queued. do nothing...
    return;
  }

  // SIGHUP: nothing to reopen (logger writes to stderr only).
  // Keep the rate-limit dance so callers stop re-queuing.
  SchedulerFeature::gScheduler->queue(
    RequestLane::ClientSlow, [process_id_requesting] {
      SDB_INFO(GENERAL, "hangup received (sender pid ", process_id_requesting,
               "); no log file to rotate");
      gProcessIdRequestingLogRotate.store(kProcessIdUnspecified);
    });
}

void SchedulerFeature::buildHangupHandler() {
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  sigfillset(&action.sa_mask);
  action.sa_flags = SA_SIGINFO;
  action.sa_sigaction = CHangupHandler;

  int res = sigaction(SIGHUP, &action, nullptr);

  if (res < 0) {
    SDB_ERROR(GENERAL,
              "cannot initialize signal handler for hang up");
  }
}

void SchedulerFeature::buildControlCHandler() {
  // Signal masking on POSIX platforms
  //
  // POSIX allows signals to be blocked using functions such as sigprocmask()
  // and pthread_sigmask(). For signals to be delivered, programs must ensure
  // that any signals registered using signal_set objects are unblocked in at
  // least one thread.
  signals::UnmaskAllSignals();

  struct sigaction action;
  memset(&action, 0, sizeof(action));
  sigfillset(&action.sa_mask);
  action.sa_flags = SA_SIGINFO;
  action.sa_sigaction = CExitHandler;

  int res = sigaction(SIGINT, &action, nullptr);
  if (res == 0) {
    res = sigaction(SIGQUIT, &action, nullptr);
    if (res == 0) {
      res = sigaction(SIGTERM, &action, nullptr);
    }
  }
  if (res < 0) {
    SDB_ERROR(GENERAL,
              "cannot initialize signal handlers for SIGINT/SIGQUIT/SIGTERM");
  }
}

}  // namespace sdb
