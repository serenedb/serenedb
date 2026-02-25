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
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/asio_ns.h"
#include "basics/logger/appender.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/signals.h"
#include "basics/system-functions.h"
#include "general_server/scheduler.h"
#include "general_server/state.h"
#include "metrics/metrics_feature.h"
#include "rest_server/server_feature.h"

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

SchedulerFeature::SchedulerFeature(Server& server)
  : SerenedFeature{server, name()},
    _scheduler(nullptr),
    _metrics_feature(sdb::metrics::GetMetrics()),
    _asio_handler(std::make_unique<AsioHandler>()) {
  setOptional(false);
}

SchedulerFeature::~SchedulerFeature() = default;

void SchedulerFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  // Different implementations of the Scheduler may require different
  // options to be set. This requires a solution here.

  // max / min number of threads
  options
    ->addOption(
      "--server.maximal-threads",
      std::string("The maximum number of request handling threads to run "
                  "(0 = use system-specific default of ") +
        std::to_string(DefaultNumberOfThreads()) + ")",
      new options::UInt64Parameter(&_nr_maximal_threads),
      options::MakeDefaultFlags(options::Flags::Dynamic))
    .setLongDescription(R"(This option determines the maximum number of
request processing threads the server is allowed to start for request handling.
If this number of threads is already running, serened does not start further
threads for request handling. The default value is
`max(32, 2 * available cores)`, so twice the number of CPU cores, but at least
32 threads.

The actual number of request processing threads is adjusted dynamically at
runtime and is between `--server.minimal-threads` and
`--server.maximal-threads`.)");

  options
    ->addOption("--server.minimal-threads",
                "The minimum number of request handling threads to run.",
                new options::UInt64Parameter(&_nr_minimal_threads),
                options::MakeDefaultFlags(options::Flags::Uncommon))
    .setLongDescription(R"(This option determines the minimum number of
request processing threads the server starts and always keeps around.)");

  // max / min number of threads

  // Concurrency throttling:
  options
    ->addOption("--server.ongoing-low-priority-multiplier",
                "Controls the number of low priority requests that can be "
                "ongoing at a given point in time, relative to the "
                "maximum number of request handling threads.",
                new options::DoubleParameter(&_ongoing_low_priority_multiplier),
                options::MakeDefaultFlags(
                  options::Flags::DefaultNoComponents, options::Flags::OnSingle,
                  options::Flags::OnCoordinator, options::Flags::Uncommon))

    .setLongDescription(R"(There are some countermeasures built into
Coordinators to prevent a cluster from being overwhelmed by too many
concurrently executing requests.

If a request is executed on a Coordinator but needs to wait for some operation
on a DB-Server, the operating system thread executing the request can often
postpone execution on the Coordinator, put the request to one side and do
something else in the meantime. When the response from the DB-Server arrives,
another worker thread continues the work. This is a form of asynchronous
implementation, which is great to achieve better thread utilization and enhance
throughput.

On the other hand, this runs the risk that work is started on new requests
faster than old ones can be finished off. Before version 3.8, this could
overwhelm the cluster over time, and lead to out-of-memory situations and other
unwanted side effects. For example, it could lead to excessive latency for
individual requests.

There is a limit as to how many requests coming from the low priority queue
(most client requests are of this type), can be executed concurrently.
The default value for this is 4 times as many as there are scheduler threads
(see `--server.minimal-threads` and --server.maximal-threads), which is good
for most workloads. Requests in excess of this are not started but remain on
the scheduler's input queue (see `--server.maximal-queue-size`).

Very occasionally, 4 is already too much. You would notice this if the latency
for individual requests is already too high because the system tries to execute
too many of them at the same time (for example, if they fight for resources).

On the other hand, in rare cases it is possible that throughput can be improved
by increasing the value, if latency is not a big issue and all requests
essentially spend their time waiting, so that a high concurrency is acceptable.
This increases memory usage, though.)");

  options
    ->addOption("--server.maximal-queue-size",
                "The size of the priority 3 FIFO.",
                new options::UInt64Parameter(&_fifo3_size))
    .setLongDescription(R"(You can specify the maximum size of the queue for
asynchronous task execution. If the queue already contains this many tasks, new
tasks are rejected until other tasks are popped from the queue. Setting this
value may help preventing an instance from being overloaded or from running out
of memory if the queue is filled up faster than the server can process
requests.)");

  options
    ->addOption("--server.unavailability-queue-fill-grade",
                "The queue fill grade from which onwards the server is "
                "considered unavailable because of an overload (ratio, "
                "0 = disable)",
                new options::DoubleParameter(
                  &_unavailability_queue_fill_grade, /*base*/ 1.0,
                  /*minValue*/ 0.0, /*maxValue*/ 1.0))
    .setLongDescription(R"(You can use this option to set a high-watermark
for the scheduler's queue fill grade, from which onwards the server starts
reporting unavailability via its availability API.

This option has a consequence for the `/_admin/server/availability` REST API
only, which is often called by load-balancers and other availability probing
systems.

The `/_admin/server/availability` REST API returns HTTP 200 if the fill
grade of the scheduler's queue is below the configured value, or HTTP 503 if
the fill grade is equal to or above it. This can be used to flag a server as
unavailable in case it is already highly loaded.

The default value for this option is `0.75` since version 3.8, i.e. 75%.

To prevent sending more traffic to an already overloaded server, it can be
sensible to reduce the default value to even `0.5`. This would mean that
instances with a queue longer than 50% of their maximum queue capacity would
return HTTP 503 instead of HTTP 200 when their availability API is probed.)");

  options->addOption(
    "--server.scheduler-queue-size",
    "The number of simultaneously queued requests inside the scheduler.",
    new options::UInt64Parameter(&_queue_size),
    options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addOption("--server.prio2-size", "The size of the priority 2 FIFO.",
                     new options::UInt64Parameter(&_fifo2_size),
                     options::MakeDefaultFlags(options::Flags::Uncommon));

  options->addOption("--server.prio1-size", "The size of the priority 1 FIFO.",
                     new options::UInt64Parameter(&_fifo1_size),
                     options::MakeDefaultFlags(options::Flags::Uncommon));
}

void SchedulerFeature::validateOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  const auto n = number_of_cores::GetValue();

  SDB_DEBUG("xxxxx", Logger::THREADS, "Detected number of processors: ", n);

  SDB_ASSERT(n > 0);
  if (options->processingResult().touched("server.maximal-threads") &&
      _nr_maximal_threads > 8 * n) {
    SDB_WARN("xxxxx", Logger::THREADS, "--server.maximal-threads (",
             _nr_maximal_threads,
             ") is more than eight times the number of cores (", n,
             "), this might overload the server");
  } else if (_nr_maximal_threads == 0) {
    _nr_maximal_threads = DefaultNumberOfThreads();
  }

  if (_nr_minimal_threads < 4) {
    SDB_WARN("xxxxx", Logger::THREADS, "--server.minimal-threads (",
             _nr_minimal_threads, ") must be at least 4");
    _nr_minimal_threads = 4;
  }

  if (_ongoing_low_priority_multiplier < 1.0) {
    SDB_WARN("xxxxx", Logger::THREADS,
             "--server.ongoing-low-priority-multiplier (",
             _ongoing_low_priority_multiplier,
             ") is less than 1.0, setting to default (4.0)");
    _ongoing_low_priority_multiplier = 4.0;
  }

  if (_nr_minimal_threads >= _nr_maximal_threads) {
    SDB_WARN("xxxxx", Logger::THREADS, "--server.maximal-threads (",
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
    server(), _nr_minimal_threads, _nr_maximal_threads, _queue_size,
    _fifo1_size, _fifo2_size, _fifo3_size, ongoing_low_priority_limit,
    _unavailability_queue_fill_grade, _metrics_feature);

  gScheduler = sched.get();

  _scheduler = std::move(sched);
}

void SchedulerFeature::start() {
  signalStuffInit();

  bool ok = _scheduler->start();
  if (!ok) {
    SDB_FATAL("xxxxx", Logger::FIXME, "the scheduler cannot be started");
  }
  SDB_DEBUG("xxxxx", Logger::STARTUP, "scheduler has started");
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
    SDB_ERROR("xxxxx", Logger::FIXME,
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
      SDB_INFO("xxxxx", Logger::FIXME, signals::Name(signal),
               " received (sender pid ", (info ? info->si_pid : 0),
               "), beginning shut down sequence");
      app::AppServer::gCtrlC.store(true);
    } else {
      SDB_FATAL("xxxxx", Logger::FIXME, signals::Name(signal),
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

  // no log rotate request queued before. now issue one.
  SchedulerFeature::gScheduler->queue(
    RequestLane::ClientSlow, [process_id_requesting] {
      try {
        SDB_INFO("xxxxx", Logger::FIXME,
                 "hangup received, about to reopen logfile (sender pid ",
                 process_id_requesting, ")");
        log::Appender::reopen();
        SDB_INFO("xxxxx", Logger::FIXME, "hangup received, reopened logfile");
      } catch (...) {
        // cannot do much if log rotate request goes wrong
      }
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
    SDB_ERROR("xxxxx", Logger::FIXME,
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
    SDB_ERROR("xxxxx", Logger::FIXME,
              "cannot initialize signal handlers for SIGINT/SIGQUIT/SIGTERM");
  }
}

}  // namespace sdb
