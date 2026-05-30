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

#include "signal_handling.h"

#include <signal.h>

#include <atomic>
#include <cstring>
#include <limits>

#include "basics/assert.h"
#include "basics/lifecycle.h"
#include "basics/logger/logger.h"
#include "basics/signals.h"
#include "general_server/scheduler_feature.h"

namespace sdb::signal_handling {
namespace {

// atomic flag to track shutdown requests
std::atomic_bool gReceivedShutdownRequest{false};

// id of process that will not be used to send SIGHUP requests
constexpr pid_t kProcessIdUnspecified{std::numeric_limits<pid_t>::min()};

static_assert(kProcessIdUnspecified != 0, "minimum pid number must be != 0");

// id of process that requested a log rotation via SIGHUP
std::atomic<pid_t> gProcessIdRequestingLogRotate{kProcessIdUnspecified};

extern "C" void CExitHandler(int signal, siginfo_t* info, void*) {
  if (signal == SIGQUIT || signal == SIGTERM || signal == SIGINT) {
    if (!gReceivedShutdownRequest.exchange(true)) {
      SDB_INFO(GENERAL, signals::Name(signal), " received (sender pid ",
               (info ? info->si_pid : 0), "), beginning shut down sequence");
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

void BuildHangupHandler() {
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  sigfillset(&action.sa_mask);
  action.sa_flags = SA_SIGINFO;
  action.sa_sigaction = CHangupHandler;

  int res = sigaction(SIGHUP, &action, nullptr);

  if (res < 0) {
    SDB_ERROR(GENERAL, "cannot initialize signal handler for hang up");
  }
}

void BuildControlCHandler() {
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

}  // namespace

void Install() {
  signals::MaskAllSignalsServer();

  struct sigaction action;
  memset(&action, 0, sizeof(action));
  sigfillset(&action.sa_mask);

  // ignore broken pipes
  action.sa_handler = SIG_IGN;

  int res = sigaction(SIGPIPE, &action, nullptr);

  if (res < 0) {
    SDB_ERROR(GENERAL, "cannot initialize signal handler for SIGPIPE");
  }

  BuildHangupHandler();
  BuildControlCHandler();
}

void Shutdown() {
  // Nothing to do: handlers are installed via sigaction(2) and will be
  // torn down by process exit. The previous SchedulerFeature variant
  // attempted to cancel asio_ns::signal_set members that were never
  // populated, so it was effectively a no-op too.
}

}  // namespace sdb::signal_handling
