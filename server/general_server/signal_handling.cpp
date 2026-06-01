////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "signal_handling.h"

#include <signal.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <cstring>

#include "basics/lifecycle.h"
#include "basics/logger/logger.h"
#include "basics/signals.h"

namespace sdb::signal_handling {
namespace {

// Counts SIGTERM/SIGINT/SIGQUIT arrivals. The first arrival wakes
// WaitForShutdown via lifecycle::BeginShutdown; the second arrival short-
// circuits to _exit(EXIT_FAILURE) so a wedged shutdown can still be killed
// with a second Ctrl-C.
std::atomic_int gSignalCount{0};

extern "C" void CExitHandler(int /*signal*/, siginfo_t* /*info*/,
                             void* /*ctx*/) {
  if (gSignalCount.fetch_add(1, std::memory_order_relaxed) == 0) {
    // First signal: flip the lifecycle "stopping" flag and wake the main
    // thread blocked in WaitForShutdown(). Both the atomic store and the
    // eventfd write inside BeginShutdown() are async-signal-safe.
    lifecycle::BeginShutdown();
    return;
  }
  // Second signal: fast-path abort. LogCrash is async-signal-safe (write(2)
  // to stderr, fixed-size stack buffer, no heap, no LogManager lookup), so
  // it's reachable from a signal context. _exit skips global destructors.
  log::LogCrash("second shutdown signal received, terminating");
  ::_exit(EXIT_FAILURE);
}

}  // namespace

void Install() {
  // The main thread is the one signals should land on -- worker threads
  // have everything except the failure-class set masked off in
  // MaskAllSignalsServer (called from InitThread).
  signals::UnmaskAllSignals();

  // SIGPIPE: ignore. Without this, writing to a closed pg-wire socket
  // would kill the process (default action for SIGPIPE is Term).
  struct sigaction ignore_action;
  std::memset(&ignore_action, 0, sizeof(ignore_action));
  sigfillset(&ignore_action.sa_mask);
  ignore_action.sa_handler = SIG_IGN;
  ::sigaction(SIGPIPE, &ignore_action, nullptr);

  // Shutdown handlers. We install AFTER absl::InstallFailureSignalHandler
  // so our SIGTERM override wins (abseil otherwise treats SIGTERM as a
  // fatal "dump stack and die" signal).
  struct sigaction action;
  std::memset(&action, 0, sizeof(action));
  sigfillset(&action.sa_mask);
  action.sa_flags = SA_SIGINFO;
  action.sa_sigaction = CExitHandler;
  ::sigaction(SIGINT, &action, nullptr);
  ::sigaction(SIGQUIT, &action, nullptr);
  ::sigaction(SIGTERM, &action, nullptr);
}

void Shutdown() {
  // Restore SIG_IGN on every signal we installed a handler for. The handler
  // dereferences lifecycle state the caller is about to tear down, so a
  // late-arriving signal during shutdown would otherwise be a UAF.
  struct sigaction action;
  std::memset(&action, 0, sizeof(action));
  sigfillset(&action.sa_mask);
  action.sa_handler = SIG_IGN;
  ::sigaction(SIGINT, &action, nullptr);
  ::sigaction(SIGQUIT, &action, nullptr);
  ::sigaction(SIGTERM, &action, nullptr);
}

}  // namespace sdb::signal_handling
