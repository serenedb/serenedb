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

#include "basics/signals.h"

#include "basics/lifecycle.h"
#include "basics/operating-system.h"

#ifdef SERENEDB_HAVE_SIGNAL_H
#include <signal.h>
#endif

namespace sdb::signals {

void MaskAllSignalsServer() {
#ifdef SERENEDB_HAVE_POSIX_THREADS
  sigset_t all;
  sigfillset(&all);
  // Keep failure-class signals unblocked so they're delivered to the
  // offending thread; absl's failure signal handler relies on that.
  sigdelset(&all, SIGSEGV);
  sigdelset(&all, SIGBUS);
  sigdelset(&all, SIGILL);
  sigdelset(&all, SIGFPE);
  sigdelset(&all, SIGABRT);
  pthread_sigmask(SIG_SETMASK, &all, nullptr);
#endif
}

void UnmaskAllSignals() {
#ifdef SERENEDB_HAVE_POSIX_THREADS
  sigset_t all;
  sigfillset(&all);
  pthread_sigmask(SIG_UNBLOCK, &all, nullptr);
#endif
}

void InstallShutdownHandlers() {
#ifdef SERENEDB_HAVE_SIGNAL_H
  struct sigaction action = {};
  action.sa_handler = +[](int) { lifecycle::BeginShutdown(); };
  sigemptyset(&action.sa_mask);
  ::sigaction(SIGTERM, &action, nullptr);
  ::sigaction(SIGINT, &action, nullptr);
  ::sigaction(SIGQUIT, &action, nullptr);
#endif
}

}  // namespace sdb::signals
