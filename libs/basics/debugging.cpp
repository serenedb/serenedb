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

#include <stdio.h>
#include <stdlib.h>

#include <csignal>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "basics/assert.h"
#include "basics/crash_handler.h"
#include "basics/logger/logger.h"

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef SDB_FAULT_INJECTION
namespace sdb {
namespace {

std::atomic_bool gHasFailurePoints{false};

// a read-write lock for thread-safe access to the failure points set
constinit absl::Mutex gFailurePointsLock{absl::kConstInit};

// a global set containing the currently registered failure points
containers::FlatHashSet<std::string> gFailurePoints;

}  // namespace

/// intentionally cause a segmentation violation or other failures
/// this is used for crash and recovery tests
void TerminateDebugging(std::string_view message) {
#ifdef SDB_DEV
  CrashHandler::setHardKill();

  // there are some reserved crash messages we use in testing the
  // crash handler
  if (message == "CRASH-HANDLER-TEST-ABORT") {
    // intentionally crashes the program!
    std::abort();
  } else if (message == "CRASH-HANDLER-TEST-TERMINATE") {
    // intentionally crashes the program!
    std::terminate();
  } else if (message == "CRASH-HANDLER-TEST-TERMINATE-ACTIVE") {
    // intentionally crashes the program!
    // note: when using ASan/UBSan, this actually does not crash
    // the program but continues.
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wexceptions"
#endif

    auto f = [] noexcept {
      // intentionally crashes the program!
      throw std::runtime_error("Intentional test error");
    };
    f();
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
    // we will get here at least with ASan/UBSan.
    std::terminate();
  } else if (message == "CRASH-HANDLER-TEST-SEGFAULT") {
    // intentionally crashes the program!
    // If we instead try to dereference a nullptr, macOS might do SIGABRT
    raise(SIGSEGV);
  } else if (message == "CRASH-HANDLER-TEST-ASSERT") {
    int a = 1;
    // intentionally crashes the program!
    SDB_ASSERT(a == 2);
  }

#endif

  // intentional crash - no need for a backtrace here
  CrashHandler::disableBacktraces();
  CrashHandler::crash(message);
}

bool ShouldFailDebugging(std::string_view value) noexcept {
  if (gHasFailurePoints.load(std::memory_order_relaxed)) {
    absl::ReaderMutexLock read_locker{&gFailurePointsLock};
    return gFailurePoints.contains(value);
  }
  return false;
}

bool AddFailurePointDebugging(std::string_view value) {
  bool added = false;
  {
    absl::WriterMutexLock write_locker{&gFailurePointsLock};
    added = gFailurePoints.emplace(value).second;
    gHasFailurePoints.store(true, std::memory_order_relaxed);
  }
  if (added) {
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "activating intentional failure point '", value,
             "'. the server will misbehave!");
  }
  return added;
}

bool RemoveFailurePointDebugging(std::string_view value) {
  bool removed = false;
  {
    absl::WriterMutexLock write_locker{&gFailurePointsLock};
    removed = gFailurePoints.erase(value) != 0;
    if (gFailurePoints.size() == 0) {
      gHasFailurePoints.store(false, std::memory_order_relaxed);
    }
  }

  if (removed) {
    SDB_INFO("xxxxx", sdb::Logger::FIXME, "cleared failure point ", value);
  }
  return removed;
}

void ClearFailurePointsDebugging() noexcept {
  size_t num_existing = 0;
  {
    absl::WriterMutexLock write_locker{&gFailurePointsLock};
    num_existing = gFailurePoints.size();
    gFailurePoints.clear();
    gHasFailurePoints.store(false, std::memory_order_relaxed);
  }

  if (num_existing > 0) {
    SDB_INFO("xxxxx", sdb::Logger::FIXME, "cleared ", num_existing,
             " failure point(s)");
  }
}

std::vector<std::string> GetFailurePointsDebugging() {
  std::vector<std::string> result;
  {
    absl::ReaderMutexLock read_locker{&gFailurePointsLock};
    result.assign_range(gFailurePoints);
  }
  absl::c_sort(result);
  return result;
}

}  // namespace sdb
#endif
