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
#include <unistd.h>

#include <csignal>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/logger/logger.h"

#ifdef SDB_FAULT_INJECTION
namespace sdb {
namespace {

std::atomic_bool gHasFailurePoints{false};

// a read-write lock for thread-safe access to the failure points set
constinit absl::Mutex gFailurePointsLock{absl::kConstInit};

// a global set containing the currently registered failure points
containers::FlatHashSet<std::string> gFailurePoints;

}  // namespace

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
    SDB_WARN(GENERAL, "activating intentional failure point '", value,
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
    SDB_INFO(GENERAL, "cleared failure point ", value);
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
    SDB_INFO(GENERAL, "cleared ", num_existing, " failure point(s)");
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

void WaitWhileFailurePointDebugging(std::string_view value) {
  const auto cleared =
    +[](std::string_view* v) noexcept { return !gFailurePoints.contains(*v); };
  absl::MutexLock lock{&gFailurePointsLock};
  gFailurePointsLock.Await(absl::Condition{cleared, &value});
}

}  // namespace sdb
#endif
