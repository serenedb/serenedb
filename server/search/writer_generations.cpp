////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "search/writer_generations.h"

#include "basics/assert.h"

namespace sdb::search {

unsigned WriterGenerations::Register() {
  absl::MutexLock lock{&_mutex};
  const auto slot = _generation & 1U;
  ++_writers[slot];
  return slot;
}

void WriterGenerations::Deregister(unsigned slot) noexcept {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(slot < 2 && _writers[slot] > 0,
             "unbalanced search-table writer deregistration");
  if (--_writers[slot] == 0) {
    _cv.SignalAll();
  }
}

bool WriterGenerations::Drain(absl::FunctionRef<bool()> cancelled,
                              absl::Duration poll) {
  absl::MutexLock lock{&_mutex};
  const auto prior = _generation & 1U;
  if (!WaitUntilEmpty(prior ^ 1U, cancelled, poll)) {
    return false;
  }
  ++_generation;
  return WaitUntilEmpty(prior, cancelled, poll);
}

bool WriterGenerations::WaitUntilEmpty(unsigned slot,
                                       absl::FunctionRef<bool()> cancelled,
                                       absl::Duration poll) {
  while (_writers[slot] != 0) {
    if (_cv.WaitWithTimeout(&_mutex, poll) && cancelled()) {
      return false;
    }
  }
  return true;
}

}  // namespace sdb::search
