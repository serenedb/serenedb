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

#include "basics/thread_id.h"

#include <string.h>
#include <unistd.h>

#include <atomic>

#include "basics/operating-system.h"
#include "basics/signals.h"

#ifdef SERENEDB_HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

namespace sdb {
namespace {

// Ever-increasing counter for thread numbers. The first thread to read
// gLocalThreadNumber gets 1, the second gets 2, etc.
std::atomic<uint64_t> gNextThreadId{1};

struct ThreadNumberHolder {
  ThreadNumberHolder() noexcept
    : value{gNextThreadId.fetch_add(1, std::memory_order_seq_cst)} {}

  uint64_t value;
};

thread_local ThreadNumberHolder gLocalThreadNumber{};

}  // namespace

pid_t CurrentProcessId() noexcept { return ::getpid(); }

uint64_t CurrentThreadNumber() noexcept { return gLocalThreadNumber.value; }

ThreadNameFetcher::ThreadNameFetcher() noexcept {
  memset(&_buffer[0], 0, sizeof(_buffer));

#ifdef SERENEDB_HAVE_SYS_PRCTL_H
  // PR_GET_NAME reads at most 16 bytes into the buffer; glibc documents this
  // call as async-signal-safe.
  prctl(PR_GET_NAME, &_buffer, 0, 0, 0);
  // be extra cautious about null-termination
  _buffer[sizeof(_buffer) - 1] = '\0';
#endif

  if (_buffer[0] == '\0') {
    // fall back to "main" if no thread name is set
    memcpy(&_buffer[0], "main", 4);
  }
}

std::string_view ThreadNameFetcher::get() const noexcept {
  // guaranteed to be properly null-terminated
  return {&_buffer[0]};
}

void InitThread(const char* name) noexcept {
  signals::MaskAllSignals();
#ifdef SERENEDB_HAVE_SYS_PRCTL_H
  if (name != nullptr) {
    prctl(PR_SET_NAME, name, 0, 0, 0);
  }
#endif
  // touch the thread-local counter so the thread acquires a stable number
  (void)gLocalThreadNumber.value;
}

}  // namespace sdb
