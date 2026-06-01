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

void InitThread(const char* name) noexcept {
  signals::MaskAllSignalsServer();
#ifdef SERENEDB_HAVE_SYS_PRCTL_H
  prctl(PR_SET_NAME, name, 0, 0, 0);
#endif
  // touch the thread-local counter so the thread acquires a stable number
  (void)gLocalThreadNumber.value;
}

}  // namespace sdb
