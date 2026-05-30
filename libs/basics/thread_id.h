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

#pragma once

#include <sys/types.h>

#include <cstdint>
#include <string_view>

namespace sdb {

/// Returns the current process id (getpid()).
pid_t CurrentProcessId() noexcept;

/// Returns a per-thread unique number. The first thread to call this gets 1,
/// the next gets 2, etc. Safe to call from async-signal-handler context.
uint64_t CurrentThreadNumber() noexcept;

/// Initialize the calling thread: mask asynchronous signals (so the dedicated
/// signal-handling thread receives them) and set the OS-visible thread name
/// (visible to `ps -T`, `gdb`, and to ThreadNameFetcher above). Call this
/// from the first line of any std::jthread body. The `name` must be a string
/// that lives at least until the call returns -- it is copied into the
/// kernel's per-thread name slot (max 15 bytes, truncated otherwise).
void InitThread(const char* name) noexcept;

/// Fetches the current thread's human-readable name into a small fixed
/// buffer. Designed to be safe to use from a signal handler: no heap
/// allocation, falls back to prctl(PR_GET_NAME, ...) which glibc documents
/// as async-signal-safe.
class ThreadNameFetcher {
 public:
  ThreadNameFetcher() noexcept;
  ThreadNameFetcher(const ThreadNameFetcher&) = delete;
  ThreadNameFetcher& operator=(const ThreadNameFetcher&) = delete;

  // The returned view is valid as long as this ThreadNameFetcher lives.
  std::string_view get() const noexcept;

 private:
  char _buffer[32];
};

}  // namespace sdb
