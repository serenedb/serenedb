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

#include "basics/lifecycle.h"

#ifdef __APPLE__
#include <fcntl.h>
#else
#include <sys/eventfd.h>
#endif
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <string>

#include "basics/log.h"

namespace sdb::lifecycle {
namespace {

std::atomic_bool gIsStopping = false;
std::string gDataDirArg;

// eventfd-backed wakeup channel. Created at static-init time so that
// even a signal handler that fires before WaitForShutdown() can write
// to it safely. write(2) on an eventfd is async-signal-safe per POSIX,
// and the 8-byte semantic counter coalesces repeated writes -- a
// single read() in WaitForShutdown drains them.
int gShutdownWriteFd = -1;

int OpenShutdownFd() noexcept {
#ifdef __APPLE__
  int fds[2];
  if (::pipe(fds) < 0) {
    static constexpr char msg[] =
      "fatal: pipe() failed during lifecycle init\n";
    ssize_t unused = ::write(STDERR_FILENO, msg, sizeof(msg) - 1);
    (void)unused;
    ::_exit(EXIT_FAILURE);
  }
  ::fcntl(fds[0], F_SETFD, FD_CLOEXEC);
  ::fcntl(fds[1], F_SETFD, FD_CLOEXEC);
  ::fcntl(fds[1], F_SETFL, ::fcntl(fds[1], F_GETFL) | O_NONBLOCK);
  gShutdownWriteFd = fds[1];
  return fds[0];
#else
  int fd = ::eventfd(0, EFD_CLOEXEC);
  if (fd < 0) {
    // No logger yet at static-init; abort via stderr write -- this is
    // a setup failure that the operator needs to see.
    static constexpr char msg[] =
      "fatal: eventfd() failed during lifecycle init\n";
    ssize_t unused = ::write(STDERR_FILENO, msg, sizeof(msg) - 1);
    (void)unused;
    ::_exit(EXIT_FAILURE);
  }
  gShutdownWriteFd = fd;
  return fd;
#endif
}

// Initialized before main() so the signal-handler write is always
// against a valid fd.
const int gShutdownFd = OpenShutdownFd();

}  // namespace

bool IsStopping() noexcept {
  return gIsStopping.load(std::memory_order_acquire);
}

void BeginShutdown() noexcept {
  // Set the flag first so a parallel IsStopping() observer sees the
  // shutdown bit regardless of which side of the read/write loses.
  gIsStopping.store(true, std::memory_order_release);
  // Wake any thread blocked in WaitForShutdown. write(2) on an
  // eventfd is async-signal-safe and the 8-byte payload is atomic
  // (the kernel rejects any other length); EINTR is impossible in
  // practice for an 8-byte write that always fits, but loop on it
  // defensively.
  const uint64_t v = 1;
  while (::write(gShutdownWriteFd, &v, sizeof(v)) < 0 && errno == EINTR) {
  }
}

void WaitForShutdown() noexcept {
  uint64_t v;
  while (true) {
    ssize_t r = ::read(gShutdownFd, &v, sizeof(v));
    if (r == sizeof(v)) {
      return;
    }
    if (r < 0 && errno == EINTR) {
      continue;
    }
    // Unexpected: eventfd reads return either 8 bytes or -1/EAGAIN
    // (only when EFD_NONBLOCK, which we don't use). Anything else is
    // a kernel-side surprise we can't recover from.
    SDB_FATAL(GENERAL, "read(eventfd) returned ", r, " errno=", errno);
  }
}

void SetDataDirArg(std::string_view arg) { gDataDirArg = arg; }

std::string_view DataDirArg() noexcept { return gDataDirArg; }

}  // namespace sdb::lifecycle
