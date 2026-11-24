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

#include "server/utils/stack_trace.h"

#ifdef __linux__

#include <absl/algorithm/container.h>
#include <absl/base/const_init.h>
#include <absl/base/thread_annotations.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
#include <fast_float/fast_float.h>
#include <fcntl.h>
#include <libunwind.h>
#include <linux/futex.h>
#include <setjmp.h>
#include <signal.h>
#include <sys/syscall.h>
#include <ucontext.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <span>
#include <string_view>
#include <system_error>

namespace sdb {
namespace {

constexpr size_t kMaxFrames = 128;
constexpr auto kThreadTimeout = std::chrono::milliseconds{100};
constexpr uint32_t kSeqMask = (uint32_t{1} << 30) - 1;

enum class Phase : uint32_t {
  Armed = 1,
  Busy = 2,
  Done = 3,
};

constexpr uint32_t Word(uint32_t seq, Phase phase) {
  return seq << 2 | static_cast<uint32_t>(phase);
}

struct Request {
  std::atomic<uint32_t> word = 0;
  std::span<const uintptr_t> stack;
  std::array<uintptr_t, kMaxFrames> frames{};
};

constinit absl::Mutex gMutex{absl::kConstInit};
constinit bool gInstalled ABSL_GUARDED_BY(gMutex) = false;
constinit uint32_t gSeq ABSL_GUARDED_BY(gMutex) = 0;
constinit Request gRequest;
constinit std::atomic<pid_t> gUnwindingTid = 0;
constinit sigjmp_buf gUnwindJump{};
constinit struct sigaction gPreviousSegv = {};
constinit struct sigaction gPreviousBus = {};

uintptr_t ProgramCounter(const ucontext_t& context) {
#if defined(__x86_64__)
  return static_cast<uintptr_t>(context.uc_mcontext.gregs[REG_RIP]);
#elif defined(__aarch64__)
  return static_cast<uintptr_t>(context.uc_mcontext.pc);
#else
  return 0;
#endif
}

std::span<const uintptr_t> Unwind(std::span<uintptr_t> frames, uintptr_t pc) {
  volatile size_t size = 0;
  gUnwindingTid.store(gettid(), std::memory_order_relaxed);
  if (sigsetjmp(gUnwindJump, 1) == 0) {
    unw_context_t context{};
    unw_cursor_t cursor{};
    if (unw_getcontext(&context) == 0 &&
        unw_init_local(&cursor, &context) == 0) {
      do {
        unw_word_t ip = 0;
        unw_get_reg(&cursor, UNW_REG_IP, &ip);
        frames[size] = ip;
        size = size + 1;
      } while (size < frames.size() && unw_step(&cursor) > 0);
    }
  }
  gUnwindingTid.store(0, std::memory_order_relaxed);
  const std::span<const uintptr_t> captured = frames.first(size);
  const auto interrupted = absl::c_find(captured, pc);
  if (pc == 0 || interrupted == captured.end()) {
    return captured;
  }
  return {interrupted, captured.end()};
}

void FutexWake(std::atomic<uint32_t>& word) {
  syscall(SYS_futex, &word, FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
}

void FutexWait(std::atomic<uint32_t>& word, uint32_t expected,
               const timespec* timeout) {
  syscall(SYS_futex, &word, FUTEX_WAIT_PRIVATE, expected, timeout, nullptr, 0);
}

void StackHandler(int /*signal*/, siginfo_t* info, void* context) {
  if (info->si_code != SI_QUEUE || info->si_pid != getpid()) {
    return;
  }
  const auto seq = static_cast<uint32_t>(info->si_value.sival_int);
  auto armed = Word(seq, Phase::Armed);
  if (!gRequest.word.compare_exchange_strong(armed, Word(seq, Phase::Busy),
                                             std::memory_order_acquire)) {
    return;
  }
  const int saved_errno = errno;
  gRequest.stack =
    Unwind(gRequest.frames, ProgramCounter(*static_cast<ucontext_t*>(context)));
  gRequest.word.store(Word(seq, Phase::Done), std::memory_order_release);
  FutexWake(gRequest.word);
  errno = saved_errno;
}

void FaultHandler(int signal, siginfo_t* info, void* context) {
  if (gUnwindingTid.load(std::memory_order_relaxed) == gettid()) {
    siglongjmp(gUnwindJump, 1);
  }
  const auto& previous = signal == SIGSEGV ? gPreviousSegv : gPreviousBus;
  if (previous.sa_flags & SA_SIGINFO) {
    previous.sa_sigaction(signal, info, context);
  } else if (previous.sa_handler != SIG_DFL && previous.sa_handler != SIG_IGN) {
    previous.sa_handler(signal);
  } else {
    sigaction(signal, &previous, nullptr);
  }
}

void InstallHandlers() {
  struct sigaction action = {};
  action.sa_sigaction = &StackHandler;
  action.sa_flags = SA_SIGINFO | SA_RESTART;
  sigfillset(&action.sa_mask);
  for (const int fault : {SIGSEGV, SIGBUS, SIGILL, SIGFPE, SIGTRAP}) {
    sigdelset(&action.sa_mask, fault);
  }
  sigaction(SIGRTMIN, &action, nullptr);

  auto chain = [](int signal, struct sigaction& previous) {
    sigaction(signal, nullptr, &previous);
    struct sigaction fault = previous;
    fault.sa_sigaction = &FaultHandler;
    fault.sa_flags = (previous.sa_flags | SA_SIGINFO) & ~SA_RESETHAND;
    sigaction(signal, &fault, nullptr);
  };
  chain(SIGSEGV, gPreviousSegv);
  chain(SIGBUS, gPreviousBus);
}

bool SignalBlocked(pid_t tid, int signal) {
  const auto path = absl::StrCat("/proc/self/task/", tid, "/status");
  const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return true;
  }
  std::array<char, 4096> buffer{};
  const auto size = ::read(fd, buffer.data(), buffer.size());
  close(fd);
  if (size <= 0) {
    return true;
  }
  constexpr std::string_view kKey = "\nSigBlk:";
  const std::string_view status{buffer.data(), static_cast<size_t>(size)};
  const auto key = status.find(kKey);
  if (key == std::string_view::npos) {
    return true;
  }
  const auto value =
    absl::StripLeadingAsciiWhitespace(status.substr(key + kKey.size()));
  uint64_t mask = 0;
  return fast_float::from_chars(value.data(), value.data() + value.size(), mask,
                                16)
             .ec != std::errc{} ||
         (mask >> (signal - 1) & 1) != 0;
}

std::vector<uintptr_t> SignalAndWait(pid_t tid, int signal)
  ABSL_EXCLUSIVE_LOCKS_REQUIRED(gMutex) {
  const uint32_t seq = ++gSeq & kSeqMask;
  gRequest.word.store(Word(seq, Phase::Armed), std::memory_order_release);
  siginfo_t info = {};
  info.si_signo = signal;
  info.si_code = SI_QUEUE;
  info.si_pid = getpid();
  info.si_uid = getuid();
  info.si_value.sival_int = static_cast<int>(seq);
  if (syscall(SYS_rt_tgsigqueueinfo, getpid(), tid, signal, &info) != 0) {
    return {};
  }
  const auto deadline = std::chrono::steady_clock::now() + kThreadTimeout;
  while (true) {
    auto word = gRequest.word.load(std::memory_order_acquire);
    if (word == Word(seq, Phase::Done)) {
      return {gRequest.stack.begin(), gRequest.stack.end()};
    }
    if (word == Word(seq, Phase::Busy)) {
      FutexWait(gRequest.word, word, nullptr);
      continue;
    }
    const auto left = deadline - std::chrono::steady_clock::now();
    if (left <= std::chrono::steady_clock::duration::zero()) {
      if (gRequest.word.compare_exchange_strong(word, 0,
                                                std::memory_order_acquire)) {
        return {};
      }
      continue;
    }
    const auto timeout = absl::ToTimespec(absl::FromChrono(left));
    FutexWait(gRequest.word, word, &timeout);
  }
}

}  // namespace

std::vector<ThreadStack> CollectStacks() {
  std::vector<ThreadStack> stacks;
  std::error_code error;
  for (const auto& entry :
       std::filesystem::directory_iterator{"/proc/self/task", error}) {
    const auto name = entry.path().filename().native();
    pid_t tid = 0;
    if (fast_float::from_chars(name.data(), name.data() + name.size(), tid)
          .ec == std::errc{}) {
      stacks.push_back({.tid = tid});
    }
  }

  absl::MutexLock lock{&gMutex};
  if (!gInstalled) {
    InstallHandlers();
    gInstalled = true;
  }
  const int signal = SIGRTMIN;
  const pid_t self = gettid();
  for (auto& stack : stacks) {
    if (stack.tid == self) {
      std::array<uintptr_t, kMaxFrames> frames{};
      const auto own = Unwind(
        frames, reinterpret_cast<uintptr_t>(__builtin_return_address(0)));
      stack.frames.assign(own.begin(), own.end());
    } else if (!SignalBlocked(stack.tid, signal)) {
      stack.frames = SignalAndWait(stack.tid, signal);
    }
  }
  return stacks;
}

}  // namespace sdb

#else

namespace sdb {

std::vector<ThreadStack> CollectStacks() { return {}; }

}  // namespace sdb

#endif
