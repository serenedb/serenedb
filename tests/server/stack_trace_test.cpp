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

#include <gtest/gtest.h>

#ifdef __linux__

#include <absl/algorithm/container.h>
#include <absl/base/config.h>
#include <absl/cleanup/cleanup.h>
#include <absl/debugging/symbolize.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/notification.h>
#include <fast_float/fast_float.h>
#include <fcntl.h>
#include <link.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

#include "server/utils/stack_trace.h"

extern "C" void SdbBadCfiSpin(const std::atomic<bool>* stop);
extern "C" void SdbHeldCfiSpin(const std::atomic<uint64_t>* hold,
                               const std::atomic<bool>* stop);
extern "C" void SdbReadAtRowStart(int fd, char* byte);

#if defined(__x86_64__)
asm(R"(
  .pushsection .text
  .p2align 4
  .type SdbReadAtRowStart,@function
SdbReadAtRowStart:
  .cfi_startproc
  mov $0x1000, %r11
  .cfi_def_cfa %r11, 16
  mov $1, %edx
  xor %eax, %eax
  .cfi_def_cfa %rsp, 8
  syscall
  ret
  .cfi_endproc
  .size SdbReadAtRowStart, .-SdbReadAtRowStart
  .popsection
)");
asm(R"(
  .pushsection .text
  .p2align 4
  .type SdbBadCfiSpin,@function
SdbBadCfiSpin:
  .cfi_startproc
  mov $0x1000, %r11
  .cfi_def_cfa %r11, 16
1:
  pause
  cmpb $0, (%rdi)
  je 1b
  .cfi_def_cfa %rsp, 8
  ret
  .cfi_endproc
  .size SdbBadCfiSpin, .-SdbBadCfiSpin
  .popsection
)");
asm(R"(
  .pushsection .text
  .p2align 4
  .type SdbHeldCfiSpin,@function
SdbHeldCfiSpin:
  .cfi_startproc
  .cfi_escape 0x0f, 0x08, 0x75, 0x00, 0x06, 0x28, 0xfa, 0xff, 0x77, 0x08
1:
  pause
  cmpb $0, (%rsi)
  je 1b
  .cfi_def_cfa %rsp, 8
  ret
  .cfi_endproc
  .size SdbHeldCfiSpin, .-SdbHeldCfiSpin
  .popsection
)");
#elif defined(__aarch64__)
asm(R"(
  .pushsection .text
  .p2align 4
  .type SdbBadCfiSpin,%function
SdbBadCfiSpin:
  .cfi_startproc
  mov x9, #0x1000
  .cfi_def_cfa x9, 16
  .cfi_offset x30, -16
1:
  ldrb w10, [x0]
  cbz w10, 1b
  .cfi_def_cfa sp, 0
  .cfi_restore x30
  ret
  .cfi_endproc
  .size SdbBadCfiSpin, .-SdbBadCfiSpin
  .popsection
)");
asm(R"(
  .pushsection .text
  .p2align 4
  .type SdbHeldCfiSpin,%function
SdbHeldCfiSpin:
  .cfi_startproc
  .cfi_escape 0x0f, 0x08, 0x70, 0x00, 0x06, 0x28, 0xfa, 0xff, 0x8f, 0x00
1:
  ldrb w10, [x1]
  cbz w10, 1b
  .cfi_def_cfa sp, 0
  ret
  .cfi_endproc
  .size SdbHeldCfiSpin, .-SdbHeldCfiSpin
  .popsection
)");
#endif

namespace sdb {
namespace {

constexpr std::chrono::milliseconds kTimeout{100};

[[clang::noinline, clang::disable_tail_calls]] void WaitInNotification(
  const absl::Notification& stop) {
  stop.WaitForNotification();
}

[[clang::noinline, clang::disable_tail_calls]] void WaitInPoll(int fd) {
  pollfd request{.fd = fd, .events = POLLIN, .revents = 0};
  while (poll(&request, 1, -1) < 0 && errno == EINTR) {
  }
}

[[clang::noinline, clang::disable_tail_calls]] void SleepInNanosleep(
  const std::atomic<bool>& stop) {
  while (!stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
}

[[clang::noinline, clang::disable_tail_calls]] void SpinInLoop(
  const std::atomic<bool>& stop) {
  while (!stop.load(std::memory_order_relaxed)) {
  }
}

[[clang::noinline, clang::disable_tail_calls]] void SpinInClockGettime(
  const std::atomic<bool>& stop) {
  timespec now{};
  while (!stop.load(std::memory_order_relaxed)) {
    clock_gettime(CLOCK_MONOTONIC, &now);
  }
}

class Thread {
 public:
  explicit Thread(std::function<void()> body)
    : _thread{[this, body = std::move(body)] {
        _tid = gettid();
        _started.Notify();
        body();
      }} {
    _started.WaitForNotification();
  }

  Thread(const Thread&) = delete;
  Thread& operator=(const Thread&) = delete;

  ~Thread() { _thread.join(); }

  pid_t Tid() const { return _tid; }

 private:
  pid_t _tid = 0;
  absl::Notification _started;
  std::thread _thread;
};

std::string FunctionName(uintptr_t pc) {
  char name[1024] = {};
  if (!absl::Symbolize(reinterpret_cast<void*>(pc), name, sizeof(name))) {
    return {};
  }
  return name;
}

const std::vector<uintptr_t>* Frames(std::span<const ThreadStack> stacks,
                                     pid_t tid) {
  const auto stack =
    absl::c_find_if(stacks, [&](const ThreadStack& s) { return s.tid == tid; });
  return stack == stacks.end() ? nullptr : &stack->frames;
}

bool HasFrameIn(std::span<const ThreadStack> stacks, pid_t tid,
                std::string_view function) {
  const auto* frames = Frames(stacks, tid);
  if (!frames) {
    return false;
  }
  for (size_t i = 0; i < frames->size(); ++i) {
    const auto pc = i == 0 ? (*frames)[i] : (*frames)[i] - 1;
    if (FunctionName(pc).find(function) != std::string::npos) {
      return true;
    }
  }
  return false;
}

std::string Describe(std::span<const ThreadStack> stacks, pid_t tid) {
  const auto* frames = Frames(stacks, tid);
  if (!frames) {
    return "no such thread";
  }
  std::string text;
  for (const auto pc : *frames) {
    absl::StrAppend(&text, "\n  ", absl::Hex(pc), " ", FunctionName(pc));
  }
  return text;
}

std::string ReadFile(const std::string& path) {
  std::array<char, 4096> buffer{};
  const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
  const auto size = fd < 0 ? -1 : ::read(fd, buffer.data(), buffer.size());
  if (fd >= 0) {
    close(fd);
  }
  return {buffer.data(), size > 0 ? static_cast<size_t>(size) : 0};
}

bool SignalPending(pid_t tid, int signal) {
  constexpr std::string_view kKey = "\nSigPnd:";
  const auto status =
    ReadFile(absl::StrCat("/proc/self/task/", tid, "/status"));
  const auto key = status.find(kKey);
  if (key == std::string::npos) {
    return false;
  }
  const auto* first = status.data() + key + kKey.size();
  while (*first == ' ' || *first == '\t') {
    ++first;
  }
  uint64_t mask = 0;
  fast_float::from_chars(first, status.data() + status.size(), mask, 16);
  return (mask >> (signal - 1) & 1) != 0;
}

std::vector<ThreadStack> CollectAnswered(pid_t tid) {
  for (int attempt = 0; attempt < 200; ++attempt) {
    auto stacks = CollectStacks(kTimeout);
    if (const auto* frames = Frames(stacks, tid); frames && !frames->empty()) {
      return stacks;
    }
  }
  return {};
}

struct Expectation {
  const Thread& thread;
  std::string_view function;
};

bool Capture(std::span<const Expectation> expected) {
  for (int attempt = 0; attempt < 200; ++attempt) {
    const auto stacks = CollectStacks(kTimeout);
    if (absl::c_all_of(expected, [&](const Expectation& e) {
          return HasFrameIn(stacks, e.thread.Tid(), e.function);
        })) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }
  return false;
}

TEST(StackTrace, EveryThreadWhereItRuns) {
  absl::Notification stop_waiting;
  std::atomic<bool> stop = false;
  int fds[2] = {};
  ASSERT_EQ(pipe(fds), 0);

  Thread notification{[&] { WaitInNotification(stop_waiting); }};
  Thread polling{[&] {
    pthread_setname_np(pthread_self(), "sdb_test_poll");
    WaitInPoll(fds[0]);
  }};
  Thread sleeping{[&] { SleepInNanosleep(stop); }};
  Thread spinning{[&] { SpinInLoop(stop); }};

  std::vector<Expectation> expected{
    {polling, "WaitInPoll"},
    {sleeping, "SleepInNanosleep"},
  };
#ifndef ABSL_HAVE_THREAD_SANITIZER
  expected.push_back({notification, "WaitInNotification"});
  expected.push_back({spinning, "SpinInLoop"});
#endif
  EXPECT_TRUE(Capture(expected));

  const auto stacks = CollectStacks(kTimeout);
  EXPECT_TRUE(HasFrameIn(stacks, gettid(), "EveryThreadWhereItRuns"));
  const auto named = absl::c_find_if(
    stacks, [&](const ThreadStack& s) { return s.tid == polling.Tid(); });
  EXPECT_TRUE(named != stacks.end() && named->name == "sdb_test_poll");

  stop_waiting.Notify();
  stop = true;
  close(fds[1]);
  close(fds[0]);
}

TEST(StackTrace, UnwindsOutOfTheVdso) {
  std::atomic<bool> stop = false;
  Thread clock{[&] { SpinInClockGettime(stop); }};
  const std::vector<Expectation> expected{{clock, "SpinInClockGettime"}};
  EXPECT_TRUE(Capture(expected));
  for (int sample = 0; sample < 20; ++sample) {
    const auto stacks = CollectAnswered(clock.Tid());
    EXPECT_TRUE(HasFrameIn(stacks, clock.Tid(), "SpinInClockGettime"))
      << "sample " << sample << ":" << Describe(stacks, clock.Tid());
  }
  stop = true;
}

TEST(StackTrace, WhileAnotherThreadHoldsTheLoaderLock) {
  absl::Notification inside;
  absl::Notification release;
  int fds[2] = {};
  ASSERT_EQ(pipe(fds), 0);
  Thread waiting{[&] { WaitInPoll(fds[0]); }};
  Thread holder{[&] {
    dl_iterate_phdr(
      [](dl_phdr_info*, size_t, void* arg) {
        auto* notifications = static_cast<absl::Notification**>(arg);
        notifications[0]->Notify();
        notifications[1]->WaitForNotification();
        return 1;
      },
      std::array{&inside, &release}.data());
  }};
  inside.WaitForNotification();

  const std::vector<Expectation> expected{{waiting, "WaitInPoll"}};
  EXPECT_TRUE(Capture(expected));

  release.Notify();
  close(fds[1]);
  close(fds[0]);
}

TEST(StackTrace, ThreadBlockingTheSignalHasNoFrames) {
  absl::Notification blocked;
  absl::Notification stop_waiting;
  Thread thread{[&] {
    sigset_t all;
    sigfillset(&all);
    pthread_sigmask(SIG_BLOCK, &all, nullptr);
    blocked.Notify();
    stop_waiting.WaitForNotification();
  }};
  blocked.WaitForNotification();

  const auto stacks = CollectStacks(kTimeout);
  const auto* frames = Frames(stacks, thread.Tid());
  EXPECT_TRUE(frames && frames->empty());
  EXPECT_FALSE(SignalPending(thread.Tid(), SIGRTMIN));

  stop_waiting.Notify();
}

TEST(StackTrace, IgnoresTheSignalFromElsewhere) {
  ASSERT_FALSE(CollectStacks(kTimeout).empty());
  ASSERT_EQ(syscall(SYS_tgkill, getpid(), gettid(), SIGRTMIN), 0);
  const auto stacks = CollectStacks(kTimeout);
  EXPECT_TRUE(HasFrameIn(stacks, gettid(), "IgnoresTheSignalFromElsewhere"));
}

#ifndef ABSL_HAVE_THREAD_SANITIZER
void WaitUntilInRead(const std::string& proc) {
  const auto in_read = absl::StrCat(SYS_read, " ");
  while (!ReadFile(absl::StrCat(proc, "/syscall")).starts_with(in_read)) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
}

TEST(StackTrace, AThreadThatCannotTakeTheSignalTimesOut) {
  int fds[2] = {};
  ASSERT_EQ(pipe(fds), 0);
  std::atomic<pid_t> child = 0;
  Thread forking{[&] {
    const pid_t pid = vfork();
    if (pid == 0) {
      child = getpid();
      char byte = 0;
      std::ignore = ::read(fds[0], &byte, 1);
      _exit(0);
    }
    int status = 0;
    waitpid(pid, &status, 0);
  }};
  while (child == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }
  WaitUntilInRead(absl::StrCat("/proc/", child.load()));

  const auto stacks = CollectStacks(kTimeout);
  const auto* frames = Frames(stacks, forking.Tid());
  EXPECT_TRUE(frames && frames->empty());
  EXPECT_TRUE(SignalPending(forking.Tid(), SIGRTMIN));

  EXPECT_EQ(write(fds[1], "x", 1), 1);
  close(fds[1]);
  close(fds[0]);
}
#endif

#if defined(__x86_64__) && !defined(ABSL_HAVE_THREAD_SANITIZER)
[[clang::noinline, clang::disable_tail_calls]] void WaitInRawRead(int fd) {
  char byte = 0;
  SdbReadAtRowStart(fd, &byte);
}

TEST(StackTrace, UnwindsWithTheRuleStartingAtTheInterruptedInstruction) {
  int fds[2] = {};
  ASSERT_EQ(pipe(fds), 0);
  Thread reading{[&] { WaitInRawRead(fds[0]); }};
  WaitUntilInRead(absl::StrCat("/proc/self/task/", reading.Tid()));

  const auto stacks = CollectAnswered(reading.Tid());
  EXPECT_TRUE(HasFrameIn(stacks, reading.Tid(), "WaitInRawRead"))
    << Describe(stacks, reading.Tid());

  close(fds[1]);
  close(fds[0]);
}
#endif

#if defined(__x86_64__) || defined(__aarch64__)
TEST(StackTrace, SurvivesCorruptUnwindInfo) {
  std::atomic<bool> stop = false;
  Thread corrupt{[&] { SdbBadCfiSpin(&stop); }};
  std::atomic<bool> other_stop = false;
  Thread spinning{[&] { SpinInLoop(other_stop); }};

#ifndef ABSL_HAVE_THREAD_SANITIZER
  const std::vector<Expectation> expected{
    {corrupt, "SdbBadCfiSpin"},
    {spinning, "SpinInLoop"},
  };
  EXPECT_TRUE(Capture(expected));
  const auto stacks = CollectAnswered(corrupt.Tid());
  const auto* frames = Frames(stacks, corrupt.Tid());
  EXPECT_TRUE(frames && frames->size() == 1);
#else
  EXPECT_FALSE(CollectStacks(kTimeout).empty());
#endif

  stop = true;
  other_stop = true;
}
#endif

#if (defined(__x86_64__) || defined(__aarch64__)) && \
  !defined(ABSL_HAVE_THREAD_SANITIZER)
TEST(StackTrace, AHandlerStuckUnwindingStopsUnwindingUntilItReturns) {
  std::atomic<uint64_t> hold = 1;
  std::atomic<bool> stop = false;
  Thread held{[&] { SdbHeldCfiSpin(&hold, &stop); }};
  absl::Cleanup release = [&] {
    hold = 0;
    stop = true;
  };

  std::vector<ThreadStack> stacks;
  for (int attempt = 0; attempt < 200; ++attempt) {
    stacks = CollectStacks(kTimeout);
    if (const auto* own = Frames(stacks, gettid()); own && own->empty()) {
      break;
    }
  }
  EXPECT_TRUE(Frames(stacks, held.Tid()));
  EXPECT_TRUE(absl::c_all_of(
    stacks, [](const ThreadStack& s) { return s.frames.empty(); }));

  hold = 0;
  const std::vector<Expectation> expected{{held, "SdbHeldCfiSpin"}};
  EXPECT_TRUE(Capture(expected));
}
#endif

}  // namespace
}  // namespace sdb

#else

TEST(StackTrace, EveryThreadWhereItRuns) {
  GTEST_SKIP() << "Requires Linux signals and /proc";
}

#endif
