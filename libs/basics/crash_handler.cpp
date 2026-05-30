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

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>

#include "basics/operating-system.h"

#ifdef SERENEDB_HAVE_SIGNAL_H
#include <signal.h>
#endif
#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <exception>
#include <mutex>
#include <string_view>
#include <thread>

#include "basics/application-exit.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/physical_memory.h"
#include "basics/process-utils.h"
#include "basics/signals.h"
#include "basics/size_limited_string.h"
#include "basics/string_utils.h"
#include "basics/thread.h"
#include "build_id/build_id.h"
#include "crash_handler.h"
#include "rest/version.h"

#ifdef __linux__
#include <elf.h>
#include <sys/auxv.h>
#endif

#include <absl/debugging/stacktrace.h>
#include <absl/debugging/symbolize.h>

namespace sdb {
namespace {

using SmallString = SizeLimitedString<4096>;

// memory reserved for the signal handler stack
std::unique_ptr<char[]> gAlternativeStackMemory;

/// an atomic that makes sure there are no races inside the signal
/// handler callback
std::atomic<bool> gCrashHandlerInvoked(false);

/// an atomic that controls whether we will log backtraces
/// (default: yes on Linux, false elsewhere) or not
std::atomic<bool> gEnableStacktraces(true);

/// kill process hard using SIGKILL, in order to circumvent core
/// file generation etc.
std::atomic<bool> gKillHard(false);

/// string with server state information. must be null-terminated
std::atomic<const char*> gStateString = nullptr;

/// kills the process with the given signal
[[noreturn]] void KillProcess(int signal) {
  if (gKillHard.load(std::memory_order_relaxed)) {
    kill(getpid(), SIGKILL);  // to kill the complete process tree.
    std::this_thread::sleep_for(std::chrono::seconds(5));
  } else {
    // restore default signal action, so that we can write a core dump and crash
    // "properly"
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND |
                   (gAlternativeStackMemory != nullptr ? SA_ONSTACK : 0);
    act.sa_handler = SIG_DFL;
    sigaction(signal, &act, nullptr);

    // resend signal to ourselves to invoke default action for the signal (e.g.
    // coredump)
    kill(::getpid(), signal);
  }

  std::abort();
}

void AppendAddress(SmallString& dst, void* pc, intptr_t base) {
  auto address = reinterpret_cast<intptr_t>(pc);
  if (base == 0) {
    // absolute address of pc
    dst.append(" [$0x");
  } else {
    // relative offset of pc
    dst.append(" [+0x");
    address -= base;
  }
  dst.appendHexValue(address, false).append("] ");
}

/// builds a log message to be logged to the logfile later
/// does not allocate any memory, so should be safe to call even
/// in context of SIGSEGV, with a broken heap etc.
void BuildLogMessage(SmallString& buffer, std::string_view context, int signal,
                     const siginfo_t* info, void* ucontext) {
  // build a crash message
  buffer.append("💥 SereneDB ").append(SERENEDB_VERSION_FULL);

  // get build-id by reference, so we can avoid a copy here
  const auto build_id = build_id::GetBuildId();
  if (!build_id.empty()) {
    buffer.append(", build-id ");
    for (uint8_t c : build_id) {
      const auto* hex = &absl::numbers_internal::kHexTable[2 * c];
      buffer.push_back(hex[0]);
      buffer.push_back(hex[1]);
    }
  }

  // append thread id
  buffer.append(", thread ")
    .appendUInt64(uint64_t(Thread::currentThreadNumber()));

#ifdef __linux__
  // append thread name
  ThreadNameFetcher name_fetcher;
  buffer.append(" [").append(name_fetcher.get()).append("]");
#endif

  // append signal number and name
  bool printed = false;
  buffer.append(" caught unexpected signal ").appendUInt64(uint64_t(signal));
  buffer.append(" (").append(signals::Name(signal));
  if (info != nullptr) {
    // signal sub type, if available
    std::string_view sub_type = signals::SubtypeName(signal, info->si_code);
    if (!sub_type.empty()) {
      buffer.append(", sub type ");
      buffer.append(sub_type);
    }
    // pid that sent the signal
    buffer.append(") from pid ").appendUInt64(uint64_t(info->si_pid));
    printed = true;
  }
  if (!printed) {
    buffer.append(")");
  }

  if (const char* ss = gStateString.load(); ss != nullptr) {
    // append application server state
    buffer.append(" in state \"").append(ss).append("\"");
  }

  if (info != nullptr && (signal == SIGSEGV || signal == SIGBUS)) {
    // dump address that was accessed when the failure occurred (this is
    // somewhat likely a nullptr)
    buffer.append(" accessing address 0x").appendHexValue(info->si_addr, false);
  }

  buffer.append(": ").append(context);

#ifdef __linux__
  {
    // AT_PHDR points to the program header, which is located after the ELF
    // header. This allows us to calculate the base address of the executable.
    auto base_addr = getauxval(AT_PHDR) - sizeof(Elf64_Ehdr);
    buffer.append(" - image base address: 0x").appendHexValue(base_addr, false);
  }
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
  // FIXME: implement ARM64 context output
  buffer.append(" ARM64 CPU context: is not available ");
#else
  if (auto ctx = static_cast<ucontext_t*>(ucontext); ctx) {
    auto append_register = [ctx, &buffer](const char* prefix, int reg) {
      buffer.append(prefix).appendHexValue(ctx->uc_mcontext.gregs[reg], false);
    };
    buffer.append(" - CPU context:");
    append_register(" rip: 0x", REG_RIP);
    append_register(", rsp: 0x", REG_RSP);
    append_register(", efl: 0x", REG_EFL);
    append_register(", rbp: 0x", REG_RBP);
    append_register(", rsi: 0x", REG_RSI);
    append_register(", rdi: 0x", REG_RDI);
    append_register(", rax: 0x", REG_RAX);
    append_register(", rbx: 0x", REG_RBX);
    append_register(", rcx: 0x", REG_RCX);
    append_register(", rdx: 0x", REG_RDX);
    append_register(", r8: 0x", REG_R8);
    append_register(", r9: 0x", REG_R9);
    append_register(", r10: 0x", REG_R10);
    append_register(", r11: 0x", REG_R11);
    append_register(", r12: 0x", REG_R12);
    append_register(", r13: 0x", REG_R13);
    append_register(", r14: 0x", REG_R14);
    append_register(", r15: 0x", REG_R15);
  }
#endif
#endif
}

void LogCrashInfo(std::string_view context, int signal, siginfo_t* info,
                  void* ucontext) try {
  // fixed buffer for constructing temporary log messages (to avoid malloc)
  SmallString buffer;

  BuildLogMessage(buffer, context, signal, info, ucontext);
  // note: SDB_FATAL() can allocate memory
  SDB_LOG(FATAL, CRASH, buffer.view());
} catch (...) {
  // we better not throw an exception from inside a signal handler
}

void LogStacktrace(void* ucontext = nullptr) try {
  if (!gEnableStacktraces.load(std::memory_order_relaxed)) {
    return;
  }

  ThreadNameFetcher name_fetcher;
  std::string_view current_thread_name = name_fetcher.get();

  // fixed buffer for constructing temporary log messages (to avoid malloc)
  SmallString buffer;

  buffer.append("Backtrace of thread ");
  buffer.appendUInt64(Thread::currentThreadNumber());
  buffer.append(" [").append(current_thread_name).append("]");

  SDB_INFO(CRASH, buffer.view());

  // number of frames to skip in backtrace output
  static constexpr int kSkipFrames = 1;
  // maximum number of stack traces to show
  static constexpr int kMaxFrames = 100;
  void* stack[kMaxFrames];
  char demangled[1024];
  int depth = ucontext ? absl::GetStackTraceWithContext(
                           stack, kMaxFrames, kSkipFrames, ucontext, nullptr)
                       : absl::GetStackTrace(stack, kMaxFrames, kSkipFrames);
#ifdef __linux__
  // The address of the program headers of the executable.
  intptr_t base = getauxval(AT_PHDR) - sizeof(Elf64_Ehdr);
#else
  intptr_t base = 0;
#endif
  for (int i = 0; i < depth; ++i) {
    buffer.clear();

    buffer.append("frame ");
    if (i < 10 - kSkipFrames) {
      buffer.push_back(' ');  // pad frame id to 2 digits length
    }
    buffer.appendUInt64(static_cast<uint64_t>(i + kSkipFrames));

    AppendAddress(buffer, stack[i], base);

    if (absl::Symbolize(stack[i], demangled, sizeof(demangled))) {
      buffer.append(demangled);
    } else {
      buffer.append("*no symbol name available for this frame");
    }
    SDB_INFO(CRASH, buffer.view());
  }
  log::Flush();
} catch (...) {
  // we better not throw an exception from inside a signal handler
}

// log info about the current process
void LogProcessInfo() {
  auto process_info = GetProcessInfoSelf();

  // fixed buffer for constructing temporary log messages (to avoid malloc)
  SmallString buffer;
  buffer.append("available physical memory: ")
    .appendUInt64(uint64_t(physical_memory::GetValue()));
  buffer.append(", rss usage: ")
    .appendUInt64(uint64_t(process_info.resident_size));
  buffer.append(", vsz usage: ")
    .appendUInt64(uint64_t(process_info.virtual_size));
  buffer.append(", threads: ")
    .appendUInt64(uint64_t(process_info.number_threads));

  SDB_INFO(CRASH, buffer.view());
}

/// Logs the reception of a signal to the logfile.
/// this is the actual function that is invoked for a deadly signal
/// (i.e. SIGSEGV, SIGBUS, SIGILL, SIGFPE...)
///
/// the following assumptions are made for this crash handler:
/// - it is invoked in fatal situations only, that we need as much information
/// as possible
///   about. thus we try logging some information into the SereneDB logfile. Our
///   logger is not async-safe right now, but everything in our own log
///   message-building routine should be async-safe. in case of a corrupted
///   heap/stack all this will fall apart. However, it is better to try using
///   our logger than doing nothing, or writing somewhere else nobody will see
///   the information later.
/// - the interesting signals are delivered from the same thread that caused
/// them. Thus we
///   will have a few stack frames of the offending thread available.
/// - it is not possible to generate the stack traces from other threads without
/// substantial
///   efforts, so we are not even trying this.
/// - Windows and macOS are currently not supported.
void CrashHandlerSignalHandler(int signal, siginfo_t* info, void* ucontext) {
  if (!gCrashHandlerInvoked.exchange(true)) {
    LogCrashInfo("signal handler invoked", signal, info, ucontext);
    LogStacktrace(ucontext);
    LogProcessInfo();
    log::Flush();
    log::Shutdown();
  } else {
    // signal handler was already entered by another thread...
    // there is not so much we can do here except waiting and then finally let
    // it crash

    // alternatively, we can get if the current thread has received the signal,
    // invoked the signal handler and while being in there, caught yet another
    // signal.
    std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  KillProcess(signal);
}

}  // namespace

void CrashHandler::logBacktrace() {
  LogStacktrace();
  log::Flush();
}

/// logs a fatal message and crashes the program
void CrashHandler::crash(std::string_view context) {
  LogCrashInfo(context, SIGABRT, /*no signal*/ nullptr,
               /*no context*/ nullptr);
  LogStacktrace();
  LogProcessInfo();
  log::Flush();
  log::Shutdown();

  // crash from here
  KillProcess(SIGABRT);
}

void CrashHandler::SetState(std::string_view state) {
  gStateString.store(state.data());
}

/// logs an assertion failure and crashes the program
[[noreturn]] void CrashHandler::assertionFailure(const char* file, int line,
                                                 const char* func,
                                                 const char* context,
                                                 std::string_view message) {
  // assemble an "assertion failured in file:line: message" string
  SmallString buffer;

  buffer.append("assertion failed in ")
    .append(file == nullptr ? "unknown file" : file)
    .append(":");
  buffer.appendUInt64(static_cast<uint64_t>(line));
  if (func != nullptr) {
    buffer.append(" [").append(func).append("]");
  }
  buffer.append(": ");
  buffer.append(context);
  if (!message.empty()) {
    buffer.append(" ; ").append(message);
  }

  crash(buffer.view());
}

/// set flag to kill process hard using SIGKILL, in order to circumvent
/// core file generation etc.
void CrashHandler::setHardKill() {
  gKillHard.store(true, std::memory_order_relaxed);
}

/// disable printing of backtraces
void CrashHandler::disableBacktraces() {
  gEnableStacktraces.store(false, std::memory_order_relaxed);
}

/// installs the crash handler globally
void CrashHandler::installCrashHandler() {
  // read environment variable that can be used to toggle the
  // crash handler
  std::string value;
  if (SdbGETENV("SERENEDB_OVERRIDE_CRASH_HANDLER", value)) {
    bool toggle = basics::string_utils::Boolean(value);
    if (!toggle) {
      // crash handler backtraces turned off
      disableBacktraces();
      // additionally, do not install signal handler nor the
      // handler for std::terminate()
      return;
    }
  }

  try {
    const size_t stack_size =
      std::max<size_t>(128 * 1024, std::max<size_t>(MINSIGSTKSZ, SIGSTKSZ));

    gAlternativeStackMemory = std::make_unique<char[]>(stack_size);

    stack_t altstack;
    altstack.ss_sp = static_cast<void*>(gAlternativeStackMemory.get());
    altstack.ss_size = stack_size;
    altstack.ss_flags = 0;
    if (sigaltstack(&altstack, nullptr) != 0) {
      gAlternativeStackMemory.release();
    }
  } catch (...) {
    // could not allocate memory for alternative stack.
    // in this case we must not modify the stack for the signal handler
    // as we have no way of signaling failure here.
    gAlternativeStackMemory.release();
  }

  // install signal handlers for the following signals
  struct sigaction act;
  sigemptyset(&act.sa_mask);
  act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO |
                 (gAlternativeStackMemory != nullptr ? SA_ONSTACK : 0);
  act.sa_sigaction = CrashHandlerSignalHandler;
  sigaction(SIGSEGV, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);
  sigaction(SIGILL, &act, nullptr);
  sigaction(SIGFPE, &act, nullptr);
  sigaction(SIGABRT, &act, nullptr);

  // install handler for std::terminate()
  std::set_terminate([] {
    SmallString buffer;
    buffer.append("handler for std::terminate() invoked ");
    if (auto e = std::current_exception()) {
      // there is an active exception going on...
      try {
        // rethrow so we can get the exception type and its message
        std::rethrow_exception(e);
      } catch (const std::exception& e) {
        buffer.append("with an std::exception: ");
        if (const char* data = e.what(); data != nullptr) {
          buffer.append(data);
        }
      } catch (...) {
        buffer.append("with an unknown exception");
      }
    } else {
      buffer.append("without active exception");
    }

    CrashHandler::crash(buffer.view());
  });
}

}  // namespace sdb
