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

#include "process-utils.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/internal/ostringstream.h>
#include <absl/strings/str_cat.h>
#include <errno.h>
#include <signal.h>
#include <spawn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>
#include <type_traits>

#include "basics/strings.h"

#if defined(SERENEDB_HAVE_MACOS_MEM_STATS)
#include <sys/sysctl.h>
#endif

#ifdef SERENEDB_HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#ifdef SERENEDB_HAVE_SIGNAL_H
#include <signal.h>
#endif

#ifdef SERENEDB_HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#ifdef SERENEDB_HAVE_MACH
#include <mach/mach_host.h>
#include <mach/mach_port.h>
#include <mach/mach_traps.h>
#include <mach/task.h>
#include <mach/thread_act.h>
#include <mach/vm_map.h>
#endif

#include <fcntl.h>
#include <sys/resource.h>

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <algorithm>
#include <string_view>

#include "basics/containers/flat_hash_map.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/number_utils.h"
#include "basics/operating-system.h"
#include "basics/page_size.h"
#include "basics/string_utils.h"
#include "basics/system-functions.h"
#include "basics/thread.h"

namespace sdb {
namespace {

#ifdef SERENEDB_HAVE_LINUX_PROC
// consumes all whitespace
void SkipWhitespace(const char*& p, const char* e) {
  while (p < e && *p == ' ') {
    ++p;
  }
}
// consumes all non-whitespace
void SkipNonWhitespace(const char*& p, const char* e) {
  if (p < e && *p == '(') {
    // special case: if the value starts with a '(', we will skip over all
    // data until we find the closing parenthesis. this is used for the process
    // name at least
    ++p;
    while (p < e && *p != ')') {
      ++p;
    }
    if (p < e && *p == ')') {
      ++p;
    }
  } else {
    // no parenthesis at start, so just skip over whitespace
    while (p < e && *p != ' ') {
      ++p;
    }
  }
}
// reads over the whitespace at the beginning plus the following data
void SkipEntry(const char*& p, const char* e) {
  SkipWhitespace(p, e);
  SkipNonWhitespace(p, e);
}
// reads a numeric entry from the buffer
template<typename T>
T ReadEntry(const char*& p, const char* e) {
  SkipWhitespace(p, e);
  const char* s = p;
  SkipNonWhitespace(p, e);
  return sdb::number_utils::AtoiUnchecked<uint64_t>(s, p);
}
#endif

bool CreatePipes(int* pipe_server_to_child, int* pipe_child_to_server) {
  if (pipe(pipe_server_to_child) == -1) {
    SDB_ERROR("xxxxx", sdb::Logger::FIXME, "cannot create pipe");
    return false;
  }

  if (pipe(pipe_child_to_server) == -1) {
    SDB_ERROR("xxxxx", sdb::Logger::FIXME, "cannot create pipe");

    close(pipe_server_to_child[0]);
    close(pipe_server_to_child[1]);

    return false;
  }

  return true;
}

void StartExternalProcessPosixSpawn(
  ExternalProcess* external, bool use_pipes,
  std::span<const std::string_view> additional_env,
  const char* file_for_std_err) {
  int pipe_server_to_child[2];
  int pipe_child_to_server[2];

  if (use_pipes) {
    bool ok = CreatePipes(pipe_server_to_child, pipe_child_to_server);

    if (!ok) {
      external->status = kExtPipeFailed;
      return;
    }
  }

  int err = 0;  // accumulate any errors we might get
  posix_spawn_file_actions_t file_actions;
  err |= posix_spawn_file_actions_init(&file_actions);
  // file actions are performed in order they were added.

  if (use_pipes) {
    err |= posix_spawn_file_actions_adddup2(&file_actions,
                                            pipe_server_to_child[0], 0);
    err |= posix_spawn_file_actions_adddup2(&file_actions,
                                            pipe_child_to_server[1], 1);

    err |=
      posix_spawn_file_actions_addclose(&file_actions, pipe_server_to_child[0]);
    err |=
      posix_spawn_file_actions_addclose(&file_actions, pipe_server_to_child[1]);
    err |=
      posix_spawn_file_actions_addclose(&file_actions, pipe_child_to_server[0]);
    err |=
      posix_spawn_file_actions_addclose(&file_actions, pipe_child_to_server[1]);
  } else {
    err |= posix_spawn_file_actions_addopen(&file_actions, 0, "/dev/null",
                                            O_RDONLY, 0);
  }

  if (file_for_std_err != nullptr) {
    err |= posix_spawn_file_actions_addopen(&file_actions, 2, file_for_std_err,
                                            O_CREAT | O_WRONLY | O_TRUNC, 0644);
  }

  posix_spawnattr_t spawn_attrs;
  err |= posix_spawnattr_init(&spawn_attrs);
  err |= posix_spawnattr_setflags(
    &spawn_attrs, POSIX_SPAWN_SETSIGDEF | POSIX_SPAWN_SETSIGMASK);
  sigset_t all;
  sigfillset(&all);
  err |= posix_spawnattr_setsigdefault(&spawn_attrs, &all);
  sigset_t none;
  sigemptyset(&none);
  err |= posix_spawnattr_setsigmask(&spawn_attrs, &none);

  absl::Cleanup cleanup = [&]() noexcept {
    posix_spawnattr_destroy(&spawn_attrs);
    posix_spawn_file_actions_destroy(&file_actions);
  };

  if (err != 0) {
    external->status = kExtPipeFailed;
    if (use_pipes) {
      close(pipe_server_to_child[0]);
      close(pipe_server_to_child[1]);
      close(pipe_child_to_server[0]);
      close(pipe_child_to_server[1]);
    }
    return;
  }

  auto extract_name = [](const char* value) noexcept {
    std::string_view v;
    if (value != nullptr) {
      v = value;
      if (auto pos = v.find('='); pos != std::string_view::npos) {
        v = v.substr(0, pos);
      }
    }
    return v;
  };

  // note the position in the envs vector for every unique environment variable.
  // we do this to make the passed environment variables unique, e.g. in case
  // the original env contains a setting with is later overriden by
  // additionalEnv. in this case we want additionalEnv to win.
  containers::FlatHashMap<std::string_view, size_t> positions;

  std::vector<char*> envs;
  for (char** e = environ; *e != nullptr; ++e) {
    positions.insert_or_assign(extract_name(*e), envs.size());
    envs.push_back(*e);
  }

  envs.reserve(envs.size() + additional_env.size() + 1);
  for (const auto& e : additional_env) {
    auto name = extract_name(e.data());
    if (auto it = positions.find(name); it != positions.end()) {
      // environment variable already set. now update it
      envs[it->second] = const_cast<char*>(e.data());
    } else {
      // new environment variable
      positions.emplace(name, envs.size());
      envs.push_back(const_cast<char*>(e.data()));
    }
  }

  SDB_ASSERT(std::unique(envs.begin(), envs.end(),
                         [&](const char* lhs, const char* rhs) {
                           return extract_name(lhs) == extract_name(rhs);
                         }) == envs.end());

  // terminate the array with a null pointer entry. this is required for
  // posix_spawnp
  envs.emplace_back(nullptr);

  int result =
    posix_spawnp(&external->pid, external->executable.c_str(), &file_actions,
                 &spawn_attrs, external->arguments, envs.data());

  if (result != 0) {
    int errno_copy = errno;
    if (errno_copy == ENOENT) {
      // We fake the old legacy behaviour here from the fork/exec times:
      external->status = kExtTerminated;
      external->exit_status = 1;
      SDB_ERROR("xxxxx", Logger::FIXME, "spawn failed: executable '",
                external->executable, "' not found");
    } else {
      external->status = kExtForkFailed;

      SDB_ERROR("xxxxx", Logger::FIXME, "spawning of executable '",
                external->executable, "' failed: ", strerror(errno_copy));
    }
    if (use_pipes) {
      close(pipe_server_to_child[0]);
      close(pipe_server_to_child[1]);
      close(pipe_child_to_server[0]);
      close(pipe_child_to_server[1]);
    }

    return;
  }

  SDB_DEBUG("xxxxx", Logger::FIXME, "spawning executable '",
            external->executable, "' succeeded, child pid: ", external->pid);

  if (use_pipes) {
    close(pipe_server_to_child[0]);
    close(pipe_child_to_server[1]);

    external->write_pipe = pipe_server_to_child[1];
    external->read_pipe = pipe_child_to_server[0];
  } else {
    external->write_pipe = -1;
    external->read_pipe = -1;
  }

  external->status = kExtRunning;
}

#ifdef SERENEDB_HAVE_GETRUSAGE

[[maybe_unused]] uint64_t GetMicrosecondsTv(struct timeval* tv) {
  time_t sec = tv->tv_sec;
  suseconds_t usec = tv->tv_usec;

  while (usec < 0) {
    usec += 1000000;
    sec -= 1;
  }

  return (sec * 1000000LL) + usec;
}

#endif

ExternalProcess* GetExternalProcess(pid_t pid) {
  if (kill(pid, 0) == 0) {
    ExternalProcess* external = new ExternalProcess();

    external->pid = pid;
    external->status = kExtRunning;

    return external;
  }

  SDB_WARN("xxxxx", sdb::Logger::FIXME, "checking for external process: '", pid,
           "' failed with error: ", strerror(errno));
  return nullptr;
}

// check for a process we didn't spawn, and check for access rights to
// send it signals.
bool KillProcess(ExternalProcess* pid, int signal) {
  SDB_ASSERT(pid != nullptr);
  if (pid == nullptr) {
    return false;
  }
  if (signal == SIGKILL) {
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "sending SIGKILL signal to process: ", pid->pid);
  }
  if (kill(pid->pid, signal) == 0) {
    return true;
  }
  return false;
}

}  // namespace

std::vector<ExternalProcess*> gExternalProcesses;
constinit absl::Mutex gExternalProcessesLock{absl::kConstInit};

ExternalProcess::~ExternalProcess() {
  SDB_ASSERT(number_arguments == 0 || arguments != nullptr);
  for (size_t i = 0; i < number_arguments; i++) {
    if (arguments[i] != nullptr) {
      delete[] arguments[i];
    }
  }
  if (arguments) {
    delete[] arguments;
  }

  if (read_pipe != -1) {
    close(read_pipe);
  }
  if (write_pipe != -1) {
    close(write_pipe);
  }
}

ExternalProcess* LookupSpawnedProcess(pid_t pid) {
  {
    std::lock_guard guard{gExternalProcessesLock};
    auto found = std::find_if(
      gExternalProcesses.begin(), gExternalProcesses.end(),
      [pid](const ExternalProcess* m) -> bool { return m->pid == pid; });
    if (found != gExternalProcesses.end()) {
      return *found;
    }
  }
  return nullptr;
}

std::optional<ExternalProcessStatus> LookupSpawnedProcessStatus(pid_t pid) {
  std::lock_guard guard{gExternalProcessesLock};
  auto found = std::find_if(
    gExternalProcesses.begin(), gExternalProcesses.end(),
    [pid](const ExternalProcess* m) -> bool { return m->pid == pid; });
  if (found != gExternalProcesses.end()) {
    ExternalProcessStatus ret;
    ret.status = (*found)->status;
    ret.exit_status = (*found)->exit_status;
    return std::optional<ExternalProcessStatus>{ret};
  }
  return std::nullopt;
}

#ifdef SERENEDB_HAVE_LINUX_PROC

ProcessInfo GetProcessInfoSelf() {
  return GetProcessInfo(Thread::currentProcessId());
}

#elif SERENEDB_HAVE_GETRUSAGE

ProcessInfo GetProcessInfoSelf() {
  ProcessInfo result;

  result._sc_clk_tck = 1000000;

  struct rusage used;
  int res = getrusage(RUSAGE_SELF, &used);

  if (res == 0) {
    result.minor_page_faults = used.ru_minflt;
    result.major_page_faults = used.ru_majflt;

    result._system_time = GetMicrosecondsTv(&used.ru_stime);
    result._user_time = GetMicrosecondsTv(&used.ru_utime);

    // ru_maxrss is the resident set size in kilobytes. need to multiply with
    // 1024 to get the number of bytes
    result.resident_size = used.ru_maxrss * SERENEDB_GETRUSAGE_MAXRSS_UNIT;
  }

#ifdef SERENEDB_HAVE_MACH
  {
    kern_return_t rc;
    thread_array_t array;
    mach_msg_type_number_t count;

    rc = task_threads(mach_task_self(), &array, &count);

    if (rc == KERN_SUCCESS) {
      unsigned int i;

      result.number_threads = count;

      for (i = 0; i < count; ++i) {
        mach_port_deallocate(mach_task_self(), array[i]);
      }

      vm_deallocate(mach_task_self(), (vm_address_t)array,
                    sizeof(thread_t) * count);
    }
  }

  {
    kern_return_t rc;
    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

    rc = task_info(mach_task_self(), TASK_BASIC_INFO, (task_info_t)&t_info,
                   &t_info_count);
    if (rc == KERN_SUCCESS) {
      result.virtual_size = t_info.virtual_size;
      result.resident_size = t_info.resident_size;
    } else {
      result.virtual_size = 0;
      result.resident_size = 0;
    }
  }
#endif

  return result;
}

#else
/// --------------------------------------------
/// transform a file time to timestamp
/// Particularities:
/// 1. FileTime can save a date at Jan, 1 1601
///    timestamp saves dates at 1970
/// --------------------------------------------

static uint64_t _TimeAmount(FILETIME* ft) {
  uint64_t ts, help;
  ts = ft->dwLowDateTime;
  help = ft->dwHighDateTime;
  help = help << 32;
  ts |= help;
  /// at moment without transformation
  return ts;
}

[[maybe_unused]] static time_t _FileTime_to_POSIX(FILETIME* ft) {
  LONGLONG ts, help;
  ts = ft->dwLowDateTime;
  help = ft->dwHighDateTime;
  help = help << 32;
  ts |= help;

  return (ts - 116444736000000000) / 10000000;
}

static ProcessInfo GetProcessInfoH(HANDLE processHandle, pid_t pid) {
  ProcessInfo result;

  PROCESS_MEMORY_COUNTERS_EX pmc;
  pmc.cb = sizeof(PROCESS_MEMORY_COUNTERS_EX);
  // compiler warning wird in kauf genommen!c
  // http://msdn.microsoft.com/en-us/library/windows/desktop/ms684874(v=vs.85).aspx
  if (GetProcessMemoryInfo(processHandle, (PPROCESS_MEMORY_COUNTERS)&pmc,
                           pmc.cb)) {
    result.major_page_faults = pmc.PageFaultCount;
    // there is not any corresponce to minflt in linux
    result.minor_page_faults = 0;

    // from MSDN:
    // "The working set is the amount of memory physically mapped to the process
    // context at a given time.
    // Memory in the paged pool is system memory that can be transferred to the
    // paging file on disk(paged) when
    // it is not being used. Memory in the nonpaged pool is system memory that
    // cannot be paged to disk as long as
    // the corresponding objects are allocated. The pagefile usage represents
    // how much memory is set aside for the
    // process in the system paging file. When memory usage is too high, the
    // virtual memory manager pages selected
    // memory to disk. When a thread needs a page that is not in memory, the
    // memory manager reloads it from the
    // paging file."

    result.resident_size = pmc.WorkingSetSize;
    result.virtual_size = pmc.PrivateUsage;
  }
  /// computing times
  FILETIME creationTime, exitTime, kernelTime, userTime;
  if (GetProcessTimes(processHandle, &creationTime, &exitTime, &kernelTime,
                      &userTime)) {
    // see remarks in
    // http://msdn.microsoft.com/en-us/library/windows/desktop/ms683223(v=vs.85).aspx
    // value in seconds
    result._sc_clk_tck = 10000000;  // 1e7
    result._system_time = _TimeAmount(&kernelTime);
    result._user_time = _TimeAmount(&userTime);
    // for computing  the timestamps of creation and exit time
    // the function '_FileTime_to_POSIX' should be called
  }
  /// computing number of threads
  HANDLE snapShot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, pid);

  if (snapShot != INVALID_HANDLE_VALUE) {
    THREADENTRY32 te32;
    te32.dwSize = sizeof(THREADENTRY32);
    if (Thread32First(snapShot, &te32)) {
      result.number_threads++;
      while (Thread32Next(snapShot, &te32)) {
        if (te32.th32OwnerProcessID == pid) {
          result.number_threads++;
        }
      }
    } else {
      SDB_ERROR("xxxxx", sdb::Logger::FIXME,
                "failed to acquire thread from snapshot - ", GetLastError());
    }
    CloseHandle(snapShot);
  } else {
    SDB_ERROR("xxxxx", sdb::Logger::FIXME,
              "failed to acquire process threads count - ", GetLastError());
  }

  return result;
}

ProcessInfo GetProcessInfoSelf() {
  return GetProcessInfoH(GetCurrentProcess(), GetCurrentProcessId());
}

ProcessInfo GetProcessInfo(pid_t pid) {
  auto external = LookupSpawnedProcess(pid);
  if (external != nullptr) {
    return GetProcessInfoH(external->_process, pid);
  }
  return {};
}

#endif

#ifdef SERENEDB_HAVE_LINUX_PROC

ProcessInfo GetProcessInfo(pid_t pid) {
  ProcessInfo result;

  char str[1024];
  memset(&str, 0, sizeof(str));

  // build filename /proc/<pid>/stat
  {
    char* p = &str[0];

    // a malloc-free sprintf...
    static constexpr const char* kProc = "/proc/";
    static constexpr const char* kStat = "/stat";

    // append /proc/
    memcpy(p, kProc, strlen(kProc));
    p += strlen(kProc);
    // append pid
    p += sdb::basics::string_utils::Itoa(uint64_t(pid), p);
    memcpy(p, kStat, strlen(kStat));
    p += strlen(kStat);
    *p = '\0';
  }

  // open file...
  int fd = SERENEDB_OPEN(&str[0], O_RDONLY);

  if (fd >= 0) {
    memset(&str, 0, sizeof(str));

    auto n = SERENEDB_READ(fd, str, static_cast<size_t>(sizeof(str)));
    close(fd);

    if (n == 0) {
      return result;
    }

    /// buffer now contains all data documented by "proc"
    /// see man 5 proc for the state of a process

    const char* p = &str[0];
    const char* e = p + n;

    SkipEntry(p, e);                                       // process id
    SkipEntry(p, e);                                       // process name
    SkipEntry(p, e);                                       // process state
    SkipEntry(p, e);                                       // ppid
    SkipEntry(p, e);                                       // pgrp
    SkipEntry(p, e);                                       // session
    SkipEntry(p, e);                                       // tty nr
    SkipEntry(p, e);                                       // tpgid
    SkipEntry(p, e);                                       // flags
    result.minor_page_faults = ReadEntry<uint64_t>(p, e);  // min flt
    SkipEntry(p, e);                                       // cmin flt
    result.major_page_faults = ReadEntry<uint64_t>(p, e);  // maj flt
    SkipEntry(p, e);                                       // cmaj flt
    result.user_time = ReadEntry<uint64_t>(p, e);          // utime
    result.system_time = ReadEntry<uint64_t>(p, e);        // stime
    SkipEntry(p, e);                                       // cutime
    SkipEntry(p, e);                                       // cstime
    SkipEntry(p, e);                                       // priority
    SkipEntry(p, e);                                       // nice
    result.number_threads = ReadEntry<int64_t>(p, e);      // num threads
    SkipEntry(p, e);                                       // itrealvalue
    SkipEntry(p, e);                                       // starttime
    result.virtual_size = ReadEntry<uint64_t>(p, e);       // vsize
    result.resident_size =
      ReadEntry<int64_t>(p, e) * page_size::GetValue();  // rss
    result.sc_clk_tck = sysconf(_SC_CLK_TCK);
  }

  return result;
}

#else
ProcessInfo GetProcessInfo(pid_t pid) {
  ProcessInfo result;

  result._sc_clk_tck = 1;

  return result;
}
#endif

void SetProcessTitle(const char* title) {
#ifdef SERENEDB_HAVE_SYS_PRCTL_H
  prctl(PR_SET_NAME, title, 0, 0, 0);
#endif
}

void CreateExternalProcess(std::string_view executable,
                           std::span<const std::string_view> arguments,
                           std::span<const std::string_view> additional_env,
                           bool use_pipes, ExternalId* pid,
                           const char* file_for_std_err) {
  const size_t n = arguments.size();
  // create the external structure
  auto external = std::make_unique<ExternalProcess>();
  external->executable = executable;
  external->number_arguments = n + 1;

  external->arguments = new (std::nothrow) char*[n + 2];

  if (external->arguments == nullptr) {
    // gracefully handle out of memory
    pid->pid = SERENEDB_INVALID_PROCESS_ID;
    return;
  }

  memset(external->arguments, 0, (n + 2) * sizeof(char*));

  external->arguments[0] =
    DuplicateString(executable.data(), executable.size());
  if (external->arguments[0] == nullptr) {
    // OOM
    pid->pid = SERENEDB_INVALID_PROCESS_ID;
    return;
  }

  for (size_t i = 0; i < n; ++i) {
    external->arguments[i + 1] =
      DuplicateString(arguments[i].data(), arguments[i].size());
    if (external->arguments[i + 1] == nullptr) {
      // OOM
      pid->pid = SERENEDB_INVALID_PROCESS_ID;
      return;
    }
  }

  external->arguments[n + 1] = nullptr;
  external->status = kExtNotStarted;

  StartExternalProcessPosixSpawn(external.get(), use_pipes, additional_env,
                                 file_for_std_err);

  if (external->status != kExtRunning && external->status != kExtTerminated) {
    pid->pid = SERENEDB_INVALID_PROCESS_ID;
    return;
  }

  SDB_DEBUG("xxxxx", sdb::Logger::FIXME, "adding process ", external->pid,
            " to list");

  // Note that the following deals with different types under windows,
  // however, this code here can be written in a platform-independent
  // way:
  pid->pid = external->pid;
  pid->read_pipe = external->read_pipe;
  pid->write_pipe = external->write_pipe;

  std::lock_guard guard{gExternalProcessesLock};

  try {
    gExternalProcesses.push_back(external.get());
    std::ignore = external.release();
  } catch (...) {
    pid->pid = SERENEDB_INVALID_PROCESS_ID;
    return;
  }
}

void ClosePipe(ExternalProcess* process, bool read) {
  if (process == nullptr ||
      (read && SERENEDB_IS_INVALID_PIPE(process->read_pipe)) ||
      (!read && SERENEDB_IS_INVALID_PIPE(process->write_pipe))) {
    return;
  }

  auto pipe = (read) ? &process->read_pipe : &process->write_pipe;

  if (*pipe != -1) {
    FILE* stream = fdopen(*pipe, "w");
    if (stream != nullptr) {
      fflush(stream);
    }
    close(*pipe);
    *pipe = -1;
  }
}

ssize_t ReadPipe(const ExternalProcess* process, char* buffer,
                 size_t buffer_size) {
  if (process == nullptr || SERENEDB_IS_INVALID_PIPE(process->read_pipe)) {
    return 0;
  }

  memset(buffer, 0, buffer_size);

  return SdbReadPointer(process->read_pipe, buffer, buffer_size);
}

bool WritePipe(const ExternalProcess* process, const char* buffer,
               size_t buffer_size) {
  if (process == nullptr || SERENEDB_IS_INVALID_PIPE(process->write_pipe)) {
    return false;
  }

  return SdbWritePointer(process->write_pipe, buffer, buffer_size);
}

ExternalProcessStatus CheckExternalProcess(
  ExternalId pid, bool wait, uint32_t timeout,
  const std::function<bool()>& deadline_reached) {
  auto status = LookupSpawnedProcessStatus(pid.pid);

  if (!status.has_value()) {
    SDB_WARN("xxxxx", sdb::Logger::FIXME,
             "checkExternal: pid not found: ", pid.pid);
    return ExternalProcessStatus{
      kExtNotFound, -1,
      absl::StrCat("the pid you're looking for is not in our list: ", pid.pid)};
  }

  if (status->status == kExtRunning || status->status == kExtStopped) {
    if (timeout > 0) {
      // if we use a timeout, it means we cannot use blocking
      wait = false;
    }

    int opts;
    if (wait) {
      opts = WUNTRACED;
    } else {
      opts = WNOHANG | WUNTRACED;
    }

    int loc = 0;
    pid_t res = 0;
    bool timeout_happened = false;
    if (timeout) {
      SDB_ASSERT((opts & WNOHANG) != 0);
      double end_time = 0.0;
      while (true) {
        res = waitpid(pid.pid, &loc, opts);
        if (res != 0) {
          break;
        }
        double now = utilities::GetMicrotime();
        if (end_time == 0.0) {
          end_time = now + timeout / 1000.0;
        } else if (now >= end_time) {
          res = pid.pid;
          timeout_happened = true;
          break;
        }
        if (deadline_reached) {
          timeout_happened = deadline_reached();
          if (timeout_happened) {
            break;
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
    } else {
      res = waitpid(pid.pid, &loc, opts);
    }

    if (res == 0) {
      if (wait) {
        status->error_message = absl::StrCat(
          "waitpid returned 0 for pid while it shouldn't ", pid.pid);
        if (timeout_happened) {
          status->status = kExtTimeout;
          status->exit_status = -1;
        } else if (WIFEXITED(loc)) {
          status->status = kExtTerminated;
          status->exit_status = WEXITSTATUS(loc);
        } else if (WIFSIGNALED(loc)) {
          status->status = kExtAborted;
          status->exit_status = WTERMSIG(loc);
        } else if (WIFSTOPPED(loc)) {
          status->status = kExtStopped;
          status->exit_status = 0;
        } else {
          status->status = kExtAborted;
          status->exit_status = 0;
        }
      } else {
        status->exit_status = 0;
      }
    } else if (res == -1) {
      if (errno == ECHILD) {
        status->status = kExtNotFound;
      }
      SetError(ERROR_SYS_ERROR);
      SDB_WARN("xxxxx", sdb::Logger::FIXME, "waitpid returned error for pid ",
               pid.pid, " (", wait, "): ", LastError());
      status->error_message = absl::StrCat("waitpid returned error for pid ",
                                           pid.pid, ": ", LastError());
    } else if (static_cast<pid_t>(pid.pid) == static_cast<pid_t>(res)) {
      if (timeout_happened) {
        status->status = kExtTimeout;
        status->exit_status = -1;
      } else if (WIFEXITED(loc)) {
        status->status = kExtTerminated;
        status->exit_status = WEXITSTATUS(loc);
      } else if (WIFSIGNALED(loc)) {
        status->status = kExtAborted;
        status->exit_status = WTERMSIG(loc);
      } else if (WIFSTOPPED(loc)) {
        status->status = kExtStopped;
        status->exit_status = 0;
      } else {
        status->status = kExtAborted;
        status->exit_status = 0;
      }
    } else {
      SDB_WARN("xxxxx", sdb::Logger::FIXME,
               "unexpected waitpid result for pid ", pid.pid, ": ", res);
      status->error_message =
        absl::StrCat("unexpected waitpid result for pid ", pid.pid, ": ", res);
    }
  } else {
    SDB_WARN("xxxxx", sdb::Logger::FIXME, "unexpected process status ",
             status->status, ": ", status->exit_status);
    status->error_message = absl::StrCat(
      "unexpected process status ", status->status, ": ", status->exit_status);
  }

  // Persist our fresh status or unlink the process
  ExternalProcess* delete_me = nullptr;
  {
    std::lock_guard guard{gExternalProcessesLock};

    auto found = std::find_if(
      gExternalProcesses.begin(), gExternalProcesses.end(),
      [pid](const ExternalProcess* m) -> bool { return (m->pid == pid.pid); });
    if (found != gExternalProcesses.end()) {
      if ((status->status != kExtRunning) && (status->status != kExtStopped) &&
          (status->status != kExtTimeout)) {
        delete_me = *found;
        std::swap(*found, gExternalProcesses.back());
        gExternalProcesses.pop_back();
      } else {
        (*found)->status = status->status;
        (*found)->exit_status = status->exit_status;
      }
    }
  }
  if (delete_me) {
    delete delete_me;
  }

  return *status;
}

ExternalProcessStatus KillExternalProcess(ExternalId pid, int signal,
                                          bool is_terminal) {
  SDB_DEBUG("xxxxx", sdb::Logger::FIXME, "Sending process: ", pid.pid,
            " the signal: ", signal);

  ExternalProcess* external = nullptr;
  {
    std::lock_guard guard{gExternalProcessesLock};

    for (auto it = gExternalProcesses.begin(); it != gExternalProcesses.end();
         ++it) {
      if ((*it)->pid == pid.pid) {
        external = (*it);
        break;
      }
    }
  }

  bool is_child = (external != nullptr);
  if (!is_child) {
    external = GetExternalProcess(pid.pid);
    if (external == nullptr) {
      SDB_DEBUG("xxxxx", sdb::Logger::FIXME,
                "kill: process not found: ", pid.pid,
                " in our starting table and it doesn't exist.");
      ExternalProcessStatus status;
      status.status = kExtNotFound;
      status.exit_status = -1;
      return status;
    }
    SDB_DEBUG("xxxxx", sdb::Logger::FIXME, "kill: process not found: ", pid.pid,
              " in our starting table - adding");

    // ok, we didn't spawn it, but now we claim the
    // ownership.
    std::lock_guard guard{gExternalProcessesLock};

    try {
      gExternalProcesses.push_back(external);
    } catch (...) {
      delete external;

      ExternalProcessStatus status;
      status.status = kExtNotFound;
      status.exit_status = -1;
      return status;
    }
  }

  SDB_ASSERT(external != nullptr);
  if (KillProcess(external, signal)) {
    external->status = kExtStopped;
    // if the process wasn't spawned by us, no waiting required.
    int count = 0;
    while (true) {
      ExternalProcessStatus status =
        CheckExternalProcess(pid, false, 0, NoDeadLine);
      if (!is_terminal) {
        // we just sent a signal, don't care whether
        // the process is gone by now.
        return status;
      }
      if ((status.status == kExtTerminated) || (status.status == kExtAborted) ||
          (status.status == kExtNotFound)) {
        // Its dead and gone - good.
        std::lock_guard guard{gExternalProcessesLock};
        for (auto it = gExternalProcesses.begin();
             it != gExternalProcesses.end(); ++it) {
          if (*it == external) {
            std::swap(*it, gExternalProcesses.back());
            gExternalProcesses.pop_back();
            break;
          }
        }
        if (!is_child && (status.status == kExtNotFound)) {
          status.status = kExtTerminated;
          status.error_message.clear();
        }
        return status;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
      if (count >= 13) {
        SDB_ASSERT(external != nullptr);
        SDB_WARN("xxxxx", sdb::Logger::FIXME,
                 "about to send SIGKILL signal to process: ", external->pid,
                 ", status: ", (int)status.status);
        KillProcess(external, SIGKILL);
      }
      if (count > 25) {
        return status;
      }
      count++;
    }
  }
  return CheckExternalProcess(pid, false, 0, NoDeadLine);
}

bool SuspendExternalProcess(ExternalId pid) {
  SDB_DEBUG("xxxxx", sdb::Logger::FIXME, "suspending process: ", pid.pid);

  return 0 == kill(pid.pid, SIGSTOP);
}

bool ContinueExternalProcess(ExternalId pid) {
  SDB_DEBUG("xxxxx", sdb::Logger::FIXME, "continueing process: ", pid.pid);

  return 0 == kill(pid.pid, SIGCONT);
}

void ShutdownProcess() {
  std::lock_guard guard{gExternalProcessesLock};
  for (auto* external : gExternalProcesses) {
    delete external;
  }
  gExternalProcesses.clear();
}

std::string SetPriority(ExternalId pid, int prio) {
#ifdef SDB_DEV
  errno = 0;
  int ret = setpriority(PRIO_PROCESS, pid.pid, prio);
  if (ret == -1) {
    int err = errno;
    if (err != 0) {
      std::string ss_str;
      absl::strings_internal::OStringStream ss{&ss_str};
      ss << "setting process priority for : '" << pid.pid
         << "' failed with error: " << strerror(err);
      return ss_str;
    }
  }
  return "";
#else
  return "only available in maintainer mode";
#endif
}

}  // namespace sdb
