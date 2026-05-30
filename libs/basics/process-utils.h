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

#include <absl/synchronization/mutex.h>
#include <sys/types.h>

#include <functional>
#include <mutex>
#include <optional>
#include <span>
#include <string>

#define SERENEDB_INVALID_PROCESS_ID (-1)

namespace sdb {

struct ProcessInfo {
  uint64_t minor_page_faults = 0;
  uint64_t major_page_faults = 0;
  uint64_t user_time = 0;
  uint64_t system_time = 0;
  int64_t number_threads = 0;
  int64_t resident_size = 0;  // resident set size in number of bytes
  uint64_t virtual_size = 0;
  uint64_t sc_clk_tck = 0;
};

enum ExternalStatus {
  kExtNotStarted = 0,  // not yet started
  kExtPipeFailed = 1,  // pipe before start failed
  kExtForkFailed = 2,  // fork failed
  kExtRunning = 3,     // running
  kExtNotFound = 4,    // unknown pid
  kExtTerminated = 5,  // process has terminated normally
  kExtAborted = 6,     // process has terminated abnormally
  kExtStopped = 7,     // process has been stopped
  kExtTimeout = 9      // waiting for the process timed out
};

struct ExternalId {
  pid_t pid = 0;
  int read_pipe = -1;
  int write_pipe = -1;
};

struct ExternalProcess : public ExternalId {
  std::string executable;
  size_t number_arguments = 0;
  char** arguments = nullptr;

  ExternalStatus status = kExtNotStarted;
  int64_t exit_status = 0;

  ExternalProcess(const ExternalProcess& other) = delete;
  ExternalProcess& operator=(const ExternalProcess& other) = delete;

  ExternalProcess() = default;
  ~ExternalProcess();
};

struct ExternalProcessStatus {
  ExternalStatus status = kExtNotStarted;
  int64_t exit_status = 0;
  std::string error_message;
};

extern std::vector<ExternalProcess*> gExternalProcesses;
extern absl::Mutex gExternalProcessesLock;

ProcessInfo GetProcessInfoSelf();

ProcessInfo GetProcessInfo(pid_t pid);

ExternalProcess* LookupSpawnedProcess(pid_t pid);

std::optional<ExternalProcessStatus> LookupSpawnedProcessStatus(pid_t pid);

void SetProcessTitle(const char* title);

// starts an external process
// `fileForStdErr` is a file name, to which we will redirect stderr of the
// external process, if the string is non-empty.
void CreateExternalProcess(std::string_view executable,
                           std::span<const std::string_view> arguments,
                           std::span<const std::string_view> additional_env,
                           bool use_pipes, ExternalId* pid,
                           const char* file_for_std_err = nullptr);

void ClosePipe(ExternalProcess* process, bool read);

ssize_t ReadPipe(const ExternalProcess* process, char* buffer,
                 size_t buffer_size);

bool WritePipe(const ExternalProcess* process, const char* buffer,
               size_t buffer_size);

ExternalProcessStatus CheckExternalProcess(
  ExternalId pid, bool wait, uint32_t timeout,
  const std::function<bool()>& deadline_reached = {});

ExternalProcessStatus KillExternalProcess(ExternalId pid, int signal,
                                          bool is_terminal);

bool SuspendExternalProcess(ExternalId pid);

bool ContinueExternalProcess(ExternalId pid);

void ShutdownProcess();

std::string SetPriority(ExternalId pid, int prio);

inline bool NoDeadLine() { return false; }

}  // namespace sdb
