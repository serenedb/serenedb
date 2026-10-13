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
////////////////////////////////////////////////////////////////////////////////

#include "server/utils/lockfile.h"

#include <absl/cleanup/cleanup.h>
#include <absl/status/status.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/operating_system.hpp>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

namespace sdb {
namespace {

// Pid lockfiles currently held by this process; cleared by ~LockfileRemover
// on normal exit so we don't leave stale `.lock` files behind.
std::vector<std::pair<std::string, int>> gOpenedFiles;
constinit absl::Mutex gOpenedFilesLock{absl::kConstInit};

struct LockfileRemover {
  ~LockfileRemover() {
    absl::WriterMutexLock locker{&gOpenedFilesLock};
    for (const auto& it : gOpenedFiles) {
      SERENEDB_CLOSE(it.second);
      std::error_code ec;
      std::filesystem::remove(it.first, ec);
    }
    gOpenedFiles.clear();
  }
};
LockfileRemover gRemover;

}  // namespace

absl::Status CreateLockFile(const char* filename) {
  absl::WriterMutexLock locker{&gOpenedFilesLock};

  for (const auto& f : gOpenedFiles) {
    if (f.first == filename) {
      return absl::OkStatus();
    }
  }

  int fd =
    SERENEDB_CREATE(filename, O_CREAT | O_EXCL | O_RDWR | SERENEDB_O_CLOEXEC,
                    S_IRUSR | S_IWUSR);
  if (fd == -1) {
    return absl::InternalError(absl::StrCat("cannot create lockfile '",
                                            filename, "': ", strerror(errno)));
  }

  std::string buf = absl::StrCat(getpid());
  if (SERENEDB_WRITE(fd, buf.c_str(), buf.size()) == -1) {
    auto status = absl::InternalError(absl::StrCat(
      "cannot write pid into lockfile '", filename, "': ", strerror(errno)));
    SERENEDB_CLOSE(fd);
    std::error_code ec;
    std::filesystem::remove(filename, ec);
    return status;
  }

  struct flock lock{};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  if (fcntl(fd, F_SETLK, &lock) == -1) {
    auto status = absl::InternalError(
      absl::StrCat("cannot lock lockfile '", filename, "': ", strerror(errno)));
    SERENEDB_CLOSE(fd);
    std::error_code ec;
    std::filesystem::remove(filename, ec);
    return status;
  }

  gOpenedFiles.emplace_back(filename, fd);
  return absl::OkStatus();
}

absl::Status VerifyLockFile(const char* filename) {
  std::error_code ec;
  if (!std::filesystem::exists(filename, ec) || ec) {
    return absl::OkStatus();
  }

  int fd = SERENEDB_OPEN(filename, O_RDWR | SERENEDB_O_CLOEXEC);
  if (fd < 0) {
    return absl::InternalError(
      absl::StrCat("cannot open lockfile '", filename,
                   "' in write mode: ", strerror(errno)));
  }
  absl::Cleanup sg = [&]() noexcept { SERENEDB_CLOSE(fd); };

  char buffer[128];
  memset(buffer, 0, sizeof(buffer));
  ssize_t n = SERENEDB_READ(fd, buffer, sizeof(buffer));

  if (n <= 0 || n == sizeof(buffer)) {
    return absl::OkStatus();
  }

  uint32_t fc = 0;
  if (!absl::SimpleAtoi(std::string_view{buffer, static_cast<size_t>(n)},
                        &fc) ||
      fc == 0) {
    return absl::OkStatus();
  }

  pid_t pid = fc;
  if (kill(pid, 0) == -1) {
    SDB_DEBUG(GENERAL, "found existing lockfile '", filename,
              "' of previous process with pid ", pid,
              ", but that process seems to be dead already");
  } else {
    SDB_DEBUG(GENERAL, "found existing lockfile '", filename,
              "' of previous process with pid ", pid,
              ", and that process seems to be still running");
  }

  struct flock lock{};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  int can_lock = fcntl(fd, F_SETLK, &lock);

  if (can_lock == 0) {
    if (fcntl(fd, F_GETLK, &lock) != 0) {
      SDB_WARN(GENERAL, "fcntl on lockfile '", filename,
               "' failed: ", strerror(errno));
    }
    return absl::OkStatus();
  }

  can_lock = errno;
  if (can_lock == EACCES || can_lock == EAGAIN) {
    return absl::FailedPreconditionError(
      absl::StrCat("database is locked by process ", pid,
                   "; please stop it first and check that the lockfile '",
                   filename, "' goes away."));
  }
  return absl::InternalError(absl::StrCat(
    "fcntl on lockfile '", filename, "' failed: ", strerror(can_lock),
    ". a possible reason is that the filesystem does not support "
    "file-locking"));
}

}  // namespace sdb
