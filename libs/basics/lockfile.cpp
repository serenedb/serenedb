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

#include "basics/lockfile.h"

#include <absl/cleanup/cleanup.h>
#include <absl/synchronization/mutex.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/error.h"
#include "basics/errors.h"
#include "basics/log.h"
#include "basics/operating-system.h"
#include "basics/string_utils.h"

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

ErrorCode CreateLockFile(const char* filename) {
  absl::WriterMutexLock locker{&gOpenedFilesLock};

  for (const auto& f : gOpenedFiles) {
    if (f.first == filename) {
      return ERROR_OK;
    }
  }

  int fd =
    SERENEDB_CREATE(filename, O_CREAT | O_EXCL | O_RDWR | SERENEDB_O_CLOEXEC,
                    S_IRUSR | S_IWUSR);
  if (fd == -1) {
    SetError(ERROR_SYS_ERROR);
    return ERROR_SYS_ERROR;
  }

  std::string buf = absl::StrCat(getpid());
  if (SERENEDB_WRITE(fd, buf.c_str(), buf.size()) == -1) {
    SetError(ERROR_SYS_ERROR);
    SERENEDB_CLOSE(fd);
    std::error_code ec;
    std::filesystem::remove(filename, ec);
    return ERROR_SYS_ERROR;
  }

  struct flock lock{};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  if (fcntl(fd, F_SETLK, &lock) == -1) {
    SetError(ERROR_SYS_ERROR);
    SERENEDB_CLOSE(fd);
    std::error_code ec;
    std::filesystem::remove(filename, ec);
    return ERROR_SYS_ERROR;
  }

  gOpenedFiles.emplace_back(filename, fd);
  return ERROR_OK;
}

ErrorCode VerifyLockFile(const char* filename) {
  std::error_code ec;
  if (!std::filesystem::exists(filename, ec) || ec) {
    return ERROR_OK;
  }

  int fd = SERENEDB_OPEN(filename, O_RDWR | SERENEDB_O_CLOEXEC);
  if (fd < 0) {
    SetError(ERROR_SYS_ERROR);
    SDB_WARN(GENERAL, "cannot open lockfile '", filename,
             "' in write mode: ", LastError());
    if (errno == EACCES) {
      return ERROR_CANNOT_WRITE_FILE;
    }
    return ERROR_INTERNAL;
  }
  absl::Cleanup sg = [&]() noexcept { SERENEDB_CLOSE(fd); };

  char buffer[128];
  memset(buffer, 0, sizeof(buffer));
  ssize_t n = SERENEDB_READ(fd, buffer, sizeof(buffer));

  if (n <= 0 || n == sizeof(buffer)) {
    return ERROR_OK;
  }

  uint32_t fc = basics::string_utils::Uint32(buffer, n);
  if (fc == 0) {
    return ERROR_OK;
  }

  pid_t pid = fc;
  if (kill(pid, 0) == -1) {
    SDB_WARN(GENERAL, "found existing lockfile '", filename,
             "' of previous process with pid ", pid,
             ", but that process seems to be dead already");
  } else {
    SDB_WARN(GENERAL, "found existing lockfile '", filename,
             "' of previous process with pid ", pid,
             ", and that process seems to be still running");
  }

  struct flock lock{};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  int can_lock = fcntl(fd, F_SETLK, &lock);

  if (can_lock == 0) {
    if (fcntl(fd, F_GETLK, &lock) != 0) {
      SetError(ERROR_SYS_ERROR);
      SDB_WARN(GENERAL, "fcntl on lockfile '", filename,
               "' failed: ", LastError());
    }
    return ERROR_OK;
  }

  can_lock = errno;
  SetError(ERROR_SYS_ERROR);
  if (can_lock != EACCES && can_lock != EAGAIN) {
    SDB_WARN(GENERAL, "fcntl on lockfile '", filename,
             "' failed: ", LastError(),
             ". a possible reason is that the filesystem does not support "
             "file-locking");
  }
  return ERROR_SERVER_DATADIR_LOCKED;
}

}  // namespace sdb
