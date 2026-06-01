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

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <memory>
#include <utility>

#include "basics/operating-system.h"
#include "basics/strings.h"

#ifdef SERENEDB_HAVE_DIRENT_H
#include <dirent.h>
#endif

#ifdef SERENEDB_HAVE_SIGNAL_H
#include <signal.h>
#endif

#ifdef SERENEDB_HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <absl/crc/crc32c.h>
#include <openssl/evp.h>
#include <unistd.h>
#include <zlib.h>

#include "basics/debugging.h"
#include "basics/directories.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/file_utils.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"
#include "files.h"

namespace sdb {
namespace {

// names of pid lockfiles currently held by SdbCreateLockFile.
std::vector<std::pair<std::string, int>> gOpenedFiles;
constinit absl::Mutex gOpenedFilesLock{absl::kConstInit};

struct LockfileRemover {
  ~LockfileRemover() {
    absl::WriterMutexLock locker{&gOpenedFilesLock};
    for (const auto& it : gOpenedFiles) {
      SERENEDB_CLOSE(it.second);
      std::ignore = SdbUnlinkFile(it.first.c_str());
    }
    gOpenedFiles.clear();
  }
};
LockfileRemover gRemover;

bool IsDirectoryPath(const char* path) {
  struct stat stbuf;
  return SERENEDB_STAT(path, &stbuf) == 0 &&
         (stbuf.st_mode & S_IFMT) == S_IFDIR;
}

void ListTreeRecursively(const char* full, const char* path,
                         std::vector<std::string>& result) {
  std::vector<std::string> dirs = SdbFilesDirectory(full);

  for (size_t j = 0; j < 2; ++j) {
    for (const auto& filename : dirs) {
      const auto new_full = basics::file_utils::BuildFilename(full, filename);
      std::string new_path;

      if (*path) {
        new_path = basics::file_utils::BuildFilename(path, filename);
      } else {
        new_path = filename;
      }

      if (j == 0) {
        if (IsDirectoryPath(new_full.c_str())) {
          result.push_back(new_path);

          if (!SdbIsSymbolicLink(new_full.c_str())) {
            ListTreeRecursively(new_full.c_str(), new_path.c_str(), result);
          }
        }
      } else if (!IsDirectoryPath(new_full.c_str())) {
        result.push_back(new_path);
      }
    }
  }
}

// Read the SERENEDB_CONFIG_PATH override, normalising separators and
// ensuring a trailing one. SdbLocateConfigDirectory is the only caller.
std::string LocateConfigDirectoryEnv() {
  std::string r;
  if (!SdbGETENV("SERENEDB_CONFIG_PATH", r)) {
    return std::string();
  }
  // Coerce '/' to platform separator and trim trailing separators.
  for (auto& c : r) {
    if (c == '/' || c == SERENEDB_DIR_SEPARATOR_CHR) {
      c = SERENEDB_DIR_SEPARATOR_CHR;
    }
  }
  while (!r.empty() && r.back() == SERENEDB_DIR_SEPARATOR_CHR) {
    r.pop_back();
  }
  r.push_back(SERENEDB_DIR_SEPARATOR_CHR);
  return r;
}

bool CopyFileContents(int src_fd, int dst_fd, size_t file_size,
                      std::string& error) {
  SDB_ASSERT(file_size > 0);

  bool rc = true;

#ifdef __linux__
  // splice() requires Linux 2.6.17 / glibc 2.5; the project is glibc-only.
  int splice_pipe[2];
  ssize_t pipe_size = 0;
  long chunk_send_remain = file_size;
  loff_t total_sent_already = 0;

  if (pipe(splice_pipe) != 0) {
    error = std::string("splice failed to create pipes: ") + strerror(errno);
    return false;
  }
  try {
    while (chunk_send_remain > 0) {
      if (pipe_size == 0) {
        pipe_size = splice(src_fd, &total_sent_already, splice_pipe[1], nullptr,
                           chunk_send_remain, SPLICE_F_MOVE);
        if (pipe_size == -1) {
          error = std::string("splice read failed: ") + strerror(errno);
          rc = false;
          break;
        }
      }

      auto sent = splice(splice_pipe[0], nullptr, dst_fd, nullptr, pipe_size,
                         SPLICE_F_MORE | SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
      if (sent == -1) {
        error = absl::StrCat("splice write failed: ", strerror(errno));
        rc = false;
        break;
      }
      pipe_size -= sent;
      chunk_send_remain -= sent;
    }
  } catch (...) {
    rc = false;
  }
  close(splice_pipe[0]);
  close(splice_pipe[1]);

  return rc;
#else
  size_t buffer_size = std::min<size_t>(file_size, 128 * 1024);
  char* buf = new (std::nothrow) char[buffer_size];
  if (buf == nullptr) {
    error = "failed to allocate temporary buffer";
    return false;
  }
  try {
    size_t chunk_remain = file_size;
    while (rc && (chunk_remain > 0)) {
      auto read_chunk = std::min(buffer_size, chunk_remain);
      ssize_t n_read = SERENEDB_READ(src_fd, buf, read_chunk);
      if (n_read < 0) {
        error = absl::StrCat("failed to read a chunk: ", strerror(errno));
        rc = false;
        break;
      }
      if (n_read == 0) {
        break;
      }
      size_t write_offset = 0;
      size_t write_remaining = static_cast<size_t>(n_read);
      while (write_remaining > 0) {
        auto n_written = SERENEDB_WRITE(dst_fd, buf + write_offset,
                                        static_cast<size_t>(write_remaining));
        if (n_written < 0) {
          error = absl::StrCat("failed to write a chunk: ", strerror(errno));
          rc = false;
          break;
        }
        write_offset += static_cast<size_t>(n_written);
        write_remaining -= static_cast<size_t>(n_written);
      }
      chunk_remain -= n_read;
    }
  } catch (...) {
    rc = false;
  }
  delete[] buf;
  return rc;
#endif
}

pid_t CurrentProcessId() { return getpid(); }

}  // namespace

int64_t SdbSizeFile(const char* path) {
  struct stat stbuf;
  if (SERENEDB_STAT(path, &stbuf) != 0) {
    return -1;
  }
  return static_cast<int64_t>(stbuf.st_size);
}

bool SdbIsRegularFile(const char* path) {
  struct stat stbuf;
  return SERENEDB_STAT(path, &stbuf) == 0 &&
         (stbuf.st_mode & S_IFMT) == S_IFREG;
}

bool SdbIsSymbolicLink(const char* path) {
  struct stat stbuf;
  return lstat(path, &stbuf) == 0 && (stbuf.st_mode & S_IFMT) == S_IFLNK;
}

bool SdbExistsFile(const char* path) {
  if (path == nullptr) {
    return false;
  }
  struct stat stbuf;
  return SERENEDB_STAT(path, &stbuf) == 0;
}

ErrorCode SdbCreateRecursiveDirectory(std::string_view path, long& system_error,
                                      std::string& system_error_str) {
  auto res = ERROR_OK;
  std::string copy{path};
  const char* p = copy.data();
  const char* s = p;

  while (*p != '\0') {
    if (*p == SERENEDB_DIR_SEPARATOR_CHR) {
      if (p - s > 0) {
        copy[p - copy.data()] = '\0';
        res = SdbCreateDirectory(copy.c_str(), system_error, system_error_str);
        if (res == ERROR_FILE_EXISTS || res == ERROR_OK) {
          system_error_str.clear();
          res = ERROR_OK;
          copy[p - copy.data()] = SERENEDB_DIR_SEPARATOR_CHR;
          s = p + 1;
        } else {
          break;
        }
      }
    }
    p++;
  }

  if ((res == ERROR_FILE_EXISTS || res == ERROR_OK) && (p - s > 0)) {
    res = SdbCreateDirectory(copy.c_str(), system_error, system_error_str);
    if (res == ERROR_FILE_EXISTS) {
      system_error_str.clear();
      res = ERROR_OK;
    }
  }

  SDB_ASSERT(res != ERROR_FILE_EXISTS);
  return res;
}

ErrorCode SdbCreateDirectory(const char* path, long& system_error,
                             std::string& system_error_str) {
  SetError(ERROR_OK);

  int res = SERENEDB_MKDIR(path, 0777);
  if (res == 0) {
    return ERROR_OK;
  }

  res = errno;
  system_error_str = absl::StrCat("failed to create directory '", path,
                                  "': ", SERENEDB_ERRORNO_STR);
  system_error = res;
  if (res == ENOENT) {
    return ERROR_FILE_NOT_FOUND;
  }
  if (res == EEXIST) {
    return ERROR_FILE_EXISTS;
  }
  if (res == EPERM) {
    return ERROR_FORBIDDEN;
  }
  return ERROR_SYS_ERROR;
}

std::string_view SdbDirname(std::string_view path) {
  size_t n = path.size();

  if (n == 0) {
    return ".";
  }
  if (n > 1 && path[n - 1] == SERENEDB_DIR_SEPARATOR_CHR) {
    return path.substr(0, n - 1);
  }
  if (n == 1 && path[0] == SERENEDB_DIR_SEPARATOR_CHR) {
    return SERENEDB_DIR_SEPARATOR_STR;
  }
  if (n == 1 && path[0] == '.') {
    return ".";
  }
  if (n == 2 && path[0] == '.' && path[1] == '.') {
    return "..";
  }

  const char* p;
  for (p = path.data() + (n - 1); path.data() < p; --p) {
    if (*p == SERENEDB_DIR_SEPARATOR_CHR) {
      break;
    }
  }

  if (path.data() == p) {
    return *p == SERENEDB_DIR_SEPARATOR_CHR ? SERENEDB_DIR_SEPARATOR_STR : ".";
  }

  return path.substr(0, p - path.data());
}

std::vector<std::string> SdbFilesDirectory(const char* path) {
  std::vector<std::string> result;

  DIR* d = opendir(path);
  if (d == nullptr) {
    return result;
  }
  absl::Cleanup guard = [&d]() noexcept { closedir(d); };

  for (struct dirent* de = readdir(d); de != nullptr; de = readdir(d)) {
    if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0) {
      result.emplace_back(de->d_name);
    }
  }
  return result;
}

std::vector<std::string> SdbFullTreeDirectory(const char* path) {
  std::vector<std::string> result;
  result.push_back("");
  ListTreeRecursively(path, "", result);
  return result;
}

ErrorCode SdbRenameFile(const char* old, const char* filename,
                        long* system_error, std::string* system_error_str) {
  int res = rename(old, filename);
  if (res != 0) {
    if (system_error != nullptr) {
      *system_error = errno;
    }
    if (system_error_str != nullptr) {
      *system_error_str = SERENEDB_ERRORNO_STR;
    }
    SetError(ERROR_SYS_ERROR);
    return ERROR_SYS_ERROR;
  }
  return ERROR_OK;
}

ErrorCode SdbUnlinkFile(const char* filename) {
  int res = SERENEDB_UNLINK(filename);
  if (res != 0) {
    int e = errno;
    SetError(ERROR_SYS_ERROR);
    SDB_TRACE(GENERAL, "cannot unlink file '", filename,
              "': ", SERENEDB_ERRORNO_STR);
    if (e == ENOENT) {
      return ERROR_FILE_NOT_FOUND;
    }
    if (e == EPERM) {
      return ERROR_FORBIDDEN;
    }
    return ERROR_SYS_ERROR;
  }
  return ERROR_OK;
}

bool Sdbfsync(int fd) {
  if (fsync(fd) == 0) {
    return true;
  }
  SetError(ERROR_SYS_ERROR);
  return false;
}

bool SdbSlurpFile(const char* filename, std::string& result) {
  SetError(ERROR_OK);
  const int fd = SERENEDB_OPEN(filename, O_RDONLY | SERENEDB_O_CLOEXEC);

  if (fd == -1) {
    SetError(ERROR_SYS_ERROR);
    return false;
  }

  absl::Cleanup guard = [&]() noexcept { SERENEDB_CLOSE(fd); };

  auto true_size = result.size();
  while (true) {
    basics::StrResizeAmortized(result, true_size + kReadBufferSize);
    auto n = SERENEDB_READ(fd, result.data() + true_size, kReadBufferSize);
    if (n == 0) {
      result.erase(true_size);
      return true;
    }
    if (n < 0) {
      result = {};
      SetError(ERROR_SYS_ERROR);
      return false;
    }
    true_size += n;
  }
}

bool SdbSlurpGzipFile(const char* filename, std::string& result) {
  SetError(ERROR_OK);
  gzFile gz_fd = gzopen(filename, "rb");
  if (!gz_fd) {
    SetError(ERROR_SYS_ERROR);
    return false;
  }
  absl::Cleanup fd_guard = [&gz_fd]() noexcept { gzclose(gz_fd); };

  auto true_size = result.size();
  while (true) {
    basics::StrResizeAmortized(result, true_size + kReadBufferSize);
    const auto n = gzread(gz_fd, result.data() + true_size, kReadBufferSize);
    if (n == 0) {
      result.erase(true_size);
      return true;
    }
    if (n < 0) {
      result = {};
      SetError(ERROR_SYS_ERROR);
      return false;
    }
    true_size += n;
  }
}

ErrorCode SdbCreateLockFile(const char* filename) {
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

  std::string buf = std::to_string(CurrentProcessId());
  int rv = SERENEDB_WRITE(fd, buf.c_str(), buf.size());
  if (rv == -1) {
    SetError(ERROR_SYS_ERROR);
    SERENEDB_CLOSE(fd);
    SERENEDB_UNLINK(filename);
    return ERROR_SYS_ERROR;
  }

  struct flock lock{};
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  if (fcntl(fd, F_SETLK, &lock) == -1) {
    SetError(ERROR_SYS_ERROR);
    SERENEDB_CLOSE(fd);
    SERENEDB_UNLINK(filename);
    return ERROR_SYS_ERROR;
  }

  gOpenedFiles.emplace_back(filename, fd);
  return ERROR_OK;
}

ErrorCode SdbVerifyLockFile(const char* filename) {
  if (!SdbExistsFile(filename)) {
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

ErrorCode SdbDestroyLockFile(const char* filename) {
  absl::WriterMutexLock locker{&gOpenedFilesLock};
  for (size_t i = 0; i < gOpenedFiles.size(); ++i) {
    if (gOpenedFiles[i].first != filename) {
      continue;
    }
    int fd = SERENEDB_OPEN(filename, O_RDWR | SERENEDB_O_CLOEXEC);
    if (fd < 0) {
      return ERROR_OK;
    }
    struct flock lock{};
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    auto res = ERROR_OK;
    if (fcntl(fd, F_SETLK, &lock) != 0) {
      res = ERROR_SYS_ERROR;
      SetError(res);
    }
    SERENEDB_CLOSE(fd);

    if (res == ERROR_OK) {
      std::ignore = SdbUnlinkFile(filename);
    }

    SERENEDB_CLOSE(gOpenedFiles[i].second);
    gOpenedFiles.erase(gOpenedFiles.begin() + i);
    return res;
  }
  return ERROR_OK;
}

std::string SdbGetAbsolutePath(std::string_view file_name,
                               std::string_view current_working_directory) {
  if (file_name.empty()) {
    return {};
  }

  // name is absolute if starts with either forward or backslash
  // file is also absolute if contains a colon
  bool is_absolute = (file_name[0] == '/' || file_name[0] == '\\' ||
                      file_name.find(':') != std::string::npos);
  if (is_absolute) {
    return std::string{file_name};
  }

  std::string result;
  if (!current_working_directory.empty()) {
    result.reserve(current_working_directory.size() + file_name.size() + 1);
    result.append(current_working_directory);
    if (current_working_directory.back() != '/') {
      result.push_back('/');
    }
    result.append(file_name);
  }
  return result;
}

std::string_view SdbHomeDirectory() {
  const char* result = getenv("HOME");
  return result == nullptr ? "." : result;
}

bool SdbCopyFile(const char* src, const char* dst, std::string& error,
                 struct stat* statbuf /*= nullptr*/) {
  int src_fd = open(src, O_RDONLY);
  if (src_fd < 0) {
    error =
      absl::StrCat("failed to open source file ", src, ": ", strerror(errno));
    return false;
  }
  int dst_fd =
    open(dst, O_EXCL | O_CREAT | O_NONBLOCK | O_WRONLY, S_IRUSR | S_IWUSR);
  if (dst_fd < 0) {
    close(src_fd);
    error = absl::StrCat("failed to open destination file ", dst, ": ",
                         strerror(errno));
    return false;
  }

  bool rc = true;
  try {
    struct stat local_stat;
    if (statbuf == nullptr) {
      SERENEDB_FSTAT(src_fd, &local_stat);
      statbuf = &local_stat;
    }
    SDB_ASSERT(statbuf != nullptr);

    size_t dsize = statbuf->st_size;
    if (dsize > 0) {
      rc = CopyFileContents(src_fd, dst_fd, dsize, error);
    }
    timeval times[2]{};
    times[0].tv_sec = SERENEDB_STAT_ATIME_SEC(*statbuf);
    times[1].tv_sec = SERENEDB_STAT_MTIME_SEC(*statbuf);

    if (fchown(dst_fd, -1 /*statbuf.st_uid*/, statbuf->st_gid) != 0) {
      error = absl::StrCat("failed to chown ", dst, ": ", strerror(errno));
      // not fatal
    }
    if (fchmod(dst_fd, statbuf->st_mode) != 0) {
      error = absl::StrCat("failed to chmod ", dst, ": ", strerror(errno));
      rc = false;
    }
    if (utimes(dst, times) != 0) {
      error =
        absl::StrCat("failed to adjust age: ", dst, ": ", strerror(errno));
      rc = false;
    }
  } catch (...) {
    rc = false;
  }

  close(src_fd);
  close(dst_fd);
  return rc;
}

bool SdbCopyAttributes(const char* src_item, const char* dst_item,
                       std::string& error) {
  struct stat statbuf;
  SERENEDB_STAT(src_item, &statbuf);

  if (chown(dst_item, -1 /*statbuf.st_uid*/, statbuf.st_gid) != 0) {
    error = absl::StrCat("failed to chown ", dst_item, ": ", strerror(errno));
  }
  if (chmod(dst_item, statbuf.st_mode) != 0) {
    error = absl::StrCat("failed to chmod ", dst_item, ": ", strerror(errno));
    return false;
  }

  timeval times[2]{};
  times[0].tv_sec = SERENEDB_STAT_ATIME_SEC(statbuf);
  times[1].tv_sec = SERENEDB_STAT_MTIME_SEC(statbuf);
  if (utimes(dst_item, times) != 0) {
    error =
      absl::StrCat("failed to adjust age: ", dst_item, ": ", strerror(errno));
    return false;
  }
  return true;
}

bool SdbCopySymlink(const char* src_item, const char* dst_item,
                    std::string& error) {
  char buffer[PATH_MAX];
  auto rc = readlink(src_item, buffer, sizeof(buffer) - 1);
  if (rc == -1) {
    error =
      absl::StrCat("failed to read symlink ", src_item, ": ", strerror(errno));
    return false;
  }
  buffer[rc] = '\0';
  if (symlink(buffer, dst_item) != 0) {
    error = absl::StrCat("failed to create symlink ", dst_item, " -> ", buffer,
                         ": ", strerror(errno));
    return false;
  }
  return true;
}

bool SdbCreateHardlink(const char* existing_file, const char* new_file,
                       std::string& error) {
  int rc = link(existing_file, new_file);
  if (rc == -1) {
    error = absl::StrCat("failed to create hard link ", new_file, ": ",
                         strerror(errno));
  }
  return rc == 0;
}

#if defined(_SYSCONFDIR_)
std::string SdbLocateConfigDirectory(const char* /*binary_path*/) {
  std::string v = LocateConfigDirectoryEnv();
  if (!v.empty()) {
    return v;
  }
  const char* dir = _SYSCONFDIR_;
  if (*dir == '\0') {
    return std::string();
  }
  size_t len = strlen(dir);
  if (dir[len - 1] != SERENEDB_DIR_SEPARATOR_CHR) {
    return std::string(dir) + "/";
  }
  return std::string(dir);
}
#else
std::string SdbLocateConfigDirectory(const char*) {
  return LocateConfigDirectoryEnv();
}
#endif

Result SdbGetDiskSpaceInfo(const char* path, uint64_t& total_space,
                           uint64_t& free_space) {
  struct statvfs stat;
  if (statvfs(path, &stat) == -1) {
    SetError(ERROR_SYS_ERROR);
    return {GetError(), LastError()};
  }

  const auto factor = static_cast<uint64_t>(stat.f_bsize);
  total_space = factor * static_cast<uint64_t>(stat.f_blocks);

  if (geteuid()) {
    free_space = factor * static_cast<uint64_t>(stat.f_bavail);
  } else {
    free_space = factor * static_cast<uint64_t>(stat.f_bfree);
  }
  return {};
}

Result SdbGetINodesInfo(const char* path, uint64_t& total_i_nodes,
                        uint64_t& free_i_nodes) {
  struct statvfs stat;
  if (statvfs(path, &stat) == -1) {
    SetError(ERROR_SYS_ERROR);
    return {GetError(), LastError()};
  }
  total_i_nodes = static_cast<uint64_t>(stat.f_files);
  free_i_nodes = static_cast<uint64_t>(stat.f_ffree);
  return {};
}

bool SdbGETENV(const char* which, std::string& value) {
  const char* v = getenv(which);
  if (v == nullptr) {
    return false;
  }
  value = v;
  return true;
}

Sha256Functor::Sha256Functor()
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  : _context(EVP_MD_CTX_new()) {
#else
  : _context(EVP_MD_CTX_create()) {
#endif
  auto* context = static_cast<EVP_MD_CTX*>(_context);
  if (context == nullptr) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }
  if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 0) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    EVP_MD_CTX_free(context);
#else
    EVP_MD_CTX_destroy(_context);
#endif
    SDB_THROW(ERROR_INTERNAL, "unable to initialize SHA256 processor");
  }
}

Sha256Functor::~Sha256Functor() {
  auto* context = static_cast<EVP_MD_CTX*>(_context);
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
  EVP_MD_CTX_free(context);
#else
  EVP_MD_CTX_destroy(context);
#endif
}

bool Sha256Functor::operator()(const char* data, size_t size) noexcept {
  auto* context = static_cast<EVP_MD_CTX*>(_context);
  return EVP_DigestUpdate(context, static_cast<const void*>(data), size) == 1;
}

std::string Sha256Functor::finalize() {
  unsigned char hash[EVP_MAX_MD_SIZE];
  unsigned int length_of_hash = 0;
  auto* context = static_cast<EVP_MD_CTX*>(_context);
  if (EVP_DigestFinal_ex(context, hash, &length_of_hash) == 0) {
    SDB_ASSERT(false);
  }
  return basics::string_utils::EncodeHex(
    reinterpret_cast<const char*>(&hash[0]), length_of_hash);
}

ErrorCode SdbCrc32File(const char* path, uint32_t* crc) {
  FILE* fin = SERENEDB_FOPEN(path, "rb");
  if (fin == nullptr) {
    return ERROR_FILE_NOT_FOUND;
  }

  char buffer[4096];
  auto res = ERROR_OK;
  *crc = 0;
  while (true) {
    size_t size_read = fread(buffer, 1, sizeof(buffer), fin);
    if (size_read < sizeof(buffer)) {
      if (feof(fin) == 0) {
        res = ERROR_FAILED;
        break;
      }
    }
    if (size_read > 0) {
      *crc = static_cast<uint32_t>(absl::ExtendCrc32c(
        absl::crc32c_t{*crc}, std::string_view{buffer, size_read}));
    } else {
      break;
    }
  }

  if (fclose(fin) != 0) {
    res = ERROR_SYS_ERROR;
    SetError(res);
  }
  return res;
}

}  // namespace sdb
