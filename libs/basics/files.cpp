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
#include <absl/strings/str_split.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <thread>
#include <type_traits>
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

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <absl/crc/crc32c.h>
#include <openssl/evp.h>
#include <zlib.h>

#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/directories.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/file_utils.h"
#include "basics/hashes.h"
#include "basics/logger/logger.h"
#include "basics/random/random_generator.h"
#include "basics/read_write_lock.h"
#include "basics/string_buffer.h"
#include "basics/string_utils.h"
#include "basics/thread.h"
#include "basics/threads.h"
#include "basics/write_locker.h"
#include "files.h"

namespace sdb {
namespace {

// whether or not we can use the splice system call on Linux
#ifdef __linux__
bool gCanUseSplice = true;
#endif

/// names of blocking files
std::vector<std::pair<std::string, int>> gOpenedFiles;

/// lock for protected access to vector OpenedFiles
constinit absl::Mutex gOpenedFilesLock{absl::kConstInit};

/// struct to remove all opened lockfiles on shutdown
struct LockfileRemover {
  ~LockfileRemover() {
    absl::WriterMutexLock locker{&gOpenedFilesLock};

    for (const auto& it : gOpenedFiles) {
      int fd = it.second;
      SERENEDB_CLOSE(fd);

      // TODO(mbkkt) why ignore?
      std::ignore = SdbUnlinkFile(it.first.c_str());
    }

    gOpenedFiles.clear();
  }
};

/// this instance will remove all lockfiles in its dtor
LockfileRemover gRemover;

bool IsSymbolicLink(const char* path, struct stat* stbuf) {
  int res = lstat(path, stbuf);

  return (res == 0) && ((stbuf->st_mode & S_IFMT) == S_IFLNK);
}

constexpr bool IsDirSeparatorChar(char c) {
  return c == SERENEDB_DIR_SEPARATOR_CHR || c == '/';
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
        if (SdbIsDirectory(new_full.c_str())) {
          result.push_back(new_path);

          if (!SdbIsSymbolicLink(new_full.c_str())) {
            ListTreeRecursively(new_full.c_str(), new_path.c_str(), result);
          }
        }
      } else if (!SdbIsDirectory(new_full.c_str())) {
        result.push_back(new_path);
      }
    }
  }
}

std::string LocateConfigDirectoryEnv() {
  std::string r;
  if (!SdbGETENV("SERENEDB_CONFIG_PATH", r)) {
    return std::string();
  }
  SdbNormalizePath(r);
  while (!r.empty() && IsDirSeparatorChar(r[r.size() - 1])) {
    r.pop_back();
  }

  r.push_back(SERENEDB_DIR_SEPARATOR_CHR);

  return r;
}

// set the application's name, should be called before the first call to
// SdbGetTempPath

std::string gApplicationName = "serenedb";

std::string GetTempPath() {
  std::string system = "";
  const char* v = getenv("TMPDIR");

  if (v == nullptr || *v == '\0') {
    system = "/tmp/";
  } else if (v[strlen(v) - 1] == '/') {
    system = v;
  } else {
    system = std::string(v) + "/";
  }
  return system;
}

ErrorCode MkDTemp(char* s, size_t /*bufferSize*/) {
  if (mkdtemp(s) != nullptr) {
    return ERROR_OK;
  }
  SetError(ERROR_SYS_ERROR);
  return ERROR_SYS_ERROR;
}

/// the actual temp path used
std::unique_ptr<char[]> gSystemTempPath;

/// user-defined temp path
std::string gUserTempPath;

class SystemTempPathSweeper {
  std::string _system_temp_path;

 public:
  ~SystemTempPathSweeper() {
    if (!_system_temp_path.empty()) {
      // delete directory iff directory is empty
      SERENEDB_RMDIR(_system_temp_path.c_str());
    }
  }

  void Init(const char* path) { _system_temp_path = path; }
};

SystemTempPathSweeper gSystemTempPathSweeperInstance;

bool CopyFileContents(int src_fd, int dst_fd, size_t file_size,
                      std::string& error) {
  SDB_ASSERT(file_size > 0);

  bool rc = true;

#ifdef __linux__
  if (gCanUseSplice) {
    // Linux-specific file-copying code based on splice()
    // The splice() system call first appeared in Linux 2.6.17; library support
    // was added to glibc in version 2.5. libmusl also has bindings for it. so
    // we simply assume it is there on Linux.
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
          pipe_size = splice(src_fd, &total_sent_already, splice_pipe[1],
                             nullptr, chunk_send_remain, SPLICE_F_MOVE);
          if (pipe_size == -1) {
            error = std::string("splice read failed: ") + strerror(errno);
            rc = false;
            break;
          }
        }

        auto sent = splice(splice_pipe[0], nullptr, dst_fd, nullptr, pipe_size,
                           SPLICE_F_MORE | SPLICE_F_MOVE | SPLICE_F_NONBLOCK);
        if (sent == -1) {
          auto e = errno;
          error = std::string("splice read failed: ") + strerror(e);
          if (e == EINVAL) {
            error +=
              ", please check if the target filesystem supports splicing, "
              "and if not, turn if off by restarting with option "
              "`--use-splice-syscall false`";
          }
          rc = false;
          break;
        }
        pipe_size -= sent;
        chunk_send_remain -= sent;
      }
    } catch (...) {
      // make sure we always close the pipes
      rc = false;
    }
    close(splice_pipe[0]);
    close(splice_pipe[1]);

    return rc;
  }
#endif

  // systems other than Linux use regular file-copying.
  // note: regular file copying will also be used on Linux
  // if we cannot use the splice() system call

  size_t buffer_size = std::min<size_t>(file_size, 128 * 1024);
  char* buf = new (std::nothrow) char[buffer_size];

  if (buf == nullptr) {
    error = "failed to allocate temporary buffer";
    return false;
  }

  try {
    size_t chunk_remain = file_size;
    while (rc && (chunk_remain > 0)) {
      auto read_chunk = static_cast<size_t>(
        std::min(buffer_size, static_cast<size_t>(chunk_remain)));
      ssize_t n_read = SERENEDB_READ(src_fd, buf, read_chunk);

      if (n_read < 0) {
        error = absl::StrCat("failed to read a chunk: ", strerror(errno));
        rc = false;
        break;
      }

      if (n_read == 0) {
        // EOF. done
        break;
      }

      size_t write_offset = 0;
      size_t write_remaining = static_cast<size_t>(n_read);
      while (write_remaining > 0) {
        // write can write less data than requested. so we must go on writing
        // until we have written out all data
        auto n_written = SERENEDB_WRITE(dst_fd, buf + write_offset,
                                        static_cast<size_t>(write_remaining));

        if (n_written < 0) {
          // error during write
          error = absl::StrCat("failed to read a chunk: ", strerror(errno));
          rc = false;
          break;
        }

        write_offset += static_cast<size_t>(n_written);
        write_remaining -= static_cast<size_t>(n_written);
      }

      chunk_remain -= n_read;
    }
  } catch (...) {
    // make sure we always close the buffer
    rc = false;
  }

  delete[] buf;
  return rc;
}

}  // namespace

void SdbNormalizePath(std::string& path) {
  for (auto& it : path) {
    if (IsDirSeparatorChar(it)) {
      it = SERENEDB_DIR_SEPARATOR_CHR;
    }
  }
}

#ifdef __linux__
void SdbSetCanUseSplice(bool value) noexcept { gCanUseSplice = value; }
#endif

// Will return a negative error number on error, typically -1
int64_t SdbSizeFile(const char* path) {
  struct stat stbuf;
  int res = SERENEDB_STAT(path, &stbuf);

  if (res != 0) {
    // an error occurred
    return (int64_t)res;
  }

  return (int64_t)stbuf.st_size;
}

bool SdbIsWritable(const char* path) {
  // we can use POSIX access() from unistd.h to check for write permissions
  return (access(path, W_OK) == 0);
}

bool SdbIsDirectory(const char* path) {
  struct stat stbuf;
  int res;

  res = SERENEDB_STAT(path, &stbuf);

  return (res == 0) && ((stbuf.st_mode & S_IFMT) == S_IFDIR);
}

bool SdbIsRegularFile(const char* path) {
  struct stat stbuf;
  int res;

  res = SERENEDB_STAT(path, &stbuf);

  return (res == 0) && ((stbuf.st_mode & S_IFMT) == S_IFREG);
}

bool SdbIsSymbolicLink(const char* path) {
  struct stat stbuf;
  int res;

  res = lstat(path, &stbuf);

  return (res == 0) && ((stbuf.st_mode & S_IFMT) == S_IFLNK);
}

bool SdbCreateSymbolicLink(const char* target, const char* linkpath,
                           std::string& error) {
  int res = symlink(target, linkpath);

  if (res < 0) {
    error = absl::StrCat("failed to create a symlink ", target, " -> ",
                         linkpath, " - ", strerror(errno));
  }
  return res == 0;
}

std::string SdbResolveSymbolicLink(std::string path, bool& had_error,
                                   bool recursive) {
  struct stat sb;
  while (IsSymbolicLink(path.c_str(), &sb)) {
    // if file is a symlink this contains the targets file name length
    // instead of the file size
    auto buffsize = sb.st_size + 1;

    // resolve symlinks
    std::vector<char> buff;
    buff.resize(buffsize);
    auto written = ::readlink(path.c_str(), buff.data(), buff.size());

    if (written) {
      path = std::string(buff.data(), buff.size());
    } else {
      // error occured while resolving
      had_error = true;
      break;
    }
    if (!recursive) {
      break;
    }
  }
  return path;
}

std::string SdbResolveSymbolicLink(std::string path, bool recursive) {
  bool ignore;
  return SdbResolveSymbolicLink(std::move(path), ignore, recursive);
}

bool SdbExistsFile(const char* path) {
  if (path == nullptr) {
    return false;
  }

  struct stat stbuf;
  int res = SERENEDB_STAT(path, &stbuf);

  return res == 0;
}

ErrorCode SdbChMod(const char* path, long mode, std::string& err) {
  int res = chmod(path, mode);

  if (res != 0) {
    auto res2 = ERROR_SYS_ERROR;
    SetError(res2);
    err = absl::StrCat("error setting desired mode ", mode, " for file ", path,
                       ": ", LastError());
    return res2;
  }

  return ERROR_OK;
}

ErrorCode SdbMTimeFile(const char* path, int64_t* mtime) {
  struct stat stbuf;
  int res = SERENEDB_STAT(path, &stbuf);

  if (res == 0) {
    *mtime = static_cast<int64_t>(stbuf.st_mtime);
    return ERROR_OK;
  }

  res = errno;
  if (res == ENOENT) {
    return ERROR_FILE_NOT_FOUND;
  }

  SetError(ERROR_SYS_ERROR);
  return GetError();
}

ErrorCode SdbCreateRecursiveDirectory(std::string_view path, long& system_error,
                                      std::string& system_error_str) {
  const char* p;
  const char* s;

  auto res = ERROR_OK;
  std::string copy{path};
  p = s = copy.data();

  while (*p != '\0') {
    if (*p == SERENEDB_DIR_SEPARATOR_CHR) {
      if (p - s > 0) {
        copy[p - copy.data()] = '\0';
        res = SdbCreateDirectory(copy.c_str(), system_error, system_error_str);

        if (res == ERROR_FILE_EXISTS || res == ERROR_OK) {
          system_error_str.clear();
          res = ERROR_OK;
          // *p = SERENEDB_DIR_SEPARATOR_CHR;
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
  // reset error flag
  SetError(ERROR_OK);

  int res = SERENEDB_MKDIR(path, 0777);

  if (res == 0) {
    return ERROR_OK;
  }

  // check errno
  res = errno;
  if (res != 0) {
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
  }

  return ERROR_SYS_ERROR;
}

ErrorCode SdbRemoveEmptyDirectory(const char* filename) {
  int res = SERENEDB_RMDIR(filename);

  if (res != 0) {
    SDB_TRACE(GENERAL, "cannot remove directory '", filename,
              "': ", SERENEDB_ERRORNO_STR);
    SetError(ERROR_SYS_ERROR);
    return ERROR_SYS_ERROR;
  }

  return ERROR_OK;
}

ErrorCode SdbRemoveDirectory(const char* filename) {
  if (SdbIsSymbolicLink(filename)) {
    SDB_TRACE(GENERAL, "removing symbolic link '", filename,
              "'");
    return SdbUnlinkFile(filename);
  } else if (SdbIsDirectory(filename)) {
    SDB_TRACE(GENERAL, "removing directory '", filename, "'");

    auto res = ERROR_OK;
    std::vector<std::string> files = SdbFilesDirectory(filename);
    for (const auto& dir : files) {
      std::string full = basics::file_utils::BuildFilename(filename, dir);

      auto subres = SdbRemoveDirectory(full.c_str());

      if (subres != ERROR_OK) {
        res = subres;
      }
    }

    if (res == ERROR_OK) {
      res = SdbRemoveEmptyDirectory(filename);
    }

    return res;
  } else if (SdbExistsFile(filename)) {
    SDB_TRACE(GENERAL, "removing file '", filename, "'");

    return SdbUnlinkFile(filename);
  } else {
    SDB_TRACE(GENERAL,
              "attempt to remove non-existing file/directory '", filename, "'");

    // TODO: why do we actually return "no error" here?
    return ERROR_OK;
  }
}

std::string_view SdbDirname(std::string_view path) {
  size_t n = path.size();

  if (n == 0) {
    // "" => "."
    return ".";
  }

  if (n > 1 && path[n - 1] == SERENEDB_DIR_SEPARATOR_CHR) {
    // .../ => ...
    return path.substr(0, n - 1);
  }

  if (n == 1 && path[0] == SERENEDB_DIR_SEPARATOR_CHR) {
    // "/" => "/"
    return SERENEDB_DIR_SEPARATOR_STR;
  } else if (n == 1 && path[0] == '.') {
    return ".";
  } else if (n == 2 && path[0] == '.' && path[1] == '.') {
    return "..";
  }

  const char* p;
  for (p = path.data() + (n - 1); path.data() < p; --p) {
    if (*p == SERENEDB_DIR_SEPARATOR_CHR) {
      break;
    }
  }

  if (path.data() == p) {
    if (*p == SERENEDB_DIR_SEPARATOR_CHR) {
      return SERENEDB_DIR_SEPARATOR_STR;
    } else {
      return ".";
    }
  }

  n = p - path.data();

  return path.substr(0, n);
}

std::string_view SdbBasename(std::string_view s) {
  auto path = s.data();
  auto n = s.size();

  if (1 < n) {
    if (IsDirSeparatorChar(path[n - 1])) {
      n -= 1;
    }
  }

  if (n == 0) {
    return "";
  }
  if (n == 1) {
    if (IsDirSeparatorChar(*path)) {
      return SERENEDB_DIR_SEPARATOR_STR;
    }
    return {path, n};
  }
  const char* p;
  for (p = path + (n - 2); path < p; --p) {
    if (IsDirSeparatorChar(*p)) {
      break;
    }
  }
  if (path == p) {
    if (IsDirSeparatorChar(*p)) {
      return {path + 1, n - 1};
    }
    return {path, n};
  }
  n -= p - path;
  return {p + 1, n - 1};
}

std::vector<std::string> SdbFilesDirectory(const char* path) {
  std::vector<std::string> result;

  DIR* d = opendir(path);

  if (d == nullptr) {
    return result;
  }

  absl::Cleanup guard = [&d]() noexcept { closedir(d); };

  struct dirent* de = readdir(d);

  while (de != nullptr) {
    if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0) {
      // may throw
      result.emplace_back(de->d_name);
    }

    de = readdir(d);
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

ssize_t SdbReadPointer(int fd, char* buffer, size_t length) {
  char* ptr = buffer;
  size_t remain_length = length;

  while (0 < remain_length) {
    ssize_t n = SERENEDB_READ(fd, ptr, static_cast<size_t>(remain_length));

    if (n < 0) {
      if (errno == EINTR) {
        continue;
      }
      SetError(ERROR_SYS_ERROR);
      SDB_ERROR(GENERAL, "cannot read: ", SERENEDB_ERRORNO_STR);
      return n;  // always negative
    } else if (n == 0) {
      break;
    }

    ptr += n;
    remain_length -= n;
  }

  SDB_ASSERT(ptr >= buffer);
  return static_cast<ssize_t>(ptr - buffer);
}

bool SdbWritePointer(int fd, const void* buffer, size_t length) {
  const char* ptr = static_cast<const char*>(buffer);

  while (0 < length) {
    auto n = SERENEDB_WRITE(fd, ptr, static_cast<size_t>(length));

    if (n < 0) {
      SetError(ERROR_SYS_ERROR);
      SDB_ERROR(GENERAL, "cannot write: ", SERENEDB_ERRORNO_STR);
      return false;
    }

    ptr += n;
    length -= n;
  }

  return true;
}

ErrorCode SdbWriteFile(const char* filename, const char* data, size_t length) {
  int fd;
  bool result;

  fd = SERENEDB_CREATE(filename, O_CREAT | O_EXCL | O_RDWR | SERENEDB_O_CLOEXEC,
                       S_IRUSR | S_IWUSR);

  if (fd == -1) {
    SetError(ERROR_SYS_ERROR);
    return ERROR_SYS_ERROR;
  }

  result = SdbWritePointer(fd, data, length);

  SERENEDB_CLOSE(fd);

  if (!result) {
    return GetError();
  }

  return ERROR_OK;
}

bool Sdbfsync(int fd) {
  int res = fsync(fd);

  if (res == 0) {
    return true;
  } else {
    SetError(ERROR_SYS_ERROR);
    return false;
  }
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
    } else if (n < 0) {
      result = {};
      SetError(ERROR_SYS_ERROR);
      return false;
    }

    true_size += n;
  }
}

bool SdbProcessFile(
  const char* filename,
  const std::function<bool(const char* block, size_t size)>& reader) {
  SetError(ERROR_OK);
  const int fd = SERENEDB_OPEN(filename, O_RDONLY | SERENEDB_O_CLOEXEC);

  if (fd == -1) {
    SetError(ERROR_SYS_ERROR);
    return false;
  }

  absl::Cleanup guard = [fd]() noexcept { SERENEDB_CLOSE(fd); };

  char buffer[16384];

  while (true) {
    ssize_t n = SERENEDB_READ(fd, &buffer[0], sizeof(buffer));

    if (n == 0) {
      return true;
    }

    if (n < 0) {
      SetError(ERROR_SYS_ERROR);
      return false;
    }

    if (!reader(&buffer[0], n)) {
      return false;
    }
  }
}

ErrorCode SdbCreateLockFile(const char* filename) {
  absl::WriterMutexLock locker{&gOpenedFilesLock};

  for (size_t i = 0; i < gOpenedFiles.size(); ++i) {
    if (gOpenedFiles[i].first == filename) {
      // file already exists
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

  pid_t pid = Thread::currentProcessId();
  std::string buf = std::to_string(pid);

  int rv = SERENEDB_WRITE(fd, buf.c_str(), static_cast<size_t>(buf.size()));

  if (rv == -1) {
    auto res = ERROR_SYS_ERROR;
    SetError(res);

    SERENEDB_CLOSE(fd);
    SERENEDB_UNLINK(filename);

    return res;
  }

  struct flock lock;

  lock.l_start = 0;
  lock.l_len = 0;
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  // try to lock pid file
  rv = fcntl(fd, F_SETLK, &lock);

  if (rv == -1) {
    auto res = ERROR_SYS_ERROR;
    SetError(res);

    SERENEDB_CLOSE(fd);
    SERENEDB_UNLINK(filename);

    return res;
  }

  gOpenedFiles.push_back(std::make_pair(filename, fd));

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
  memset(buffer, 0,
         sizeof(buffer));  // not really necessary, but this shuts up valgrind
  ssize_t n = SERENEDB_READ(fd, buffer, static_cast<size_t>(sizeof(buffer)));

  if (n <= 0 || n == sizeof(buffer)) {
    return ERROR_OK;
  }

  uint32_t fc = basics::string_utils::Uint32(&buffer[0], n);

  if (fc == 0) {
    // invalid pid value
    return ERROR_OK;
  }

  pid_t pid = fc;

  // check for the existence of previous process via kill command

  // from man 2 kill:
  //   If sig is 0, then no signal is sent, but existence and permission checks
  //   are still performed; this can be used to check for the existence of a
  //   process ID or process group ID that the caller is permitted to signal.
  if (kill(pid, 0) == -1) {
    SDB_WARN(GENERAL, "found existing lockfile '", filename,
             "' of previous process with pid ", pid,
             ", but that process seems to be dead already");
  } else {
    SDB_WARN(GENERAL, "found existing lockfile '", filename,
             "' of previous process with pid ", pid,
             ", and that process seems to be still running");
  }

  struct flock lock;

  lock.l_start = 0;
  lock.l_len = 0;
  lock.l_type = F_WRLCK;
  lock.l_whence = SEEK_SET;
  // try to lock pid file
  int can_lock = fcntl(fd, F_SETLK, &lock);  // Exclusive (write) lock

  // file was not yet locked; could be locked
  if (can_lock == 0) {
    // lock.l_type = F_UNLCK;
    if (0 != fcntl(fd, F_GETLK, &lock)) {
      SetError(ERROR_SYS_ERROR);
      SDB_WARN(GENERAL, "fcntl on lockfile '", filename,
               "' failed: ", LastError());
    }

    return ERROR_OK;
  }

  // error!
  can_lock = errno;
  SetError(ERROR_SYS_ERROR);

  // from man 2 fcntl: "If a conflicting lock is held by another process,
  // this call returns -1 and sets errno to EACCES or EAGAIN."
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
    if (gOpenedFiles[i].first == filename) {
      int fd = SERENEDB_OPEN(filename, O_RDWR | SERENEDB_O_CLOEXEC);

      if (fd < 0) {
        return ERROR_OK;
      }

      struct flock lock;

      lock.l_start = 0;
      lock.l_len = 0;
      lock.l_type = F_UNLCK;
      lock.l_whence = SEEK_SET;
      // release the lock
      auto res = ERROR_OK;
      if (0 != fcntl(fd, F_SETLK, &lock)) {
        res = ERROR_SYS_ERROR;
        SetError(res);
      }
      SERENEDB_CLOSE(fd);

      if (res == ERROR_OK) {
        // TODO(mbkkt) why ignore?
        std::ignore = SdbUnlinkFile(filename);
      }

      // close lock file descriptor
      fd = gOpenedFiles[i].second;
      SERENEDB_CLOSE(fd);

      gOpenedFiles.erase(gOpenedFiles.begin() + i);

      return res;
    }
  }

  return ERROR_OK;
}

std::string_view SdbGetFilename(std::string_view filename) {
  size_t pos = filename.find_last_of("\\/:");
  if (pos == std::string::npos) {
    return filename;
  }
  return filename.substr(pos + 1);
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

std::string_view SdbBinaryName(std::string_view argv0) {
  auto result = SdbBasename(argv0);
  if (result.ends_with(".exe")) {
    return result.substr(0, result.size() - 4);
  }
  return result;
}

std::string SdbLocateBinaryPath(const char* argv0) {
  std::string binary_path;

  // check if name contains a '/' ( or '\' for windows)
  const char* p = argv0;

  for (; *p && *p != SERENEDB_DIR_SEPARATOR_CHR; ++p) {
  }

  // contains a path
  if (*p) {
    binary_path = SdbDirname(argv0);
  } else {
    // check PATH variable
    std::string pv;
    if (SdbGETENV("PATH", pv)) {
      auto files = absl::StrSplit(pv, ':');
      for (const auto& prefix : files) {
        std::string full;
        if (!prefix.empty()) {
          full = basics::file_utils::BuildFilename(prefix, argv0);
        } else {
          full = basics::file_utils::BuildFilename(".", argv0);
        }

        if (SdbExistsFile(full.c_str())) {
          binary_path = prefix;
          break;
        }
      }
    }
  }

  return binary_path;
}

std::string SdbGetInstallRoot(const char* binary_path,
                              const char* install_path) {
  SDB_ASSERT(binary_path);
  SDB_ASSERT(install_path);
  // First lets remove trailing (back) slashes from the bill:
  auto install_path_length = strlen(install_path);
  if (install_path[install_path_length - 1] == SERENEDB_DIR_SEPARATOR_CHR) {
    --install_path_length;
  }

  auto binary_path_length = strlen(binary_path);
  if (binary_path[binary_path_length - 1] == SERENEDB_DIR_SEPARATOR_CHR) {
    --binary_path_length;
  }

  if (install_path_length > binary_path_length) {
    return SERENEDB_DIR_SEPARATOR_STR;
  }

  for (size_t i = 1; i < install_path_length; ++i) {
    if (binary_path[binary_path_length - i] !=
        install_path[install_path_length - i]) {
      return SERENEDB_DIR_SEPARATOR_STR;
    }
  }
  return std::string(binary_path, binary_path_length - install_path_length);
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
      // only copy non-empty files
      rc = CopyFileContents(src_fd, dst_fd, dsize, error);
    }
    timeval times[2];
    memset(times, 0, sizeof(times));
    times[0].tv_sec = SERENEDB_STAT_ATIME_SEC(*statbuf);
    times[1].tv_sec = SERENEDB_STAT_MTIME_SEC(*statbuf);

    if (fchown(dst_fd, -1 /*statbuf.st_uid*/, statbuf->st_gid) != 0) {
      error = absl::StrCat("failed to chown ", dst, ": ", strerror(errno));
      // rc = false;  no, this is not fatal...
    }
    if (fchmod(dst_fd, statbuf->st_mode) != 0) {
      error = absl::StrCat("failed to chmod ", dst, ": ", strerror(errno));
      rc = false;
    }

#ifdef HAVE_FUTIMES
    if (futimes(dstFD, times) != 0) {
      error =
        std::string("failed to adjust age: ") + dst + ": " + strerror(errno);
      rc = false;
    }
#else
    if (utimes(dst, times) != 0) {
      error =
        absl::StrCat("failed to adjust age: ", dst, ": ", strerror(errno));
      rc = false;
    }
#endif
  } catch (...) {
    // make sure we always close file handles
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
    //    return false;
  }
  if (chmod(dst_item, statbuf.st_mode) != 0) {
    error = absl::StrCat("failed to chmod ", dst_item, ": ", strerror(errno));
    return false;
  }

  timeval times[2];
  memset(times, 0, sizeof(times));
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

  return 0 == rc;
}

std::string_view SdbHomeDirectory() {
  const char* result = getenv("HOME");

  if (result == nullptr) {
    return ".";
  }

  return result;
}

void SdbSetApplicationName(std::string_view name) { gApplicationName = name; }

// The TempPath is set but not created
void SdbSetTempPath(std::string_view temp) { gUserTempPath = temp; }

std::string SdbGetTempPath() {
  char* path = gSystemTempPath.get();

  if (path == nullptr) {
    std::string system;
    if (gUserTempPath.empty()) {
      system = GetTempPath();
    } else {
      system = gUserTempPath;
    }

    // Strip any double back/slashes from the string.
    while (system.find(SERENEDB_DIR_SEPARATOR_STR SERENEDB_DIR_SEPARATOR_STR) !=
           std::string::npos) {
      system = basics::string_utils::Replace(
        system,
        std::string(SERENEDB_DIR_SEPARATOR_STR SERENEDB_DIR_SEPARATOR_STR),
        std::string(SERENEDB_DIR_SEPARATOR_STR));
    }

    // remove trailing DIR_SEPARATOR
    while (!system.empty() && IsDirSeparatorChar(system[system.size() - 1])) {
      system.pop_back();
    }

    // and re-append it
    system.push_back(SERENEDB_DIR_SEPARATOR_CHR);

    if (gUserTempPath.empty()) {
      system += gApplicationName + "_XXXXXX";
    }

    int tries = 0;
    while (true) {
      // copy to a character array
      gSystemTempPath.reset(new char[system.size() + 1]);
      path = gSystemTempPath.get();
      CopyString(path, system.c_str(), system.size());

      auto res = ERROR_OK;
      if (!gUserTempPath.empty()) {
        // --temp.path was specified
        if (SdbIsDirectory(system.c_str())) {
          // temp directory already exists. now simply use it
          break;
        }

        res = 0 == SERENEDB_MKDIR(gUserTempPath.c_str(), 0700) ? ERROR_OK : [] {
          SetError(ERROR_SYS_ERROR);
          return ERROR_SYS_ERROR;
        }();
      } else {
        // no --temp.path was specified
        // fill template and create directory
        tries = 9;

        // create base directories of the new directory (but ignore any failures
        // if they already exist. if this fails, the following mkDTemp will
        // either succeed or fail and return an error
        try {
          long system_error;
          std::string system_error_str;
          auto base_directory = SdbDirname(gSystemTempPath.get());
          if (base_directory.size() <= 1) {
            base_directory = gSystemTempPath.get();
          }
          // create base directory if it does not yet exist
          // TODO(mbkkt) why ignore?
          std::ignore = SdbCreateRecursiveDirectory(
            base_directory, system_error, system_error_str);
        } catch (...) {
        }

        // fill template string (XXXXXX) with some pseudo-random value and
        // create the directory
        res = MkDTemp(gSystemTempPath.get(), system.size() + 1);
      }

      if (res == ERROR_OK) {
        break;
      }

      // directory could not be created
      // this may be a race, a permissions problem or something else
      if (++tries >= 10) {
#ifdef SDB_DEV
        SDB_ERROR(GENERAL, "UserTempPath: ", gUserTempPath,
                  ", system: ", system, ", user temp path exists: ",
                  SdbIsDirectory(gUserTempPath.c_str()), ", res: ", res,
                  ", SystemTempPath: ", gSystemTempPath.get());
#endif
        SDB_FATAL(GENERAL,
                  "failed to create a temporary directory - giving up!");
        FatalErrorAbort();
      }
      // sleep for a random amout of time and try again soon
      // with this, we try to avoid races between multiple processes
      // that try to create temp directories at the same time
      std::this_thread::sleep_for(
        std::chrono::milliseconds{5 + random::Interval(uint16_t{20})});
    }

    gSystemTempPathSweeperInstance.Init(gSystemTempPath.get());
  }

  return std::string(path);
}

ErrorCode SdbGetTempName(const char* directory, std::string& result,
                         bool create_file, long& system_error,
                         std::string& error_message) {
  std::string temp = SdbGetTempPath();

  std::string dir;
  if (directory != nullptr) {
    dir = basics::file_utils::BuildFilename(temp, directory);
  } else {
    dir = temp;
  }

  // remove trailing DIR_SEPARATOR
  while (!dir.empty() && IsDirSeparatorChar(dir[dir.size() - 1])) {
    dir.pop_back();
  }

  auto res = SdbCreateRecursiveDirectory(dir, system_error, error_message);

  if (res != ERROR_OK) {
    return res;
  }

  if (!SdbIsDirectory(dir.c_str())) {
    error_message = dir + " exists and is not a directory!";
    return ERROR_CANNOT_CREATE_DIRECTORY;
  }

  int tries = 0;
  while (tries++ < 10) {
    auto pid = Thread::currentProcessId();

    auto temp_name =
      absl::StrCat("tmp-", pid, "-", random::Interval(UINT32_MAX));

    auto filename = basics::file_utils::BuildFilename(dir, temp_name);

    if (SdbExistsFile(filename.c_str())) {
      error_message = absl::StrCat("Tempfile already exists! ", filename);
    } else {
      if (create_file) {
        FILE* fd = SERENEDB_FOPEN(filename.c_str(), "wb");

        if (fd != nullptr) {
          fclose(fd);
          result = filename;
          return ERROR_OK;
        }
      } else {
        result = filename;
        return ERROR_OK;
      }
    }

    // next try
  }

  return ERROR_CANNOT_CREATE_TEMP_FILE;
}

std::string SdbLocateInstallDirectory(const char* argv0,
                                      const char* binary_path) {
  std::string this_path = SdbLocateBinaryPath(argv0);
  std::string ret = SdbGetInstallRoot(this_path.c_str(), binary_path);
  if (ret.length() != 1 || ret != SERENEDB_DIR_SEPARATOR_STR) {
    ret += SERENEDB_DIR_SEPARATOR_CHR;
  }
  return ret;
}

#if defined(_SYSCONFDIR_)

std::string SdbLocateConfigDirectory(const char* binary_path) {
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
  } else {
    return std::string(dir);
  }
}

#else

std::string SdbLocateConfigDirectory(const char*) {
  return LocateConfigDirectoryEnv();
}

#endif

bool SdbPathIsAbsolute(std::string_view path) { return path.starts_with('/'); }

/// return the amount of total and free disk space for the given path
Result SdbGetDiskSpaceInfo(const char* path, uint64_t& total_space,
                           uint64_t& free_space) {
  struct statvfs stat;

  if (statvfs(path, &stat) == -1) {
    SetError(ERROR_SYS_ERROR);
    return {GetError(), LastError()};
  }

  const auto factor = static_cast<uint64_t>(stat.f_bsize);

  total_space = factor * static_cast<uint64_t>(stat.f_blocks);

  // sbuf.bfree is total free space available to root
  // sbuf.bavail is total free space available to unprivileged user
  // sbuf.bavail <= sbuf.bfree ... pick correct based upon effective user id
  if (geteuid()) {
    // non-zero user is unprivileged, or -1 if error. take more conservative
    // size
    free_space = factor * static_cast<uint64_t>(stat.f_bavail);
  } else {
    // root user can access all disk space
    free_space = factor * static_cast<uint64_t>(stat.f_bfree);
  }
  return {};
}

/// return the amount of total and free inodes for the given path.
/// always returns 0 on Windows
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
  value.clear();
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
    SDB_THROW(sdb::ERROR_OUT_OF_MEMORY);
  }
  if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 0) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    EVP_MD_CTX_free(context);
#else
    EVP_MD_CTX_destroy(_context);
#endif
    SDB_THROW(sdb::ERROR_INTERNAL, "unable to initialize SHA256 processor");
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
    } else if (n < 0) {
      result = {};
      SetError(ERROR_SYS_ERROR);
      return false;
    }

    true_size += n;
  }
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
    size_t size_read = fread(&buffer[0], 1, sizeof(buffer), fin);

    if (size_read < sizeof(buffer)) {
      if (feof(fin) == 0) {
        res = ERROR_FAILED;
        break;
      }
    }

    if (size_read > 0) {
      *crc = static_cast<uint32_t>(absl::ExtendCrc32c(
        absl::crc32c_t{*crc}, std::string_view{&buffer[0], size_read}));
    } else /* if (sizeRead <= 0) */ {
      break;
    }
  }

  if (0 != fclose(fin)) {
    res = ERROR_SYS_ERROR;
    SetError(res);
    // otherwise keep original error
  }

  return res;
}

}  // namespace sdb
