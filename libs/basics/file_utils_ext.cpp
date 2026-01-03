////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "file_utils_ext.hpp"

#include <absl/strings/str_cat.h>

#include <cstdint>

#include "basics/assert.h"

#if defined(__APPLE__)
#include <sys/param.h>  // for MAXPATHLEN
#endif

#include <sys/stat.h>
#include <sys/types.h>

#ifdef _WIN32
#define NO_SHLWAPI_STRFCNS
#include <Shlwapi.h>
#include <io.h>  // for _get_osfhandle

#include "windows.h"
#pragma comment(lib, "Shlwapi.lib")

#else

#include <dirent.h>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#endif  // _WIN32

#include "basics/logger/logger.h"
#include "basics/network_utils.hpp"
#include "basics/process_utils.hpp"

namespace {

#ifdef _WIN32

// workaround for path MAX_PATH
const std::filesystem::path::string_type path_prefix(L"\\\\?\\");

std::filesystem::path::string_type ensure_path_prefix(
  const std::filesystem::path::string_type& path) {
  if (path.size() >= path_prefix.size() &&
      !path.compare(0, path_prefix.size(), path_prefix)) {
    return path;
  } else {
    auto res = path_prefix;
    res.append(path);
    return res;
  }
}

#endif

inline int PathStats(FILE_STAT_T& info, const irs::path_char_t* path) {
#ifdef WIN32
  if (wcslen(path) >= path_prefix.size() &&
      !wcsncmp(path, path_prefix.c_str(), path_prefix.size())) {
    path += path_prefix
              .size();  // skip prefix to avoid mix of slashes in prefixed path
  }
#endif

// MSVC2013 _wstat64(...) reports ENOENT for '\' terminated paths
// MSVC2015/MSVC2017 treat '\' terminated paths properly
#if defined(_MSC_VER) && _MSC_VER == 1800
  auto parts = irs::file_utils::PathParts(path);

  return file_stat(
    parts.basename.empty() ? std::wstring(parts.dirname).c_str() : path, &info);
#else
  return FILE_STAT(path, &info);
#endif
}

}  // namespace

namespace irs {
namespace file_utils {

void FileDeleter::operator()(void* f) const noexcept {
#ifdef _WIN32
  if (f != nullptr && f != INVALID_HANDLE_VALUE) {
    CloseHandle(f);
  }
#else
  if (f) {
    const int fd = HANDLE_CAST(f);
    if (fd < 0) {
      return;  // invalid desriptor
    }
    ::close(fd);
  }
#endif
}

void LockFileDeleter::operator()(void* handle) const {
  if (handle) {
#ifdef _WIN32
    if (handle != INVALID_HANDLE_VALUE) {
      ::CloseHandle(handle);
    }
#else
    const int fd = HANDLE_CAST(handle);
    if (fd < 0) {
      return;  // invalid desriptor
    }
    flock(fd, LOCK_UN);  // unlock file
    close(fd);           // close file
#endif
  }
}

static bool Exists(const path_char_t* file) {
#ifdef _WIN32
  return TRUE == ::PathFileExists(file);
#else
  return 0 == access(file, F_OK);
#endif
}

size_t Fwrite(void* fd, const void* buf, size_t size) {
  size_t left = size;
  auto current = static_cast<const uint8_t*>(buf);
#ifdef _WIN32
  static_assert(sizeof(size_t) > sizeof(DWORD),
                "sizeof(size_t) > sizeof(DWORD)");
  constexpr size_t maxWrite = MAXDWORD;
  while (left > 0) {
    DWORD to_write = static_cast<DWORD>((std::min)(maxWrite, left));
    DWORD written{0};
    if (WriteFile(fd, current, to_write, &written, NULL)) {
      left -= written;
      current += written;
    } else {
      break;
    }
  }
#else
  constexpr size_t kWriteLimit = 1UL << 30;
  const int descriptor = HANDLE_CAST(fd);
  while (left > 0) {
    size_t to_write = (std::min)(left, kWriteLimit);
    const ssize_t written = ::write(descriptor, current, to_write);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    left -= written;
    current += written;
  }
#endif
  return size - left;
}

size_t Fread(void* fd, void* buf, size_t size) {
  size_t left = size;
  auto current = static_cast<uint8_t*>(buf);
#ifdef _WIN32
  constexpr size_t maxRead = MAXDWORD;
  while (left > 0) {
    DWORD to_read = static_cast<DWORD>((std::min)(maxRead, left));
    DWORD read{0};
    if (ReadFile(fd, current, to_read, &read, NULL) && read > 0) {
      left -= read;
      current += read;
    } else {
      break;
    }
  }
#else
  constexpr size_t kReadLimit = 0x7ffff000;
  const int descriptor = HANDLE_CAST(fd);
  while (left > 0) {
    size_t to_read = (std::min)(left, kReadLimit);
    const ssize_t read = ::read(descriptor, current, to_read);
    if (read < 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    } else if (read > 0) {
      left -= read;
      current += read;
    } else {
      break;  // EOF reached
    }
  }
#endif
  return size - left;
}

int Fseek(void* fd, long pos, int origin) {
#ifdef _WIN32
  LARGE_INTEGER li;
  li.QuadPart = pos;
  DWORD moveMethod;
  switch (origin) {
    case SEEK_SET:
      moveMethod = FILE_BEGIN;
      break;
    case SEEK_CUR:
      moveMethod = FILE_CURRENT;
      break;
    case SEEK_END:
      moveMethod = FILE_END;
      break;
    default:
      SDB_ASSERT(false);
      return 0;
  }
  return !SetFilePointerEx(fd, li, nullptr, moveMethod);
#else
  return lseek(HANDLE_CAST(fd), pos, origin) == -1;
#endif
}

int Ferror(void*) { return static_cast<int>(GET_ERROR()); }

long Ftell(void* fd) {
#ifdef _WIN32
  LARGE_INTEGER liOfs = {0};
  LARGE_INTEGER liNew = {0};
  if (SetFilePointerEx(fd, liOfs, &liNew, FILE_CURRENT)) {
    return static_cast<long>(liNew.LowPart);
  } else {
    return -1;
  }
#else
  return lseek(HANDLE_CAST(fd), 0, SEEK_CUR);
#endif
}

#ifdef _WIN32
namespace {
constexpr DWORD FS_DEFERRED_DELETE_TIMEOUT = 10;
constexpr int CREATE_FILE_TRIES = 3;
}  // namespace

bool verify_lock_file(const path_char_t* file) {
  if (!exists(file)) {
    return false;  // not locked
  }

  HANDLE handle =
    ::CreateFileW(file,
                  GENERIC_WRITE | GENERIC_READ,  // write access
                  0,                      // prevent access from other processes
                  NULL,                   // default security attributes
                  OPEN_EXISTING,          // open existing file
                  FILE_ATTRIBUTE_NORMAL,  // use normal file attributes
                  NULL);

  if (INVALID_HANDLE_VALUE == handle) {
    if (ERROR_SHARING_VIOLATION == GET_ERROR()) {
      return true;  // locked
    }
    return false;  // not locked
  }

  char buf[256]{};
  // read file content
  {
    DWORD size;
    const BOOL res = ::ReadFile(handle, buf, sizeof buf, &size, NULL);
    ::CloseHandle(handle);

    if (FALSE == res || 0 == size || sizeof buf == size) {
      // invalid file format or string too long
      return false;  // not locked
    }
  }

  // check hostname
  const size_t len = strlen(buf);
  if (!is_same_hostname(buf, len)) {
    SDB_INFO(absl::StrCat("Index locked by another host, hostname: '",
                          std::string_view{buf, len}, "', file: '", ToStr(file),
                          "'"));
    return true;  // locked
  }

  // check pid
  const char* pid = buf + len + 1;
  if (is_valid_pid(pid)) {
    SDB_INFO(absl::StrCat("Index locked by another process, PID: '", pid,
                          "', file: '", ToStr(file), "'"));
    return true;  // locked
  }

  return false;  // not locked
}

lock_handle_t create_lock_file(const path_char_t* file) {
  HANDLE fd = INVALID_HANDLE_VALUE;
  // windows has a deferred deletion and with rapid locking/unlocking same lock
  // file could fail to be created so we try several times
  int try_count = CREATE_FILE_TRIES;
  do {
    fd = ::CreateFileW(file,
                       GENERIC_WRITE,  // write access
                       0,              // prevent access from other processes
                       NULL,           // default security attributes
                       CREATE_ALWAYS,  // always create file
                       FILE_FLAG_DELETE_ON_CLOSE,  // allow OS to remove file
                                                   // when process finished
                       NULL);

    if (ERROR_ACCESS_DENIED != GET_ERROR()) {
      break;
    } else {
      ::Sleep(FS_DEFERRED_DELETE_TIMEOUT);  // give some time for file system to
                                            // finalize deletion
    }
  } while ((--try_count) > 0);

  if (INVALID_HANDLE_VALUE == fd) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Unable to create lock file: '", ToStr(file),
                           "', error: ", GET_ERROR()));
    return nullptr;
  }

  lock_handle_t handle(reinterpret_cast<void*>(fd));
  char buf[256];

  // write hostname to lock file
  if (const int err = get_host_name(buf, sizeof buf - 1)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Unable to get hostname, error: ", err));
    return nullptr;
  }

  if (!file_utils::write(fd, buf, strlen(buf) + 1)) {  // include terminate 0
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Unable to write lock file: '", ToStr(file),
                           "', error: ", GET_ERROR()));
    return nullptr;
  }

  // write PID to lock file
  const size_t size = sprintf(buf, "%d", get_pid());
  if (!file_utils::write(fd, buf, size)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Unable to write lock file: '", ToStr(file),
                           "', error: ", GET_ERROR()));
    return nullptr;
  }

  // flush buffers
  if (::FlushFileBuffers(fd) <= 0) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Unable to flush lock file: '", ToStr(file),
                           "', error: ", GET_ERROR()));
    return nullptr;
  }

  return handle;
}

bool file_sync(const path_char_t* file) noexcept {
  HANDLE handle =
    ::CreateFileW(file, GENERIC_WRITE, FILE_SHARE_WRITE | FILE_SHARE_READ, NULL,
                  OPEN_EXISTING, 0, NULL);
  if (INVALID_HANDLE_VALUE == handle) {
    return false;
  }
  const bool res = ::FlushFileBuffers(handle) > 0;
  ::CloseHandle(handle);
  return res;
}

bool file_sync(int fd) noexcept {
  // Attempt to convert file descriptor into an operating system file handle
  HANDLE handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));

  // An invalid file system handle was returned.
  if (handle == INVALID_HANDLE_VALUE) {
    return false;
  }

  return !FlushFileBuffers(handle);
}

#else

bool VerifyLockFile(const path_char_t* file) {
  if (!Exists(file)) {
    return false;  // not locked
  }

  const int fd = ::open(file, O_RDONLY);

  if (fd < 0) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Unable to open lock file '", file,
                           "' for verification, error: ", errno));
    return false;  // not locked
  }

  char buf[256];
  ssize_t size;
  // read file content
  {
    lock_handle_t handle(reinterpret_cast<void*>(fd));

    // try to apply advisory lock on lock file
    if (flock(fd, LOCK_EX | LOCK_NB)) {
      if (EWOULDBLOCK == errno) {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Lock file '", file, "' is already locked"));
        return true;  // locked
      } else {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Unable to apply lock on lock file: '", file,
                               "', error: ", errno));
        return false;  // not locked
      }
    }

    size = Fread(handle.get(), buf, sizeof buf);
  }

  if (size <= 0 || sizeof buf == size) {
    // invalid file format or string too long
    return false;  // not locked
  }
  buf[size] = 0;  // set termination 0

  // check hostname
  const size_t len = strlen(buf);  // hostname length
  if (!IsSameHostname(buf, len)) {
    SDB_INFO("xxxxx", sdb::Logger::IRESEARCH,
             "Index locked by another host, hostname: '",
             std::string_view{buf, len}, "', file: '", file, "'");
    return true;  // locked
  }

  if (len >= size_t(size)) {
    // no PID in buf
    return false;  // not locked
  }

  // check pid
  const char* pid = buf + len + 1;
  if (IsValidPid(pid)) {
    SDB_INFO("xxxxx", sdb::Logger::IRESEARCH,
             "Index locked by another process, PID: '", pid, "', file: '", file,
             "'");
    return true;  // locked
  }

  return false;  // not locked
}

lock_handle_t CreateLockFile(const path_char_t* file) {
  const int fd = ::open(file, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);

  if (fd < 0) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Unable to create lock file: '", file, "', error: ", errno));
    return nullptr;
  }

  lock_handle_t handle(reinterpret_cast<void*>(fd));
  char buf[256];

  // write hostname to lock file
  if (const int err = GetHostName(buf, sizeof buf - 1)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Unable to get hostname, error: ", err));
    return nullptr;
  }

  if (!file_utils::Write(reinterpret_cast<void*>(fd), buf,
                         strlen(buf) + 1)) {  // include terminated 0
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Unable to write lock file: '", file, "', error: ", errno));
    return nullptr;
  }

  // write PID to lock file
  size_t size = sprintf(buf, "%d", GetPid());
  if (!file_utils::Write(reinterpret_cast<void*>(fd), buf, size)) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Unable to write lock file: '", file, "', error: ", errno));
    return nullptr;
  }

  // flush buffers
  if (fsync(fd)) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Unable to write lock file: '", file, "', error: ", errno));
    return nullptr;
  }

  // try to apply advisory lock on lock file
  if (flock(fd, LOCK_EX)) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Unable to write lock file: '", file, "', error: ", errno));
    return nullptr;
  }

  return handle;
}

bool FileSync(const path_char_t* file) noexcept {
  const int handle = ::open(file, O_WRONLY, S_IRWXU);
  if (handle < 0) {
    return false;
  }

  const bool res = (0 == fsync(handle));
  close(handle);
  return res;
}

bool FileSync(int fd) noexcept { return 0 == fsync(fd); }

#endif  // _WIN32

bool Absolute(bool& result, const path_char_t* path) noexcept {
  if (!path) {
    return false;
  }

#ifdef _WIN32
  if (MAX_PATH > wcslen(path)) {
    result = !PathIsRelativeW(path);
  } else {
    // ensure that PathIsRelativeW(...) is given a value shorter than MAX_PATH
    // still ok since to determine if absolute only need the start of the path
    irs::basic_string<wchar_t> buf(path, MAX_PATH - 1);  // -1 for '\0'

    result = !PathIsRelativeW(buf.c_str());
  }
#else
  result = path[0] == '/';  // a null terminated string is at least size 1
#endif

  return true;
}

bool BlockSize(FILE_BLKSIZE_T& result, const path_char_t* file) noexcept {
  SDB_ASSERT(file != nullptr);
#ifdef _WIN32
  // TODO FIXME find a workaround
  IRS_IGNORE(file);
  result = 512;

  return true;
#else
  FILE_STAT_T info;

  if (0 != PathStats(info, file)) {
    return false;
  }

  result = info.st_blksize;

  return true;
#endif  // _WIN32
}

bool ByteSize(uint64_t& result, const path_char_t* file) noexcept {
  SDB_ASSERT(file != nullptr);
  FILE_STAT_T info;

  if (0 != PathStats(info, file)) {
    return false;
  }

  result = info.st_size;

  return true;
}

bool ByteSize(uint64_t& result, void* fd) noexcept {
#ifdef _WIN32
  LARGE_INTEGER li;

  if (!GetFileSizeEx(fd, &li)) {
    return false;
  }
  result = static_cast<uint64_t>(li.QuadPart);
  return true;
#else
  return ByteSize(result, HANDLE_CAST(fd));
#endif
}

bool ByteSize(uint64_t& result, int fd) noexcept {
  FILE_STAT_T info;

  if (0 != FILE_FSTAT(fd, &info)) {
    return false;
  }

  result = info.st_size;

  return true;
}

bool Exists(bool& result, const path_char_t* file) noexcept {
  SDB_ASSERT(file != nullptr);
  FILE_STAT_T info;

  result = 0 == PathStats(info, file);

  if (!result && ENOENT != errno) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to get stat, error ", errno,
                           " path: ", ToStr(file)));
  }

  return true;
}

bool ExistsDirectory(bool& result, const path_char_t* name) noexcept {
  SDB_ASSERT(name != nullptr);
  FILE_STAT_T info;

  result = 0 == PathStats(info, name);

  if (result) {
#ifdef _WIN32
    result = (info.st_mode & _S_IFDIR) > 0;
#else
    result = (info.st_mode & S_IFDIR) > 0;
#endif
  } else if (ENOENT != errno) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to get stat, error ", errno,
                           " path: ", ToStr(name)));
  }

  return true;
}

bool ExistsFile(bool& result, const path_char_t* name) noexcept {
  SDB_ASSERT(name != nullptr);
  FILE_STAT_T info;

  result = 0 == PathStats(info, name);

  if (result) {
#ifdef _WIN32
    result = (info.st_mode & _S_IFREG) > 0;
#else
    result = (info.st_mode & S_IFREG) > 0;
#endif
  } else if (ENOENT != errno) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to get stat, error ", errno,
                           " path: ", ToStr(name)));
  }

  return true;
}

bool Mtime(time_t& result, const path_char_t* file) noexcept {
  SDB_ASSERT(file != nullptr);
  FILE_STAT_T info;

  if (0 != PathStats(info, file)) {
    return false;
  }

  result = info.st_mtime;

  return true;
}

handle_t Open(const path_char_t* path, OpenMode mode, int advice) noexcept {
  // Should be Read or Write but not both or none of them
  SDB_ASSERT(((mode & OpenMode::Read) == OpenMode::Invalid) !=
             ((mode & OpenMode::Write) == OpenMode::Invalid));
#ifdef _WIN32
  if (!path) {
    return handle_t(nullptr);
  }

  DWORD desiredAccess{0};
  // as *nix does not have sharing restrictions, we put none also
  // intentionally do not add FILE_SHARE_DELETE to detect bugs
  // with not properly closed files (error will be triggered on cleanup with
  // error code 32)
  DWORD sharing_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
  DWORD create_disposition = OPEN_EXISTING;
  auto read_write = mode & (OpenMode::Write | OpenMode::Read);
  switch (read_write) {
    case OpenMode::Read:
      desiredAccess = GENERIC_READ;
      break;
    case OpenMode::Write:
      desiredAccess = GENERIC_WRITE | GENERIC_READ;
      create_disposition =
        CREATE_ALWAYS;  // while opening for write we infer creation
      break;
    default:
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Invalid OpenMode ", static_cast<int>(mode),
                             " specified for file ", ToStr(path)));
      SDB_ASSERT(false);
      return handle_t(nullptr);
  }
  DWORD dwFlags = FILE_ATTRIBUTE_NORMAL | (static_cast<DWORD>(advice));
  // Implement Alignment and File Access Requirements from MSDN
  // if ((mode & OpenMode::Direct) == OpenMode::Direct) {
  //  dwFlags |= FILE_FLAG_NO_BUFFERING;
  //}
  HANDLE hFile = INVALID_HANDLE_VALUE;
  int try_count = CREATE_FILE_TRIES;
  do {
    hFile = CreateFile(path, desiredAccess, sharing_mode, NULL,
                       create_disposition, dwFlags, NULL);
    if (hFile != INVALID_HANDLE_VALUE) {
      return handle_t(hFile);
    }
    // this could be pending deletion blocking, if we are recreating file - this
    // is ok, we just try again if this is reading, file is gone, we give up
    if (ERROR_ACCESS_DENIED != GET_ERROR() || OpenMode::Write != mode) {
      break;
    } else {
      Sleep(FS_DEFERRED_DELETE_TIMEOUT);
    }
  } while ((--try_count) > 0);
  return handle_t(nullptr);
#else
  auto posix_mode = (((OpenMode::Read & mode) == OpenMode::Read)
                       ? O_RDONLY
                       : (O_CREAT | O_TRUNC | O_WRONLY));
#ifndef __APPLE__
  // Implement Alignment and File Access Requirements
  // if ((mode & OpenMode::Direct) == OpenMode::Direct) {
  //  posix_mode |= O_DIRECT;
  //}
#endif
  auto fd = ::open(path ? path : "/dev/null", posix_mode, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Failed to open file, error: ", errno, ", path: ", path));
    return handle_t(nullptr);
  }
#ifdef __APPLE__
  // Implement Alignment and File Access Requirements
  // if ((mode & OpenMode::Direct) == OpenMode::Direct) {
  //  fcntl(fd, F_NOCACHE, 1);
  //}
#endif
#if (_XOPEN_SOURCE >= 600 || _POSIX_C_SOURCE >= 200112L) && !defined(__APPLE__)
  posix_fadvise(fd, 0, 0, advice);
#endif
  return handle_t(reinterpret_cast<void*>(fd));
#endif
}

handle_t Open(void* file, OpenMode mode, int advice) noexcept {
  // Should be Read or Write but not both or none of them
  SDB_ASSERT(((mode & OpenMode::Read) == OpenMode::Invalid) !=
             ((mode & OpenMode::Write) == OpenMode::Invalid));
#ifdef _WIN32
  // win32 approach is to get the original filename of the handle and open it
  // again due to a bug from the 1980's the file name is garanteed to not change
  // while the file is open
  constexpr auto size = MAX_PATH + 1;  // +1 for \0
  TCHAR path[size];
  auto length = GetFinalPathNameByHandle(file, path, size - 1,
                                         VOLUME_NAME_DOS);  // -1 for \0

  if (!length) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Failed to get filename from file handle, error ",
                           GET_ERROR()));

    return nullptr;
  }

  if (length < size) {
    path[length] = '\0';
    return open(path, mode, advice);
  }

  SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Required file path buffer size of ", length + 1,
                         " is greater than the expected size of ", size,
                         ", malloc necessary"));  // +1 for \0

  auto buf_size = length + 1;  // +1 for \0
  auto buf = std::make_unique<TCHAR[]>(buf_size);

  length = GetFinalPathNameByHandle(file, buf.get(), buf_size - 1,
                                    VOLUME_NAME_DOS);  // -1 for \0

  if (length && length < buf_size) {
    buf[length] = '\0';
    return open(buf.get(), mode, advice);
  }

  SDB_ERROR(
    "xxxxx", sdb::Logger::IRESEARCH,
    absl::StrCat("Failed to get filename from file handle, inconsistent length "
                 "detected, first ",
                 buf_size, " then ", length + 1));  // +1 for \0

  return nullptr;
#elif defined(__APPLE__)
  // MacOS approach is to open the original file via the file descriptor link
  // under /dev/fd the link is garanteed to point to the original inode even if
  // the original file was removed unfortunatly this results in the original
  // file descriptor being dup()'ed therefore try to get the original file name
  // from the existing descriptor and reopen it
  // FIXME TODO assume that the file has not moved and was not deleted
  auto fd = handle_cast(file);
  char path[MAXPATHLEN + 1];  // F_GETPATH requires a buffer of size at least
                              // MAXPATHLEN, +1 for \0

  if (0 > fd || 0 > fcntl(fd, F_GETPATH, path)) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Failed to get file path from file handle, error ", errno));
    return nullptr;
  }

  return open(path, mode, advice);
#else
  // posix approach is to open the original file via the file descriptor link
  // under /proc/self/fd the link is guaranteed to point to the original inode
  // even if the original file was removed
  auto fd = HANDLE_CAST(file);
  // approximate maximum number of chars, +1 for \0
  char path[14 + sizeof(fd) * 3 + 1];

  if (0 > fd || 0 > sprintf(path, "/proc/self/fd/%d", fd)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat(
                "Failed to get system handle from file handle, error ", errno));
    return nullptr;
  }

  return Open(path, mode, advice);
#endif
}

bool Mkdir(const path_char_t* path, bool create_new) noexcept {
  bool result;

  if (!ExistsDirectory(result, path)) {
    return false;  // failure checking directory existence
  }

  if (result) {
    return !create_new;  // directory already exists.
  }

  // we do not check existence of anything other than directory, as race
  // conditions will arise on second check. Just rely on OS file manager to deal
  // with all name conflicts.

  auto parts = PathParts(path);

  if (!parts.dirname.empty()) {
    // need a null terminated string for use with ::mkdir()/::CreateDirectoryW()
    std::filesystem::path::string_type parent(parts.dirname);
    if (!Mkdir(
          parent.c_str(),
          false)) {  // intermediate path parts can exist, this is ok anyway
      return false;
    }
  }

#ifdef _WIN32
  bool abs;

  if (!absolute(abs, path)) {
    return false;
  }

  // '\\?\' cannot be used with relative paths
  if (!abs) {
    if (0 == ::CreateDirectoryW(path, nullptr)) {
      if (GET_ERROR() != ERROR_ALREADY_EXISTS || createNew) {
        // failed to create directory  or directory exist, but we are asked to
        // perform creation

        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Failed to create relative path: '", ToStr(path),
                               "', error ", GET_ERROR()));
        return false;
      }
    }
    return true;
  }

  // workaround for path MAX_PATH
  auto dirname = std::filesystem::path::string_type(path);

  // 'path_prefix' cannot be used with paths containing a mix of native and
  // non-native path separators
  std::replace(dirname.data(), dirname.data() + dirname.size(), L'/',
               file_path_delimiter);

  dirname = ensure_path_prefix(dirname);

  if (0 == ::CreateDirectoryW(dirname.c_str(), nullptr)) {
    if (GET_ERROR() != ERROR_ALREADY_EXISTS || createNew) {
      // failed to create directory  or directory exist, but we are asked to
      // perform creation

      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to create absolute path: '", ToStr(path),
                             "', error ", GET_ERROR()));

      return false;
    }
  }
#else
  if (0 != ::mkdir(path, S_IRWXU | S_IRWXG | S_IRWXO)) {
    if (errno != EEXIST || create_new) {
      // failed to create directory  or directory exist, but we are asked to
      // perform creation
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Failed to create path: '", path, "', error ", errno));
      return false;
    }
  }
#endif

  return true;
}

bool Move(const path_char_t* src_path, const path_char_t* dst_path) noexcept {
#ifdef _WIN32
  return 0 != ::MoveFileExW(src_path, dst_path,
                            MOVEFILE_REPLACE_EXISTING | MOVEFILE_COPY_ALLOWED);
#else
  return 0 == ::rename(src_path, dst_path);
#endif
}

PathPartsT PathParts(const path_char_t* path) noexcept {
  if (!path) {
    return PathPartsT();
  }

  bool have_extension = false;
  PathPartsT::ref_t dirname = PathPartsT::ref_t{};
  size_t stem_end = 0;

  for (size_t i = 0;; ++i) {
    switch (*(path + i)) {
#ifdef _WIN32
      case L'\\':  // fall through
      case L'/':
#else
      case '/':
#endif
        have_extension = false;
        dirname = PathPartsT::ref_t(path, i);
        break;
#ifdef _WIN32
      case L'.':
#else
      case '.':
#endif
        have_extension = true;
        stem_end = i;
        break;
#ifdef _WIN32
      case L'\0': {
#else
      case '\0': {
#endif
        PathPartsT result;

        if (have_extension) {
          result.extension = PathPartsT::ref_t(
            path + stem_end + 1, i - stem_end - 1);  // +1/-1 for delimiter
        } else {
          stem_end = i;
        }

        if (!dirname.data()) {
          result.basename = PathPartsT::ref_t(path, i);
          result.stem = PathPartsT::ref_t(path, stem_end);
        } else {
          auto stem_start = dirname.size() + 1;  // +1 for delimiter

          result.basename =
            PathPartsT::ref_t(path + stem_start, i - stem_start);
          result.stem =
            PathPartsT::ref_t(path + stem_start, stem_end - stem_start);
        }

        result.dirname = std::move(dirname);

        return result;
      }
      default: {
      }  // NOOP
    }
  }
}

bool ReadCwd(
  std::basic_string<std::filesystem::path::value_type>& result) noexcept {
  try {
#ifdef _WIN32
    auto size = GetCurrentDirectory(0, nullptr);

    if (!size) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat(
          "Failed to get length of the current working directory, error ",
          GET_ERROR()));

      return false;
    }

    if (size >= result.size()) {
      result.resize(size + 1);  // allocate space for cwd, +1 for '\0'
    }

    size = GetCurrentDirectory(size, result.data());

    // if error or more space required than available
    if (!size || size >= result.size()) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Failed to get the current working directory, error ",
                     GET_ERROR()));

      return false;
    }

    if (result.size() >= path_prefix.size() &&
        !result.compare(0, path_prefix.size(), path_prefix)) {
      result = result.substr(path_prefix.size());
      size -= uint32_t(path_prefix.size());  // path_prefix size <= DWORD max
    }

    result.resize(size);  // truncate buffer to size of cwd
#else
    result.resize(result.capacity());  // use up the entire buffer (noexcept)

    if (result.empty()) {
      // workaround for implementations of std::basic_string without a buffer
      char buf[PATH_MAX];

      if (nullptr != getcwd(buf, PATH_MAX)) {
        result.assign(buf);

        return true;
      }
    } else if (nullptr != getcwd(result.data(), result.size())) {
      // truncate buffer to size of cwd
      result.resize(std::strlen(result.data()));

      return true;
    }

    if (ERANGE != errno) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Failed to get the current working directory, error ",
                     errno));

      return false;
    }

    struct DeleterT {
      void operator()(char* ptr) const { free(ptr); }
    };
    std::unique_ptr<char, DeleterT> pcwd(getcwd(nullptr, 0));

    if (!pcwd) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Failed to allocate the current working directory, error ",
                     errno));

      return false;
    }

    result.assign(pcwd.get());
#endif

    return true;
  } catch (const std::bad_alloc& e) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Memory allocation failure while getting the "
                           "current working directory: ",
                           e.what()));
  } catch (const std::exception& e) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat(
        "Caught exception while getting the current working directory: ",
        e.what()));
  }

  return false;
}

void EnsureAbsolute(std::filesystem::path& path) {
  if (!path.is_absolute()) {
    std::filesystem::path::string_type str;
    ReadCwd(str);

    path = std::filesystem::path{str} / path;
  }
}

bool Remove(const path_char_t* path) noexcept {
  try {
    // a reusable buffer for a full path used during recursive removal
    std::basic_string<std::filesystem::path::value_type> buf;

    // must remove each directory entry recursively (ignore result, check final
    // ::remove() instead)
    VisitDirectory(
      path,
      [path, &buf](const path_char_t* name) -> bool {
        buf.assign(path);
        buf += std::filesystem::path::preferred_separator;
        buf += name;
        Remove(buf.c_str());

        return true;
      },
      false);
  } catch (...) {
    return false;  // possibly a malloc error
  }

#ifdef _WIN32
  bool abs;

  if (!absolute(abs, path)) {
    return false;
  }

  // '\\?\' cannot be used with relative paths
  if (!abs) {
    bool result;
    auto res = exists_directory(result, path) && result
                 ? ::RemoveDirectoryW(path)
                 : ::DeleteFileW(path);

    if (!res) {  // 0 == error
      const auto system_error = GET_ERROR();
      if (ERROR_FILE_NOT_FOUND == system_error) {
        // file is just not here, so we are done actually
        SDB_DEBUG(absl::StrCat("Failed to remove path: '", ToStr(path),
                               "', error ", system_error));
      } else {
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Failed to remove path: '", ToStr(path),
                               "', error ", system_error));
      }
      return false;
    }

    return true;
  }

  // workaround for path MAX_PATH
  auto fullpath = std::filesystem::path::string_type(path);

  // 'path_prefix' cannot be used with paths containing a mix of native and
  // non-native path separators
  std::replace(fullpath.data(), fullpath.data() + fullpath.size(), L'/',
               file_path_delimiter);

  fullpath = ensure_path_prefix(fullpath);

  bool result;
  auto res = exists_directory(result, path) && result
               ? ::RemoveDirectoryW(fullpath.c_str())
               : ::DeleteFileW(fullpath.c_str());

  if (!res) {  // 0 == error
    const auto system_error = GET_ERROR();
    if (ERROR_FILE_NOT_FOUND == system_error) {
      // file is just not here, so we are done actually
      SDB_DEBUG(absl::StrCat("Failed to remove path: '", ToStr(path),
                             "', error ", system_error));
    } else {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to remove path: '", ToStr(path),
                             "', error ", system_error));
    }

    return false;
  }
#else
  auto res = ::remove(path);

  if (res) {                // non-0 == error
    if (ENOENT == errno) {  // file is just not here, so we are done actually
      SDB_DEBUG("xxxxx", sdb::Logger::IRESEARCH, "Failed to remove path: '",
                path, "', error ", errno);
    } else {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Failed to remove path: '",
                path, "', error ", errno);
    }
    return false;
  }
#endif

  return true;
}

bool SetCwd(const path_char_t* path) noexcept {
#ifdef _WIN32
  bool abs;

  if (!absolute(abs, path)) {
    return false;
  }

  // '\\?\' cannot be used with relative paths
  if (!abs) {
    return 0 != SetCurrentDirectory(path);
  }

  // workaround for path MAX_PATH
  auto fullpath = std::filesystem::path::string_type(path);

  // 'path_prefix' cannot be used with paths containing a mix of native and
  // non-native path separators
  std::replace(fullpath.data(), fullpath.data() + fullpath.size(), L'/',
               file_path_delimiter);
  fullpath = ensure_path_prefix(fullpath);

  return 0 != SetCurrentDirectory(fullpath.c_str());
#else
  return 0 == chdir(path);
#endif
}

bool VisitDirectory(const path_char_t* name,
                    const std::function<bool(const path_char_t* name)>& visitor,
                    bool include_dot_dir /*= true*/) {
#ifdef _WIN32
  std::wstring dirname(name);

  if (dirname.empty()) {
    return false;  // match Posix behaviour for empty-string directory
  }

  dirname += L"\\*";  // FindFirstFile requires name to end with '\*'

  WIN32_FIND_DATA direntry;
  HANDLE dir = ::FindFirstFile(dirname.c_str(), &direntry);

  if (dir == INVALID_HANDLE_VALUE) {
    return false;
  }

  bool terminate = false;

  do {
    if (include_dot_dir ||
        !((direntry.cFileName[0] == L'.' && direntry.cFileName[1] == L'\0') ||
          (direntry.cFileName[0] == L'.' && direntry.cFileName[1] == L'.' &&
           direntry.cFileName[2] == L'\0'))) {
      terminate = !visitor(direntry.cFileName);
    }
  } while (!terminate && ::FindNextFile(dir, &direntry));

  ::FindClose(dir);

  return !terminate;
#else
  DIR* dir = opendir(name);

  if (!dir) {
    return false;
  }

  struct dirent* direntry;
  bool terminate = false;

  while (!terminate && (direntry = readdir(dir))) {
    if (include_dot_dir ||
        !((direntry->d_name[0] == '.' && direntry->d_name[1] == '\0') ||
          (direntry->d_name[0] == '.' && direntry->d_name[1] == '.' &&
           direntry->d_name[2] == '\0'))) {
      terminate = !visitor(direntry->d_name);
    }
  }

  closedir(dir);

  return !terminate;
#endif
}

}  // namespace file_utils
}  // namespace irs
