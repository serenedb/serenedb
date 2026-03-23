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

#include "file_utils.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <utility>

#include "basics/number_utils.h"
#include "basics/process-utils.h"

#ifdef SERENEDB_HAVE_DIRENT_H
#include <dirent.h>
#endif

#ifdef SERENEDB_HAVE_DIRECT_H
#include <direct.h>
#endif
#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <sys/stat.h>

#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/string_utils.h"

using namespace sdb;
namespace {

const std::function<bool(std::string_view)> kPassAllFilter =
  [](std::string_view) { return false; };

enum class StatResultType {
  Error,  // in case it cannot be determined
  Directory,
  SymLink,
  File,
  Other  // potentially file
};

StatResultType DoStatResultType(const struct stat& stbuf) {
  if (S_ISDIR(stbuf.st_mode)) {
    return StatResultType::Directory;
  }

  if (S_ISLNK(stbuf.st_mode)) {
    return StatResultType::SymLink;
  }

  if ((stbuf.st_mode & S_IFMT) == S_IFREG) {
    return StatResultType::File;
  }

  return StatResultType::Other;
}

StatResultType DoStatResultType(const char* path) {
  struct stat stbuf;
  int res = SERENEDB_STAT(path, &stbuf);
  if (res != 0) {
    return StatResultType::Error;
  }
  return DoStatResultType(stbuf);
}

void ProcessFiles(const char* directory,
                  const std::function<void(std::string_view)>& cb) {
  DIR* d = opendir(directory);

  if (d == nullptr) {
    auto res = ERROR_SYS_ERROR;
    SetError(res);

    auto message = absl::StrCat("failed to enumerate files in directory '",
                                directory, "': ", LastError());
    SDB_THROW(res, std::move(message));
  }

  absl::Cleanup guard = [&]() noexcept { closedir(d); };

  std::string rcs;
  dirent* de = readdir(d);

  while (de != nullptr) {
    // stringify filename (convert to std::string). we take this performance
    // hit because we will need to strlen anyway and need to pass it to a
    // function that will likely use its stringified value anyway
    rcs.assign(de->d_name);

    if (rcs != "." && rcs != "..") {
      // run callback function
      cb(rcs);
    }

    // advance to next entry
    de = readdir(d);
  }
}

}  // namespace
namespace sdb::basics::file_utils {

////////////////////////////////////////////////////////////////////////////////
/// removes trailing path separators from path
///
/// path will be modified in-place
////////////////////////////////////////////////////////////////////////////////

std::string_view RemoveTrailingSeparator(std::string_view name) {
  return string_utils::RTrim(name, SERENEDB_DIR_SEPARATOR_STR);
}

////////////////////////////////////////////////////////////////////////////////
/// normalizes path
///
/// path will be modified in-place
////////////////////////////////////////////////////////////////////////////////

void NormalizePath(std::string& name) {
  absl::c_replace(name, '/', SERENEDB_DIR_SEPARATOR_CHR);
}

////////////////////////////////////////////////////////////////////////////////
/// creates a filename
////////////////////////////////////////////////////////////////////////////////

std::string BuildFilename(const char* path, const char* name) {
  SDB_ASSERT(path != nullptr);
  SDB_ASSERT(name != nullptr);

  std::string result(path);

  if (!result.empty()) {
    result = RemoveTrailingSeparator(result);
    if (result.length() != 1 || result[0] != SERENEDB_DIR_SEPARATOR_CHR) {
      result += SERENEDB_DIR_SEPARATOR_CHR;
    }
  }

  if (!result.empty() && *name == SERENEDB_DIR_SEPARATOR_CHR) {
    // skip initial forward slash in name to avoid having two forward slashes in
    // result
    result.append(name + 1);
  } else {
    result.append(name);
  }
  NormalizePath(result);  // in place

  return result;
}

std::string BuildFilename(std::string_view path, std::string_view name) {
  std::string result(path);

  if (!result.empty()) {
    result = RemoveTrailingSeparator(result);
    if (result.length() != 1 || result[0] != SERENEDB_DIR_SEPARATOR_CHR) {
      result += SERENEDB_DIR_SEPARATOR_CHR;
    }
  }

  if (!result.empty() && !name.empty() &&
      name[0] == SERENEDB_DIR_SEPARATOR_CHR) {
    // skip initial forward slash in name to avoid having two forward slashes in
    // result
    result.append(name.data() + 1, name.size() - 1);
  } else {
    result.append(name);
  }
  NormalizePath(result);  // in place

  return result;
}

static void ThrowFileReadError(std::string_view filename) {
  SetError(ERROR_SYS_ERROR);

  auto message =
    absl::StrCat("read failed for file '", filename, "': ", LastError());
  SDB_TRACE("xxxxx", sdb::Logger::FIXME, message);

  SDB_THROW(ERROR_SYS_ERROR, std::move(message));
}

static void ThrowFileWriteError(std::string_view filename) {
  SetError(ERROR_SYS_ERROR);

  auto message =
    absl::StrCat("write failed for file '", filename, "': ", LastError());
  SDB_TRACE("xxxxx", sdb::Logger::FIXME, "", message);

  SDB_THROW(ERROR_SYS_ERROR, message);
}

static void ThrowFileCreateError(std::string_view filename) {
  SetError(ERROR_SYS_ERROR);

  auto message =
    absl::StrCat("failed to create file '", filename, "': ", LastError());
  SDB_TRACE("xxxxx", sdb::Logger::FIXME, "", message);

  SDB_THROW(ERROR_SYS_ERROR, message);
}

static void FillString(int fd, std::string_view filename, size_t filesize,
                       std::string& result) {
  constexpr size_t kChunkSize = 8192;

  result.clear();
  result.reserve(filesize + 2 * kChunkSize);

  size_t pos = 0;

  while (true) {
    result.resize(result.size() + kChunkSize);

    ssize_t n =
      SERENEDB_READ(fd, result.data() + pos, static_cast<size_t>(kChunkSize));

    if (n == 0) {
      break;
    }

    if (n < 0) {
      ThrowFileReadError(filename);
    }

    pos += static_cast<size_t>(n);
  }

  result.resize(pos);
}

void Slurp(const char* filename, std::string& result) {
  int64_t filesize = SdbSizeFile(filename);
  if (filesize < 0) {
    ThrowFileReadError(filename);
  }

  int fd = SERENEDB_OPEN(filename, O_RDONLY | SERENEDB_O_CLOEXEC);

  if (fd == -1) {
    ThrowFileReadError(filename);
  }

  absl::Cleanup sg = [&]() noexcept { SERENEDB_CLOSE(fd); };

  FillString(fd, filename, static_cast<size_t>(filesize), result);
}

std::string Slurp(const char* filename) {
  std::string result;
  Slurp(filename, result);
  return result;
}

void Spit(const char* filename, std::string_view s, bool sync) {
  auto* ptr = s.data();
  auto len = s.size();

  int fd =
    SERENEDB_CREATE(filename, O_WRONLY | O_CREAT | O_TRUNC | SERENEDB_O_CLOEXEC,
                    S_IRUSR | S_IWUSR | S_IRGRP);

  if (fd == -1) {
    ThrowFileCreateError(filename);
  }

  absl::Cleanup sg = [&]() noexcept { SERENEDB_CLOSE(fd); };

  while (0 < len) {
    auto n = SERENEDB_WRITE(fd, ptr, static_cast<size_t>(len));

    if (n < 0) {
      ThrowFileWriteError(filename);
    }

    ptr += n;
    len -= n;
  }

  if (sync) {
    // intentionally ignore this error as there is nothing we can do about it
    Sdbfsync(fd);
  }
}

void AppendToFile(const char* filename, std::string_view s, bool sync) {
  auto* ptr = s.data();
  auto len = s.size();
  int fd = SERENEDB_OPEN(filename, O_WRONLY | O_APPEND | SERENEDB_O_CLOEXEC);

  if (fd == -1) {
    ThrowFileWriteError(filename);
  }

  absl::Cleanup sg = [&]() noexcept { SERENEDB_CLOSE(fd); };

  while (0 < len) {
    auto n = SERENEDB_WRITE(fd, ptr, static_cast<size_t>(len));

    if (n < 0) {
      ThrowFileWriteError(filename);
    }

    ptr += n;
    len -= n;
  }

  if (sync) {
    // intentionally ignore this error as there is nothing we can do about it
    Sdbfsync(fd);
  }
}

ErrorCode Remove(const char* file_name) {
  const auto success = 0 == std::remove(file_name);

  if (!success) {
    SetError(ERROR_SYS_ERROR);
    return ERROR_SYS_ERROR;
  }

  return ERROR_OK;
}

bool CreateDirectory(const char* name, ErrorCode* error_number) {
  if (error_number != nullptr) {
    *error_number = ERROR_OK;
  }

  return CreateDirectory(name, 0777, error_number);
}

bool CreateDirectory(const char* name, int mask, ErrorCode* error_number) {
  if (error_number != nullptr) {
    *error_number = ERROR_OK;
  }

  auto result = SERENEDB_MKDIR(name, static_cast<mode_t>(mask));

  if (result != 0) {
    int res = errno;
    if (res == EEXIST && IsDirectory(name)) {
      result = 0;
    } else {
      auto error_code = ERROR_SYS_ERROR;
      SetError(error_code);
      if (error_number != nullptr) {
        *error_number = error_code;
      }
    }
  }

  return result == 0;
}

/// will not copy files/directories for which the filter function
/// returns true (now wrapper for version below with Sdbcopy_recursive_e
/// filter)
bool CopyRecursive(const char* source, const char* target,
                   const std::function<bool(std::string_view)>& filter,
                   std::string& error) {
  // "auto lambda" will not work here
  std::function<CopyRecursiveState(std::string_view)> lambda =
    [&filter](std::string_view pathname) -> CopyRecursiveState {
    return filter(pathname) ? CopyRecursiveState::Ignore
                            : CopyRecursiveState::Copy;
  };

  return CopyRecursive(source, target, lambda, error);

}  // copyRecursive (bool filter())

/// will not copy files/directories for which the filter function
/// returns true
bool CopyRecursive(
  const char* source, const char* target,
  const std::function<CopyRecursiveState(std::string_view)>& filter,
  std::string& error) {
  if (IsDirectory(source)) {
    return CopyDirectoryRecursive(source, target, filter, error);
  }

  switch (filter(source)) {
    case CopyRecursiveState::Ignore:
      return true;  // original ERROR_OK implies "false", seems wrong

    case CopyRecursiveState::Copy:
      return SdbCopyFile(source, target, error);

    case CopyRecursiveState::Link:
      return SdbCreateHardlink(source, target, error);

    default:
      return false;  // ERROR_BAD_PARAMETER seems wrong since returns "true"
  }
}

/// will not copy files/directories for which the filter function
/// returns true
bool CopyDirectoryRecursive(
  const char* source, const char* target,
  const std::function<CopyRecursiveState(std::string_view)>& filter,
  std::string& error) {
  bool rc_bool = true;

  // these strings will be recycled over and over
  std::string dst = absl::StrCat(target, SERENEDB_DIR_SEPARATOR_STR);
  const size_t dst_prefix_length = dst.size();
  std::string src = absl::StrCat(source, SERENEDB_DIR_SEPARATOR_STR);
  const size_t src_prefix_length = src.size();

  DIR* filedir = opendir(source);

  if (filedir == nullptr) {
    error = absl::StrCat("directory ", source, " not found");
    return false;
  }

  struct dirent* one_item = nullptr;

  // do not use readdir_r() here anymore as it is not safe and deprecated
  // in newer versions of libc:
  // http://man7.org/linux/man-pages/man3/readdir_r.3.html
  // the man page recommends to use plain readdir() because it can be expected
  // to be thread-safe in reality, and newer versions of POSIX may require its
  // thread-safety formally, and in addition obsolete readdir_r() altogether
  while ((one_item = (readdir(filedir))) != nullptr && rc_bool) {
    const char* fn = one_item->d_name;

    // Now iterate over the items.
    // check its not the pointer to the upper directory:
    if (!strcmp(fn, ".") || !strcmp(fn, "..")) {
      continue;
    }

    // add current filename to prefix
    src.resize(src_prefix_length);
    SDB_ASSERT(src.back() == SERENEDB_DIR_SEPARATOR_CHR);
    src.append(fn);

    auto filter_result = filter(src);

    if (filter_result != CopyRecursiveState::Ignore) {
      // prepare dst filename
      dst.resize(dst_prefix_length);
      SDB_ASSERT(dst.back() == SERENEDB_DIR_SEPARATOR_CHR);
      dst.append(fn);

      // figure out the type of the directory entry.
      StatResultType type = StatResultType::Error;
      struct stat stbuf;
      int res = SERENEDB_STAT(src.c_str(), &stbuf);
      if (res == 0) {
        type = DoStatResultType(stbuf);
      }

      switch (filter_result) {
        case CopyRecursiveState::Ignore:
          SDB_ASSERT(false);
          break;

        case CopyRecursiveState::Copy:
          // Handle subdirectories:
          if (type == StatResultType::Directory) {
            long system_error;
            auto rc = SdbCreateDirectory(dst.c_str(), system_error, error);
            if (rc != ERROR_OK && rc != ERROR_FILE_EXISTS) {
              rc_bool = false;
              break;
            }
            if (!CopyDirectoryRecursive(src.c_str(), dst.c_str(), filter,
                                        error)) {
              rc_bool = false;
              break;
            }
            if (!SdbCopyAttributes(src.c_str(), dst.c_str(), error)) {
              rc_bool = false;
              break;
            }
          } else if (type == StatResultType::SymLink) {
            if (!SdbCopySymlink(src.c_str(), dst.c_str(), error)) {
              rc_bool = false;
            }
          } else {
            // optimized version that reuses the already retrieved stat data
            rc_bool = SdbCopyFile(src.c_str(), dst.c_str(), error, &stbuf);
          }
          break;

        case CopyRecursiveState::Link:
          if (!SdbCreateHardlink(src.c_str(), dst.c_str(), error)) {
            rc_bool = false;
          }
          break;
      }
    }
  }
  closedir(filedir);

  return rc_bool;
}

std::vector<std::string> ListFiles(const char* directory) {
  std::vector<std::string> result;

  ::ProcessFiles(directory, [&](std::string_view filename) {
    result.emplace_back(filename);
  });

  return result;
}

size_t CountFiles(const char* directory) {
  size_t result = 0;

  ::ProcessFiles(directory, [&](std::string_view) { ++result; });

  return result;
}

bool IsDirectory(const char* path) {
  return DoStatResultType(path) == ::StatResultType::Directory;
}

bool IsSymbolicLink(const char* path) {
  return DoStatResultType(path) == ::StatResultType::SymLink;
}

bool IsRegularFile(const char* path) {
  return DoStatResultType(path) == ::StatResultType::File;
}

bool Exists(const char* path) {
  return DoStatResultType(path) != ::StatResultType::Error;
}

off_t Size(const char* path) {
  int64_t result = SdbSizeFile(path);

  if (result < 0) {
    return (off_t)0;
  }

  return (off_t)result;
}

std::string_view StripExtension(std::string_view path,
                                std::string_view extension) {
  size_t pos = path.rfind(extension);
  if (pos == std::string::npos) {
    return path;
  }

  auto last = path.substr(pos);
  if (last == extension) {
    return path.substr(0, pos);
  }

  return path;
}

FileResult ChangeDirectory(const char* path) {
  int res = SERENEDB_CHDIR(path);

  if (res == 0) {
    return FileResult();
  } else {
    return FileResult(errno);
  }
}

FileResultString CurrentDirectory() {
  size_t len = 1000;
  std::unique_ptr<char[]> current(new char[len]);

  while (SERENEDB_GETCWD(current.get(), (int)len) == nullptr) {
    if (errno == ERANGE) {
      len += 1000;
      current.reset(new char[len]);
    } else {
      return FileResultString(errno, ".");
    }
  }

  std::string result = current.get();

  return FileResultString(result);
}

std::string HomeDirectory() { return std::string{SdbHomeDirectory()}; }

std::string ConfigDirectory(const char* binary_path) {
  std::string dir = SdbLocateConfigDirectory(binary_path);

  if (dir.empty()) {
    return CurrentDirectory().result();
  }

  return dir;
}

std::string_view Dirname(std::string_view name) { return SdbDirname(name); }

void MakePathAbsolute(std::string& path) {
  std::string cwd = file_utils::CurrentDirectory().result();

  if (path.empty()) {
    path = cwd;
  } else {
    std::string p = SdbGetAbsolutePath(path, cwd);
    if (!p.empty()) {
      path = p;
    }
  }
}

namespace {

std::string SlurpProgramInternal(std::string_view program,
                                 std::span<const std::string_view> more_args) {
  const ExternalProcess* process;
  ExternalId external;
  ExternalProcessStatus res;
  std::string output;
  std::vector<std::string_view> additional_env;
  char buf[1024];

  CreateExternalProcess(program, more_args, additional_env, true, &external);
  if (external.pid == SERENEDB_INVALID_PROCESS_ID) {
    auto res = ERROR_SYS_ERROR;
    SetError(res);

    SDB_TRACE("xxxxx", sdb::Logger::FIXME, "open failed for file '", program,
              "': ", LastError());
    SDB_THROW(res);
  }
  process = LookupSpawnedProcess(external.pid);
  if (process == nullptr) {
    auto res = ERROR_SYS_ERROR;
    SetError(res);

    SDB_TRACE("xxxxx", sdb::Logger::FIXME, "process gone? '", program,
              "': ", LastError());
    SDB_THROW(res);
  }
  bool error = false;
  while (true) {
    auto n_read = ReadPipe(process, buf, sizeof(buf));
    if (n_read <= 0) {
      if (n_read < 0) {
        error = true;
      }
      break;
    }
    output.append(buf, n_read);
  }
  res = CheckExternalProcess(external, true, 0, NoDeadLine);
  if (error) {
    SDB_THROW(ERROR_SYS_ERROR);
  }
  // Note that we intentionally ignore the exit code of the sub process here
  // since we have always done so and do not want to break things.
  return output;
}

}  // namespace

std::string SlurpProgram(std::string_view program) {
  static constexpr std::array<std::string_view, 1> kMoreArgs = {"version"};
  return SlurpProgramInternal(program, kMoreArgs);
}

#ifdef SERENEDB_HAVE_GETPWUID
std::optional<uid_t> FindUser(std::string_view name_or_id) noexcept {
  // We avoid getpwuid and getpwnam because they pose problems when
  // we build static binaries with glibc (because of /etc/nsswitch.conf).
  // However, we know that `id` exists for basically all Linux variants
  // and for Mac.
  try {
    const std::array<std::string_view, 2> args{"-u", name_or_id};
    auto output = SlurpProgramInternal("/usr/bin/id", args);
    string_utils::TrimInPlace(output);
    bool valid = false;
    uid_t uid_number = number_utils::AtoiPositive<int>(
      output.data(), output.data() + output.size(), valid);
    if (valid) {
      return {uid_number};
    }
  } catch (const std::exception&) {
  }
  return {std::nullopt};
}

std::optional<std::string> FindUserName(uid_t id) noexcept {
  // For Linux (and other Unixes), we avoid this function because it
  // poses problems when we build static binaries with glibc (because of
  // /etc/nsswitch.conf).
  try {
    absl::AlphaNum id_str(id);
    const std::array<std::string_view, 2> args{"passwd", id_str.Piece()};
    std::string output = SlurpProgramInternal("/usr/bin/getent", args);
    string_utils::TrimInPlace(output);
    std::vector<std::string_view> parts = absl::StrSplit(output, ':');
    if (parts.size() >= 1) {
      return {std::string{parts[0]}};
    }
  } catch (const std::exception&) {
  }
  return {std::nullopt};
}
#endif

#ifdef SERENEDB_HAVE_GETGRGID
std::optional<gid_t> FindGroup(std::string_view name_or_id) noexcept {
  // For Linux (and other Unixes), we avoid these functions because they
  // pose problems when we build static binaries with glibc (because of
  // /etc/nsswitch.conf).
  try {
    const std::array<std::string_view, 2> args{"group", name_or_id};
    std::string output = SlurpProgramInternal("/usr/bin/getent", args);
    string_utils::TrimInPlace(output);
    std::vector<std::string_view> parts = absl::StrSplit(output, ':');
    if (parts.size() >= 3) {
      bool valid = false;
      uid_t gid_number = number_utils::AtoiPositive<int>(
        parts[2].data(), parts[2].data() + parts[2].size(), valid);
      if (valid) {
        return {gid_number};
      }
    }
  } catch (const std::exception&) {
  }
  return {std::nullopt};
}
#endif

#ifdef SERENEDB_HAVE_INITGROUPS
void InitGroups(std::string_view user_name, gid_t group_id) noexcept {
#ifdef __linux__
  // For Linux, calling initgroups poses problems with statically linked
  // binaries, since /etc/nsswitch.conf can then lead to crashes on
  // older Linux distributions. Therefore, we need to do the groups lookup
  // ourselves using the groups command. Then we can use setgroups to
  // achieve the desired result.
  try {
    const std::array<std::string_view, 1> args{user_name};
    std::string output = SlurpProgramInternal("/usr/bin/groups", args);
    string_utils::TrimInPlace(output);
    auto pos = output.find(':');
    if (pos != std::string::npos) {
      output = output.substr(pos + 1);
    }
    std::vector<std::string_view> parts = absl::StrSplit(output, ' ');
    std::vector<gid_t> group_ids{group_id};
    for (const auto& part : parts) {
      std::optional<gid_t> gid_number = FindGroup(part);
      if (gid_number && gid_number.value() != group_id) {
        group_ids.push_back(gid_number.value());
      }
    }
    setgroups(group_ids.size(), group_ids.data());
  } catch (const std::exception&) {
  }
#else
  // For other unixes (including Mac), we can use the OS call.
  initgroups(userName.c_str(), groupId);
#endif
}
#endif

}  // namespace sdb::basics::file_utils
