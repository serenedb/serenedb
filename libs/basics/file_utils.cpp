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
#include <errno.h>
#include <fcntl.h>
#include <string.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <memory>
#include <system_error>
#include <utility>

#ifdef SERENEDB_HAVE_DIRENT_H
#include <dirent.h>
#endif

#ifdef SERENEDB_HAVE_DIRECT_H
#include <direct.h>
#endif
#include <sys/stat.h>
#include <unistd.h>

#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"
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
  SDB_TRACE(GENERAL, message);

  SDB_THROW(ERROR_SYS_ERROR, std::move(message));
}

static void ThrowFileWriteError(std::string_view filename) {
  SetError(ERROR_SYS_ERROR);

  auto message =
    absl::StrCat("write failed for file '", filename, "': ", LastError());
  SDB_TRACE(GENERAL, "", message);

  SDB_THROW(ERROR_SYS_ERROR, message);
}

static void ThrowFileCreateError(std::string_view filename) {
  SetError(ERROR_SYS_ERROR);

  auto message =
    absl::StrCat("failed to create file '", filename, "': ", LastError());
  SDB_TRACE(GENERAL, "", message);

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
  std::error_code ec;
  uintmax_t filesize = std::filesystem::file_size(filename, ec);
  if (ec) {
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
    // intentionally ignore this error -- nothing we can do about it.
    std::ignore = fsync(fd);
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

bool Exists(const char* path) {
  return DoStatResultType(path) != ::StatResultType::Error;
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

std::string HomeDirectory() {
  const char* home = std::getenv("HOME");
  return home == nullptr ? std::string{"."} : std::string{home};
}

std::string ConfigDirectory(const char* /*binary_path*/) {
  // SERENEDB_CONFIG_PATH overrides everything; otherwise fall back to the
  // compile-time _SYSCONFDIR_ (if set non-empty), else CWD.
  if (const char* env = std::getenv("SERENEDB_CONFIG_PATH");
      env != nullptr && *env != '\0') {
    std::string r{env};
    while (!r.empty() && r.back() == SERENEDB_DIR_SEPARATOR_CHR) {
      r.pop_back();
    }
    r.push_back(SERENEDB_DIR_SEPARATOR_CHR);
    return r;
  }
#if defined(_SYSCONFDIR_)
  if (const char* dir = _SYSCONFDIR_; *dir != '\0') {
    std::string r{dir};
    if (r.back() != SERENEDB_DIR_SEPARATOR_CHR) {
      r.push_back(SERENEDB_DIR_SEPARATOR_CHR);
    }
    return r;
  }
#endif
  std::error_code ec;
  auto cwd = std::filesystem::current_path(ec);
  return ec ? std::string{"."} : cwd.string();
}

std::string_view Dirname(std::string_view path) {
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

}  // namespace sdb::basics::file_utils
