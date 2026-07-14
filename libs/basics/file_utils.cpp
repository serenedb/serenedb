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
#include "basics/log.h"
#include "basics/string_utils.h"
#include "pg/sql_exception_macro.h"

using namespace sdb;
namespace {

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
  auto message =
    absl::StrCat("read failed for file '", filename, "': ", strerror(errno));
  SDB_TRACE(GENERAL, message);

  THROW_SQL_ERROR(ERR_MSG(std::move(message)));
}

static void ThrowFileWriteError(std::string_view filename) {
  auto message =
    absl::StrCat("write failed for file '", filename, "': ", strerror(errno));
  SDB_TRACE(GENERAL, "", message);

  THROW_SQL_ERROR(ERR_MSG(message));
}

static void ThrowFileCreateError(std::string_view filename) {
  auto message =
    absl::StrCat("failed to create file '", filename, "': ", strerror(errno));
  SDB_TRACE(GENERAL, "", message);

  THROW_SQL_ERROR(ERR_MSG(message));
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

bool IsDirectory(const char* path) {
  return DoStatResultType(path) == ::StatResultType::Directory;
}

}  // namespace sdb::basics::file_utils
