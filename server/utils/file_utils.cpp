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

#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcode_for_file_access.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/string_utils.hpp>

namespace sdb::utils::file_utils {

////////////////////////////////////////////////////////////////////////////////
/// removes trailing path separators from path
///
/// path will be modified in-place
////////////////////////////////////////////////////////////////////////////////

namespace {

std::string_view RemoveTrailingSeparator(std::string_view name) {
  return name.substr(0, name.find_last_not_of(SERENEDB_DIR_SEPARATOR_STR) + 1);
}

////////////////////////////////////////////////////////////////////////////////
/// normalizes path
///
/// path will be modified in-place
////////////////////////////////////////////////////////////////////////////////

void NormalizePath(std::string& name) {
  absl::c_replace(name, '/', SERENEDB_DIR_SEPARATOR_CHR);
}

}  // namespace

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

void ThrowFileError(std::string_view action, std::string_view path, int error) {
  THROW_SQL_ERROR(
    ERR_CODE(irs::pg::ErrcodeForFileAccess(error)),
    ERR_MSG("could not ", action, " \"", path, "\": ", std::strerror(error)));
}

void ThrowFileError(std::string_view action, std::string_view path,
                    const std::error_code& ec) {
  ThrowFileError(action, path, ec.value());
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
      ThrowFileError("read file", filename, errno);
    }

    pos += static_cast<size_t>(n);
  }

  result.resize(pos);
}

void Slurp(const char* filename, std::string& result) {
  std::error_code ec;
  uintmax_t filesize = std::filesystem::file_size(filename, ec);
  if (ec) {
    ThrowFileError("read file", filename, ec);
  }

  int fd = SERENEDB_OPEN(filename, O_RDONLY | SERENEDB_O_CLOEXEC);
  if (fd == -1) {
    ThrowFileError("read file", filename, errno);
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
    ThrowFileError("create file", filename, errno);
  }

  absl::Cleanup sg = [&]() noexcept { SERENEDB_CLOSE(fd); };

  while (0 < len) {
    auto n = SERENEDB_WRITE(fd, ptr, static_cast<size_t>(len));

    if (n < 0) {
      ThrowFileError("write file", filename, errno);
    }

    ptr += n;
    len -= n;
  }

  if (sync && fsync(fd) != 0) {
    ThrowFileError("write file", filename, errno);
  }
}

void SyncDirectory(const std::filesystem::path& directory) {
  const int fd = ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  const bool synced = fd >= 0 && ::fsync(fd) == 0;
  const int error = errno;
  if (fd >= 0) {
    ::close(fd);
  }
  if (!synced) {
    ThrowFileError("fsync directory", directory.string(), error);
  }
}

void ReplaceFile(const std::filesystem::path& path, std::string_view content) {
  auto temp = path;
  temp += ".tmp";
  Spit(temp.string(), content, true);
  std::error_code ec;
  std::filesystem::rename(temp, path, ec);
  if (ec) {
    std::error_code ignored;
    std::filesystem::remove(temp, ignored);
    ThrowFileError("rename", temp.string(), ec);
  }
  SyncDirectory(path.parent_path());
}

}  // namespace sdb::utils::file_utils
