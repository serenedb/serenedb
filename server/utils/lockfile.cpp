////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "server/utils/lockfile.h"

#include <absl/status/status.h>
#include <absl/strings/str_cat.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/file.h>

#include <iresearch/utils/operating_system.hpp>
#include <string>

namespace sdb {

absl::Status LockFile(std::string_view path) {
  const std::string name{path};
  const int fd = SERENEDB_OPEN(name.c_str(), O_RDONLY | SERENEDB_O_CLOEXEC);
  if (fd < 0) {
    return absl::InternalError(
      absl::StrCat("cannot open '", name, "': ", strerror(errno)));
  }
  if (flock(fd, LOCK_EX | LOCK_NB) == 0) {
    return absl::OkStatus();
  }
  const int error = errno;
  SERENEDB_CLOSE(fd);
  if (error == EWOULDBLOCK) {
    return absl::FailedPreconditionError(
      absl::StrCat("the data directory is in use: '", name,
                   "' is locked by another server; stop it first"));
  }
  return absl::InternalError(
    absl::StrCat("cannot lock '", name, "': ", strerror(error),
                 "; the file system may not support file locks"));
}

}  // namespace sdb
