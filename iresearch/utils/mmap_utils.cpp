////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "mmap_utils.hpp"

#include <absl/strings/str_cat.h>

#include <cstring>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/pg/errcode_for_file_access.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::mmap_utils {

void MMapHandle::close() noexcept {
  if (_addr != MAP_FAILED) {
    if (_dontneed) {
      advise(IR_MADVICE_DONTNEED);
    }
    munmap(_addr, _size);
  }

  if (_fd >= 0) {
    ::POSIX_CLOSE(static_cast<int>(_fd));
    _rm.Decrease(1);
  }
  init();
}

void MMapHandle::init() noexcept {
  _fd = -1;
  _addr = MAP_FAILED;
  _size = 0;
  _dontneed = false;
}

void MMapHandle::open(const path_char_t* path) {
  SDB_ASSERT(path);

  close();

  _rm.Increase(1);
  const int fd = ::POSIX_OPEN(path, O_RDONLY);
  if (fd < 0) {
    const int error = errno;
    _rm.Decrease(1);
    THROW_SQL_ERROR(ERR_CODE(pg::ErrcodeForFileAccess(error)),
                    ERR_MSG("Failed to open file '", file_utils::ToStr(path),
                            "': ", std::strerror(error)));
  }

  _fd = fd;

  uint64_t size;

  if (!file_utils::ByteSize(size, fd)) {
    const int error = errno;
    close();
    THROW_SQL_ERROR(
      ERR_CODE(pg::ErrcodeForFileAccess(error)),
      ERR_MSG("Failed to get the size of file '", file_utils::ToStr(path),
              "': ", std::strerror(error)));
  }

  if (size) {
    _size = size;

    // TODO(mbkkt) Needs benchmark?
    //  1. MAP_SHARED can make more sense than MAP_PRIVATE
    //     both ok for us, because file is read only
    //     but with it we probably can avoid COW kernel overhead.
    //  2. MAP_POPULATE | MAP_LOCKED instead of read to vector stuff?
    void* addr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);

    if (MAP_FAILED == addr) {
      const int error = errno;
      close();
      THROW_SQL_ERROR(ERR_CODE(pg::ErrcodeForFileAccess(error)),
                      ERR_MSG("Failed to mmap file '", file_utils::ToStr(path),
                              "': ", std::strerror(error)));
    }

    _addr = addr;
    _residency.Reset((size + file_utils::kPage - 1) / file_utils::kPage);
  }
}

}  // namespace irs::mmap_utils
