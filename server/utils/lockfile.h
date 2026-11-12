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

#pragma once

#include <absl/status/statusor.h>

#include <cstdint>
#include <string_view>
#include <utility>

namespace sdb {

class FileLock {
 public:
  enum class Mode : uint8_t {
    Shared,
    Exclusive,
  };

  static absl::StatusOr<FileLock> Take(std::string_view path, Mode mode);

  FileLock() = default;
  FileLock(FileLock&& other) noexcept : _fd{std::exchange(other._fd, -1)} {}
  FileLock& operator=(FileLock&& other) noexcept {
    std::swap(_fd, other._fd);
    return *this;
  }
  ~FileLock();

 private:
  explicit FileLock(int fd) noexcept : _fd{fd} {}

  int _fd = -1;
};

}  // namespace sdb
