////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <absl/crc/crc32c.h>

#include "iresearch/utils/shared.hpp"

namespace irs {

class Crc32c {
  static constexpr uint32_t kCrc32Xor = 0xffffffff;

 public:
  explicit Crc32c(uint32_t seed = 0) noexcept : _value{seed ^ kCrc32Xor} {}

  IRS_FORCE_INLINE void process_bytes(const void* data, size_t size) noexcept {
    // We patched abseil ExtendCrc32c to make crc32c streaming
    // so we can pass 0 instead of default 0xffffffff
    _value = absl::ExtendCrc32c(
      _value, std::string_view{static_cast<const char*>(data), size}, 0);
  }

  IRS_FORCE_INLINE void copy_bytes(void* dst, const void* src,
                                   size_t size) noexcept {
    // Unfortunately we cannot patch abseil MemcpyCrc32c
    // so we need to apply xor before call this function and undo xor after
    const absl::crc32c_t init{static_cast<uint32_t>(_value) ^ kCrc32Xor};
    const auto res = absl::MemcpyCrc32c(dst, src, size, init);
    _value = absl::crc32c_t{static_cast<uint32_t>(res) ^ kCrc32Xor};
  }

  IRS_FORCE_INLINE uint32_t checksum() const noexcept {
    return static_cast<uint32_t>(_value) ^ kCrc32Xor;
  }

 private:
  absl::crc32c_t _value;
};

}  // namespace irs
