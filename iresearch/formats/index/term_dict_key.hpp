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

#include <absl/base/internal/endian.h>

#include <bit>
#include <cstdint>
#include <cstring>

#include "iresearch/utils/containers/small_vector.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr size_t kKeyPad = 2 * sizeof(uint64_t);
inline constexpr size_t kInlineKeyBytes = 256;

struct Probe {
  const byte_type* data;
  size_t size;
};

inline bytes_view KeyOf(Probe probe) noexcept {
  return {probe.data, probe.size};
}

class PaddedKey {
 public:
  PaddedKey() : _bytes(kKeyPad, byte_type{0}) {}
  explicit PaddedKey(bytes_view key) { Assign(key); }
  PaddedKey(const PaddedKey&) = delete;
  PaddedKey& operator=(const PaddedKey&) = delete;

  byte_type* Resize(size_t size) {
    _bytes.resize(size + kKeyPad, boost::container::default_init);
    std::memset(_bytes.data() + size, 0, kKeyPad);
    return _bytes.data();
  }

  Probe Assign(bytes_view key) {
    auto* data = Resize(key.size());
    if (!key.empty()) {
      std::memcpy(data, key.data(), key.size());
    }
    return View();
  }

  Probe Assign(bytes_view prefix, bytes_view key) {
    auto* data = Resize(prefix.size() + key.size());
    std::memcpy(data, prefix.data(), prefix.size());
    if (!key.empty()) {
      std::memcpy(data + prefix.size(), key.data(), key.size());
    }
    return View();
  }

  const byte_type* Data() const noexcept { return _bytes.data(); }
  Probe View() const noexcept { return {_bytes.data(), Size()}; }
  bytes_view Key() const noexcept { return {_bytes.data(), Size()}; }

 private:
  size_t Size() const noexcept { return _bytes.size() - kKeyPad; }

  containers::SmallVector<byte_type, kInlineKeyBytes> _bytes;
};

IRS_FORCE_INLINE inline size_t PaddedMismatch(const byte_type* lhs,
                                              const byte_type* rhs,
                                              size_t size) noexcept {
  for (size_t i = 0; i < size; i += sizeof(uint64_t)) {
    uint64_t diff = absl::little_endian::Load64(lhs + i) ^
                    absl::little_endian::Load64(rhs + i);
    if (const size_t left = size - i; left < sizeof(uint64_t)) {
      diff &= (uint64_t{1} << (8 * left)) - 1;
    }
    if (diff != 0) {
      return i + std::countr_zero(diff) / 8;
    }
  }
  return size;
}

inline uint64_t HeadAt(bytes_view key, size_t skip) noexcept {
  if (skip >= key.size()) {
    return 0;
  }
  const auto* p = key.data() + skip;
  const size_t size = key.size() - skip;
  if (size >= sizeof(uint64_t)) {
    return absl::big_endian::Load64(p);
  }
  if (size >= sizeof(uint32_t)) {
    return uint64_t{absl::big_endian::Load32(p)} << 32 |
           uint64_t{absl::big_endian::Load32(p + size - sizeof(uint32_t))}
             << (64 - 8 * size);
  }
  if (size >= sizeof(uint16_t)) {
    return uint64_t{absl::big_endian::Load16(p)} << 48 |
           uint64_t{absl::big_endian::Load16(p + size - sizeof(uint16_t))}
             << (64 - 8 * size);
  }
  return uint64_t{*p} << 56;
}

}  // namespace irs::term_dict
