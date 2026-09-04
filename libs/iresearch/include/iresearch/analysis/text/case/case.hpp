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

#include <absl/strings/ascii.h>

#include <bit>
#include <cstddef>
#include <cstring>
#include <string_view>

#include "basics/shared.hpp"
#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::casing {

template<bool ToLower, typename Bytes>
IRS_FORCE_INLINE inline Bytes CaseConvertAscii(Bytes b) noexcept {
  constexpr uint8_t kLo = ToLower ? 'A' : 'a';
  const auto hit = std::bit_cast<Bytes>((b - kLo) <= uint8_t{25});
  return b ^ (hit & uint8_t{0x20});
}

template<bool ToLower>
IRS_FORCE_INLINE inline void CaseConvertAscii(char* dst, const char* src,
                                              size_t n) noexcept {
  if constexpr (ToLower) {
    absl::ascii_internal::AsciiStrToLower(dst, src, n);
  } else {
    absl::ascii_internal::AsciiStrToUpper(dst, src, n);
  }
}

inline constexpr size_t kCaseLane = 16;

template<bool ToLower>
IRS_FORCE_INLINE inline void CaseConvertAsciiWide(char* dst, const char* src,
                                                  size_t n) noexcept {
  using Lane = uint8_t __attribute__((vector_size(kCaseLane)));
  SDB_ASSERT(n >= sizeof(Lane));
  const auto convert = [](char* out, const char* in) IRS_FORCE_INLINE {
    Lane lane;
    std::memcpy(&lane, in, sizeof lane);
    lane = CaseConvertAscii<ToLower>(lane);
    std::memcpy(out, &lane, sizeof lane);
  };
  size_t at = 0;
  for (; at + sizeof(Lane) < n; at += sizeof(Lane)) {
    convert(dst + at, src + at);
  }
  convert(dst + n - sizeof(Lane), src + n - sizeof(Lane));
}

template<bool ToLower>
class AsciiFoldRing {
 public:
  IRS_FORCE_INLINE void Fold(size_t offset, classify::Block block) noexcept {
    SDB_ASSERT(offset % kBlock == 0);
    const auto folded = CaseConvertAscii<ToLower>(block);
    byte_type* dst = _ring + (offset % kRingBytes);
    std::memcpy(dst, &folded, sizeof folded);
    if (dst == _ring) {
      std::memcpy(_ring + kRingBytes, dst, kTermViewSlack);
    }
  }

  IRS_FORCE_INLINE void FoldAt(const byte_type* data, size_t size,
                               size_t offset) noexcept {
    SDB_ASSERT(offset < size);
    Fold(offset, size - offset >= kBlock
                   ? classify::Load(data + offset)
                   : classify::LoadPadded(data + offset, size - offset));
  }

  IRS_FORCE_INLINE const char* Bytes(size_t begin) const noexcept {
    return reinterpret_cast<const char*>(_ring) + (begin % kRingBytes);
  }

 private:
  static constexpr size_t kBlock = classify::kClassifyBlock;
  static constexpr size_t kRingBytes = 8 * kBlock;

  alignas(kBlock) byte_type _ring[kRingBytes + kTermViewSlack]{};
};

constexpr size_t CaseConvertUtf8Bound(size_t size) noexcept {
  return size + size / 2 + utf8_utils::kMaxCharSize;
}

// 1:1 simple case mapping of a whole UTF-8 value into `dst` (capacity at
// least CaseConvertUtf8Bound(in.size())); invalid bytes are copied verbatim.
template<bool ToLower>
size_t CaseConvertUtf8(std::string_view in, byte_type* dst) {
  static_assert(utf8_utils::kSimpleCaseMaxUtf8Growth <= 1);
  auto* out = dst;
  const auto* it = reinterpret_cast<const byte_type*>(in.data());
  const auto* end = it + in.size();
  while (it != end) {
    const auto* cp_start = it;
    uint32_t cp = utf8_utils::ToChar32(it, end);
    if (cp == utf8_utils::kInvalidChar32) [[unlikely]] {
      *out++ = *cp_start;
      continue;
    }
    if (cp < 0x80) {
      const auto c = static_cast<unsigned char>(cp);
      if constexpr (ToLower) {
        cp = absl::ascii_tolower(c);
      } else {
        cp = absl::ascii_toupper(c);
      }
    } else if constexpr (ToLower) {
      cp = utf8_utils::CharToLowerSimple(cp);
    } else {
      cp = utf8_utils::CharToUpperSimple(cp);
    }
    out += utf8_utils::FromChar32(cp, out);
  }
  return out - dst;
}

}  // namespace irs::analysis::casing
