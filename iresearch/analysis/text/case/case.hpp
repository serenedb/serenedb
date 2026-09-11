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

#include "iresearch/utils/shared.hpp"
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
inline constexpr size_t kCaseBulk = 512;

template<bool ToLower, size_t Bytes>
IRS_FORCE_INLINE inline void CaseConvertAsciiLanes(char* dst, const char* src,
                                                   size_t n) noexcept {
  using Lane = uint8_t __attribute__((vector_size(Bytes)));
  SDB_ASSERT(n >= Bytes);
  const auto convert = [&](size_t at) IRS_FORCE_INLINE {
    Lane lane;
    std::memcpy(&lane, src + at, sizeof lane);
    lane = CaseConvertAscii<ToLower>(lane);
    std::memcpy(dst + at, &lane, sizeof lane);
  };
  size_t at = 0;
  for (; at + Bytes < n; at += Bytes) {
    convert(at);
  }
  convert(n - Bytes);
}

template<bool ToLower>
IRS_FORCE_INLINE inline void CaseConvertAsciiWide(char* dst, const char* src,
                                                  size_t n) noexcept {
  CaseConvertAsciiLanes<ToLower, kCaseLane>(dst, src, n);
}

template<bool ToLower>
IRS_FORCE_INLINE inline void CaseConvertAsciiExact(char* dst, const char* src,
                                                   size_t n) noexcept {
  if (n >= kCaseLane) {
    CaseConvertAsciiLanes<ToLower, kCaseLane>(dst, src, n);
    return;
  }
  if (n >= kCaseLane / 2) {
    CaseConvertAsciiLanes<ToLower, kCaseLane / 2>(dst, src, n);
    return;
  }
  constexpr uint8_t kLo = ToLower ? 'A' : 'a';
  for (size_t i = 0; i < n; ++i) {
    const auto c = static_cast<uint8_t>(src[i]);
    dst[i] = static_cast<char>(
      c ^ (static_cast<uint8_t>(c - kLo) <= uint8_t{25} ? 0x20U : 0U));
  }
}

template<bool ToLower>
IRS_NO_INLINE inline void CaseConvertAsciiTerm(char* dst, const char* src,
                                               size_t n) noexcept {
  if (n >= kCaseBulk) {
    CaseConvertAscii<ToLower>(dst, src, n);
    return;
  }
  if (n < kCaseLane) {
    CaseConvertAsciiLanes<ToLower, kCaseLane / 2>(dst, src, n);
    return;
  }
  if (n < 2 * kCaseLane) {
    CaseConvertAsciiLanes<ToLower, kCaseLane>(dst, src, n);
    return;
  }
  CaseConvertAsciiLanes<ToLower, 2 * kCaseLane>(dst, src, n);
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

template<bool ToLower>
size_t CaseConvertUtf8(std::string_view in, byte_type* dst) {
  static_assert(utf8_utils::kSimpleCaseMaxUtf8Growth <= 1);
  auto* out = dst;
  const auto* it = reinterpret_cast<const byte_type*>(in.data());
  const auto* end = it + in.size();
  while (it != end) {
    if (*it < 0x80) {
      if (static_cast<size_t>(end - it) < classify::kClassifyBlock) {
        const auto c = static_cast<char>(*it++);
        *out++ = static_cast<byte_type>(ToLower ? absl::ascii_tolower(c)
                                                : absl::ascii_toupper(c));
        continue;
      }
      const auto block = classify::Load(it);
      const auto folded = CaseConvertAscii<ToLower>(block);
      std::memcpy(out, &folded, sizeof folded);
      const auto high =
        classify::MoveMask(std::bit_cast<classify::Cmp>(block) < 0);
      if (high == 0) {
        it += classify::kClassifyBlock;
        out += classify::kClassifyBlock;
        continue;
      }
      const auto ascii = std::countr_zero(high);
      it += ascii;
      out += ascii;
    }
    const auto* cp_start = it;
    uint32_t cp = utf8_utils::ToChar32(it, end);
    if (cp == utf8_utils::kInvalidChar32) [[unlikely]] {
      *out++ = *cp_start;
      continue;
    }
    if constexpr (ToLower) {
      cp = utf8_utils::CharToLowerSimple(cp);
    } else {
      cp = utf8_utils::CharToUpperSimple(cp);
    }
    out += utf8_utils::FromChar32(cp, out);
  }
  return out - dst;
}

bool AsciiCaseSafe(const char* locale_name) noexcept;

bool SimpleCaseSafe(const char* locale_name) noexcept;

}  // namespace irs::analysis::casing
