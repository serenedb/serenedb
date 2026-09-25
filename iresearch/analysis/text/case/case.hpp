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

#include <array>
#include <bit>
#include <cstddef>
#include <cstring>
#include <string_view>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/shared.hpp"
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
inline constexpr auto kTwoByteCase = [] {
  std::array<uint16_t, 0x800> table{};
  for (uint32_t cp = 0x80; cp < 0x800; ++cp) {
    const uint32_t mapped = ToLower ? utf8_utils::CharToLowerSimple(cp)
                                    : utf8_utils::CharToUpperSimple(cp);
    if (mapped >= 0x80 && mapped < 0x800) {
      table[cp] = static_cast<uint16_t>(((0xC0 | (mapped >> 6)) << 8) | 0x80 |
                                        (mapped & 0x3F));
    }
  }
  return table;
}();

struct ScriptBlock {
  classify::Block bytes;
  uint32_t prefix;
};

IRS_FORCE_INLINE inline bool IsScriptLead(byte_type b) noexcept {
  return b == 0xC3 || (b & 0xFE) == 0xD0;
}

template<bool ToLower>
IRS_FORCE_INLINE inline ScriptBlock ConvertScriptBlock(classify::Block b,
                                                       uint32_t live) noexcept {
  using classify::Block;
  using classify::Cmp;
  const Block zero{};
  const Block prev = __builtin_shufflevector(
    b, zero, 32, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
    18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30);
  const Block next = __builtin_shufflevector(
    b, zero, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32);
  const auto bytes = [](Cmp c) IRS_FORCE_INLINE {
    return std::bit_cast<Block>(c);
  };
  const Cmp ascii = std::bit_cast<Cmp>(b) >= int8_t{0};
  const Cmp cont = (b & uint8_t{0xC0}) == uint8_t{0x80};
  const Cmp next_cont = (next & uint8_t{0xC0}) == uint8_t{0x80};
  const Cmp d0 = b == uint8_t{0xD0};
  const Cmp d1 = b == uint8_t{0xD1};
  const Cmp after_c3 = cont & (prev == uint8_t{0xC3});
  const Cmp after_d0 = cont & (prev == uint8_t{0xD0});
  const Cmp after_d1 = cont & (prev == uint8_t{0xD1});
  const Block hi = b & uint8_t{0xF0};
  const Block next_hi = next & uint8_t{0xF0};
  Cmp irregular{};
  Block delta{};
  if constexpr (ToLower) {
    delta += bytes((b - uint8_t{'A'}) <= uint8_t{25}) & uint8_t{0x20};
    delta +=
      bytes(after_c3 & (b <= uint8_t{0x9E}) & (b != uint8_t{0x97})) &
      uint8_t{0x20};
    delta += bytes(after_d0 & (hi == uint8_t{0x80})) & uint8_t{0x10};
    delta += bytes(after_d0 & (hi == uint8_t{0x90})) & uint8_t{0x20};
    delta += bytes(after_d0 & (hi == uint8_t{0xA0})) & uint8_t{0xE0};
    delta +=
      bytes(d0 & ((next_hi == uint8_t{0x80}) | (next_hi == uint8_t{0xA0}))) &
      uint8_t{0x01};
    delta += bytes(after_d1 & (b >= uint8_t{0xA0}) &
                   ((b & uint8_t{0x01}) == uint8_t{0})) &
             uint8_t{0x01};
  } else {
    delta += bytes((b - uint8_t{'a'}) <= uint8_t{25}) & uint8_t{0xE0};
    delta += bytes(after_c3 & (b >= uint8_t{0xA0}) & (b <= uint8_t{0xBE}) &
                   (b != uint8_t{0xB7})) &
             uint8_t{0xE0};
    irregular = after_c3 & (b == uint8_t{0xBF});
    delta += bytes(after_d0 & (hi == uint8_t{0xB0})) & uint8_t{0xE0};
    delta += bytes(after_d1 & (hi == uint8_t{0x80})) & uint8_t{0x20};
    delta += bytes(after_d1 & (hi == uint8_t{0x90})) & uint8_t{0xF0};
    delta += bytes(d1 & next_cont & (next_hi <= uint8_t{0x90})) & uint8_t{0xFF};
    delta += bytes(after_d1 & (b >= uint8_t{0xA0}) &
                   ((b & uint8_t{0x01}) != uint8_t{0})) &
             uint8_t{0xFF};
  }
  const uint32_t lead =
    classify::MoveMask(((b == uint8_t{0xC3}) | d0 | d1) & next_cont);
  const uint32_t valid =
    (classify::MoveMask(ascii | ((after_c3 | after_d0 | after_d1) & ~irregular)) |
     lead) &
    live;
  auto prefix = static_cast<uint32_t>(std::countr_one(valid));
  if (prefix != 0 && ((lead >> (prefix - 1)) & 1) != 0) {
    --prefix;
  }
  return {b + delta, prefix};
}

template<bool ToLower>
IRS_ALIGN_HOT size_t CaseConvertUtf8(std::string_view in, byte_type* dst) {
  static_assert(utf8_utils::kSimpleCaseMaxUtf8Growth <= 1);
  auto* out = dst;
  const auto* it = reinterpret_cast<const byte_type*>(in.data());
  const auto* end = it + in.size();
  const bool scripts = in.size() >= classify::kClassifyBlock;
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
    if (scripts && IsScriptLead(*it) &&
        static_cast<size_t>(end - it) >= classify::kClassifyBlock) {
      const auto [converted, prefix] =
        ConvertScriptBlock<ToLower>(classify::Load(it), ~uint32_t{0});
      if (prefix != 0) {
        std::memcpy(out, &converted, sizeof converted);
        it += prefix;
        out += prefix;
        continue;
      }
    }
    if (*it >= 0xC2 && *it < 0xE0 && end - it >= 2 && (it[1] & 0xC0) == 0x80) {
      const uint16_t mapped =
        kTwoByteCase<ToLower>[((it[0] & 0x1F) << 6) | (it[1] & 0x3F)];
      if (mapped != 0) {
        out[0] = static_cast<byte_type>(mapped >> 8);
        out[1] = static_cast<byte_type>(mapped);
        it += 2;
        out += 2;
        continue;
      }
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
