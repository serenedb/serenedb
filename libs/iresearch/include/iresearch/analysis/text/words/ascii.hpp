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

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/string_type.hpp>

#include "iresearch/analysis/text/words/masks.hpp"

namespace irs::analysis::words {

enum WbClass : uint8_t {
  kOT = 0,
  kAL,
  kNU,
  kEX,
  kML,
  kMN,
  kMNL,
  kSQ,
  kWS,
  kNL,
};

inline constexpr auto kWbClass = [] {
  std::array<uint8_t, 256> t{};
  for (int c = 'A'; c <= 'Z'; ++c) {
    t[c] = kAL;
  }
  for (int c = 'a'; c <= 'z'; ++c) {
    t[c] = kAL;
  }
  for (int c = '0'; c <= '9'; ++c) {
    t[c] = kNU;
  }
  t['_'] = kEX;
  t[':'] = kML;
  t[','] = kMN;
  t[';'] = kMN;
  t['.'] = kMNL;
  t['\''] = kSQ;
  t[' '] = kWS;
  t['\r'] = kNL;
  t['\n'] = kNL;
  t['\v'] = kNL;
  t['\f'] = kNL;
  return t;
}();

constexpr bool IsWordClass(uint8_t cls) noexcept {
  static_assert(kAL + 1 == kNU && kNU + 1 == kEX);
  return cls >= kAL && cls <= kEX;
}

struct Segment {
  uint32_t begin;
  uint32_t end;
  bool ascii_only;
  bool has_alpha;
  bool has_digit;
};

inline void AdvanceWordRun(const byte_type* b, size_t& i, size_t n,
                           bool& has_alpha, bool& has_digit) {
  while (n - i >= classify::kClassifyBlock) {
    const auto m = ClassifyWordBlock(b + i);
    const uint32_t nonword = ~m.word;
    if (nonword != 0) {
      const auto k = static_cast<uint32_t>(std::countr_zero(nonword));
      const uint32_t run = (uint32_t{1} << k) - 1;
      has_alpha |= (m.alpha & run) != 0;
      has_digit |= (m.digit & run) != 0;
      i += k;
      return;
    }
    has_alpha |= m.alpha != 0;
    has_digit |= m.digit != 0;
    i += classify::kClassifyBlock;
  }
  while (i < n) {
    const uint8_t cls = kWbClass[b[i]];
    if (!IsWordClass(cls)) {
      return;
    }
    has_alpha |= cls == kAL;
    has_digit |= cls == kNU;
    ++i;
  }
}

IRS_FORCE_INLINE inline size_t ExtendWordRun(const byte_type* b, size_t i,
                                             size_t n, bool& has_alpha,
                                             bool& has_digit) noexcept {
  while (i + 1 < n) {
    const uint8_t c = kWbClass[b[i]];
    const uint8_t last = kWbClass[b[i - 1]];
    const uint8_t next = kWbClass[b[i + 1]];
    if (last == kAL && next == kAL && (c == kML || c == kMNL || c == kSQ)) {
      ++i;
      AdvanceWordRun(b, i, n, has_alpha, has_digit);
      continue;
    }
    if (last == kNU && next == kNU && (c == kMN || c == kMNL || c == kSQ)) {
      ++i;
      AdvanceWordRun(b, i, n, has_alpha, has_digit);
      continue;
    }
    break;
  }
  return i;
}

IRS_FORCE_INLINE inline uint32_t BridgeMask(const WordBridgeMasks& m) noexcept {
  return (m.mid_al & (m.alpha << 1) & (m.alpha >> 1)) |
         (m.mid_nu & (m.digit << 1) & (m.digit >> 1));
}

template<typename Emit>
IRS_FORCE_INLINE void ScanAsciiRuns(duckdb::string_t value, Emit&& emit) {
  const auto* b = reinterpret_cast<const byte_type*>(value.GetData());
  const size_t n = value.GetSize();
  size_t i = 0;
  const auto drain = [&](const WordBridgeMasks& m, size_t blk,
                         uint32_t slow) IRS_FORCE_INLINE {
    uint32_t w = m.word;
    while (w != 0) {
      const auto s = static_cast<uint32_t>(std::countr_zero(w));
      const auto len = static_cast<uint32_t>(std::countr_one(w >> s));
      const size_t start = blk + s;
      if (s + len == classify::kClassifyBlock) [[unlikely]] {
        bool has_alpha = (m.alpha >> s) != 0;
        bool has_digit = (m.digit >> s) != 0;
        AdvanceWordRun(b, i, n, has_alpha, has_digit);
        i = ExtendWordRun(b, i, n, has_alpha, has_digit);
        emit(Segment{static_cast<uint32_t>(start), static_cast<uint32_t>(i),
                     true, has_alpha, has_digit});
        return;
      }
      const uint32_t runmask = ((1u << len) - 1) << s;
      bool has_alpha = (m.alpha & runmask) != 0;
      bool has_digit = (m.digit & runmask) != 0;
      const size_t end = start + len;
      if (((slow >> (s + len)) & 1u) == 0) [[likely]] {
        emit(Segment{static_cast<uint32_t>(start), static_cast<uint32_t>(end),
                     true, has_alpha, has_digit});
        w &= ~runmask;
        continue;
      }
      const size_t extended = ExtendWordRun(b, end, n, has_alpha, has_digit);
      emit(Segment{static_cast<uint32_t>(start),
                   static_cast<uint32_t>(extended), true, has_alpha,
                   has_digit});
      if (extended != end) [[unlikely]] {
        i = extended;
        return;
      }
      w &= ~runmask;
    }
  };
  while (n - i >= classify::kClassifyBlock) {
    const auto m = ClassifyWordBridgeBlock(b + i);
    const size_t blk = i;
    i = blk + classify::kClassifyBlock;
    drain(m, blk, BridgeMask(m) | (1u << (classify::kClassifyBlock - 1)));
  }
  while (i < n) {
    const auto m = ClassifyWordBridge(classify::LoadPadded(b + i, n - i));
    const size_t blk = i;
    i = n;
    drain(m, blk, BridgeMask(m));
  }
}

template<typename Emit>
IRS_FORCE_INLINE void ScanAscii(duckdb::string_t value, Emit&& emit) {
  const auto* b = reinterpret_cast<const byte_type*>(value.GetData());
  const size_t n = value.GetSize();
  size_t i = 0;
  while (i < n) {
    const size_t seg_begin = i;
    const uint8_t c0 = kWbClass[b[i]];
    bool has_alpha = false;
    bool has_digit = false;
    switch (c0) {
      case kWS:
        do {
          ++i;
        } while (i < n && kWbClass[b[i]] == kWS);
        break;
      case kNL:
        i += (b[i] == '\r' && i + 1 < n && b[i + 1] == '\n') ? 2 : 1;
        break;
      case kAL:
      case kNU:
      case kEX:
        AdvanceWordRun(b, i, n, has_alpha, has_digit);
        i = ExtendWordRun(b, i, n, has_alpha, has_digit);
        break;
      default:
        ++i;
        break;
    }
    emit(Segment{static_cast<uint32_t>(seg_begin), static_cast<uint32_t>(i),
                 true, has_alpha, has_digit});
  }
}

}  // namespace irs::analysis::words
