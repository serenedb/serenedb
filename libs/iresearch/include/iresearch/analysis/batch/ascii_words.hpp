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
#include <string_view>

#include "iresearch/analysis/batch/classify.hpp"

namespace irs::analysis {

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

// ASCII slice of UAX#29 WordBreakProperty: A-Za-z ALetter, 0-9 Numeric,
// '_' ExtendNumLet, ':' MidLetter, ',' ';' MidNum, '.' MidNumLet,
// '\'' Single_Quote, ' ' WSegSpace, CR/LF/VT/FF Newline, rest Other
// (TAB and '"' included: TAB is Cc, Double_Quote only matters for Hebrew).
constexpr auto kWbClass = [] {
  std::array<uint8_t, 128> t{};
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

struct AsciiSegment {
  uint32_t begin;
  uint32_t end;
  bool has_alpha;
  bool has_digit;
};

// Advances `i` over a run of word bytes ([A-Za-z0-9_] = WB classes AL/NU/EX),
// OR-ing has_alpha over any letter and has_digit over any digit -- identical to
// the scalar per-byte loop, but blocks are classified with one movemask each
// (the find-first-non-word exit is the idiom the compiler cannot form). Stops
// at the first non-word byte or end.
inline void AdvanceWordRun(const unsigned char* b, size_t& i, size_t n,
                           bool& has_alpha, bool& has_digit) {
  constexpr uint32_t kBlockMask =
    kClassifyBlock >= 32 ? 0xFFFFFFFFu : (uint32_t{1} << kClassifyBlock) - 1;
  while (n - i >= kClassifyBlock) {
    const auto m = ClassifyWordBlock(reinterpret_cast<const byte_type*>(b + i));
    const uint32_t nonword = ~m.word & kBlockMask;
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
    i += kClassifyBlock;
  }
  while (i < n) {
    const uint8_t cls = kWbClass[b[i]];
    if (cls != kAL && cls != kNU && cls != kEX) {
      return;
    }
    has_alpha |= cls == kAL;
    has_digit |= cls == kNU;
    ++i;
  }
}

// ASCII word segmentation matching the vendored boost::text runtime exactly
// (the oracle tests pin it): WB3d (WSegSpace run), WB5/8/9/10/13a/13b
// ({ALetter,Numeric,ExtendNumLet}+ glue), the one-lookahead mid rules WB6/7
// (AL x {MidLetter,MidNumLet,SQ} x AL) and WB11/12 (NU x {MidNum,MidNumLet,
// SQ} x NU); newline bytes are single-char segments (the vendored build
// never merges CRxLF despite UAX#29 WB3); everything else is a WB999 break.
// Calls `emit` for every segment in order.
template<typename Emit>
void ScanAsciiWords(std::string_view value, Emit&& emit) {
  const auto* b = reinterpret_cast<const unsigned char*>(value.data());
  const size_t n = value.size();
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
        ++i;
        break;
      case kAL:
      case kNU:
      case kEX: {
        AdvanceWordRun(b, i, n, has_alpha, has_digit);
        while (i + 1 < n) {
          const uint8_t c = kWbClass[b[i]];
          const uint8_t last = kWbClass[b[i - 1]];
          const uint8_t next = kWbClass[b[i + 1]];
          if (last == kAL && next == kAL &&
              (c == kML || c == kMNL || c == kSQ)) {
            ++i;
            AdvanceWordRun(b, i, n, has_alpha, has_digit);
            continue;
          }
          if (last == kNU && next == kNU &&
              (c == kMN || c == kMNL || c == kSQ)) {
            ++i;
            AdvanceWordRun(b, i, n, has_alpha, has_digit);
            continue;
          }
          break;
        }
        break;
      }
      default:
        ++i;
        break;
    }
    emit(AsciiSegment{static_cast<uint32_t>(seg_begin),
                      static_cast<uint32_t>(i), has_alpha, has_digit});
  }
}

}  // namespace irs::analysis
