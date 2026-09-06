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
///
/// State machine derived from turbopuffer/alyze (MIT), src/uax29/word.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/string_type.hpp>
#include <limits>
#include <utility>

#include "iresearch/analysis/text/words/ascii.hpp"
#include "iresearch/analysis/text/words/tables.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::words {
namespace detail {

enum class WbState : uint8_t {
  Begin = 0,
  Other,
  CR,
  Newline,
  WSeg,
  ALetter,
  Hebrew,
  Numeric,
  Katakana,
  ExtendNumLet,
  RIOdd,
  HebrewSQ,
  ALetterMid,
  NumericMid,
  HebrewDQ,
};

inline constexpr size_t kWbStateCount = 15;

enum class WbAction : uint8_t {
  Break = 0,
  Glue,
  Pend,
  Defer,
  Skip,
};

struct WbTransition {
  WbAction action;
  WbState state;
};

inline constexpr bool IsDeferredState(WbState s) noexcept {
  return s >= WbState::ALetterMid;
}

inline constexpr WbState BaseState(WbProp p) noexcept {
  switch (p) {
    case kCR:
      return WbState::CR;
    case kLF:
    case kNewline:
      return WbState::Newline;
    case kWSegSpace:
      return WbState::WSeg;
    case kALetter:
      return WbState::ALetter;
    case kHebrew:
      return WbState::Hebrew;
    case kNumeric:
      return WbState::Numeric;
    case kKatakana:
      return WbState::Katakana;
    case kExtendNumLet:
      return WbState::ExtendNumLet;
    case kRI:
      return WbState::RIOdd;
    default:
      return WbState::Other;
  }
}

using WbRow = std::array<WbTransition, kWbPropCount>;
using WbTable = std::array<WbRow, kWbStateCount>;

inline constexpr WbRow BreakRow() noexcept {
  using enum WbAction;
  using enum WbState;
  WbRow row{};
  for (size_t p = 0; p < kWbPropCount; ++p) {
    row[p] = {Break, BaseState(static_cast<WbProp>(p))};
  }
  row[kExtend] = {Skip, Other};
  row[kFormat] = {Skip, Other};
  row[kZWJ] = {Skip, Other};
  return row;
}

inline constexpr WbRow DeferRow() noexcept {
  using enum WbAction;
  using enum WbState;
  WbRow row{};
  row.fill({Defer, Other});
  row[kExtend] = {Skip, Other};
  row[kFormat] = {Skip, Other};
  row[kZWJ] = {Skip, Other};
  return row;
}

inline constexpr void DropWb4(WbRow& row) noexcept {
  using enum WbAction;
  using enum WbState;
  row[kExtend] = {Break, Other};
  row[kFormat] = {Break, Other};
  row[kZWJ] = {Break, Other};
}

inline constexpr auto kWbTransitions = [] {
  using enum WbAction;
  using enum WbState;
  WbTable t{};
  {
    WbRow row = BreakRow();
    DropWb4(row);
    t[std::to_underlying(WbState::Begin)] = row;
  }
  t[std::to_underlying(WbState::Other)] = BreakRow();
  {
    WbRow row = BreakRow();
    row[kLF] = {Glue, Newline};
    DropWb4(row);
    t[std::to_underlying(WbState::CR)] = row;
  }
  {
    WbRow row = BreakRow();
    DropWb4(row);
    t[std::to_underlying(WbState::Newline)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kWSegSpace] = {Glue, WSeg};
    row[kExtend] = {Glue, Other};
    row[kFormat] = {Glue, Other};
    row[kZWJ] = {Glue, Other};
    t[std::to_underlying(WbState::WSeg)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kALetter] = {Glue, ALetter};
    row[kHebrew] = {Glue, Hebrew};
    row[kNumeric] = {Glue, Numeric};
    row[kMidLetter] = {Glue, ALetterMid};
    row[kMidNumLet] = {Glue, ALetterMid};
    row[kSingleQuote] = {Glue, ALetterMid};
    row[kExtendNumLet] = {Glue, ExtendNumLet};
    t[std::to_underlying(WbState::ALetter)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kALetter] = {Glue, ALetter};
    row[kHebrew] = {Glue, Hebrew};
    row[kNumeric] = {Glue, Numeric};
    row[kMidLetter] = {Glue, ALetterMid};
    row[kMidNumLet] = {Glue, ALetterMid};
    row[kSingleQuote] = {Glue, HebrewSQ};
    row[kDoubleQuote] = {Glue, HebrewDQ};
    row[kExtendNumLet] = {Glue, ExtendNumLet};
    t[std::to_underlying(WbState::Hebrew)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kNumeric] = {Glue, Numeric};
    row[kALetter] = {Glue, ALetter};
    row[kHebrew] = {Glue, Hebrew};
    row[kMidNum] = {Glue, NumericMid};
    row[kMidNumLet] = {Glue, NumericMid};
    row[kSingleQuote] = {Glue, NumericMid};
    row[kExtendNumLet] = {Glue, ExtendNumLet};
    t[std::to_underlying(WbState::Numeric)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kKatakana] = {Glue, Katakana};
    row[kExtendNumLet] = {Glue, ExtendNumLet};
    t[std::to_underlying(WbState::Katakana)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kALetter] = {Glue, ALetter};
    row[kHebrew] = {Glue, Hebrew};
    row[kNumeric] = {Glue, Numeric};
    row[kKatakana] = {Glue, Katakana};
    row[kExtendNumLet] = {Glue, ExtendNumLet};
    t[std::to_underlying(WbState::ExtendNumLet)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kRI] = {Glue, Other};
    t[std::to_underlying(WbState::RIOdd)] = row;
  }
  {
    WbRow row = BreakRow();
    row[kALetter] = {Glue, ALetter};
    row[kHebrew] = {Glue, Hebrew};
    t[std::to_underlying(WbState::HebrewSQ)] = row;
  }
  {
    WbRow row = DeferRow();
    row[kALetter] = {Glue, ALetter};
    row[kHebrew] = {Glue, Hebrew};
    t[std::to_underlying(WbState::ALetterMid)] = row;
  }
  {
    WbRow row = DeferRow();
    row[kNumeric] = {Glue, Numeric};
    t[std::to_underlying(WbState::NumericMid)] = row;
  }
  {
    WbRow row = DeferRow();
    row[kHebrew] = {Glue, Hebrew};
    t[std::to_underlying(WbState::HebrewDQ)] = row;
  }
  for (auto& row : t) {
    for (auto& e : row) {
      if (e.action == Glue && IsDeferredState(e.state)) {
        e.action = Pend;
      }
    }
  }
  return t;
}();

static_assert([] {
  for (size_t s = 0; s < kWbStateCount; ++s) {
    for (size_t p = 0; p < kWbPropCount; ++p) {
      const auto t = kWbTransitions[s][p];
      if (t.action == WbAction::Break &&
          t.state != BaseState(static_cast<WbProp>(p))) {
        return false;
      }
    }
  }
  return true;
}());

inline constexpr auto kWbAsciiProp = [] {
  std::array<uint8_t, 128> t{};
  for (uint32_t c = 0; c < 128; ++c) {
    t[c] = WbLookup(c);
  }
  return t;
}();

static_assert([] {
  for (uint32_t c = 0; c < 128; ++c) {
    if ((kWbAsciiProp[c] & kWbExtPictFlag) != 0) {
      return false;
    }
    const auto p = static_cast<WbProp>(kWbAsciiProp[c]);
    const uint8_t cls = kWbClass[c];
    const bool matches = [&] {
      switch (cls) {
        case kAL:
          return p == kALetter;
        case kNU:
          return p == kNumeric;
        case kEX:
          return p == kExtendNumLet;
        case kML:
          return p == kMidLetter;
        case kMN:
          return p == kMidNum;
        case kMNL:
          return p == kMidNumLet;
        case kSQ:
          return p == kSingleQuote;
        case kWS:
          return p == kWSegSpace;
        case kNL:
          return p == kCR || p == kLF || p == kNewline;
        default:
          return p == kOther || p == kDoubleQuote;
      }
    }();
    if (!matches) {
      return false;
    }
  }
  return true;
}());

inline constexpr WbState AsciiWordState(uint8_t cls) noexcept {
  if (cls == kNU) {
    return WbState::Numeric;
  }
  if (cls == kEX) {
    return WbState::ExtendNumLet;
  }
  return WbState::ALetter;
}

inline constexpr uint8_t kTierNone = 0;
inline constexpr uint8_t kTierWord = 1;
inline constexpr uint8_t kTierSpace = 2;
inline constexpr uint8_t kTierExcluded = 3;

// States from which every ASCII word byte and every space behaves
// uniformly (word x word and space x space glue, any other pairing
// breaks), making whole word/space runs consumable straight from window
// masks. Katakana ('_' glues, letters break) and HebrewSQ (letters glue,
// digits break) are asymmetric; deferred states carry a pending boundary
// -- all excluded and left to the scalar dispatch.
inline constexpr auto kTierClass = [] {
  using enum WbState;
  std::array<uint8_t, kWbStateCount> t{};
  t.fill(kTierExcluded);
  t[std::to_underlying(WbState::Begin)] = kTierNone;
  t[std::to_underlying(WbState::Other)] = kTierNone;
  t[std::to_underlying(WbState::CR)] = kTierNone;
  t[std::to_underlying(WbState::Newline)] = kTierNone;
  t[std::to_underlying(WbState::RIOdd)] = kTierNone;
  t[std::to_underlying(WbState::WSeg)] = kTierSpace;
  t[std::to_underlying(WbState::ALetter)] = kTierWord;
  t[std::to_underlying(WbState::Hebrew)] = kTierWord;
  t[std::to_underlying(WbState::Numeric)] = kTierWord;
  t[std::to_underlying(WbState::ExtendNumLet)] = kTierWord;
  return t;
}();

inline constexpr uint8_t kSegNonAscii = 1;
inline constexpr uint8_t kSegAlpha = 2;
inline constexpr uint8_t kSegDigit = 4;

// Consumes alternating word/space runs straight off one classified window
// starting at i (which must hold a word or space byte): a run of the open
// segment's class extends it, a class flip flushes [seg_start, .) with the
// flags accumulated in `cur`, and the first complex byte (mid, newline,
// quote, high) or the window edge stops consumption -- the segment stays
// open, so the caller's pending/bridge machinery is never bypassed.
// Returns the state of the last consumed run.
template<typename Flush>
IRS_FORCE_INLINE inline WbState ConsumeWordSpaceRuns(const unsigned char* b,
                                                     uint8_t tier, size_t& i,
                                                     size_t& seg_start,
                                                     uint8_t& cur,
                                                     Flush&& flush) {
  const auto m =
    ClassifyWordSpaceBlock(reinterpret_cast<const byte_type*>(b + i));
  const auto limit =
    static_cast<uint32_t>(std::countr_zero(~(m.word | m.space)));
  auto open = tier;
  uint32_t pos = 0;
  while (pos < limit) {
    const bool is_word = ((m.word >> pos) & 1u) != 0;
    const auto run = is_word ? kTierWord : kTierSpace;
    if (run != open) {
      flush(i + pos, cur);
      seg_start = i + pos;
      cur = 0;
      open = run;
    }
    const auto len = static_cast<uint32_t>(
      std::countr_one((is_word ? m.word : m.space) >> pos));
    if (is_word) {
      const uint32_t rm =
        (len >= classify::kClassifyBlock ? ~0u : ((1u << len) - 1)) << pos;
      cur |= ((m.alpha & rm) != 0 ? kSegAlpha : uint8_t{0}) |
             ((m.digit & rm) != 0 ? kSegDigit : uint8_t{0});
    }
    pos += len;
  }
  i += limit;
  return open == kTierWord ? AsciiWordState(kWbClass[b[i - 1]]) : WbState::WSeg;
}

}  // namespace detail

struct UnicodeSegment {
  uint32_t begin;
  uint32_t end;
  bool ascii_only;
  bool has_ascii_alpha;
  bool has_ascii_digit;
};

template<typename Emit>
IRS_FORCE_INLINE void ScanUnicode(duckdb::string_t value, Emit&& emit) {
  const auto* b = reinterpret_cast<const unsigned char*>(value.GetData());
  const size_t n = value.GetSize();
  using detail::kSegAlpha;
  using detail::kSegDigit;
  using detail::kSegNonAscii;
  using detail::WbAction;
  using detail::WbState;
  constexpr size_t kNoPending = std::numeric_limits<size_t>::max();

  size_t i = 0;
  size_t seg_start = 0;
  size_t pending = kNoPending;
  auto state = WbState::Begin;
  bool zwj = false;
  uint8_t cur = 0;
  uint8_t held = 0;

  const auto flush = [&](size_t end, uint8_t flags) {
    if (end > seg_start) {
      emit(UnicodeSegment{static_cast<uint32_t>(seg_start),
                          static_cast<uint32_t>(end),
                          (flags & kSegNonAscii) == 0, (flags & kSegAlpha) != 0,
                          (flags & kSegDigit) != 0});
    }
  };

  while (i < n) {
    const unsigned char byte = b[i];
    uint8_t cls;
    bool extpict = false;
    size_t cp_len = 1;
    uint8_t contrib = 0;

    if (byte < 0x80) [[likely]] {
      const auto tier = detail::kTierClass[std::to_underlying(state)];
      if ((IsWordClass(kWbClass[byte]) || byte == ' ') &&
          tier != detail::kTierExcluded && n - i >= classify::kClassifyBlock) {
        SDB_ASSERT(pending == kNoPending);
        zwj = false;
        state = detail::ConsumeWordSpaceRuns(b, tier, i, seg_start, cur, flush);
        continue;
      }
      cls = detail::kWbAsciiProp[byte];
      contrib = cls == kALetter   ? kSegAlpha
                : cls == kNumeric ? kSegDigit
                                  : uint8_t{0};
    } else {
      const auto* p = reinterpret_cast<const byte_type*>(b + i);
      const uint32_t cp =
        utf8_utils::ToChar32(p, reinterpret_cast<const byte_type*>(b + n));
      cp_len = static_cast<size_t>(reinterpret_cast<const unsigned char*>(p) -
                                   (b + i));
      const uint8_t props = WbLookup(cp);
      cls = props & kWbPropMask;
      extpict = (props & kWbExtPictFlag) != 0;
      contrib = kSegNonAscii;
    }

    const auto [action, next] =
      detail::kWbTransitions[std::to_underlying(state)][cls];
    if (action > WbAction::Skip) {
      SDB_UNREACHABLE();
    }

    switch (action) {
      case WbAction::Break:
        if (zwj && extpict) {
          cur |= contrib;
        } else {
          flush(i, cur);
          seg_start = i;
          cur = contrib;
        }
        state = next;
        break;
      case WbAction::Glue:
        if (pending != kNoPending) {
          cur |= held | contrib;
          held = 0;
          pending = kNoPending;
        } else {
          cur |= contrib;
        }
        state = next;
        break;
      case WbAction::Pend:
        SDB_ASSERT(pending == kNoPending);
        pending = i;
        held = contrib;
        state = next;
        break;
      case WbAction::Defer:
        SDB_ASSERT(pending != kNoPending);
        flush(pending, cur);
        seg_start = pending;
        cur = held;
        held = 0;
        pending = kNoPending;
        state = next;
        continue;
      case WbAction::Skip:
        if (pending != kNoPending) {
          held |= contrib;
        } else {
          cur |= contrib;
        }
        break;
    }
    zwj = cls == kZWJ;
    i += cp_len;
  }

  if (pending != kNoPending) {
    flush(pending, cur);
    seg_start = pending;
    cur = held;
  }
  flush(n, cur);
}

}  // namespace irs::analysis::words
