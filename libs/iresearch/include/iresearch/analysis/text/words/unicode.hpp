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
#include <initializer_list>
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

inline constexpr size_t kWbStateCount =
  std::to_underlying(WbState::HebrewDQ) + 1;

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
using WbGlue = std::pair<WbProp, WbState>;

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

inline constexpr WbRow NoWb4Row() noexcept {
  using enum WbAction;
  using enum WbState;
  WbRow row = BreakRow();
  row[kExtend] = {Break, Other};
  row[kFormat] = {Break, Other};
  row[kZWJ] = {Break, Other};
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

inline constexpr WbRow WithGlues(WbRow row,
                                 std::initializer_list<WbGlue> glues) noexcept {
  for (const auto& [prop, state] : glues) {
    row[prop] = {WbAction::Glue, state};
  }
  return row;
}

inline constexpr auto kWbTransitions = [] {
  using enum WbAction;
  using enum WbState;
  WbTable t{};
  const auto at = [&](WbState s) -> WbRow& { return t[std::to_underlying(s)]; };
  at(Begin) = NoWb4Row();
  at(Other) = BreakRow();
  at(CR) = WithGlues(NoWb4Row(), {{kLF, Newline}});
  at(Newline) = NoWb4Row();
  at(WSeg) = WithGlues(
    BreakRow(),
    {{kWSegSpace, WSeg}, {kExtend, Other}, {kFormat, Other}, {kZWJ, Other}});
  at(ALetter) = WithGlues(BreakRow(), {{kALetter, ALetter},
                                       {kHebrew, Hebrew},
                                       {kNumeric, Numeric},
                                       {kMidLetter, ALetterMid},
                                       {kMidNumLet, ALetterMid},
                                       {kSingleQuote, ALetterMid},
                                       {kExtendNumLet, ExtendNumLet}});
  at(Hebrew) = WithGlues(BreakRow(), {{kALetter, ALetter},
                                      {kHebrew, Hebrew},
                                      {kNumeric, Numeric},
                                      {kMidLetter, ALetterMid},
                                      {kMidNumLet, ALetterMid},
                                      {kSingleQuote, HebrewSQ},
                                      {kDoubleQuote, HebrewDQ},
                                      {kExtendNumLet, ExtendNumLet}});
  at(Numeric) = WithGlues(BreakRow(), {{kNumeric, Numeric},
                                       {kALetter, ALetter},
                                       {kHebrew, Hebrew},
                                       {kMidNum, NumericMid},
                                       {kMidNumLet, NumericMid},
                                       {kSingleQuote, NumericMid},
                                       {kExtendNumLet, ExtendNumLet}});
  at(Katakana) = WithGlues(
    BreakRow(), {{kKatakana, Katakana}, {kExtendNumLet, ExtendNumLet}});
  at(ExtendNumLet) = WithGlues(BreakRow(), {{kALetter, ALetter},
                                            {kHebrew, Hebrew},
                                            {kNumeric, Numeric},
                                            {kKatakana, Katakana},
                                            {kExtendNumLet, ExtendNumLet}});
  at(RIOdd) = WithGlues(BreakRow(), {{kRI, Other}});
  at(HebrewSQ) =
    WithGlues(BreakRow(), {{kALetter, ALetter}, {kHebrew, Hebrew}});
  at(ALetterMid) =
    WithGlues(DeferRow(), {{kALetter, ALetter}, {kHebrew, Hebrew}});
  at(NumericMid) = WithGlues(DeferRow(), {{kNumeric, Numeric}});
  at(HebrewDQ) = WithGlues(DeferRow(), {{kHebrew, Hebrew}});
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

enum class Tier : uint8_t {
  None = 0,
  Word,
  Space,
  Excluded,
};

inline constexpr auto kTierClass = [] {
  std::array<Tier, kWbStateCount> t{};
  t.fill(Tier::Excluded);
  t[std::to_underlying(WbState::Begin)] = Tier::None;
  t[std::to_underlying(WbState::Other)] = Tier::None;
  t[std::to_underlying(WbState::CR)] = Tier::None;
  t[std::to_underlying(WbState::Newline)] = Tier::None;
  t[std::to_underlying(WbState::RIOdd)] = Tier::None;
  t[std::to_underlying(WbState::WSeg)] = Tier::Space;
  t[std::to_underlying(WbState::ALetter)] = Tier::Word;
  t[std::to_underlying(WbState::Hebrew)] = Tier::Word;
  t[std::to_underlying(WbState::Numeric)] = Tier::Word;
  t[std::to_underlying(WbState::ExtendNumLet)] = Tier::Word;
  return t;
}();

inline constexpr uint8_t kSegNonAscii = 1;
inline constexpr uint8_t kSegAlpha = 2;
inline constexpr uint8_t kSegDigit = 4;

template<typename Flush>
IRS_FORCE_INLINE inline WbState ConsumeWordSpaceRuns(const byte_type* b,
                                                     Tier tier, size_t& i,
                                                     size_t& seg_start,
                                                     uint8_t& cur,
                                                     Flush&& flush) {
  const auto m = ClassifyWordSpaceBlock(b + i);
  const auto limit =
    static_cast<uint32_t>(std::countr_zero(~(m.word | m.space)));
  auto open = tier;
  uint32_t pos = 0;
  while (pos < limit) {
    const bool is_word = ((m.word >> pos) & 1u) != 0;
    const auto run = is_word ? Tier::Word : Tier::Space;
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
  return open == Tier::Word ? AsciiWordState(kWbClass[b[i - 1]])
                            : WbState::WSeg;
}

}  // namespace detail

template<typename Emit>
IRS_FORCE_INLINE void ScanUnicode(duckdb::string_t value, Emit&& emit) {
  const auto* b = reinterpret_cast<const byte_type*>(value.GetData());
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

  const auto flush = [&](size_t end, uint8_t flags) IRS_FORCE_INLINE {
    if (end > seg_start) {
      emit(Segment{static_cast<uint32_t>(seg_start), static_cast<uint32_t>(end),
                   (flags & kSegNonAscii) == 0, (flags & kSegAlpha) != 0,
                   (flags & kSegDigit) != 0});
    }
  };

  while (i < n) {
    const byte_type byte = b[i];
    uint8_t cls;
    bool extpict = false;
    size_t cp_len = 1;
    uint8_t contrib = 0;

    if (byte < 0x80) [[likely]] {
      const auto tier = detail::kTierClass[std::to_underlying(state)];
      if ((IsWordClass(kWbClass[byte]) || byte == ' ') &&
          tier != detail::Tier::Excluded && n - i >= classify::kClassifyBlock) {
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
      const auto* p = b + i;
      const uint32_t cp = utf8_utils::ToChar32(p, b + n);
      cp_len = static_cast<size_t>(p - (b + i));
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
