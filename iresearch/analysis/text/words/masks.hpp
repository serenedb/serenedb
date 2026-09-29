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

#include <cstdint>

#include "iresearch/analysis/text/classify/block_masks.hpp"

namespace irs::analysis::words {
namespace detail {

struct WordCmps {
  classify::Cmp word;
  classify::Cmp alpha;
  classify::Cmp digit;
};

IRS_FORCE_INLINE inline WordCmps WordCmpsOf(classify::Block b) noexcept {
  const classify::Cmp digit = (b >= '0') & (b <= '9');
  const auto folded = b | uint8_t{0x20};
  const classify::Cmp alpha = (folded >= 'a') & (folded <= 'z');
  return {digit | alpha | (b == '_'), alpha, digit};
}

}  // namespace detail

struct WordMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
};

IRS_FORCE_INLINE inline WordMasks ClassifyWordBlock(
  const byte_type* block) noexcept {
  const auto c = detail::WordCmpsOf(classify::Load(block));
  return {classify::MoveMask(c.word), classify::MoveMask(c.alpha),
          classify::MoveMask(c.digit)};
}

template<bool KeepNonAscii = false>
IRS_FORCE_INLINE inline uint32_t ClassifyAlnum(classify::Block b) noexcept {
  const auto c = detail::WordCmpsOf(b);
  if constexpr (KeepNonAscii) {
    return classify::MoveMask(c.alpha | c.digit | (b >= uint8_t{0x80}));
  } else {
    return classify::MoveMask(c.alpha | c.digit);
  }
}

template<bool KeepNonAscii = false>
IRS_FORCE_INLINE inline uint32_t ClassifyAlnumBlock(
  const byte_type* block) noexcept {
  return ClassifyAlnum<KeepNonAscii>(classify::Load(block));
}

IRS_FORCE_INLINE inline uint32_t ClassifyAlphaBlock(
  const byte_type* block) noexcept {
  return classify::MoveMask(detail::WordCmpsOf(classify::Load(block)).alpha);
}

struct WordBridgeMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
  uint32_t mid_al;
  uint32_t mid_nu;
};

IRS_FORCE_INLINE inline WordBridgeMasks ClassifyWordBridge(
  classify::Block b) noexcept {
  const auto c = detail::WordCmpsOf(b);
  const classify::Cmp mid = (b == '.') | (b == '\'');
  const classify::Cmp mid_al = mid | (b == ':');
  const classify::Cmp mid_nu = mid | (b == ',') | (b == ';');
  return {classify::MoveMask(c.word), classify::MoveMask(c.alpha),
          classify::MoveMask(c.digit), classify::MoveMask(mid_al),
          classify::MoveMask(mid_nu)};
}

IRS_FORCE_INLINE inline WordBridgeMasks ClassifyWordBridgeBlock(
  const byte_type* block) noexcept {
  return ClassifyWordBridge(classify::Load(block));
}

struct WordSegmentMasks {
  uint32_t word;
  uint32_t alpha;
  uint32_t digit;
  uint32_t wide;
  uint32_t space;
  uint32_t single;
  uint32_t single2;
};

IRS_FORCE_INLINE inline bool IsWideLetterLead(byte_type lead) noexcept {
  return (lead >= 0xC3 && lead <= 0xCA) || (lead & 0xFE) == 0xD0 ||
         lead == 0xD3 || lead == 0xDA;
}

IRS_FORCE_INLINE inline bool IsWideLetter(byte_type lead,
                                          byte_type next) noexcept {
  return IsWideLetterLead(lead) && (next & 0xC0) == 0x80 &&
         !(lead == 0xC3 && (next == 0x97 || next == 0xB7));
}

IRS_FORCE_INLINE inline WordSegmentMasks ClassifyWordSegments(
  classify::Block b, uint32_t live) noexcept {
  const auto c = detail::WordCmpsOf(b);
  const uint32_t ascii =
    ~classify::MoveMask(std::bit_cast<classify::Cmp>(b) < 0);
  const classify::Cmp lead =
    (static_cast<classify::Block>(b - uint8_t{0xC3}) <= uint8_t{0xCA - 0xC3}) |
    ((b & uint8_t{0xFE}) == uint8_t{0xD0}) | (b == uint8_t{0xD3}) |
    (b == uint8_t{0xDA});
  const uint32_t cont =
    classify::MoveMask((b & uint8_t{0xC0}) == uint8_t{0x80});
  const uint32_t excluded =
    classify::MoveMask(b == uint8_t{0xC3}) &
    (classify::MoveMask((b == uint8_t{0x97}) | (b == uint8_t{0xB7})) >> 1);
  const uint32_t leads = classify::MoveMask(lead) & (cont >> 1) & ~excluded;
  const uint32_t wide = leads | (leads << 1);
  const uint32_t alpha = classify::MoveMask(c.alpha) | wide;
  const uint32_t digit = classify::MoveMask(c.digit);
  const uint32_t space = classify::MoveMask(b == ' ');
  const uint32_t mid_letter = classify::MoveMask(b == ':');
  const uint32_t mid_both = classify::MoveMask((b == '.') | (b == '\''));
  const uint32_t mid_num = classify::MoveMask((b == ',') | (b == ';'));
  const uint32_t mids = mid_letter | mid_both | mid_num;
  const uint32_t bridge =
    ((mid_letter | mid_both) & (alpha << 1) & (alpha >> 1)) |
    ((mid_num | mid_both) & (digit << 1) & (digit >> 1));
  const uint32_t punct2 =
    classify::MoveMask(b == uint8_t{0xC2}) & (cont >> 1) &
    ~(classify::MoveMask((b == uint8_t{0x85}) | (b == uint8_t{0xAA}) |
                         (b == uint8_t{0xAD}) | (b == uint8_t{0xB5}) |
                         (b == uint8_t{0xB7}) | (b == uint8_t{0xBA})) >>
      1);
  const uint32_t ascii_word = classify::MoveMask(c.word);
  const uint32_t known_next = (ascii | leads | punct2) >> 1;
  const uint32_t lone_mids = mids & ~bridge & known_next & ~uint32_t{1};
  const uint32_t others =
    ascii & ~ascii_word & ~space & ~mids & ~classify::MoveMask(b == '\r');
  return {ascii_word | wide | bridge,           alpha, digit, wide, space,
          (others | lone_mids | punct2) & live, punct2};
}

}  // namespace irs::analysis::words
